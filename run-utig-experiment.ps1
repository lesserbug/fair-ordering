# ==========================================================
# --- 0. 导入 AWS 模块 ---
# ==========================================================
Import-Module AWS.Tools.SimpleSystemsManagement

# ==========================================================
# --- 1. 配置：检查你的“路标”和项目名 ---
# ==========================================================
$ec2TagValue = "bft-test"
$projectName = "utig" # [!! 更改 !!]
$basePort = 8000

# [!! 确认 !!] 路径是 Go 模块的根目录 (包含 go.mod)
$projectDir = "/root/fair-ordering/$projectName"
# 这是我们编译后给程序起的名字
$executableName = "utig_node" # [!! 更改 !!]


# ==========================================================
# --- 1.5. ✨ 实验参数配置 ---
# ==========================================================
# [!! 注意 !!] 您可以为 utig 设置与 SpeedFair 不同的参数
$paramF = 14
$paramGamma = 1
$paramLoInterval = 600
$paramLoSize = 120
$paramTxRate = 300 # (Node 0 的 Tx Rate)
$paramSimDuration = 35
$leaderWaitDelay = 1 # [!! 新增 !!] Leader 启动前等待的秒数
# ==========================================================


# ==========================================================
# --- 2. 本地任务：获取 IP 和 *实例ID* ---
# ==========================================================
Write-Host "--- Starting [Local Task] ---"
Write-Host "Step 1: Getting Instance IPs AND IDs from AWS..."
# ... (这部分不变) ...
$instances = aws ec2 describe-instances --filters "Name=tag:Project,Values=$ec2TagValue" "Name=instance-state-name,Values=running" --query "Reservations[].Instances[].{ID:InstanceId, IP:PrivateIpAddress}" | ConvertFrom-Json
if ($null -eq $instances) {
    Write-Host "ERROR: No instances found!"
    return
}
Write-Host "  Success! Found $($instances.Count) instances."


# ==========================================================
# --- 3. 本地任务：生成 *你的新格式* Config ---
# ==========================================================
Write-Host "Step 2: Generating NEW config.json content..."
# ... (这部分不变) ...
$nodeConfig = @{}
$nodeId = 0
foreach ($instance in $instances) {
    $nodeConfig["$nodeId"] = "$($instance.IP):$($basePort)"    
    $nodeId++
}
$configObject = @{ nodes = $nodeConfig }
$configJsonString = $configObject | ConvertTo-Json -Compress

# --- (Base64 不变) ---
$bytes     = [System.Text.Encoding]::UTF8.GetBytes($configJsonString)
$configB64 = [System.Convert]::ToBase64String($bytes)

Write-Host "  Success! Config content generated."


# ==========================================================
# --- 4. 🚀 远程任务：首先启动 Node 1 到 4 ---
# ==========================================================
Write-Host ""
Write-Host "--- Starting [Remote Task] (Phase 1/3) ---"
Write-Host "Step 3: Sending commands to Nodes 1..$($instances.Count - 1)..."

# [!! 路径修复 !!] 定义我们将在 pkg 目录中操作的文件
$configPath = "./pkg/config.json"
$executablePath = "./pkg/$executableName" # 可执行文件将被编译到 pkg 目录中

$nodeId = 1
foreach ($instance in $instances[1..($instances.Count - 1)]) {
    
    $instanceId = $instance.ID
    Write-Host "   -> Preparing command for Node $nodeId (Instance: $instanceId)..."

    $logRedirection = "> /dev/null 2>&1"
    Write-Host "        (This is Node $nodeId. Logging will be DISCARDED)"

    # [!! 关键 !!] Node 1-4 的 tx-rate 必须为 0
    $runParameters = "-config='$configPath' -nodes='$nodeId' -f=$paramF -gamma=$paramGamma -lo-interval=$paramLoInterval -lo-size=$paramLoSize   -tx-rate=$paramTxRate -sim-duration=$paramSimDuration"
    Write-Host "        (Node $nodeId will have tx-rate=0)"

    # --- (Base64 不变) ---
    # $configB64 已在第 3 节中生成
    
    # [!! 最终修复: go build + stdbuf !!]
    $shellCommands = @(
      'set -euxo pipefail',
      ('cd {0}' -f $projectDir), # cd 到模块根目录
      ("echo {0} | base64 -d > {1}" -f $configB64, $configPath), 
      ('pkill -f {0} || true' -f $executableName), 
      ('HOME=/root /usr/bin/go build -o {0} ./pkg' -f $executablePath), 
      ('chown ec2-user:ec2-user {0} {1}' -f $configPath, $executablePath), 
      ('sudo -u ec2-user HOME=/home/ec2-user nohup stdbuf -oL {0} {1} {2} </dev/null &' -f $executablePath, $runParameters, $logRedirection)
    )
    $parametersPs = @{ commands = $shellCommands }
    Send-SSMCommand `
      -DocumentName 'AWS-RunShellScript' `
      -Parameter $parametersPs `
      -InstanceId $instanceId | Out-Null
    
    Write-Host "        Sent command to Node $nodeId."
    $nodeId++
}
Write-Host "  Phase 1 complete. Nodes 1-$($instances.Count - 1) are starting."


# ==========================================================
# --- 4.5. [!! 新增 !!] 等待片刻，让 Follower 启动服务 ---
# ==========================================================
Write-Host ""
Write-Host "--- Starting [Remote Task] (Phase 2/3) ---"
Write-Host "Step 4: Waiting $leaderWaitDelay second(s) to allow follower services to initialize..."
Start-Sleep -Seconds $leaderWaitDelay


# ==========================================================
# --- 5. 🚀 远程任务：最后启动 Node 0 ---
# ==========================================================
Write-Host ""
Write-Host "--- Starting [Remote Task] (Phase 3/3) ---"
Write-Host "Step 5: Sending command to Node 0 (LAST)..."

$nodeId = 0
$instance = $instances[0] 
$instanceId = $instance.ID
Write-Host "   -> Preparing final command for Node $nodeId (Instance: $instanceId)..."

$logRedirection = "> ./pkg/bft.log 2>&1"
Write-Host "        (This is Node 0. Logging will be SAVED to ./pkg/bft.log)"

# [!! 关键 !!] Node 0 (客户端) 将使用您在 Section 1.5 中定义的 $paramTxRate
$runParameters = "-config='$configPath' -nodes='$nodeId' -f=$paramF -gamma=$paramGamma -lo-interval=$paramLoInterval -lo-size=$paramLoSize -tx-rate=$paramTxRate -sim-duration=$paramSimDuration"
Write-Host "        (Node 0 will have tx-rate=$paramTxRate)"


# --- (Base64 不变) ---
# $configB64 已在第 3 节中生成

# [!! 最终修复: go build + stdbuf !!]
$shellCommands = @(
    'set -euxo pipefail',
    ('cd {0}' -f $projectDir), # cd 到模块根目录
    ("echo {0} | base64 -d > {1}" -f $configB64, $configPath), 
    ('pkill -f {0} || true' -f $executableName), 
    ('HOME=/root /usr/bin/go build -o {0} ./pkg' -f $executablePath), 
    ('chown ec2-user:ec2-user {0} {1}' -f $configPath, $executablePath), 
    ('sudo -u ec2-user HOME=/home/ec2-user nohup stdbuf -oL {0} {1} {2} </dev/null &' -f $executablePath, $runParameters, $logRedirection)
)
$parametersPs = @{ commands = $shellCommands }
Send-SSMCommand `
    -DocumentName 'AWS-RunShellScript' `
    -Parameter $parametersPs `
    -InstanceId $instanceId | Out-Null

Write-Host "        Sent command to Node $nodeId."
Write-Host ""
Write-Host "--- All commands sent! ---"
Write-Host "Your '$projectName' experiment is starting. Node 0 was started last."
Write-Host "Script finished."