# ==========================================================
# --- 0. 导入 AWS 模块 ---
# ==========================================================
Import-Module AWS.Tools.SimpleSystemsManagement

# ==========================================================
# --- 1. 配置：检查你的“路标”和项目名 ---
# ==========================================================
$ec2TagValue = "bft-test"
$projectName = "SpeedFair_simplify"
$basePort = 8000
# [!! 确认 !!] 路径是 Go 模块的根目录 (包含 go.mod)
$projectDir = "/root/fair-ordering/$projectName"
# 这是我们编译后给程序起的名字
$executableName = "speedfair_node"

# ==========================================================
# --- 1.5. ✨ 实验参数配置 ---
# ==========================================================
$paramF = 8
$paramGamma = 1
$paramLoInterval = 400
$paramLoSize = 50
$paramTxRate = 200 # [!! 注意 !!] 这现在是 Node 0 的专属 Tx Rate
$paramSimDuration = 35
$leaderWaitDelay = 2 # [!! 新增 !!] Leader 启动前等待的秒数

# ==========================================================
# --- 2. 本地任务：获取 IP 和 *实例ID* ---
# ==========================================================
Write-Host "--- Starting [Local Task] ---"
Write-Host "Step 1: Getting Instance IPs AND IDs from AWS..."

$instances = aws ec2 describe-instances --filters "Name=tag:Project,Values=$ec2TagValue" "Name=instance-state-name,Values=running" --query "Reservations[].Instances[].{ID:InstanceId, IP:PrivateIpAddress}" | ConvertFrom-Json
if ($null -eq $instances) {
    Write-Host "ERROR: No instances found!"
    return
}
Write-Host " Success! Found $($instances.Count) instances."

# ==========================================================
# --- 3. 本地任务：生成 *你的新格式* Config ---
# ==========================================================
Write-Host "Step 2: Generating NEW config.json content..."

$nodeConfig = @{}
$nodeId = 0
foreach ($instance in $instances) {
    $nodeConfig["$nodeId"] = "$($instance.IP):$($basePort)"
    $nodeId++
}
$configObject = @{ nodes = $nodeConfig }
$configJsonString = $configObject | ConvertTo-Json -Compress

# --- (Base64 不变) ---
$bytes = [System.Text.Encoding]::UTF8.GetBytes($configJsonString)
$configB64 = [System.Convert]::ToBase64String($bytes)

Write-Host " Success! Config content generated."

# ==========================================================
# --- 4. 🚀 [!! 已修改 !!] 远程任务：分两步启动节点 ---
# ==========================================================
Write-Host ""
Write-Host "--- Starting [Remote Task] ---"

# --- 定义文件路径 ---
$configPath = "./pkg/config.json"
$executablePath = "./pkg/$executableName"

# --- 4A. [!! 新增 !!] 首先，启动所有 Follower 节点 (Node 1 到 N-1) ---
Write-Host "Step 3a: Sending commands to all Follower nodes (1 to $($instances.Count - 1))..."

# 从索引 1 (Node 1) 开始循环，跳过 0
for ($nodeId = 1; $nodeId -lt $instances.Count; $nodeId++) {
    $instance = $instances[$nodeId]
    $instanceId = $instance.ID
    Write-Host " -> Preparing command for Follower Node $nodeId (Instance: $instanceId)..."

    # Follower 节点的参数 (tx-rate=0)
    $logRedirection = "> ./pkg/bft.log 2>&1"
    $nodeTxRate = 0
    Write-Host " (This is Node $nodeId. Logging to bft.log, tx-rate=0)"
    
    $runParameters = "-config='$configPath' -nodes='$nodeId' -f=$paramF -gamma=$paramGamma -lo-interval=$paramLoInterval -lo-size=$paramLoSize -tx-rate=$nodeTxRate -sim-duration=$paramSimDuration"

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
    Send-SSMCommand -DocumentName 'AWS-RunShellScript' -Parameter $parametersPs -InstanceId $instanceId | Out-Null
    Write-Host " Sent command to Node $nodeId."
}

# --- 4B. [!! 新增 !!] 等待片刻，让 Follower 启动服务 ---
Write-Host ""
Write-Host "Step 3b: All followers started. Waiting $leaderWaitDelay seconds to allow services to initialize..."
Start-Sleep -Seconds $leaderWaitDelay

# --- 4C. [!! 新增 !!] 最后，启动 Leader 节点 (Node 0) ---
Write-Host ""
Write-Host "Step 3c: Starting Leader (Node 0)..."

$nodeId = 0
$instance = $instances[0]
$instanceId = $instance.ID
Write-Host " -> Preparing command for Leader Node $nodeId (Instance: $instanceId)..."

# Leader 节点的参数 (使用配置的 tx-rate)
$logRedirection = "> ./pkg/bft.log 2>&1"
$nodeTxRate = $paramTxRate
Write-Host " (This is Node 0. Logging to bft.log, tx-rate=$nodeTxRate)"

$runParameters = "-config='$configPath' -nodes='$nodeId' -f=$paramF -gamma=$paramGamma -lo-interval=$paramLoInterval -lo-size=$paramLoSize -tx-rate=$nodeTxRate -sim-duration=$paramSimDuration"

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
Send-SSMCommand -DocumentName 'AWS-RunShellScript' -Parameter $parametersPs -InstanceId $instanceId | Out-Null
Write-Host " Sent command to Node $nodeId."


Write-Host ""
Write-Host "--- All commands sent! ---"
Write-Host "Your '$projectName' experiment is starting."
Write-Host "Script finished."