# ==========================================================
# --- 0. 导入 AWS 模块 ---
# ==========================================================
Import-Module AWS.Tools.SimpleSystemsManagement

# ==========================================================
# --- 1. CONFIGURATION: Check your "Tag" here ---
# ==========================================================
$ec2TagValue = "bft-test"
$cloneDir = "/root" # [!! 已修复 !!] 我们将统一使用 root 的家目录
$repoName = "fair-ordering"

# ==========================================================
# --- 2. Get Instance Count ---
# ==========================================================
Write-Host "--- Starting [Local Task] ---"
Write-Host "Step 1: Getting instance count..."
$instances = aws ec2 describe-instances --filters "Name=tag:Project,Values=$ec2TagValue" "Name=instance-state-name,Values=running" --query "Reservations[].Instances[].InstanceId" | ConvertFrom-Json
if ($null -eq $instances) {
    Write-Host "ERROR: No instances found! Please check your tag value."
    return
}
$instanceCount = $instances.Count
Write-Host "  Success! Found $instanceCount instances."
Write-Host "(Note: config.json is no longer created by this script.)"


# ==========================================================
# --- 3. Command all EC2 instances to Download Code ---
# ==========================================================
Write-Host ""
Write-Host "--- Starting [Remote Task] ---"
Write-Host "Step 3: Commanding all $instanceCount nodes to download code AND install Go..."

# 1. Define the Linux commands
$shellCommands = @(
    "set -euxo pipefail",
    "sudo dnf install -y git golang", # Install Go
    "cd $cloneDir",                  # [!! 已修复 !!] Go to /root
    "rm -rf $repoName",
    "git clone https://github.com/lesserbug/fair-ordering.git $repoName"
    # [!! 已修复 !!] chown 命令已删除，不再需要
)

# 2. Create the Parameters *object*
$parametersPs = @{ commands = $shellCommands }

# 3. Create the Target *object*
$target = @{
    Key = "tag:Project"
    Values = @($ec2TagValue)
}

# 4. Send the command using the correct object syntax
Send-SSMCommand `
  -DocumentName 'AWS-RunShellScript' `
  -Parameter $parametersPs `
  -Targets $target | Out-Null # Pass the $target object here

Write-Host ""
Write-Host "--- All tasks have been sent! ---"
Write-Host "Code download & Go install command sent. Please check AWS 'Run Command' history for 'Success'."
Write-Host "Script finished."