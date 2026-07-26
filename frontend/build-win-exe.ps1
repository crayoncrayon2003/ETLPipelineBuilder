$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

$nodeVersion = node -p "process.versions.node"
$nodeParts = $nodeVersion.Split(".")
if ([int]$nodeParts[0] -lt 22 -or ([int]$nodeParts[0] -eq 22 -and [int]$nodeParts[1] -lt 12)) {
    throw "Node.js 22.12 or later is required. Current version: $nodeVersion"
}

npm ci
npm run build-win-exe
