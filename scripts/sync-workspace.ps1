<#
.SYNOPSIS
    Updates the Databricks CLI auth profile with the workspace URL from the latest Terraform apply.

.DESCRIPTION
    Run this script after each `terraform apply` in simple-databricks-deployment.
    It reads the workspace_url output from Terraform and writes it to ~/.databrickscfg
    so that `databricks bundle` commands pick up the correct host without any manual edits.

.EXAMPLE
    .\scripts\sync-workspace.ps1
    # Then authenticate:
    databricks auth login
#>

param(
    [string]$Profile = "DEFAULT",
    [string]$TfRelDir = "..\..\simple-databricks-deployment\terraform"
)

$TfDir = Join-Path $PSScriptRoot $TfRelDir
if (-not (Test-Path $TfDir)) {
    Write-Error "Terraform directory not found: $TfDir"
    Write-Error "Ensure simple-databricks-deployment is checked out as a sibling of this repo."
    exit 1
}

Write-Host "Reading workspace URL from Terraform outputs..." -ForegroundColor Cyan
$workspaceHost = terraform -chdir $TfDir output -raw workspace_url 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "terraform output failed. Make sure you have run `terraform apply` in simple-databricks-deployment."
    exit 1
}

$fullUrl = "https://$workspaceHost"
Write-Host "Workspace URL: $fullUrl" -ForegroundColor Green

databricks configure --host $fullUrl --profile $Profile
if ($LASTEXITCODE -ne 0) {
    Write-Error "databricks configure failed."
    exit 1
}

Write-Host ""
Write-Host "Databricks CLI profile '$Profile' updated." -ForegroundColor Green
Write-Host "Next step: databricks auth login --profile $Profile" -ForegroundColor Yellow
