<#
.SYNOPSIS
    Creates the three demo Entra groups and one test user per group.

.DESCRIPTION
    Idempotent — safe to run multiple times. Creates:
      Groups : sg-dbplat-standard-readers, sg-dbplat-pii-readers, sg-dbplat-data-stewards
      Users  : Norma Redacta (standard), Seymour Cleartext (PII), Stewart Tagger (steward)

    After running, manually add the three groups to the Databricks account at
    https://accounts.azuredatabricks.net -> User Management -> Groups
    (or enable SCIM provisioning from Entra to do this automatically).

.PARAMETER Password
    Initial password for all three demo users. Must satisfy your tenant's
    password complexity policy.

.PARAMETER TenantDomain
    UPN suffix for new users, e.g. contoso.onmicrosoft.com or a verified custom domain.
    Defaults to the tenant's default domain (read from az account show).

.EXAMPLE
    .\scripts\bootstrap-groups.ps1 -Password "Dbx@Demo2025!"
#>
param(
    [Parameter(Mandatory)]
    [string] $Password,

    [string] $TenantDomain = ""
)

if (-not $TenantDomain) {
    Write-Host "Detecting tenant domain..."
    $TenantDomain = az account show --query tenantDefaultDomain -o tsv
}
Write-Host "Tenant domain: $TenantDomain"

# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------

$groupNames = @(
    "sg-dbplat-standard-readers",
    "sg-dbplat-pii-readers",
    "sg-dbplat-data-stewards"
)

$groupIds = @{}

foreach ($name in $groupNames) {
    $existing = az ad group show --group $name 2>$null | ConvertFrom-Json
    if ($existing) {
        Write-Host "Group '$name' already exists — skipping." -ForegroundColor Yellow
        $groupIds[$name] = $existing.id
    } else {
        Write-Host "Creating group '$name'..."
        $group = az ad group create --display-name $name --mail-nickname $name | ConvertFrom-Json
        $groupIds[$name] = $group.id
        Write-Host "  Created ($($group.id))" -ForegroundColor Green
    }
}

# ---------------------------------------------------------------------------
# Users — names chosen to match their access tier
#
#   Norma Redacta    → standard-readers  (her data is always redacted)
#   Seymour Cleartext → pii-readers      (sees more, in cleartext)
#   Stewart Tagger   → data-stewards     (stewards the tags)
# ---------------------------------------------------------------------------

$users = @(
    [PSCustomObject]@{ DisplayName = "Norma Redacta";    Nickname = "norma.redacta";    Group = "sg-dbplat-standard-readers" }
    [PSCustomObject]@{ DisplayName = "Seymour Cleartext"; Nickname = "seymour.cleartext"; Group = "sg-dbplat-pii-readers" }
    [PSCustomObject]@{ DisplayName = "Stewart Tagger";   Nickname = "stewart.tagger";   Group = "sg-dbplat-data-stewards" }
)

$summary = @()

foreach ($u in $users) {
    $upn = "$($u.Nickname)@$TenantDomain"

    $existing = az ad user show --id $upn 2>$null | ConvertFrom-Json
    if ($existing) {
        Write-Host "User '$upn' already exists — skipping creation." -ForegroundColor Yellow
        $userId = $existing.id
    } else {
        Write-Host "Creating user '$($u.DisplayName)' ($upn)..."
        $user = az ad user create `
            --display-name  $u.DisplayName `
            --user-principal-name $upn `
            --password $Password `
            --force-change-password-next-sign-in false | ConvertFrom-Json
        $userId = $user.id
        Write-Host "  Created ($userId)" -ForegroundColor Green
    }

    $groupId  = $groupIds[$u.Group]
    $isMember = az ad group member check --group $groupId --member-id $userId --query value -o tsv
    if ($isMember -eq "true") {
        Write-Host "  Already a member of '$($u.Group)'" -ForegroundColor Yellow
    } else {
        Write-Host "  Adding to '$($u.Group)'..."
        az ad group member add --group $groupId --member-id $userId | Out-Null
        Write-Host "  Added" -ForegroundColor Green
    }

    $summary += [PSCustomObject]@{
        Name     = $u.DisplayName
        UPN      = $upn
        Group    = $u.Group
        Password = $Password
    }
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

Write-Host ""
Write-Host "Demo users ready:" -ForegroundColor Cyan
$summary | Format-Table -AutoSize

Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Add the three groups to the Databricks account:"
Write-Host "     https://accounts.azuredatabricks.net -> User Management -> Groups"
Write-Host "  2. Run the governance-setup job to apply catalog grants and column masks."
Write-Host "  3. Log in as each user to test masked vs unmasked PII access."
