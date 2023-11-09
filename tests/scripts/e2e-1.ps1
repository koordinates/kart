Set-StrictMode -Version 2.0
$ErrorActionPreference = 'stop'

# Taken from psake https://github.com/psake/psake
<#
.SYNOPSIS
  This is a helper function that runs a scriptblock and checks the PS variable $lastexitcode
  to see if an error occcured. If an error is detected then an exception is thrown.
  This function allows you to run command-line programs without having to
  explicitly check the $lastexitcode variable.
.EXAMPLE
  exec { svn info $repository_trunk } "Error executing SVN. Please verify SVN command-line client is installed"
#>
function Exec
{
    [CmdletBinding()]
    param(
        [Parameter(Position=0,Mandatory=1)][scriptblock]$cmd,
        [Parameter(Position=1,Mandatory=0)][string]$errorMessage = ("Error executing command {0}" -f $cmd)
    )
    Write-Output "$ $cmd"
    & $cmd
    if ($lastexitcode -ne 0) {
        throw ("Exec: (${LastExitCode}) ${ErrorMessage}")
    }
}


$TEST_GPKG=(Join-Path $PSScriptRoot '..\data\e2e.gpkg')
Write-Output "Test data is at: ${TEST_GPKG}"

$TMP_GUID=([string] [System.Guid]::NewGuid())
$TMP_PATH=(New-Item -ItemType Directory -Path (Join-Path ([System.IO.Path]::GetTempPath()) "kart-e2e.${TMP_GUID}"))
Write-Output "Using temp folder: ${TMP_PATH}"

$KART_PATH=(Get-Command kart).source
If ((Get-Item $KART_PATH).Directory.Name -eq 'Scripts') {
    # Virtualenv
    $KART_PREFIX=(Get-Item (Get-Command kart).source).Directory.Parent.FullName
} Else {
    # Installation
    $KART_PREFIX=(Get-Item (Get-Command kart).source).DirectoryName
}
Write-Output "Kart is at: ${KART_PATH} (Prefix: ${KART_PREFIX})"

# Spatialite
$SPATIALITE=("${KART_PREFIX}\_internal\mod_spatialite" -replace '\\', '/').ToLower()

New-Item -ItemType Directory -Path "${TMP_PATH}\test"
Push-Location "${TMP_PATH}\test"
try {
    Exec { kart -vvvv install tab-completion --shell auto }
    Exec { kart init --initial-branch=main . }
    Exec { kart -v config --local 'user.name' 'Kart E2E Test 1' }
    Exec { kart -v config --local 'user.email' 'kart-e2e-test-1@email.invalid' }
    Exec { kart -v config --local 'core.pager' false }
    Exec { kart import "GPKG:${TEST_GPKG}" "mylayer" }

    Exec { kart log }
    Exec { kart checkout }
    Exec { kart switch -c 'edit-1' }
    Write-Output "$  <updating working copy> sqlite3"

    $script_py = @"
def main(ctx, args):
    with ctx.obj.repo.working_copy.tabular.session() as sess:
        sess.execute(
            "INSERT INTO mylayer (fid, geom) VALUES (999, GeomFromEWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));"
        )
"@

    $script_py | Out-File "script.py";
    Exec { kart ext-run script.py }
    Exec { kart status }
    Exec { kart diff --crs=EPSG:3857 }
    Exec { kart commit -m 'my-commit' }
    Exec { kart switch 'main' }
    Exec { kart status }
    Exec { kart merge 'edit-1' --no-ff -m 'my-merge'}
    Exec { kart log }
}
catch {
    Write-Output ">>> E2E Error: $($PSItem.ToString())"
}
finally {
    Pop-Location
    Remove-Item -Force -Recurse "$TMP_PATH"
}

Write-Output ">>> E2E Success"
