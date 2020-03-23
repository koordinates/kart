Set-StrictMode -Version 2.0
$ErrorActionPreference = 'stop'

$TEST_GPKG=(Join-Path $PSScriptRoot '..\data\e2e.gpkg')
Write-Output "Test data is at: ${TEST_GPKG}"

$TMP_GUID=([string] [System.Guid]::NewGuid())
$TMP_PATH=(New-Item -ItemType Directory -Path (Join-Path ([System.IO.Path]::GetTempPath()) "sno-e2e.${TMP_GUID}"))
Write-Output "Using temp folder: ${TMP_PATH}"

$SNO_PATH=(Get-Command sno).source
$SNO_DIR=(Get-Item (Get-Command sno).source).DirectoryName
$SPATIALITE=("${SNO_PATH}\mod_spatialite" -replace '\\', '/').ToLower()
Write-Output "Sno is at: ${SNO_PATH}"

New-Item -ItemType Directory -Path "${TMP_PATH}\test"
Push-Location "${TMP_PATH}\test"
Set-PSDebug -Trace 1
try {
    sno init .
    sno -v config --local 'user.name' 'Sno E2E Test 1'
    sno -v config --local 'user.email' 'sno-e2e-test-1@email.invalid'
    sno -v config --local 'core.pager' false
    sno import "GPKG:${TEST_GPKG}:mylayer"

    sno log
    sno checkout
    sno switch -c 'edit-1'
    & (Join-Path $SNO_DIR 'sqlite3.exe') --bail test.gpkg "
      SELECT load_extension('$SPATIALITE');
      SELECT EnableGpkgMode();
      INSERT INTO mylayer (fid, geom) VALUES (999, GeomFromEWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));
    "
    sno status
    sno diff
    sno commit -m 'my-commit'
    sno switch 'master'
    sno status
    sno merge 'edit-1' --no-ff
    sno log

    Set-PSDebug -Trace 0
}
catch {
    Set-PSDebug -Trace 0
    Write-Output "❗️ E2E: Error"
}
finally {
    Pop-Location
    Remove-Item -Recurse "$TMP_PATH\test"
}

Write-Output "✅ E2E: Success"
