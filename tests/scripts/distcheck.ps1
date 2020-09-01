Set-StrictMode -Version 2.0
$ErrorActionPreference = 'stop'

$SNO_PATH=(Get-Command sno).source
If ((Get-Item $SNO_PATH).Directory.Name -eq 'Scripts') {
    # Virtualenv
    $SNO_PREFIX=(Get-Item (Get-Command sno).source).Directory.Parent.FullName
    throw (">>> Error: Found Sno in a Virtualenv at $SNO_PREFIX")
} Else {
    # Installation
    $SNO_PREFIX=(Get-Item (Get-Command sno).source).DirectoryName
}
Write-Output "Sno is at: ${SNO_PATH} (Prefix: ${SNO_PREFIX})"

# Check PROJ and GDAL data files are installed
if (! (Test-Path "${SNO_PREFIX}\osgeo\data\proj\proj.db")) {
    Write-Output ">>> Error: Couldn't find PROJ data files"
    Exit 1
}
if (! (Test-Path "${SNO_PREFIX}\osgeo\data\gdal\gdalvrt.xsd")) {
    Write-Output ">>> Error: Couldn't find GDAL data files"
    Exit 1
}

Write-Output ">>> Success"
