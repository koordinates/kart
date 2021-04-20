Set-StrictMode -Version 2.0
$ErrorActionPreference = 'stop'

$KART_PATH=(Get-Command kart).source
If ((Get-Item $KART_PATH).Directory.Name -eq 'Scripts') {
    # Virtualenv
    $KART_PREFIX=(Get-Item (Get-Command kart).source).Directory.Parent.FullName
    throw (">>> Error: Found Kart in a Virtualenv at $KART_PREFIX")
} Else {
    # Installation
    $KART_PREFIX=(Get-Item (Get-Command kart).source).DirectoryName
}
Write-Output "Kart is at: ${KART_PATH} (Prefix: ${KART_PREFIX})"

$SNO_PATH=(Get-Command sno).source
Write-Output "Found Sno at: ${SNO_PATH}"

# Check PROJ and GDAL data files are installed
if (! (Test-Path "${KART_PREFIX}\osgeo\data\proj\proj.db")) {
    Write-Output ">>> Error: Couldn't find PROJ data files"
    Exit 1
}
if (! (Test-Path "${KART_PREFIX}\osgeo\data\gdal\gdalvrt.xsd")) {
    Write-Output ">>> Error: Couldn't find GDAL data files"
    Exit 1
}

Write-Output ">>> Success"
