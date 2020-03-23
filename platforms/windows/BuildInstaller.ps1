Set-StrictMode -Version 2.0
$ErrorActionPreference = 'stop'

if ($Env:WIX) {
    Write-Output ">>> Found Wix at: $Env:WIX"
    $WIXBIN = (Join-Path $Env:WIX 'bin\')
}

$SIGNTOOL='C:\Program Files (x86)\Windows Kits\10\App Certification Kit\signtool.exe'
if ($Env:SIGNTOOL) {
    $SIGNTOOL = $Env:SIGNTOOL
}

if ($Env:SNO_INSTALL_VERSION) {
    $INSTALLVER=$Env:SNO_INSTALL_VERSION
    $MSINAME="Sno-${$INSTALLVER}.msi"
} else {
    $INSTALLVER='0.0.0'
    $MSINAME='Sno.msi'
}
Write-Output ">>> Installer version: $INSTALLVER"

Push-Location -Path $PSScriptRoot
try {
    Write-Output '>>> Wix: Collecting files from dist\sno ...'
    & "${WIXBIN}heat" dir .\dist\sno -o .\build\AppFiles.wxs -nologo -scom -frag -srd -sreg -gg -cg CG_AppFiles -dr APPDIR
    if (!$?) {
        Write-Output "heat $LastExitCode"
        exit $LastExitCode
    }

    Write-Output '>>> Wix: Compiling ...'
    & "${WIXBIN}candle" -nologo -v -arch x64 -dVersion="$INSTALLVER" sno.wxs .\build\AppFiles.wxs -o .\build\
    if (!$?) {
        exit $LastExitCode
    }

    Write-Output '>>> Wix: Building Installer ...'
    & "${WIXBIN}light" -nologo -v -b .\dist\sno `
        -o ".\dist\${MSINAME}" `
        .\build\sno.wixobj .\build\AppFiles.wixobj `
        -ext WixUIExtension -cultures:en-us
    if (!$?) {
        exit $LastExitCode
    }

    if ($Env:SIGNCERTKEY) {
        Write-Output '>>> Signing Installer ...'
        & $SIGNTOOL sign `
            /f "$Env:SIGNCERTKEY" `
            /p "$Env:SIGNCERTPW" `
            /d 'Sno Installer' `
            /t http://timestamp.verisign.com/scripts/timstamp.dll `
            /v ".\dist\${MSINAME}"
        if (!$?) {
            exit $LastExitCode
        }
    }
}
finally {
    Pop-Location
}

$MSIPATH=(Join-Path $PSScriptRoot "dist\${MSINAME}")
Write-Output ">>> Success! Created: $MSIPATH"
