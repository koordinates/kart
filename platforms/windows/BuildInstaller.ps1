Set-StrictMode -Version 2.0
$ErrorActionPreference = 'stop'

if ($Env:WIX) {
    Write-Output ">>> Found Wix at: $Env:WIX"
    $WIXBIN = (Join-Path $Env:WIX 'bin\')
}

if ($Env:SIGNTOOL) {
    $SIGNTOOL=$Env:SIGNTOOL
} Else {
    $SIGNTOOL=(Join-Path $Env:WindowsSdkVerBinPath 'x64\signtool.exe')
}

if ($Env:SNO_INSTALL_VERSION) {
    $INSTALLVER=$Env:SNO_INSTALL_VERSION
    $MSINAME="Sno-${INSTALLVER}.msi"
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
        $TS_SERVERS=@(
            'http://timestamp.digicert.com',
            'http://timestamp.globalsign.com/scripts/timstamp.dll',
            'http://timestamp.geotrust.com/tsa',
            'http://timestamp.comodoca.com/rfc3161'
        )

        foreach ($TS in $TS_SERVERS) {
            Write-Output ">>> Signing installer (w/ $TS) ..."
            & $SIGNTOOL sign `
            /f "$Env:SIGNCERTKEY" `
            /p "$Env:SIGNCERTPW" `
            /d 'Sno Installer' `
            /fd 'sha256' `
            /tr $TS `
            /v ".\dist\${MSINAME}"
            if ($?) {
                break
            }
        }
        if (!$?) {
            Write-Output "Error signing installer, tried lots of timestamp servers"
            exit $LastExitCode
        }

        & $SIGNTOOL verify /pa ".\dist\${MSINAME}"
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
