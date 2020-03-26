Set-StrictMode -Version 2.0
$ErrorActionPreference = 'stop'

$SRC = Join-Path $PSScriptRoot '..\..'

if ($Env:SIGNTOOL) {
    $SIGNTOOL=$Env:SIGNTOOL
} Else {
    $SIGNTOOL=(Join-Path $Env:WindowsSdkVerBinPath 'x64\signtool.exe')
}

7z x "$(Join-Path $SRC 'vendor\dist\vendor-Windows.zip')" "-o$(Join-Path $SRC 'vendor\dist')" -aoa

Push-Location -Path $SRC
try {
    .\venv\Scripts\pyinstaller `
        --clean -y `
        --workpath platforms\windows\build `
        --distpath platforms\windows\dist `
        sno.spec
    if (!$?) {
        exit $LastExitCode
    }

    if ($Env:SIGNCERTKEY) {
        $BINARIES=@('sno.exe', 'git2.dll')
        $TS_SERVERS=@(
            'http://timestamp.digicert.com',
            'http://timestamp.globalsign.com/scripts/timstamp.dll',
            'http://timestamp.geotrust.com/tsa',
            'http://timestamp.comodoca.com/rfc3161'
        )

        foreach ($BIN in $BINARIES) {
            foreach ($TS in $TS_SERVERS) {
                Write-Output ">>> Signing $BIN (w/ $TS) ..."
                & $SIGNTOOL sign `
                /f "$Env:SIGNCERTKEY" `
                /p "$Env:SIGNCERTPW" `
                /d 'Sno CLI' `
                /tr $TS `
                /v (Join-Path '.\platforms\windows\dist\sno' $BIN)
                if ($?) {
                    break
                }
            }
            if (!$?) {
                Write-Output "Error signing $BIN, tried lots of timestamp servers"
                exit $LastExitCode
            }

            & $SIGNTOOL verify /pa (Join-Path '.\platforms\windows\dist\sno' $BIN)
            if (!$?) {
                exit $LastExitCode
            }
        }
    }

    platforms\windows\dist\sno\sno.exe --version
    if (!$?) {
        exit $LastExitCode
    }
}
finally {
    Pop-Location
}

$DISTPATH=(Join-Path $PSScriptRoot "dist\sno")
Write-Output ">>> Success! Created app in: $DISTPATH"
