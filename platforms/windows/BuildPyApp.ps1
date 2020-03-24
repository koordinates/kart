Set-StrictMode -Version 2.0
$ErrorActionPreference = 'stop'

$SRC = Join-Path $PSScriptRoot '..\..'

$SIGNTOOL='C:\Program Files (x86)\Windows Kits\10\App Certification Kit\signtool.exe'
if ($Env:SIGNTOOL) {
    $SIGNTOOL = $Env:SIGNTOOL
}

7z x "$(Join-Path $SRC 'vendor\dist\vendor-Windows.zip')" "-o$(Join-Path $SRC 'vendor\dist')" -aoa

Push-Location -Path $SRC
Get-Location
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
        Write-Output '>>> Signing sno.exe ...'
        & $SIGNTOOL sign `
            /f "$Env:SIGNCERTKEY" `
            /p "$Env:SIGNCERTPW" `
            /d 'Sno CLI' `
            /t http://timestamp.verisign.com/scripts/timstamp.dll `
            /v .\platforms\windows\dist\sno\sno.exe
        if (!$?) {
            exit $LastExitCode
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
