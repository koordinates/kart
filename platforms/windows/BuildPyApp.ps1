Set-StrictMode -Version 2.0
$ErrorActionPreference = 'stop'

if (-not (Get-Command -erroraction 'silentlycontinue' signtool)) {
    $Env:PATH += ";${Env:WindowsSdkVerBinPath}\x64\signtool.exe"
}
if ($Env:SIGN_AZURE_CERTIFICATE) {
    Write-Output ">>> Checking for AzureSignTool: " (Get-Command azuresigntool).Path
}

$SRC = Join-Path $PSScriptRoot '..\..'

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

    if ($Env:SIGN_AZURE_CERTIFICATE) {
        $BINARIES=@('sno.exe', 'git2.dll')
        $TS_SERVERS=@(
            'http://timestamp.globalsign.com/scripts/timstamp.dll',
            'http://timestamp.digicert.com',
            'http://timestamp.geotrust.com/tsa',
            'http://timestamp.comodoca.com/rfc3161'
        )

        foreach ($BIN in $BINARIES) {
            foreach ($TS in $TS_SERVERS) {
                Write-Output ">>> Signing $BIN (w/ $TS) ..."
                & azuresigntool sign `
                --azure-key-vault-url="$Env:SIGN_AZURE_VAULT" `
                --azure-key-vault-client-id="$Env:SIGN_AZURE_CLIENTID" `
                --azure-key-vault-client-secret="$Env:SIGN_AZURE_CLIENTSECRET" `
                --azure-key-vault-certificate="$Env:SIGN_AZURE_CERTIFICATE" `
                --description-url="https://sno.earth" `
                --description="Sno CLI" `
                --timestamp-rfc3161="$TS" `
                --verbose `
                (Join-Path '.\platforms\windows\dist\sno' $BIN)
                if ($?) {
                    break
                }
            }
            if (!$?) {
                Write-Output "Error signing $BIN, tried lots of timestamp servers"
                exit $LastExitCode
            }

            & signtool verify /pa (Join-Path '.\platforms\windows\dist\sno' $BIN)
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
