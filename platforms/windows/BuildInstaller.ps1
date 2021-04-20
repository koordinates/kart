Set-StrictMode -Version 2.0
$ErrorActionPreference = 'stop'

if ($Env:WIX) {
    Write-Output ">>> Found Wix at: $Env:WIX"
    $WIXBIN = (Join-Path $Env:WIX 'bin\')
}

if (-not (Get-Command -erroraction 'silentlycontinue' signtool)) {
    $Env:PATH += ";${Env:WindowsSdkVerBinPath}\x64\signtool.exe"
}
if ($Env:SIGN_AZURE_CERTIFICATE) {
    Write-Output ">>> Checking for AzureSignTool: " (Get-Command azuresigntool).Path
}

if ($Env:KART_INSTALLER_VERSION) {
    $INSTALLVER=$Env:KART_INSTALLER_VERSION

    if ($Env:KART_VERSION) {
        $MSINAME="Kart-${Env:KART_VERSION}.msi"
    } else {
        $MSINAME="Kart-${INSTALLVER}.msi"
    }
} else {
    $INSTALLVER='0.0.0'
    $MSINAME='Kart.msi'
}
Write-Output ">>> Installer version: $INSTALLVER"

Push-Location -Path $PSScriptRoot
try {
    Write-Output '>>> Wix: Collecting files from dist\kart ...'
    & "${WIXBIN}heat" dir .\dist\kart -o .\build\AppFiles.wxs -nologo -scom -frag -srd -sreg -gg -cg CG_AppFiles -dr APPDIR
    if (!$?) {
        Write-Output "heat $LastExitCode"
        exit $LastExitCode
    }

    Write-Output '>>> Wix: Compiling ...'
    & "${WIXBIN}candle" -nologo -v -arch x64 -dVersion="$INSTALLVER" kart.wxs .\build\AppFiles.wxs -o .\build\
    if (!$?) {
        exit $LastExitCode
    }

    Write-Output '>>> Wix: Building Installer ...'
    & "${WIXBIN}light" -nologo -v -b .\dist\kart `
        -o ".\dist\${MSINAME}" `
        .\build\kart.wixobj .\build\AppFiles.wixobj `
        -ext WixUIExtension -cultures:en-us `
        -ext WixUtilExtension
    if (!$?) {
        exit $LastExitCode
    }

    if ($Env:SIGN_AZURE_CERTIFICATE) {
        $TS_SERVERS=@(
            'http://timestamp.globalsign.com/scripts/timstamp.dll',
            'http://timestamp.digicert.com',
            'http://timestamp.geotrust.com/tsa',
            'http://timestamp.comodoca.com/rfc3161'
        )

        foreach ($TS in $TS_SERVERS) {
            Write-Output ">>> Signing $MSINAME (w/ $TS) ..."
            & azuresigntool sign `
            --azure-key-vault-url="$Env:SIGN_AZURE_VAULT" `
            --azure-key-vault-client-id="$Env:SIGN_AZURE_CLIENTID" `
            --azure-key-vault-client-secret="$Env:SIGN_AZURE_CLIENTSECRET" `
            --azure-key-vault-certificate="$Env:SIGN_AZURE_CERTIFICATE" `
            --description-url="https://www.kartproject.org" `
            --description="Kart Installer" `
            --timestamp-rfc3161="$TS" `
            --verbose `
            (Join-Path '.\dist' $MSINAME)
            if ($?) {
                break
            }
        }
        if (!$?) {
            Write-Output "Error signing $MSINAME, tried lots of timestamp servers"
            exit $LastExitCode
        }

        & signtool verify /pa (Join-Path '.\dist' $MSINAME)
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
