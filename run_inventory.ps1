<#
.SYNOPSIS
    FortiCNAPP Resource Inventory & Sizing CLI (PowerShell Wrapper)
    Compatible con Windows (PowerShell 5.1+), Linux/macOS (PowerShell Core) y Cloud Shells.

.DESCRIPTION
    Ejecuta el análisis de inventario para FortiCNAPP en AWS, Azure o GCP.
    Crea automáticamente un entorno virtual aislado (.venv), instala las dependencias de
    requirements.txt y utiliza las credenciales presentes en el equipo o en la Cloud Shell.

.PARAMETER Provider
    Proveedor cloud a analizar: 'azure', 'aws', 'gcp' o 'calculate'.

.EXAMPLE
    .\run_inventory.ps1 azure
    .\run_inventory.ps1 azure --all-subscriptions --encoded-output
    .\run_inventory.ps1 aws --all-accounts
    .\run_inventory.ps1 aws --profile my-sso-profile
    .\run_inventory.ps1 gcp --scope organizations/1234567890
    .\run_inventory.ps1 calculate aws_resource_counts.json azure_resource_counts.json
#>

[CmdletBinding()]
param(
    [Parameter(Position=0, Mandatory=$false)]
    [string]$Provider,

    [Parameter(Position=1, ValueFromRemainingArguments=$true)]
    [string[]]$ExtraArgs = @()
)

$ErrorActionPreference = "Stop"

# Directorios principales
$ScriptDir = $PSScriptRoot
if (-not $ScriptDir) {
    $ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
}
$VenvDir   = Join-Path $ScriptDir ".venv"
$ReqFile   = Join-Path $ScriptDir "requirements.txt"
$StampFile = Join-Path $VenvDir ".requirements_installed"

# Determinar rutas relativas de python en venv según plataforma
if ($IsWindows -or ($env:OS -like "*Windows*")) {
    $VenvPython = Join-Path $VenvDir "Scripts\python.exe"
    $VenvPip    = Join-Path $VenvDir "Scripts\pip.exe"
} else {
    $VenvPython = Join-Path $VenvDir "bin/python"
    $VenvPip    = Join-Path $VenvDir "bin/pip"
}

# Funciones de registro con color
function Write-LogInfo([string]$msg) {
    Write-Host "[INFO] " -ForegroundColor Cyan -NoNewline
    Write-Host $msg
}

function Write-LogOk([string]$msg) {
    Write-Host "[OK] " -ForegroundColor Green -NoNewline
    Write-Host $msg
}

function Write-LogWarn([string]$msg) {
    Write-Host "[AVISO] " -ForegroundColor Yellow -NoNewline
    Write-Host $msg
}

function Write-LogError([string]$msg) {
    Write-Host "[ERROR] " -ForegroundColor Red -NoNewline
    Write-Host $msg
}

function Show-Help() {
    Write-Host "FortiCNAPP Resource Inventory & Sizing CLI" -ForegroundColor White
    Write-Host ""
    Write-Host "Ejecuta el análisis de inventario para FortiCNAPP en AWS, Azure o GCP."
    Write-Host "Instala automáticamente dependencias en un entorno virtual aislado (.venv)"
    Write-Host "y utiliza las credenciales locales del PC o del Cloud Shell respectivo."
    Write-Host ""
    Write-Host "USO:" -ForegroundColor White
    Write-Host "  .\run_inventory.ps1 <proveedor> [opciones adicionales del script]"
    Write-Host ""
    Write-Host "PROVEEDORES DISPONIBLES:" -ForegroundColor White
    Write-Host "  azure       Ejecuta el escaneo de recursos en Microsoft Azure"
    Write-Host "  aws         Ejecuta el escaneo de recursos en Amazon Web Services"
    Write-Host "  gcp         Ejecuta el escaneo de recursos en Google Cloud Platform"
    Write-Host "  calculate   Calcula los recursos facturables a partir de los JSON generados"
    Write-Host ""
    Write-Host "DETALLES DE AUTENTICACIÓN Y ALCANCE POR NUBE:" -ForegroundColor White
    Write-Host ""
    Write-Host "  1. Azure (count_azure_resources.py):" -ForegroundColor Cyan
    Write-Host "     - Credenciales: Usa DefaultAzureCredential (Azure CLI 'az login', Managed Identity"
    Write-Host "       en Azure Cloud Shell, o variables AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, etc.)."
    Write-Host "     - Alcance por defecto: Todo el tenant con todas las suscripciones habilitadas (--all-subscriptions)."
    Write-Host "     - Opciones de alcance:"
    Write-Host "         --subscription <ID...>      Limita el escaneo a una o varias suscripciones específicas"
    Write-Host "         --management-group <MG_ID>  Escanea recursivamente bajo un grupo de administración"
    Write-Host "         --encoded-output            Imprime bloque gzip+base64 para copiar y pegar fácilmente"
    Write-Host ""
    Write-Host "  2. AWS (count_aws_resources.py):" -ForegroundColor Cyan
    Write-Host "     - Credenciales: Cadena estándar de AWS (variables de entorno, ~/.aws/credentials,"
    Write-Host "       SSO en ~/.aws/config, rol IAM de instancia o contenedor en AWS CloudShell)."
    Write-Host "     - Alcance por defecto: Detecta automáticamente si hay una AWS Organization activa"
    Write-Host "       con permisos para escanear todas las cuentas (--all-accounts). Si no, escanea la cuenta actual."
    Write-Host "     - Opciones de alcance:"
    Write-Host "         --profile <nombre>          Usa un perfil específico de ~/.aws/config (ej. perfil SSO)"
    Write-Host "         --account-id <ID...>        Asume --role-name en las cuentas indicadas"
    Write-Host "         --scan-profiles <P1...>     Escanea múltiples perfiles CLI independientes"
    Write-Host "         --encoded-output            Imprime bloque gzip+base64 para copiar y pegar fácilmente"
    Write-Host ""
    Write-Host "  3. GCP (count_gcp_resources.py):" -ForegroundColor Cyan
    Write-Host "     - Credenciales: Application Default Credentials (ADC) ('gcloud auth application-default login',"
    Write-Host "       GOOGLE_APPLICATION_CREDENTIALS, o credenciales automáticas en Google Cloud Shell)."
    Write-Host "     - Alcance por defecto: Proyecto activo individual (GOOGLE_CLOUD_PROJECT o gcloud config)."
    Write-Host "     - Opciones de alcance:"
    Write-Host "         --scope organizations/<ID>  Escanea toda la organización en Cloud Asset Inventory"
    Write-Host "         --scope folders/<ID>        Escanea una carpeta completa"
    Write-Host "         --no-smoothing              Conteo en tiempo real sin promedio histórico (más rápido)"
    Write-Host "         --encoded-output            Imprime bloque gzip+base64 para copiar y pegar fácilmente"
    Write-Host ""
    Write-Host "EJEMPLOS DE USO:" -ForegroundColor White
    Write-Host "  .\run_inventory.ps1 azure"
    Write-Host "  .\run_inventory.ps1 azure --all-subscriptions --encoded-output"
    Write-Host "  .\run_inventory.ps1 aws --all-accounts"
    Write-Host "  .\run_inventory.ps1 aws --profile my-sso-profile"
    Write-Host "  .\run_inventory.ps1 gcp --scope organizations/1234567890"
    Write-Host "  .\run_inventory.ps1 calculate aws_resource_counts.json azure_resource_counts.json"
    Write-Host ""
}

# ------------------------------------------------------------------------------
# 1. Búsqueda de un intérprete Python compatible (>= 3.10)
# ------------------------------------------------------------------------------
function Find-CompatiblePython {
    $candidates = @("python3.13", "python3.12", "python3.11", "python3.10", "python3", "python", "py")
    foreach ($cmd in $candidates) {
        $resolved = Get-Command $cmd -ErrorAction SilentlyContinue
        if ($resolved) {
            try {
                if ($cmd -eq "py") {
                    & py -3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" 2>$null
                } else {
                    & $resolved.Source -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" 2>$null
                }
                if ($LASTEXITCODE -eq 0) {
                    return $resolved.Source
                }
            } catch {}
        }
    }
    return $null
}

function Prompt-And-Install-Python {
    Write-Host ""
    Write-LogWarn "Python 3.10+ no está instalado en este equipo (requerido para los SDKs de nube)."

    if ($IsWindows -or ($env:OS -like "*Windows*")) {
        $wingetCmd = Get-Command winget -ErrorAction SilentlyContinue
        $chocoCmd = Get-Command choco -ErrorAction SilentlyContinue

        if ($wingetCmd) {
            Write-Host "  Gestor detectado: Windows Package Manager (winget)" -ForegroundColor Yellow
            $resp = Read-Host "¿Desea instalar Python 3.11 automáticamente mediante winget? [s/N]"
            if ($resp -match "^[sSyY]") {
                Write-LogInfo "Instalando Python 3.11 mediante winget..."
                & winget install --id Python.Python.3.11 --exact --source winget --accept-package-agreements --accept-source-agreements
                $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
                return $true
            }
        } elseif ($chocoCmd) {
            Write-Host "  Gestor detectado: Chocolatey" -ForegroundColor Yellow
            $resp = Read-Host "¿Desea instalar Python 3.11 mediante Chocolatey? [s/N]"
            if ($resp -match "^[sSyY]") {
                Write-LogInfo "Instalando Python 3.11 mediante choco..."
                & choco install python311 -y
                $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
                return $true
            }
        } else {
            $resp = Read-Host "¿Desea descargar e instalar Python 3.11 desde python.org? [s/N]"
            if ($resp -match "^[sSyY]") {
                $installerUrl = "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe"
                $installerPath = Join-Path $env:TEMP "python-3.11.9-installer.exe"
                Write-LogInfo "Descargando instalador de Python 3.11..."
                Invoke-WebRequest -Uri $installerUrl -OutFile $installerPath
                Write-LogInfo "Iniciando instalación silenciosa de Python 3.11..."
                Start-Process -FilePath $installerPath -ArgumentList "/quiet InstallAllUsers=1 PrependPath=1 Include_pip=1" -Wait
                Remove-Item $installerPath -Force -ErrorAction SilentlyContinue
                $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
                return $true
            }
        }
    } else {
        $brewCmd = Get-Command brew -ErrorAction SilentlyContinue
        $aptCmd = Get-Command apt-get -ErrorAction SilentlyContinue
        $dnfCmd = Get-Command dnf -ErrorAction SilentlyContinue

        if ($brewCmd) {
            $resp = Read-Host "¿Desea instalar Python 3.11 mediante Homebrew (brew install python@3.11)? [s/N]"
            if ($resp -match "^[sSyY]") {
                & brew install python@3.11
                return $true
            }
        } elseif ($aptCmd) {
            $resp = Read-Host "¿Desea instalar Python 3.11 mediante apt-get? [s/N]"
            if ($resp -match "^[sSyY]") {
                & sudo apt-get update
                & sudo apt-get install -y python3.11 python3.11-venv python3.11-dev
                return $true
            }
        } elseif ($dnfCmd) {
            $resp = Read-Host "¿Desea instalar Python 3.11 mediante dnf? [s/N]"
            if ($resp -match "^[sSyY]") {
                & sudo dnf install -y python3.11 python3.11-pip
                return $true
            }
        }
    }

    return $false
}

# ------------------------------------------------------------------------------
# 2. Configuración y preparación del entorno virtual (.venv)
# ------------------------------------------------------------------------------
function Setup-VirtualEnv {
    if (-not (Test-Path $VenvPython)) {
        Write-LogInfo "Buscando intérprete Python compatible (requerido Python 3.10+)..."
        $pyBin = Find-CompatiblePython
        if (-not $pyBin) {
            if (Prompt-And-Install-Python) {
                $pyBin = Find-CompatiblePython
            }
        }

        if (-not $pyBin) {
            Write-LogError "No se encontró ningún intérprete Python 3.10 o superior disponible en el sistema."
            $defaultPy = Get-Command python -ErrorAction SilentlyContinue
            if (-not $defaultPy) { $defaultPy = Get-Command python3 -ErrorAction SilentlyContinue }
            if ($defaultPy) {
                try {
                    $ver = & $defaultPy.Source -c "import sys; print('.'.join(map(str, sys.version_info[:3])))" 2>$null
                    Write-LogError "Versión de Python detectada: $ver"
                } catch {}
            }
            Write-LogError "Por favor instale Python 3.10 o superior (https://www.python.org/downloads/)."
            exit 1
        }

        Write-LogInfo "Creando entorno virtual en '$VenvDir' con $pyBin..."
        try {
            & $pyBin -m venv $VenvDir
            if ($LASTEXITCODE -ne 0) {
                Write-LogError "Error al crear el entorno virtual con: $pyBin -m venv $VenvDir"
                exit 1
            }
            Write-LogOk "Entorno virtual creado exitosamente."
        } catch {
            Write-LogError "Fallo al ejecutar la creación del venv: $_"
            exit 1
        }
    }

    # Verificar si es necesario instalar o actualizar dependencias de requirements.txt
    $needsInstall = $false
    if (-not (Test-Path $StampFile)) {
        $needsInstall = $true
    } elseif (Test-Path $ReqFile) {
        $reqTime = (Get-Item $ReqFile).LastWriteTimeUtc
        $stampTime = (Get-Item $StampFile).LastWriteTimeUtc
        if ($reqTime -gt $stampTime) {
            $needsInstall = $true
        }
    }

    if ($needsInstall) {
        Write-LogInfo "Instalando / actualizando dependencias en el entorno virtual (.venv)..."
        try {
            & $VenvPython -m pip install --upgrade pip --quiet 2>$null
            if (Test-Path $ReqFile) {
                & $VenvPython -m pip install -r $ReqFile
                if ($LASTEXITCODE -ne 0) {
                    Write-LogError "Error al instalar las dependencias desde $ReqFile"
                    exit 1
                }
            } else {
                Write-LogError "No se encontró el archivo $ReqFile"
                exit 1
            }

            Set-Content -Path $StampFile -Value (Get-Date).ToString("o")
            Write-LogOk "Dependencias instaladas y listas."
        } catch {
            Write-LogError "Fallo durante la instalación de dependencias: $_"
            exit 1
        }
    }
}

# ------------------------------------------------------------------------------
# 3. Validaciones y configuración de alcance según el proveedor y Cloud Shell
# ------------------------------------------------------------------------------
function Prepare-AzureEnv {
    $isHelp = $false
    $hasScope = $false
    foreach ($arg in $script:ExtraArgs) {
        if ($arg -in "-h", "--help") { $isHelp = $true }
        if ($arg -match "^(--subscription|--all-subscriptions|--management-group)$") {
            $hasScope = $true
        }
    }

    if ($isHelp) { return }

    # Para Azure: escanear el tenant completo con todas las suscripciones por defecto
    if (-not $hasScope) {
        Write-LogInfo "[Azure] Modo tenant: escaneando todas las suscripciones del tenant (--all-subscriptions)..."
        $script:ExtraArgs += "--all-subscriptions"
    }

    if ([string]::IsNullOrEmpty($env:AZURE_CLIENT_ID) -and [string]::IsNullOrEmpty($env:AZURE_SUBSCRIPTION_ID)) {
        $azCmd = Get-Command az -ErrorAction SilentlyContinue
        if (-not $azCmd -and [string]::IsNullOrEmpty($env:IDENTITY_ENDPOINT)) {
            Write-LogWarn "[Azure] No se detectó inicio de sesión en Azure CLI ni Managed Identity."
            Write-LogWarn "[Azure] Ejecute 'az login' previamente si no está en Azure Cloud Shell."
        }
    }
}

function Prepare-AwsEnv {
    $isHelp = $false
    $hasAccountSelection = $false
    foreach ($arg in $script:ExtraArgs) {
        if ($arg -in "-h", "--help") { $isHelp = $true }
        if ($arg -match "^(--all-accounts|--account-id|--scan-profiles)$") {
            $hasAccountSelection = $true
        }
    }

    if ($isHelp) { return }

    $awsDir = Join-Path $HOME ".aws"
    $hasCreds = (-not [string]::IsNullOrEmpty($env:AWS_ACCESS_KEY_ID)) -or `
                (-not [string]::IsNullOrEmpty($env:AWS_PROFILE)) -or `
                (Test-Path $awsDir) -or `
                (-not [string]::IsNullOrEmpty($env:AWS_CONTAINER_CREDENTIALS_RELATIVE_URI)) -or `
                (-not [string]::IsNullOrEmpty($env:AWS_EXECUTION_ENV))

    if (-not $hasCreds) {
        Write-LogWarn "[AWS] No se detectaron credenciales en variables de entorno ni en $HOME/.aws."
        Write-LogWarn "[AWS] Asegúrese de configurar 'aws configure', 'aws sso login' o pasar --profile."
    }

    # Si no se indicó selección explícita de cuentas, verificar si hay AWS Organization activa
    if (-not $hasAccountSelection) {
        Write-LogInfo "[AWS] Verificando si existe AWS Organization activa para escanear todas las cuentas..."
        $pyCheck = @"
import sys, boto3
profile = None
args = sys.argv[1:]
for i, a in enumerate(args):
    if a == '--profile' and i + 1 < len(args):
        profile = args[i + 1]
try:
    s = boto3.Session(profile_name=profile) if profile else boto3.Session()
    client = s.client('organizations')
    client.describe_organization()
    paginator = client.get_paginator('list_accounts')
    for page in paginator.paginate():
        break
    print('ORG_OK')
except Exception:
    print('NO_ORG')
"@
        try {
            $orgStatus = & $VenvPython -c $pyCheck @($script:ExtraArgs) 2>$null
            if ($orgStatus -and ($orgStatus.Trim() -eq "ORG_OK")) {
                Write-LogOk "[AWS] Se detectó AWS Organization con acceso a cuentas. Escaneando todas las cuentas (--all-accounts)..."
                $script:ExtraArgs += "--all-accounts"
            } else {
                Write-LogInfo "[AWS] No se detectó AWS Organization o no hay permisos suficientes. Escaneando únicamente la cuenta actual..."
            }
        } catch {
            Write-LogInfo "[AWS] Escaneando únicamente la cuenta actual..."
        }
    }
}

function Prepare-GcpEnv {
    $isHelp = $false
    $hasScope = $false
    foreach ($arg in $script:ExtraArgs) {
        if ($arg -in "-h", "--help") { $isHelp = $true }
        if ($arg -eq "--scope") { $hasScope = $true }
    }

    # Para GCP: enfoque en un proyecto individual (por defecto)
    $currentProj = $env:GOOGLE_CLOUD_PROJECT
    if (-not $currentProj) { $currentProj = $env:GCLOUD_PROJECT }
    if (-not $currentProj) {
        $gcloudCmd = Get-Command gcloud -ErrorAction SilentlyContinue
        if ($gcloudCmd) {
            $gcpProj = & gcloud config get-value project 2>$null
            if ($gcpProj -and $gcpProj -ne "(unset)") {
                $env:GOOGLE_CLOUD_PROJECT = $gcpProj.Trim()
                $currentProj = $env:GOOGLE_CLOUD_PROJECT
            }
        }
    }

    if (-not $isHelp -and -not $hasScope) {
        if ($currentProj) {
            Write-LogInfo "[GCP] Modo proyecto único: Escaneando proyecto activo '$currentProj'..."
        } else {
            Write-LogInfo "[GCP] Modo proyecto único: Escaneando proyecto determinado por Application Default Credentials (ADC)..."
        }
    }

    $adcFile = [System.IO.Path]::Combine($HOME, ".config", "gcloud", "application_default_credentials.json")
    if ([string]::IsNullOrEmpty($env:GOOGLE_APPLICATION_CREDENTIALS) -and -not (Test-Path $adcFile)) {
        if ([string]::IsNullOrEmpty($env:CLOUD_SHELL)) {
            Write-LogWarn "[GCP] No se detectaron credenciales de aplicación por defecto (ADC)."
            Write-LogWarn "[GCP] Si el script lo requiere, ejecute previamente: gcloud auth application-default login"
        }
    }
}

# ------------------------------------------------------------------------------
# 4. Procesamiento de argumentos
# ------------------------------------------------------------------------------
if ([string]::IsNullOrEmpty($Provider)) {
    Show-Help
    exit 1
}

$NormalizedProvider = $Provider.ToLower()

switch ($NormalizedProvider) {
    { $_ -in "-h", "--help", "help" } {
        Show-Help
        exit 0
    }
    "azure" {
        $TargetScript  = "count_azure_resources.py"
        $OutputDefault = "azure_resource_counts.json"
    }
    "aws" {
        $TargetScript  = "count_aws_resources.py"
        $OutputDefault = "aws_resource_counts.json"
    }
    "gcp" {
        $TargetScript  = "count_gcp_resources.py"
        $OutputDefault = "gcp_resource_counts.json"
    }
    "calculate" {
        $TargetScript  = "calculate_billable_resources.py"
        $OutputDefault = ""
    }
    default {
        Write-LogError "Proveedor desconocido: '$Provider'"
        Write-Host ""
        Show-Help
        exit 1
    }
}

# ------------------------------------------------------------------------------
# 5. Inicialización de entorno e invocación
# ------------------------------------------------------------------------------
Setup-VirtualEnv

switch ($NormalizedProvider) {
    "azure" { Prepare-AzureEnv }
    "aws"   { Prepare-AwsEnv }
    "gcp"   { Prepare-GcpEnv }
}

$TargetScriptPath = Join-Path $ScriptDir $TargetScript

Write-Host ""
Write-LogInfo "Iniciando análisis para: $($NormalizedProvider.ToUpper())..."
Write-LogInfo "Ejecutando: python $TargetScript $($ExtraArgs -join ' ')"
Write-Host "--------------------------------------------------------------------------------"

# Ejecutar el script objetivo con sus argumentos
try {
    & $VenvPython $TargetScriptPath @ExtraArgs
    $scriptExit = $LASTEXITCODE
} catch {
    Write-LogError "Error durante la ejecución del script: $_"
    exit 1
}

Write-Host "--------------------------------------------------------------------------------"

if ($scriptExit -eq 0) {
    Write-LogOk "Ejecución finalizada con éxito."
    if ($OutputDefault) {
        Write-Host ""
        Write-LogInfo "Archivo de resultados generado: $OutputDefault"
        Write-LogInfo "Para calcular los recursos facturables de FortiCNAPP, ejecute:"
        Write-Host "  .\run_inventory.ps1 calculate $OutputDefault" -ForegroundColor White
    }
} elseif ($scriptExit -eq 2 -and $NormalizedProvider -eq "aws") {
    Write-LogWarn "Ejecución finalizada con código 2 (algunas cuentas no pudieron escanearse; conteo parcial)."
    if ($OutputDefault) {
        Write-LogInfo "Archivo de resultados generado: $OutputDefault"
    }
} else {
    Write-LogError "La ejecución finalizó con errores (código de salida: $scriptExit)."
}

exit $scriptExit
