#!/usr/bin/env bash
# ==============================================================================
# FortiCNAPP Resource Inventory & Sizing Script (Unified Wrapper)
# Soporte para: Azure, AWS, GCP
# Compatible con: PC local y Cloud Shells (Azure, AWS, Google Cloud Shell)
# ==============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"
REQ_FILE="${SCRIPT_DIR}/requirements.txt"
STAMP_FILE="${VENV_DIR}/.requirements_installed"

# Colores para salida interactiva (portables en Bash 3.2+ y terminales POSIX)
if [ -t 1 ]; then
    RED=$(printf '\033[0;31m')
    GREEN=$(printf '\033[0;32m')
    YELLOW=$(printf '\033[1;33m')
    BLUE=$(printf '\033[0;34m')
    CYAN=$(printf '\033[0;36m')
    BOLD=$(printf '\033[1m')
    NC=$(printf '\033[0m')
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    CYAN=''
    BOLD=''
    NC=''
fi

log_info() {
    echo -e "${CYAN}[INFO]${NC} $1"
}

log_ok() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[AVISO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

show_help() {
    cat << EOF
${BOLD}FortiCNAPP Resource Inventory & Sizing CLI${NC}

Ejecuta el análisis de inventario para FortiCNAPP en AWS, Azure o GCP.
Instala automáticamente dependencias en un entorno virtual aislado (.venv)
y utiliza las credenciales locales del PC o del Cloud Shell respectivo.

${BOLD}USO:${NC}
  $0 <proveedor> [opciones adicionales del script]

${BOLD}PROVEEDORES DISPONIBLES:${NC}
  ${BOLD}azure${NC}       Ejecuta el escaneo de recursos en Microsoft Azure
  ${BOLD}aws${NC}         Ejecuta el escaneo de recursos en Amazon Web Services
  ${BOLD}gcp${NC}         Ejecuta el escaneo de recursos en Google Cloud Platform
  ${BOLD}calculate${NC}   Calcula los recursos facturables a partir de los JSON generados

${BOLD}DETALLES DE AUTENTICACIÓN Y ALCANCE POR NUBE:${NC}

  ${CYAN}1. Azure (count_azure_resources.py):${NC}
     - ${BOLD}Credenciales:${NC} Usa DefaultAzureCredential (Azure CLI 'az login', Managed Identity
       en Azure Cloud Shell, o variables AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, etc.).
     - ${BOLD}Alcance por defecto:${NC} Todo el tenant con todas las suscripciones habilitadas (--all-subscriptions).
     - ${BOLD}Opciones de alcance:${NC}
         --subscription <ID...>      Limita el escaneo a una o varias suscripciones específicas
         --management-group <MG_ID>  Escanea recursivamente bajo un grupo de administración
         --encoded-output            Imprime bloque gzip+base64 para copiar y pegar fácilmente

  ${CYAN}2. AWS (count_aws_resources.py):${NC}
     - ${BOLD}Credenciales:${NC} Cadena estándar de AWS (variables de entorno, ~/.aws/credentials,
       SSO en ~/.aws/config, rol IAM de instancia o contenedor en AWS CloudShell).
     - ${BOLD}Alcance por defecto:${NC} Detecta automáticamente si hay una AWS Organization activa
       con permisos para escanear todas las cuentas (--all-accounts). Si no, escanea la cuenta actual.
     - ${BOLD}Opciones de alcance:${NC}
         --profile <nombre>          Usa un perfil específico de ~/.aws/config (ej. perfil SSO)
         --account-id <ID...>        Asume --role-name en las cuentas indicadas
         --scan-profiles <P1...>     Escanea múltiples perfiles CLI independientes
         --encoded-output            Imprime bloque gzip+base64 para copiar y pegar fácilmente

  ${CYAN}3. GCP (count_gcp_resources.py):${NC}
     - ${BOLD}Credenciales:${NC} Application Default Credentials (ADC) ('gcloud auth application-default login',
       GOOGLE_APPLICATION_CREDENTIALS, o credenciales automáticas en Google Cloud Shell).
     - ${BOLD}Alcance por defecto:${NC} Proyecto activo individual (GOOGLE_CLOUD_PROJECT o gcloud config).
     - ${BOLD}Opciones de alcance:${NC}
         --scope organizations/<ID>  Escanea toda la organización en Cloud Asset Inventory
         --scope folders/<ID>        Escanea una carpeta completa
         --no-smoothing              Conteo en tiempo real sin promedio histórico (más rápido)
         --encoded-output            Imprime bloque gzip+base64 para copiar y pegar fácilmente

${BOLD}EJEMPLOS DE USO:${NC}
  $0 azure
  $0 azure --all-subscriptions --encoded-output
  $0 aws --all-accounts
  $0 aws --profile my-sso-profile
  $0 gcp --scope organizations/1234567890
  $0 calculate aws_resource_counts.json azure_resource_counts.json

EOF
}

# ------------------------------------------------------------------------------
# 1. Detección, validación y auto-instalación interactiva de Python (>= 3.10)
# ------------------------------------------------------------------------------
find_compatible_python() {
    local candidates=("python3.13" "python3.12" "python3.11" "python3.10" "python3" "python")
    for cmd in "${candidates[@]}"; do
        if command -v "$cmd" >/dev/null 2>&1; then
            if "$cmd" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' >/dev/null 2>&1; then
                echo "$cmd"
                return 0
            fi
        fi
    done
    return 1
}

prompt_and_install_python() {
    local pkg_mgr=""
    local install_cmd=""
    local sudo_cmd=""

    if [ "$(id -u)" -ne 0 ]; then
        if command -v sudo >/dev/null 2>&1; then
            sudo_cmd="sudo "
        fi
    fi

    if [[ "${OSTYPE:-}" == "darwin"* ]]; then
        if command -v brew >/dev/null 2>&1; then
            pkg_mgr="Homebrew (macOS)"
            install_cmd="brew install python@3.11"
        fi
    elif [ -f /etc/debian_version ] || command -v apt-get >/dev/null 2>&1; then
        pkg_mgr="apt (Debian/Ubuntu)"
        install_cmd="${sudo_cmd}apt-get update && ${sudo_cmd}apt-get install -y python3.11 python3.11-venv python3.11-dev 2>/dev/null || ${sudo_cmd}apt-get install -y python3 python3-venv python3-pip"
    elif command -v dnf >/dev/null 2>&1; then
        pkg_mgr="dnf (Fedora/RHEL/AL2023)"
        install_cmd="${sudo_cmd}dnf install -y python3.11 python3.11-pip 2>/dev/null || ${sudo_cmd}dnf install -y python3 python3-pip"
    elif command -v yum >/dev/null 2>&1; then
        pkg_mgr="yum (CentOS/RHEL)"
        install_cmd="${sudo_cmd}yum install -y python3.11 python3.11-pip 2>/dev/null || ${sudo_cmd}yum install -y python3 python3-pip"
    elif command -v pacman >/dev/null 2>&1; then
        pkg_mgr="pacman (Arch Linux)"
        install_cmd="${sudo_cmd}pacman -Sy --noconfirm python python-pip"
    elif command -v apk >/dev/null 2>&1; then
        pkg_mgr="apk (Alpine Linux)"
        install_cmd="${sudo_cmd}apk add --no-cache python3 py3-pip"
    elif command -v zypper >/dev/null 2>&1; then
        pkg_mgr="zypper (openSUSE/SLES)"
        install_cmd="${sudo_cmd}zypper install -y python311 python311-pip"
    fi

    if [ -z "$install_cmd" ]; then
        log_warn "No se identificó un gestor de paquetes automático compatible para instalar Python."
        return 1
    fi

    echo ""
    log_warn "Python 3.10+ no está instalado en este equipo (requerido para los SDKs de nube)."
    echo -e "  ${BOLD}Gestor detectado:${NC} ${pkg_mgr}"
    echo -e "  ${BOLD}Comando a ejecutar:${NC} ${install_cmd}"
    echo ""

    if [ ! -t 0 ]; then
        log_error "Entorno no interactivo; no se puede solicitar confirmación para la instalación."
        return 1
    fi

    echo -n "¿Desea ejecutar la instalación automática de Python ahora? [s/N]: "
    local confirm
    read -r confirm
    case "$confirm" in
        [sSyY]|[sS][iI])
            log_info "Ejecutando instalación..."
            if bash -c "$install_cmd"; then
                log_ok "Instalación de Python completada."
                export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
                return 0
            else
                log_error "La instalación automática no se completó correctamente."
                return 1
            fi
            ;;
        *)
            log_info "Instalación cancelada por el usuario."
            return 1
            ;;
    esac
}

# ------------------------------------------------------------------------------
# 2. Configuración y preparación del entorno virtual (.venv)
# ------------------------------------------------------------------------------
setup_virtualenv() {
    if [ ! -f "${VENV_DIR}/bin/python" ]; then
        log_info "Buscando intérprete Python compatible (requerido Python 3.10+)..."
        local py_bin
        if ! py_bin=$(find_compatible_python); then
            # Intentar asistente interactivo de instalación
            if prompt_and_install_python; then
                py_bin=$(find_compatible_python || true)
            fi
        fi

        if [ -z "${py_bin:-}" ]; then
            log_error "No se encontró ningún intérprete Python 3.10 o superior disponible en el sistema."
            if command -v python3 >/dev/null 2>&1; then
                local current_ver
                current_ver=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:3])))' 2>/dev/null || echo "desconocida")
                log_error "Versión actual instalada: python3 (${current_ver})"
            fi
            log_error "Instale Python 3.10+ manualmente (ej. en Mac: 'brew install python@3.11', en Ubuntu: 'sudo apt install python3.11 python3.11-venv')."
            exit 1
        fi

        log_info "Creando entorno virtual en: ${VENV_DIR} con ${py_bin}..."
        if ! "$py_bin" -m venv "${VENV_DIR}" 2>/dev/null; then
            # Si falla en Debian/Ubuntu por falta de python3-venv, ofrecer instalarlo
            if command -v apt-get >/dev/null 2>&1 && [ -t 0 ]; then
                echo ""
                log_warn "La creación del entorno virtual falló. Es probable que falte el paquete 'python3-venv'."
                echo -n "¿Desea instalar 'python3-venv' automáticamente mediante apt? [s/N]: "
                local confirm_venv
                read -r confirm_venv
                case "$confirm_venv" in
                    [sSyY]|[sS][iI])
                        local sudo_cmd=""
                        [ "$(id -u)" -ne 0 ] && sudo_cmd="sudo "
                        ${sudo_cmd}apt-get update && ${sudo_cmd}apt-get install -y python3-venv
                        "$py_bin" -m venv "${VENV_DIR}"
                        ;;
                    *)
                        exit 1
                        ;;
                esac
            else
                log_error "Error al crear el entorno virtual con '${py_bin} -m venv ${VENV_DIR}'."
                log_error "En sistemas basados en Debian/Ubuntu, instale: sudo apt-get install python3-venv"
                exit 1
            fi
        fi
        log_ok "Entorno virtual creado exitosamente."
    fi

    # Verificar si es necesario instalar o actualizar dependencias de requirements.txt
    local needs_install=0
    if [ ! -f "${STAMP_FILE}" ]; then
        needs_install=1
    elif [ -f "${REQ_FILE}" ] && [ "${REQ_FILE}" -nt "${STAMP_FILE}" ]; then
        needs_install=1
    fi

    if [ "$needs_install" -eq 1 ]; then
        log_info "Instalando / actualizando dependencias en el entorno virtual (.venv)..."
        "${VENV_DIR}/bin/python" -m pip install --upgrade pip --quiet >/dev/null 2>&1 || true

        if [ -f "${REQ_FILE}" ]; then
            if ! "${VENV_DIR}/bin/python" -m pip install -r "${REQ_FILE}"; then
                log_error "Error al instalar las dependencias de ${REQ_FILE}."
                exit 1
            fi
        else
            log_error "No se encontró el archivo ${REQ_FILE}."
            exit 1
        fi

        touch "${STAMP_FILE}"
        log_ok "Dependencias instaladas y listas."
    fi
}

# ------------------------------------------------------------------------------
# 3. Validaciones y configuración de alcance según el proveedor y Cloud Shell
# ------------------------------------------------------------------------------
prepare_azure_environment() {
    local is_help=0
    local has_scope=0
    for arg in "${EXTRA_ARGS[@]}"; do
        case "$arg" in
            -h|--help)
                is_help=1
                ;;
            --subscription|--all-subscriptions|--management-group)
                has_scope=1
                ;;
        esac
    done

    if [ "$is_help" -eq 1 ]; then
        return 0
    fi

    # Para Azure: escanear el tenant completo con todas las suscripciones por defecto
    if [ "$has_scope" -eq 0 ]; then
        log_info "[Azure] Modo tenant: escaneando todas las suscripciones del tenant (--all-subscriptions)..."
        EXTRA_ARGS+=("--all-subscriptions")
    fi

    # Comprobación informativa de credenciales
    if [ -z "${AZURE_CLIENT_ID:-}" ] && [ -z "${AZURE_SUBSCRIPTION_ID:-}" ]; then
        if ! command -v az >/dev/null 2>&1 && [ -z "${IDENTITY_ENDPOINT:-}" ]; then
            log_warn "[Azure] No se detectó inicio de sesión en Azure CLI ni Managed Identity."
            log_warn "[Azure] Ejecute 'az login' previamente si no está en Azure Cloud Shell."
        fi
    fi
}

prepare_aws_environment() {
    local is_help=0
    local has_account_selection=0
    for arg in "${EXTRA_ARGS[@]}"; do
        case "$arg" in
            -h|--help)
                is_help=1
                ;;
            --all-accounts|--account-id|--scan-profiles)
                has_account_selection=1
                ;;
        esac
    done

    if [ "$is_help" -eq 1 ]; then
        return 0
    fi

    # En AWS CloudShell o EC2 se dispone de metadatos o variables de contenedor.
    # En PC local se verifica ~/.aws o variables de entorno.
    if [ -z "${AWS_ACCESS_KEY_ID:-}" ] && [ -z "${AWS_PROFILE:-}" ] && [ ! -d "${HOME}/.aws" ]; then
        if [ -z "${AWS_CONTAINER_CREDENTIALS_RELATIVE_URI:-}" ] && [ -z "${AWS_EXECUTION_ENV:-}" ]; then
            log_warn "[AWS] No se detectaron credenciales en variables de entorno ni ~/.aws."
            log_warn "[AWS] Asegúrese de haber configurado 'aws configure', 'aws sso login' o pasar --profile."
        fi
    fi

    # Para AWS: si no se indicó selección explícita de cuentas, verificar si hay AWS Organization activa
    if [ "$has_account_selection" -eq 0 ]; then
        log_info "[AWS] Verificando si existe AWS Organization activa para escanear todas las cuentas..."
        local org_status
        org_status=$("${VENV_DIR}/bin/python" -c "
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
" "${EXTRA_ARGS[@]}" 2>/dev/null || echo "NO_ORG")

        if [ "$org_status" = "ORG_OK" ]; then
            log_ok "[AWS] Se detectó AWS Organization con acceso a cuentas. Escaneando todas las cuentas (--all-accounts)..."
            EXTRA_ARGS+=("--all-accounts")
        else
            log_info "[AWS] No se detectó AWS Organization o no hay permisos suficientes. Escaneando únicamente la cuenta actual..."
        fi
    fi
}

prepare_gcp_environment() {
    local is_help=0
    local has_scope=0
    for arg in "${EXTRA_ARGS[@]}"; do
        case "$arg" in
            -h|--help)
                is_help=1
                ;;
            --scope)
                has_scope=1
                ;;
        esac
    done

    # Para GCP: enfoque en un proyecto individual (por defecto)
    local current_proj="${GOOGLE_CLOUD_PROJECT:-${GCLOUD_PROJECT:-}}"
    if [ -z "$current_proj" ]; then
        if command -v gcloud >/dev/null 2>&1; then
            local gcp_proj
            gcp_proj=$(gcloud config get-value project 2>/dev/null || true)
            if [ -n "$gcp_proj" ] && [ "$gcp_proj" != "(unset)" ]; then
                export GOOGLE_CLOUD_PROJECT="$gcp_proj"
                current_proj="$gcp_proj"
            fi
        fi
    fi

    if [ "$is_help" -eq 0 ] && [ "$has_scope" -eq 0 ]; then
        if [ -n "$current_proj" ]; then
            log_info "[GCP] Modo proyecto único: Escaneando proyecto activo '${BOLD}${current_proj}${NC}'..."
        else
            log_info "[GCP] Modo proyecto único: Escaneando proyecto determinado por Application Default Credentials (ADC)..."
        fi
    fi

    # Comprobación de credenciales ADC en GCP
    local adc_file="${HOME}/.config/gcloud/application_default_credentials.json"
    if [ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ] && [ ! -f "$adc_file" ]; then
        if [ -z "${CLOUD_SHELL:-}" ]; then
            log_warn "[GCP] No se detectaron credenciales de aplicación por defecto (ADC)."
            log_warn "[GCP] Si el script lo requiere, ejecute previamente: gcloud auth application-default login"
        fi
    fi
}

# ------------------------------------------------------------------------------
# 4. Procesamiento de argumentos principales
# ------------------------------------------------------------------------------
if [ $# -eq 0 ]; then
    show_help
    exit 1
fi

PROVIDER_INPUT="$1"
shift

PROVIDER="$(echo "$PROVIDER_INPUT" | tr '[:upper:]' '[:lower:]')"

case "$PROVIDER" in
    -h|--help|help)
        show_help
        exit 0
        ;;
    azure)
        TARGET_SCRIPT="count_azure_resources.py"
        OUTPUT_DEFAULT="azure_resource_counts.json"
        ;;
    aws)
        TARGET_SCRIPT="count_aws_resources.py"
        OUTPUT_DEFAULT="aws_resource_counts.json"
        ;;
    gcp)
        TARGET_SCRIPT="count_gcp_resources.py"
        OUTPUT_DEFAULT="gcp_resource_counts.json"
        ;;
    calculate)
        TARGET_SCRIPT="calculate_billable_resources.py"
        OUTPUT_DEFAULT=""
        ;;
    *)
        log_error "Proveedor desconocido: '${PROVIDER_INPUT}'"
        echo ""
        show_help
        exit 1
        ;;
esac

# ------------------------------------------------------------------------------
# 5. Inicialización de entorno e invocación
# ------------------------------------------------------------------------------
setup_virtualenv

EXTRA_ARGS=("$@")

# Preparación específica del proveedor y configuración de alcance
case "$PROVIDER" in
    azure)
        prepare_azure_environment
        ;;
    aws)
        prepare_aws_environment
        ;;
    gcp)
        prepare_gcp_environment
        ;;
esac

echo ""
PROVIDER_UPPER=$(echo "$PROVIDER" | tr '[:lower:]' '[:upper:]')
log_info "Iniciando análisis para: ${BOLD}${PROVIDER_UPPER}${NC}..."
log_info "Ejecutando: python ${TARGET_SCRIPT} ${EXTRA_ARGS[*]}"
echo "--------------------------------------------------------------------------------"

# Ejecutar el script dentro del entorno virtual pasando todos los argumentos adicionales
"${VENV_DIR}/bin/python" "${SCRIPT_DIR}/${TARGET_SCRIPT}" "${EXTRA_ARGS[@]}"
EXIT_CODE=$?

echo "--------------------------------------------------------------------------------"

if [ $EXIT_CODE -eq 0 ]; then
    log_ok "Ejecución finalizada con éxito."
    if [ -n "$OUTPUT_DEFAULT" ]; then
        echo ""
        log_info "Archivo de resultados generado: ${BOLD}${OUTPUT_DEFAULT}${NC}"
        log_info "Para calcular los recursos facturables de FortiCNAPP, ejecute:"
        echo -e "  ${BOLD}$0 calculate ${OUTPUT_DEFAULT}${NC}"
    fi
elif [ $EXIT_CODE -eq 2 ] && [ "$PROVIDER" = "aws" ]; then
    log_warn "Ejecución finalizada con código 2 (algunas cuentas no pudieron escanearse; conteo parcial)."
    if [ -n "$OUTPUT_DEFAULT" ]; then
        log_info "Archivo de resultados generado: ${BOLD}${OUTPUT_DEFAULT}${NC}"
    fi
else
    log_error "La ejecución finalizó con errores (código de salida: ${EXIT_CODE})."
fi

exit $EXIT_CODE
