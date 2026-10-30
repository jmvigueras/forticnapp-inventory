# FortiCNAPP Cloud Resource Sizing Tool

Herramienta autocontenida para el análisis de inventario y dimensionamiento de licencias de **FortiCNAPP** en entornos multicloud (**Amazon Web Services**, **Microsoft Azure** y **Google Cloud Platform**).

---

## ¿Qué hace esta herramienta?

- **Inventario automatizado:** Consulta las APIs de gestión de la nube para contabilizar los recursos y servicios computables por FortiCNAPP (máquinas virtuales, imágenes, clústeres y contenedores, funciones serverless, bases de datos gestionadas, buckets de almacenamiento, almacenes de datos y discos).
- **Acceso exclusivo de solo lectura:** Solo consulta el plano de control mediante permisos estándar de lectura. No modifica ningún recurso, no accede al contenido de almacenamiento ni a datos sensibles, y no recopila información confidencial de clientes.
- **Autocontenida:** Gestiona automáticamente su propio entorno virtual aislado de Python (`.venv`) e instala las dependencias necesarias.
- **Detección interactiva de Python:** Si el equipo no dispone de Python 3.10+, identifica el gestor de paquetes del sistema (Homebrew, apt, dnf, winget, etc.) y ofrece instalarlo tras confirmación del usuario.
- **Multiplataforma y compatible con Cloud Shell:** Diseñada para ejecutarse tanto en estaciones de trabajo locales (Linux, macOS, Windows) como en las terminales web integradas de los proveedores (**AWS CloudShell**, **Azure Cloud Shell** y **Google Cloud Shell**) aprovechando la sesión activa.

---

## Requisitos previos

- **Credenciales activas** en el equipo o Cloud Shell donde se ejecute:
  - **Azure:** Sesión iniciada con Azure CLI (`az login`), credenciales de Azure Cloud Shell o variables de entorno (`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`).
  - **AWS:** Credenciales configuradas en `~/.aws/credentials`, perfil SSO en `~/.aws/config`, variables de entorno o sesión de AWS CloudShell.
  - **GCP:** Credenciales de aplicación por defecto (`gcloud auth application-default login`), variables de entorno o sesión de Google Cloud Shell.
- **Permisos mínimos de solo lectura:**
  - **Azure:** Roles `Reader` y `Resource Graph Reader` a nivel de suscripción o tenant/grupo de administración.
  - **AWS:** Política gestionada `ReadOnlyAccess` (y permisos de lectura en AWS Organizations si se escanea org-wide).
  - **GCP:** Rol `roles/cloudasset.viewer` sobre el proyecto o la organización.

---

## Uso rápido

Se proporcionan dos scripts ejecutores equivalentes: **`run_inventory.sh`** (Bash) y **`run_inventory.ps1`** (PowerShell).

### En Linux, macOS o Cloud Shells (Bash)

Dar permisos de ejecución la primera vez:
```bash
chmod +x run_inventory.sh
```

Ejecución por proveedor:
```bash
# Microsoft Azure (analiza todo el tenant por defecto)
./run_inventory.sh azure

# Amazon Web Services (detecta AWS Organization y escanea todas las cuentas por defecto)
./run_inventory.sh aws

# Google Cloud Platform (analiza el proyecto activo por defecto)
./run_inventory.sh gcp

# Calcular recursos facturables con el JSON resultante
./run_inventory.sh calculate aws_resource_counts.json azure_resource_counts.json
```

---

### En Windows, PowerShell Core o Azure Cloud Shell (PowerShell)

```powershell
# Microsoft Azure (analiza todo el tenant por defecto)
.\run_inventory.ps1 azure

# Amazon Web Services (detecta AWS Organization y escanea todas las cuentas por defecto)
.\run_inventory.ps1 aws

# Google Cloud Platform (analiza el proyecto activo por defecto)
.\run_inventory.ps1 gcp

# Calcular recursos facturables con el JSON resultante
.\run_inventory.ps1 calculate aws_resource_counts.json azure_resource_counts.json
```

---

## Alcance y opciones por nube

Los scripts configuran de forma automática el alcance óptimo para cada proveedor, pero permiten personalizar el escaneo pasando argumentos adicionales:

### 1. Microsoft Azure (`azure`)
- **Alcance por defecto:** Todo el tenant con todas las suscripciones habilitadas (`--all-subscriptions`).
- **Opciones frecuentes:**
  - Limitar a una o más suscripciones: `--subscription <ID_1> <ID_2>`
  - Escaneo por grupo de administración: `--management-group <MG_ID>`
  - Salida comprimida para copiar y pegar: `--encoded-output`

### 2. Amazon Web Services (`aws`)
- **Alcance por defecto:** Si la cuenta pertenece a una AWS Organization con permisos para listar cuentas, escanea automáticamente todas las cuentas activas (`--all-accounts`). Si no, escanea la cuenta actual.
- **Opciones frecuentes:**
  - Usar un perfil específico de `~/.aws/config`: `--profile <nombre_perfil>`
  - Escanear una o más cuentas asumiendo rol: `--account-id <ID_1> <ID_2> [--role-name <Rol>]`
  - Escanear perfiles independientes: `--scan-profiles <perfil1> <perfil2>`
  - Salida comprimida para copiar y pegar: `--encoded-output`

### 3. Google Cloud Platform (`gcp`)
- **Alcance por defecto:** Escaneo del proyecto activo individual configurado en `gcloud` o `GOOGLE_CLOUD_PROJECT`.
- **Opciones frecuentes:**
  - Escanear una organización completa: `--scope organizations/<ORG_ID>`
  - Escanear una carpeta: `--scope folders/<FOLDER_ID>`
  - Escaneo en tiempo real sin promedio histórico: `--no-smoothing`
  - Salida comprimida para copiar y pegar: `--encoded-output`

---

## Resultados y cálculo de dimensionamiento

1. Al finalizar, el script imprime por consola una tabla resumen de los recursos contabilizados y escribe un archivo JSON detallado:
   - Azure: `azure_resource_counts.json`
   - AWS: `aws_resource_counts.json`
   - GCP: `gcp_resource_counts.json`

2. Para convertir los recuentos en **unidades facturables de FortiCNAPP**, ejecuta la opción `calculate` pasando uno o más archivos generados:

```bash
./run_inventory.sh calculate aws_resource_counts.json azure_resource_counts.json gcp_resource_counts.json
```

El script solicitará (o admitirá mediante parámetros opcionales como `--deployed-agents` o `--developers`) los datos complementarios que no residen en la nube para generar el resumen final de licenciamiento.

---

## Licencia

Distribuido bajo la licencia [Apache 2.0](LICENSE).
