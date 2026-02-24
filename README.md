# Weather ETL Pipeline with Prefect

Una pipeline completa de ETL para datos meteorológicos que obtiene pronósticos del clima y los almacena en MongoDB, orquestado con Prefect.

## 🏗️ Arquitectura

La pipeline sigue un patrón de 5 etapas:

1. **EXTRACT (Geocoding)** → Resuelve el nombre de la ciudad a coordenadas usando Open-Meteo
2. **EXTRACT (Forecast)** → Obtiene datos horarios de pronóstico meteorológico
3. **STAGE** → Convierte JSON a DataFrame de pandas con metadatos
4. **TRANSFORM** → Limpia datos, valida tipos, agrega features
5. **LOAD** → Upsert idempotente a MongoDB con índices únicos

## 📦 Características

- ✅ **Orquestación con Prefect** - Observabilidad, retries, scheduling
- ✅ **MongoDB upsert idempotente** - Sin duplicados, reutilizable
- ✅ **Validación de seguridad** - Verificación de country_code
- ✅ **TLS/SSL** - Conectividad segura a MongoDB Atlas
- ✅ **GitHub Actions** - Ejecución automática programada desde GitHub
- ✅ **Configuración por .env** - Manejo seguro de credenciales

## 🚀 Inicio Rápido

### Instalación Local

```bash
# Clonar repositorio
git clone https://github.com/pablitotechi/api.git
cd api

# Crear entorno virtual
python3 -m venv .venv
source .venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

### Configuración

Crear archivo `.env` con tus credenciales:

```env
MONGO_URI=mongodb+srv://user:password@cluster.mongodb.net/?retryWrites=true&w=majority
DB_NAME=clima_data
COLLECTION_NAME=clima_data
DEFAULT_CITY=San José
DEFAULT_COUNTRY_CODE=CR
DEFAULT_TIMEZONE=America/Costa_Rica
SCHEDULE_CRON=0 2 * * *
```

### Ejecutar la Pipeline

**Opción 1: Ejecución directa**

```bash
python weather_pipeline.py
```

**Opción 2: Con Prefect (con observabilidad)**

```bash
# Ejecutar una vez
python weather_pipeline_prefect.py

# O iniciar el dashboard
prefect server start  # En otra terminal

# Y ejecutar el flow
python weather_pipeline_prefect.py
```

**Opción 3: Desplegar con scheduling**

```bash
# Crear deployment
prefect deployment build weather_pipeline_prefect.py:weather_etl_scheduled \
  -n "weather-daily-cr" \
  -q "default" \
  --cron "0 2 * * *"

# Aplicar deployment
prefect deployment apply weather_etl_scheduled-deployment.yaml

# Iniciar agente
prefect agent start -q default
```

## 🔄 Ejecución desde GitHub

La pipeline se ejecuta automáticamente cada día a las 2:00 AM UTC mediante GitHub Actions.

### Configurar Secrets en GitHub

1. Ve a **Settings → Secrets and variables → Actions**
2. Agrega estos secrets:
   - `MONGO_URI` - URL de conexión a MongoDB
   - `DB_NAME` - Nombre de la base de datos
   - `COLLECTION_NAME` - Nombre de la colección
   - `DEFAULT_CITY` - Ciudad por defecto
   - `DEFAULT_COUNTRY_CODE` - Código de país
   - `DEFAULT_TIMEZONE` - Zona horaria IANA

### Ver Execuciones

- **GitHub Actions**: https://github.com/pablitotechi/api/actions
- **Prefect Dashboard**: http://127.0.0.1:4200 (local)

## 📊 API Externas

- **Open-Meteo Geocoding** - https://geocoding-api.open-meteo.com/v1/search
- **Open-Meteo Forecast** - https://api.open-meteo.com/v1/forecast
- **MongoDB Atlas** - Almacenamiento de datos

## 📁 Estructura de Archivos

```
.
├── weather_pipeline.py              # Core ETL (5 etapas)
├── weather_pipeline_prefect.py      # Orquestación con Prefect
├── mongo_test.py                    # Test de conectividad
├── deploy_github.py                 # Script de deployment
├── .env                             # Configuración (SECRETO)
├── .github/
│   ├── workflows/
│   │   └── weather-etl.yml         # GitHub Actions workflow
│   └── copilot-instructions.md     # Docs para AI agents
├── prefect.yaml                    # Config de Prefect
└── requirements.txt                # Dependencias Python
```

## 🔐 Patrones de Seguridad

- ✅ Nunca commiteamos `.env` (está en `.gitignore`)
- ✅ MONGO_URI y secretos en GitHub Secrets
- ✅ Validación estricta de country_code en geocoding
- ✅ TLS/SSL obligatorio para MongoDB
- ✅ Timeouts en todas las conexiones HTTP

## 📝 Parámetros Personalizables

```python
from weather_pipeline_prefect import weather_etl_flow

# Ejecutar para otra ciudad
result = weather_etl_flow(
    city="Madrid",
    country_code="ES",
    timezone_name="Europe/Madrid"
)
```

## 🧪 Testing

```bash
# Test de conectividad a MongoDB
python mongo_test.py

# Ejecución de prueba
python weather_pipeline.py
```

## 📚 Documentación Adicional

Ver [.github/copilot-instructions.md](.github/copilot-instructions.md) para:
- Arquitectura detallada
- Patrones de proyecto
- Flujos de desarrollo
- Puntos de integración

## 🤝 Contribuir

Los cambios se pueden pushear directamente. GitHub Actions ejecutará la validación automáticamente.

## 📄 Licencia

Este proyecto es de código abierto. Úsalo libremente.

---

**Última actualización:** 23 de febrero de 2026
