# Proyecto de Enriquecimiento de Datos con Big Data

Este repositorio contiene un flujo completo de procesamiento de datos utilizando Python, SQLite y GitHub Actions. A lo largo del proyecto, se han llevado a cabo los siguientes pasos:

1. **Ingesta de Datos:** Obtención de datos desde la API RandomUser y almacenamiento en SQLite.
2. **Limpieza de Datos:** Eliminación de duplicados, manejo de valores nulos y normalización.
3. **Enriquecimiento de Datos:** Integración de nuevas fuentes de datos para mejorar la calidad del dataset.
4. **Automatización con GitHub Actions:** Ejecución automatizada del pipeline de procesamiento y generación de reportes.

## Instalación y Configuración

### Requisitos Previos
- Python 3.8+
- SQLite3
- Pandas
- Git

### Clonar el Repositorio
```sh
 git clone https://github.com/LuisIUD/bigdata20251_actividad1-1.git
 cd bigdata20251_actividad1-1
```

### Instalación de Dependencias
```sh
pip install -r requirements.txt
```

## Ejecución Manual

### 1. Ejecutar el Proceso de Ingesta
```sh
python src/bigdata/ingesta.py
```

### 2. Ejecutar el Proceso de Limpieza
```sh
python src/bigdata/cleaning.py
```

### 3. Ejecutar el Proceso de Enriquecimiento
```sh
python src/bigdata/transform.py
```

## Automatización con GitHub Actions

El repositorio cuenta con un workflow de GitHub Actions que ejecuta automáticamente el procesamiento de datos cada vez que se realiza un `push`.

### Flujo de Trabajo Automatizado
El archivo `.github/workflows/bigdata.yml` está configurado para:
1. Configurar el entorno y las dependencias.
2. Ejecutar los scripts de ingesta, limpieza y enriquecimiento de datos.
3. Generar y almacenar los resultados.
4. Registrar logs y reportes en la carpeta `src/bigdata/static/auditoria/`.

## Resultados y Evidencias
- **Base de Datos:** `src/bigdata/static/db/user_data.db`
- **Dataset Limpio:** `src/xlsx/cleaned_data.csv`
- **Dataset Enriquecido:** `src/xlsx/enriched_data.csv`
- **Reporte de Auditoría:** `src/bigdata/static/auditoria/enriched_report.txt`

## Contribuciones
Las contribuciones son bienvenidas. Para proponer mejoras, realiza un fork del repositorio y crea un pull request.

## Licencia
Este proyecto se distribuye bajo la licencia MIT.

