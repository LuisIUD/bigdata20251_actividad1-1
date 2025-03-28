import pandas as pd
import os
from datetime import datetime

# Rutas de archivos
CLEANED_DATA_PATH = "src/xlsx/cleaned_data.csv"
DATASET_PATH = "src/csv/dataset.csv"
ENRICHED_DATA_PATH = "src/xlsx/enriched_data.csv"
AUDIT_PATH = "src/bigdata/static/auditoria/enriched_report.txt"

# Crear directorios si no existen
os.makedirs(os.path.dirname(ENRICHED_DATA_PATH), exist_ok=True)
os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)

def cargar_dataset_base():
    """Carga el dataset base desde el archivo limpio."""
    return pd.read_csv(CLEANED_DATA_PATH)

def cargar_fuente_adicional():
    """Carga la fuente de datos adicional desde el archivo dataset.csv."""
    return pd.read_csv(DATASET_PATH)

def integrar_datasets(df_base, df_adicional):
    """Realiza la unión de los datasets."""
    df_enriched = pd.concat([df_base, df_adicional], ignore_index=True)
    return df_enriched

def generar_reporte(df_base, df_adicional, df_enriched):
    """Genera un reporte de auditoría con los cambios realizados."""
    with open(AUDIT_PATH, "w", encoding="utf-8") as f:
        f.write("=== REPORTE DE ENRIQUECIMIENTO ===\n")
        f.write(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"Registros en dataset base: {len(df_base)}\n")
        f.write(f"Registros en dataset adicional: {len(df_adicional)}\n")
        f.write(f"Total registros en dataset enriquecido: {len(df_enriched)}\n")

def guardar_datos(df):
    """Guarda el dataset enriquecido en un archivo CSV."""
    df.to_csv(ENRICHED_DATA_PATH, index=False)

def main():
    print("Cargando dataset base...")
    df_base = cargar_dataset_base()
    
    print("Cargando fuente adicional...")
    df_adicional = cargar_fuente_adicional()
    
    print("Integrando datasets...")
    df_enriched = integrar_datasets(df_base, df_adicional)
    
    print("Guardando dataset enriquecido...")
    guardar_datos(df_enriched)
    
    print("Generando reporte de auditoría...")
    generar_reporte(df_base, df_adicional, df_enriched)
    
    print("Proceso de enriquecimiento completado.")

if __name__ == "__main__":
    main()
