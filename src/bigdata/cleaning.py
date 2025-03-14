import sqlite3
import pandas as pd
import os
from datetime import datetime

# Rutas de archivos
DB_PATH = "src/bigdata/static/db/user_data.db"
CLEANED_DATA_PATH = "src/xlsx/cleaned_data.csv"
AUDIT_PATH = "src/static/auditoria/cleaning_report.txt"

# Crear directorios si no existen
os.makedirs(os.path.dirname(CLEANED_DATA_PATH), exist_ok=True)
os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)

def cargar_datos():
    """Carga los datos desde la base de datos local."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM usuarios", conn)
    conn.close()
    return df

def analizar_datos(df):
    """Realiza un análisis exploratorio de los datos y retorna un resumen."""
    resumen = {
        "total_registros": len(df),
        "duplicados": df.duplicated().sum(),
        "valores_nulos": df.isnull().sum().to_dict(),
        "tipos_de_datos": df.dtypes.to_dict()
    }
    return resumen

def limpiar_datos(df):
    """Aplica transformaciones para limpiar los datos."""
    df = df.drop_duplicates()
    df = df.dropna() 
    return df

def guardar_datos(df):
    """Guarda los datos limpios en un archivo CSV."""
    df.to_csv(CLEANED_DATA_PATH, index=False)

def generar_reporte(resumen_antes, resumen_despues):
    """Genera un reporte de auditoría con los cambios realizados."""
    with open(AUDIT_PATH, "w", encoding="utf-8") as f:
        f.write("=== REPORTE DE LIMPIEZA ===\n")
        f.write(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("Antes de la limpieza:\n")
        f.write(str(resumen_antes) + "\n\n")
        
        f.write("Después de la limpieza:\n")
        f.write(str(resumen_despues) + "\n")

def main():
    print("Cargando datos...")
    df = cargar_datos()
    
    print("Analizando datos...")
    resumen_antes = analizar_datos(df)
    
    print("Limpiando datos...")
    df_limpio = limpiar_datos(df)
    
    print("Guardando datos...")
    guardar_datos(df_limpio)
    
    print("Generando reporte...")
    resumen_despues = analizar_datos(df_limpio)
    generar_reporte(resumen_antes, resumen_despues)
    
    print("Proceso de limpieza completado.")

if __name__ == "__main__":
    main()
