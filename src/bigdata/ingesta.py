import sqlite3
import requests
import os
import random
from datetime import datetime

# Ruta de la base de datos
DB_PATH = "src/bigdata/static/db/user_data.db"

# Crear directorio si no existe
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def obtener_datos_api(cantidad=5000):
    """Obtiene una gran cantidad de datos desde la API de RandomUser."""
    url = f"https://randomuser.me/api/?results={cantidad}&nat=us"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get("results", [])
    else:
        print("Error al obtener datos de la API")
        return []

def eliminar_y_recrear_tabla():
    """Elimina la tabla existente y la recrea sin restricciones UNIQUE."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS usuarios")  # Elimina la tabla si ya existe

    cursor.execute('''
        CREATE TABLE usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT,
            apellido TEXT,
            email TEXT,
            genero TEXT,
            pais TEXT,
            fecha_extraccion TEXT
        )
    ''')

    conn.commit()
    conn.close()

def introducir_datos_erroneos(conn):
    """Duplica registros y agrega valores nulos intencionalmente."""
    cursor = conn.cursor()

    # Obtener todos los registros
    cursor.execute("SELECT * FROM usuarios")
    registros = cursor.fetchall()

    # Duplicar un 10% de los registros
    num_duplicados = int(len(registros) * 0.10)
    for _ in range(num_duplicados):
        registro = random.choice(registros)
        cursor.execute("INSERT INTO usuarios (nombre, apellido, email, genero, pais, fecha_extraccion) VALUES (?, ?, ?, ?, ?, ?)", registro[1:])

    # Introducir valores nulos en un 5% de los registros
    num_nulos = int(len(registros) * 0.05)
    for _ in range(num_nulos):
        registro = random.choice(registros)
        cursor.execute("UPDATE usuarios SET nombre=NULL WHERE id=?", (registro[0],))

    conn.commit()

def guardar_datos_en_db(datos):
    """Guarda los datos obtenidos de la API en la base de datos y agrega errores intencionales."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    fecha_extraccion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for usuario in datos:
        cursor.execute(
            "INSERT INTO usuarios (nombre, apellido, email, genero, pais, fecha_extraccion) VALUES (?, ?, ?, ?, ?, ?)",
            (
                usuario['name']['first'], 
                usuario['name']['last'], 
                usuario['email'], 
                usuario['gender'], 
                usuario['location']['country'], 
                fecha_extraccion
            )
        )

    # Introducir errores intencionales (duplicados y nulos)
    introducir_datos_erroneos(conn)

    conn.commit()
    conn.close()

def main():
    print("Eliminando y recreando tabla...")
    eliminar_y_recrear_tabla()  # Solo eliminará la tabla una vez

    print("Obteniendo datos de la API...")
    datos_api = obtener_datos_api()

    print("Guardando datos en la base de datos...")
    guardar_datos_en_db(datos_api)

    print("Ingesta de datos completada.")

if __name__ == "__main__":
    main()
