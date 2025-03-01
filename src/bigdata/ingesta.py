import sys
sys.stdout.reconfigure(encoding='utf-8')

import requests
import json
import sqlite3
import pandas as pd
import os
from datetime import datetime

class UserDataProcessor:
    """Clase para procesar datos de usuarios obtenidos de la API RandomUser"""
    
    def __init__(self, base_url="https://randomuser.me/api/"):
        """
        Inicializa el procesador de datos de usuarios.
        
        Args:
            base_url (str): URL base de la API
        """
        self.base_url = base_url
        self.data = {}
        self.conn = None
        self.db_path = 'src/user_data.db'
        self.sample_path = 'src/sample_users.xlsx'
        self.audit_path = 'src/audit_log.txt'
        
        # Crear estructura de directorios si no existen
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.sample_path), exist_ok=True)
    
    def obtener_datos_api(self, cantidad=5):
        """
        Obtiene datos de la API de usuarios aleatorios.
        
        Args:
            cantidad (int): Número de usuarios a obtener
            
        Returns:
            dict: Datos obtenidos o diccionario vacío en caso de error
        """
        url = f"{self.base_url}?results={cantidad}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            self.data = response.json()
            return self.data
        except requests.exceptions.RequestException as error:
            print(f"Error al obtener datos del API: {error}")
            self.data = {}
            return {}
    
    def crear_base_datos(self):
        """
        Crea una base de datos SQLite para almacenar información de usuarios.
        """
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            apellido TEXT NOT NULL,
            email TEXT UNIQUE,
            genero TEXT,
            pais TEXT,
            fecha_extraccion TEXT
        )
        ''')
        
        self.conn.commit()
        print(f"Base de datos creada en {self.db_path}")
    
    def insertar_datos(self):
        """
        Inserta los datos obtenidos de la API en la base de datos.
        
        Returns:
            int: Número de registros insertados
        """
        if not self.conn:
            self.crear_base_datos()
            
        cursor = self.conn.cursor()
        registros_insertados = 0
        fecha_extraccion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            if 'results' in self.data:
                for usuario in self.data['results']:
                    cursor.execute('''
                    INSERT OR REPLACE INTO usuarios 
                    (nombre, apellido, email, genero, pais, fecha_extraccion)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        usuario['name']['first'],
                        usuario['name']['last'],
                        usuario['email'],
                        usuario['gender'],
                        usuario['location']['country'],
                        fecha_extraccion
                    ))
                    registros_insertados += 1
            
            self.conn.commit()
            print(f"Se insertaron {registros_insertados} registros en la base de datos")
        except Exception as e:
            self.conn.rollback()
            print(f"Error al insertar datos: {e}")
        
        return registros_insertados
    
    def generar_muestra(self):
        """
        Genera un archivo Excel con una muestra de los datos almacenados.
        """
        if not self.conn:
            print("No hay conexión a la base de datos")
            return
        
        query = "SELECT * FROM usuarios"
        df = pd.read_sql_query(query, self.conn)
        
        df.to_excel(self.sample_path, index=False)
        print(f"Archivo de muestra generado en {self.sample_path}")
        
        return df
    
    def generar_auditoria(self):
        """
        Genera un archivo de auditoría comparando los datos de la API con los de la base de datos.
        """
        if not self.conn:
            print("No hay conexión a la base de datos")
            return
        
        query = "SELECT * FROM usuarios"
        df_db = pd.read_sql_query(query, self.conn)
        
        registros_api = len(self.data.get('results', []))
        registros_db = len(df_db)
        
        with open(self.audit_path, 'w', encoding='utf-8') as f:
            f.write("=== REPORTE DE AUDITORÍA ===\n")
            f.write(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"Registros obtenidos del API: {registros_api}\n")
            f.write(f"Registros almacenados en la base de datos: {registros_db}\n\n")
            
            if registros_api == registros_db:
                f.write("✓ ÉXITO: Todos los registros fueron almacenados correctamente.\n")
            else:
                f.write(f"⚠ ADVERTENCIA: Hay una diferencia de {abs(registros_api - registros_db)} registros.\n")
        
        print(f"Archivo de auditoría generado en {self.audit_path}")
    
    def procesar_datos_completo(self, cantidad=5):
        """
        Ejecuta el proceso de obtención, almacenamiento y auditoría de los datos.
        
        Args:
            cantidad (int): Número de usuarios a obtener
        """
        print("Iniciando proceso de recolección de datos...")

        print("Obteniendo datos de la API...")
        datos = self.obtener_datos_api(cantidad)
        
        if not datos:
            print("No se pudieron obtener datos. Proceso finalizado.")
            return False
        
        print("Creando base de datos...")
        self.crear_base_datos()
        
        print("Insertando datos...")
        self.insertar_datos()
        
        print("Generando archivo de muestra...")
        self.generar_muestra()
        
        print("Generando archivo de auditoría...")
        self.generar_auditoria()
        
        print("Proceso completado exitosamente.")
        return True
    
    def mostrar_registros(self):
        """
        Muestra los registros almacenados en la base de datos.
        """
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path)
            
        query = "SELECT * FROM usuarios"
        df = pd.read_sql_query(query, self.conn)
        
        if len(df) > 0:
            print(f"\nRegistros en la base de datos ({len(df)} total):")
            print(df.to_string(index=False))
        else:
            print("No hay registros en la base de datos.")
        
        return df
    
    def cerrar_conexion(self):
        """
        Cierra la conexión a la base de datos.
        """
        if self.conn:
            self.conn.close()
            print("Conexión a la base de datos cerrada.")


# Ejecución del código
if __name__ == "__main__":
    procesador = UserDataProcessor()
    
    try:
        procesador.procesar_datos_completo(cantidad=5)
        procesador.mostrar_registros()
    finally:
        procesador.cerrar_conexion()
