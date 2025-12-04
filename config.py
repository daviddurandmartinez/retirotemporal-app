import streamlit as st
import sys
from decouple import config

def get_sql_server_config():
    try:
        return {
            "DRIVER": config("DRIVER"),
            "SERVER": config("SERVER"),
            "USER": config("USER"),    
            "PASSWORD": config("PASSWORD"),
            "DATABASE": config("DATABASE")
        }
    except Exception as e:
        if 'streamlit' in sys.modules and st.runtime.exists(): # Esto asegura que si la sección [database] no existe, se muestre un error claro en Streamlit.
            st.error(f"Error al cargar la sección [database]: {e}")
        return None
    
# Cargar la configuración principal
SQL_SERVER_CONFIG = get_sql_server_config()

# Definir constantes de la aplicación (usadas por app.py y database_connector.py)
# Si SQL_SERVER_CONFIG falla o está incompleto, asignamos valores de error.
if SQL_SERVER_CONFIG and all(SQL_SERVER_CONFIG.values()):
    # Valores específicos proporcionados por el usuario
    TARGET_TABLE = "origen.retiro_temporal" # Nombre de la tabla de destino en SQL Server
    KEY_COLUMN = "id" # Columna(s) clave para la comparación
else:
    TARGET_TABLE = "ERROR_TABLE_CHECK_CONFIG"
    KEY_COLUMN = "ERROR_ID_CHECK_CONFIG"

# CONFIGURACIÓN ADICIONAL
FILE_ENCODING = 'utf-8'