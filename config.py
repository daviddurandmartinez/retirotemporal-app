import streamlit as st
import sys

def get_sql_server_config():
    """
    Retorna la configuración de la base de datos cargada desde st.secrets['database'].
    """
    try:
        # La clave 'database' debe coincidir con la sección en .streamlit/secrets.toml
        db_config = st.secrets["database"]
        
        # Mapeo de keys de .toml (snake_case) a las keys internas deseadas (UPPERCASE)
        # Usamos .get() por si faltan campos, pero si el resultado es None, fallará la conexión
        return {
            "DRIVER": db_config.get("DRIVER"),
            "SERVER": db_config.get("SERVER"),
            "USER": db_config.get("USER"),    
            "PASSWORD": db_config.get("PASSWORD"),
            "DATABASE": db_config.get("DATABASE")
        }
    except Exception as e:
        if 'streamlit' in sys.modules and st.runtime.exists(): # Esto asegura que si la sección [database] no existe, se muestre un error claro en Streamlit.
            st.error(f"Error al cargar la sección [database] de secrets.toml: {e}")
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