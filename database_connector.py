import pandas as pd
from sqlalchemy import create_engine, text
import urllib # Necesario para codificar la contraseña en la URI de SQLAlchemy
from config import SQL_SERVER_CONFIG, TARGET_TABLE, KEY_COLUMN

#############################################
## CONEXION A SQL SERVER USANDO SQLALCHEMY ##
#############################################

def create_sqlalchemy_engine():
    """
    Crea y devuelve un motor (Engine) de SQLAlchemy para SQL Server.
    """
    # Verificar si la configuración fue cargada correctamente
    if not SQL_SERVER_CONFIG or not all(SQL_SERVER_CONFIG.values()):
        return None
    
    driver_name = SQL_SERVER_CONFIG["DRIVER"].strip('{}')
    username = SQL_SERVER_CONFIG["USER"]
    server = SQL_SERVER_CONFIG["SERVER"]
    database = SQL_SERVER_CONFIG["DATABASE"]
    password = urllib.parse.quote_plus(SQL_SERVER_CONFIG["PASSWORD"]) # codificar la contraseña para la URI para evitar problemas con caracteres especiales
    
    # Construir la cadena de conexión de SQLAlchemy para SQL Server usando pyodbc
    conn_str = (
        f'mssql+pyodbc://{username}:{password}@{server}/{database}?'
        f'driver={driver_name}'
    )
    
    try:
        # El pool_recycle es una buena práctica para conexiones de larga duración
        engine = create_engine(conn_str, pool_recycle=3600)
        return engine
    except Exception as e:
        print(f"Error creando el motor de SQLAlchemy: {e}")
        return None

#############################################
## DESCARGA DE DATOS DE SQL SERVER A EXCEL ##
#############################################

def fetch_data_to_excel(table_name=TARGET_TABLE):
    """
    Descarga todos los datos de la tabla de destino a un DataFrame.
    Crea y desecha el motor de SQLAlchemy en la llamada.
    """
    engine = create_sqlalchemy_engine()
    if engine is None:
        return None, "Error al crear el motor de base de datos."

    try:
        # El bloque 'with' asegura que connection.close() se llame al salir.
        with engine.connect() as connection:
            query = f"SELECT * FROM {table_name}"      
            # FIX: Usar la conexión (connection) en lugar del motor (engine) en pd.read_sql
            df = pd.read_sql(query, connection) 
            return df, "Datos descargados correctamente."
    except Exception as e:
        return None, f"Error al descargar datos: {e}"
    finally:
        # Aseguramos que el motor se deseche al finalizar la operación de descarga.
        if engine:
            engine.dispose()

##########################################
## CARGA DE DATOS DE EXCEL A SQL SERVER ##
##########################################

def generate_merge_query(df: pd.DataFrame, table_name: str, id_column: str) -> str:
    #Genera una consulta SQL MERGE dinámica para la operación Upsert. (Lógica sin cambios, ya era correcta)
    #columns = ", ".join([f"[{col}]" for col in df.columns])
    # Columnas que se pueden insertar/actualizar (excluyendo la clave de identidad)
    updatable_cols = [col for col in df.columns if col != id_column]
    set_clauses = ", ".join([f"TARGET.[{col}] = SOURCE.[{col}]" for col in df.columns if col != id_column])
    insert_columns = ", ".join([f"[{col}]" for col in updatable_cols])
    insert_values = ", ".join([f"SOURCE.[{col}]" for col in updatable_cols])
    
    merge_sql = f"""
    MERGE INTO {table_name} AS TARGET
    USING (
        SELECT * FROM #TEMP_EXCEL_DATA
    ) AS SOURCE ON (TARGET.[{id_column}] = SOURCE.[{id_column}])
    
    WHEN MATCHED THEN
        UPDATE SET
            {set_clauses}
            
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({insert_columns})
        VALUES ({insert_values});
    """
    return merge_sql

def run_upsert_process(df_excel: pd.DataFrame, engine):
    """
    Carga el DataFrame a una tabla temporal y ejecuta la sentencia MERGE.
    """
    # Usamos SQL_SERVER_CONFIG para verificar la disponibilidad de la configuración
    if df_excel.empty or not SQL_SERVER_CONFIG or not all(SQL_SERVER_CONFIG.values()):
        return False, "DataFrame vacío o configuración de BD no disponible. Revise el archivo Excel y secrets.toml."
        
    table_name = TARGET_TABLE
    id_column = KEY_COLUMN
    temp_table_name = "#TEMP_EXCEL_DATA" 

    try:
        # 'engine.begin()' inicia una transacción, asegurando el commit/rollback automático y utiliza una conexión del pool.
        with engine.begin() as connection:
            
            # PASO 1: Cargar el DataFrame a la tabla temporal
            df_excel.to_sql(
                name=temp_table_name, 
                con=connection, 
                if_exists='replace', 
                index=False
                #dtype={col: df_excel[col].dtype.name for col in df_excel.columns}
            ) 
            
            # PASO 2: Generar y ejecutar la consulta MERGE
            merge_query = generate_merge_query(df_excel, table_name, id_column) 
            connection.execute(text(merge_query))
            
            # FIX: Retornar la tupla (bool, str) para el desempaquetado correcto en app.py
            return True, f"Proceso completado exitosamente"

    except Exception as e:
        # FIX: Retornar la tupla (bool, str) para el desempaquetado correcto en app.py
        return False, f"Error durante la ejecución del proceso ETL/MERGE: {e}"