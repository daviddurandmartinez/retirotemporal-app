import streamlit as st
import pandas as pd
import io # Para manejo de archivos en memoria
# Las importaciones de numpy y re han sido eliminadas ya que no se utilizan.
from database_connector import create_sqlalchemy_engine, run_upsert_process, fetch_data_to_excel
# Importar TARGET_TABLE y KEY_COLUMN directamente de config
from config import TARGET_TABLE, KEY_COLUMN
from PIL import Image
from streamlit_option_menu import option_menu

def main():

    ###############
    ## STREAMLIT ##
    ###############

    #Imagenes en caché para realizar una sola subida    
    @st.cache_resource
    def load_image(image_path):
        return Image.open(image_path)
    ruta_logo = "assets/logo_newport.png"
    img = load_image(ruta_logo)

    st.set_page_config(
        page_title="Sincronización de archivos", 
        page_icon=img,
        layout="wide", # Usamos wide para más espacio
        initial_sidebar_state="collapsed"
    )

    st.title("Sincronización de Datos")
    ##st.sidebar.header("Opciones de Archivo")
    with st.sidebar:
        selected = option_menu("Menu Principal", ["Home"], 
            icons=['house', 'gear'], menu_icon="cast", default_index=1)
        selected

    ############################################
    ## CARGA DE ARCHIVO EXCEL PARA SQL SERVER ##
    ############################################

    st.header("1. Cargar y Sincronizar Datos")
    st.markdown("Sube un archivo Excel para actualizar o insertar registros en la base de datos")

    engine = create_sqlalchemy_engine() # Inicialización del motor una sola vez 

    if engine is None:
        # Mensaje de error ajustado para indicar la nueva sección [database]
        st.warning("Verifica que los drivers de ODBC estén instalados y las credenciales sean correctas. Revisa la sección [database] en secrets.toml.")
        return 
    # Cargar archivo
    uploaded_file = st.file_uploader("Sube tu archivo Excel (.xlsx)", type=["xlsx"])

    if uploaded_file is not None:
        try:
            # Leer archivo a DataFrame y asegurar que los datos se leen como bytes para compatibilidad
            df_excel = pd.read_excel(io.BytesIO(uploaded_file.read()))
            
            # Validación simple de columnas
            if KEY_COLUMN not in df_excel.columns:
                 # Usamos KEY_COLUMN directamente
                 st.error(f"Error de archivo: La columna de ID ('{KEY_COLUMN}') definida en 'config.py' no se encontró en el Excel.")
                 return

            st.subheader("Vista Previa de Datos de Excel:")
            st.dataframe(df_excel.head())

            # Botón para ejecutar el proceso
            if st.button(f"Ejecutar Sincronización"):
                with st.spinner(f'Ejecutando Sincronizacion de excel con base de datos'):
                    # La función run_upsert_process gestiona la transacción y el cierre de conexión.
                    success, message = run_upsert_process(df_excel, engine)
                    
                    if success:
                        st.success(message)
                        st.balloons()
                    else:
                        st.error(f"Fallo del proceso: {message}")
                    
        except Exception as e:
            # Captura errores inesperados al procesar el archivo Excel.
            st.error(f"Error inesperado al procesar el archivo o la lógica: {e}")

    #####################################
    ## DESCARGA DE DATOS DE SQL SERVER ##
    #####################################

    st.header("2. Descargar Datos")
    st.markdown("Descarga el contenido actual de la tabla a un archivo Excel")
    
    if st.button("Cargar y Descargar Datos de la Tabla", key="btn_download"):
        with st.spinner(f'Cargando datos de la tabla `{TARGET_TABLE}`...'):
            # fetch_data_to_excel crea y desecha su propio engine.
            df_output, message = fetch_data_to_excel()
        
        if df_output is not None:
            st.success("Datos cargados correctamente")
            st.dataframe(df_output.head())
            
            # Convertir DataFrame a Excel en memoria (mejor práctica para Streamlit)
            excel_buffer = io.BytesIO()
            df_output.to_excel(excel_buffer, index=False, engine='xlsxwriter')
            excel_buffer.seek(0)
            
            st.download_button(
                label="Descargar archivo",
                data=excel_buffer,
                file_name=f"export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.error(f"Fallo al descargar los datos: {message}")

if __name__ == "__main__":
    main()