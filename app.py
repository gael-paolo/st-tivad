import streamlit as st
import os
import pandas as pd
import google.generativeai as genai
from google.cloud import storage
import io
from io import BytesIO
import zipfile
import json

st.set_page_config(page_title="DataFrame Filter App")

st.header("Datos de Mercado ~ Importaciones")

# Función para descargar el DataFrame filtrado como archivo comprimido
@st.cache_data
def download_compressed_xlsx(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')

    output.seek(0)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        zip_file.writestr("filtered_dataframe.xlsx", output.getvalue())
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

# Input para las credenciales de GCP
gcp_credentials = st.file_uploader("Sube tus credenciales de Google Cloud (archivo JSON)", type="json")

# Input para la API key de Gemini
gemini_api_key = st.text_input("Ingresa tu API key de Gemini")

# Verifica que ambos inputs han sido proporcionados
if gcp_credentials and gemini_api_key:
    # Guarda el archivo de credenciales temporalmente y configura la API de Google Cloud
    gcp_credentials_json = json.load(gcp_credentials)
    with open("temp_gcp_credentials.json", "w") as f:
        json.dump(gcp_credentials_json, f)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "temp_gcp_credentials.json"
    
    # Configura la API key de Gemini
    genai.configure(api_key=gemini_api_key)

    def apply_prompt_template(user_input):
        return f"""
        Eres un asistente de IA especializado en análisis de datos con Python y la librería Pandas. 
        El usuario tiene un DataFrame llamado 'df' y quiere filtrarlo según las condiciones proporcionadas.
        
        Las columnas y tipos de dato para el dataframe son:
        FRV, FECHA (Date), NIT, REGISTRO, CHASIS, MOTOR, PARTIDA, AÑO_MODELO (numérico), MARCA, CLASE, MODELO, VERSION, CILINDRADA (numérico), 
        PAIS_ORIGEN, NRO_PUERTAS (numérico), TRACCION, CAPACIDAD_CARGA, NRO_RUEDAS, COMBUSTIBLE, AÑO_FABRICACION (numérico), DUI, FOB, PROVEEDOR, 
        DOCUMENTO, IMPORTADOR, LEVANTE, CANAL, FOB_2, FOB_3, ÚLTIMO_ENVÍO_RUAT, CLASE_SELECT, GAP_IMP (numérico), 
        MARCA_SELECT, TIPO_SELECT, CLUST_IMPORTADOR (entero), IMPORTADOR_AGRUPADO (factor), MERCADO, TIPO_COMERCIO, COMPETENCIA, SUBSEGMENTO.

        Cuando el usuario pida "fecha", "año" o "gestión", usa solo la columna "FECHA" con funciones de fecha/hora.
        Cuando el usuario mencione MARCA, usa solo el campo MARCA y convierte el valor de entrada a mayúsculas.
        Cuando el usuario pida subsegmento, convierte el valor de entrada a mayúsculas.
        Cuando el usuario pida el Importador, usa solamente y nada mas que el campo "importador_agrupado".
        Todos los nombres de las variables se encuentran en mayúsculas, no necesariamente los valores de cada variable.

        Las únicas Opciones disponibles en los campos tipo_comercio, mercado, marca_select son:
        - "tipo_comercio": "Minorista", "Distribuidor"
        - "mercado": "Gris", "Formal"
        - "marca_select" y "tipo_select": "YES", "NO"
        - "Competencia": "COMPETENCIA","OTRO"

        Proporciona código Python para filtrar 'df' basado en: {user_input}.
        Devuelve solo el código sin ningún comentario.
        """

    def get_gemini_code(user_input):
        prompt = apply_prompt_template(user_input)
        model = genai.GenerativeModel('gemini-pro')
        response = model.generate_content(prompt)
        cleaned_code = response.text.strip()
        
        if "```" in cleaned_code:
            cleaned_code = cleaned_code.split('```')[1]
            cleaned_code = cleaned_code.replace('python', '').strip()
        return cleaned_code

    def execute_python_code(code, df):
        local_vars = {'df': df}
        try:
            clean_code = "\n".join(line for line in code.splitlines() if not line.strip().startswith("#"))
            modified_code = "result = " + clean_code
            exec(modified_code, {}, local_vars)
            filtered_df = local_vars['result']
            return filtered_df
        except SyntaxError as e:
            raise ValueError(f"Syntax error in the generated code: {e}")
        except KeyError as e:
            raise ValueError(f"KeyError in the generated code: {e}")
        except Exception as e:
            raise ValueError(f"Error executing the code: {e}")

    @st.cache_data
    def load_data():
        bucket_name = "bkmarket"
        source_blob_name = "tablon.parquet"

        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(source_blob_name)

        content = blob.download_as_bytes()

        df = pd.read_parquet(BytesIO(content))

        df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce')
        df.drop(['Year', 'Mes'], axis=1, inplace=True)
        df.columns = df.columns.str.upper()

        return df

    df = load_data()

    st.write("Vista Previa Total:")
    st.dataframe(df.head())
    st.write(f"Número Registros Totales: {len(df)}")

    user_input = st.text_input("Ingresa tus condiciones de filtrado: ")

    submit = st.button("Aplicar Filtro")

    if submit:
        try:
            code = get_gemini_code(user_input)
            st.subheader("Código Python Generado:")
            st.code(code)
            
            st.write(f"Filas en el DataFrame original: {len(df)}")
            
            filtered_df = execute_python_code(code, df)
            
            st.write(f"Filas en el DataFrame filtrado: {len(filtered_df)}")
            
            st.write("Aquí hay una vista previa del DataFrame filtrado:")
            st.dataframe(filtered_df.head())
            
            with st.spinner('Generando archivo comprimido...'):
                zip_data = download_compressed_xlsx(filtered_df)
                st.download_button(
                    label="Descargar DataFrame filtrado (comprimido)",
                    data=zip_data,
                    file_name="filtered_dataframe.zip",
                    mime="application/zip")

        except ValueError as e:
            st.error(f"Error: {e}")

else:
    st.warning("Por favor, sube tus credenciales de GCP y proporciona la API key de Gemini para continuar.")
