import streamlit as st
import pandas as pd
#import gspread
#from streamlit_gsheets import GSheetsConnection
import time
from datetime import datetime
from tokens import username, password
from utils import highlight, rellenar_df_vacio, impo_update

def fetch_data():
    turnos, arribos, pendiente_desconsolidar, existente_plz, existente_alm = impo_update(username, password)
    # Formateo
    arribos = arribos[['terminal', 'turno', 'contenedor', 'cliente', 'bookings', 'tipo_cnt', 'tiempo_transcurrido', 'Estado']]
    arribos.columns = ['Terminal', 'Turno', 'Contenedor', 'Cliente', 'Bookings', 'Tipo', 'Temp.', 'Estado']
    arribos['Cliente'] = arribos['Cliente'].apply(lambda x: x[:10] + "..." if len(x) > 10 else x)
    arribos['Bookings'] = arribos['Bookings'].apply(lambda x: x[:10] + "..." if len(x) > 10 else x)
    arribos['Turno'] = arribos['Turno'].apply(lambda x: f"{str(x)[:-2]}:{str(x)[-2:]}")

    pendiente_desconsolidar = pendiente_desconsolidar[['contenedor', 'cliente', 'Entrega', 'vto_vacio', 'tipo_cnt', 'peso','Estado']]
    pendiente_desconsolidar.columns = ['Contenedor', 'Cliente', 'Entrega', 'Vto. Vacio', 'Tipo', 'Peso', 'Estado']
    pendiente_desconsolidar['Entrega'] = pendiente_desconsolidar['Entrega'].fillna('-')
    pendiente_desconsolidar['Cliente'] = pendiente_desconsolidar['Cliente'].apply(lambda x: x[:10] + "..." if len(x) > 10 else x)

    turnos['cliente'] = turnos['cliente'].apply(lambda x: x[:10] + "..." if len(x) > 10 else x)
    turnos['desc_merc'] = turnos['desc_merc'].apply(lambda x: x[:10] + "..." if len(x) > 10 else x)
    turnos['ubicacion'] = turnos['ubicacion'].str.strip()

    verificaciones_impo = turnos[(turnos['tipo_oper'] == 'Importacion') & (turnos['destino'] == 'Verificacion')]
    verificaciones_impo = verificaciones_impo[['dia', 'cliente', 'desc_merc', 'contenedor', 'Envase', 'cantidad', 'ubicacion', 'Estado']]
    verificaciones_impo.columns = ['Dia', 'Cliente', 'Desc. Merc.', 'Contenedor', 'Envase', 'Cant.', 'Ubic.', 'Estado']

    retiros = turnos[(turnos['tipo_oper'] == 'Importacion') & (turnos['destino'] == 'Retiro')]
    retiros = retiros[['dia', 'cliente', 'conocim1', 'contenedor', 'Envase', 'cantidad', 'ubicacion', 'Estado']]
    retiros.columns = ['Dia', 'Cliente', 'Conocim.', 'Contenedor', 'Envase', 'Cant.', 'Ubic.', 'Estado']
    retiros['Conocim.'] = retiros['Conocim.'].str.strip()

    otros_impo = turnos[(turnos['tipo_oper'] == 'Importacion') & (~turnos['destino'].isin(['Retiro', 'Verificacion']))]
    otros_impo = otros_impo[['dia', 'hora', 'id', 'cliente', 'contenedor', 'Envase', 'cantidad', 'ubicacion', 'Estado']]
    otros_impo.columns = ['Dia', 'Hora', 'Operacion', 'Cliente', 'Contenedor', 'Envase', 'Cant.', 'Ubic.', 'Estado']

    #Se rellenan los dataframes vacios
    arribos = rellenar_df_vacio(arribos)
    pendiente_desconsolidar = rellenar_df_vacio(pendiente_desconsolidar)
    verificaciones_impo = rellenar_df_vacio(verificaciones_impo)
    retiros = rellenar_df_vacio(retiros)
    otros_impo = rellenar_df_vacio(otros_impo)

    return arribos, pendiente_desconsolidar, verificaciones_impo, retiros, otros_impo

def show_page():
    # Load data
    arribos, pendiente_desconsolidar, verificaciones_impo, retiros, otros_impo = fetch_data()

    col_logo, col_title = st.columns([2, 5])
    with col_logo:
        st.image('logo.png')
    with col_title:
        current_day = datetime.now().strftime("%d/%m/%Y")
        st.title(f"Operaciones de IMPO a partir del {current_day}")

    # Create two columns
    col1, col2 = st.columns(2)

    # Column 1: Arribos
    with col1:
        st.header("Arribos Contenedores")
        st.dataframe(arribos.style.apply(highlight, axis=1).set_properties(subset=['Cliente'], **{'width': '20px'}), hide_index=True, use_container_width=True)

    # Column 2: Pendiente Desconsolidar
    with col2:
        st.header("Pendiente Desconsolidar y Vacios")
        st.dataframe(pendiente_desconsolidar.style.apply(highlight, axis=1).format(precision=0), hide_index=True, use_container_width=True)

    st.header("Turnos")
    col3, col4 = st.columns(2)
    with col3:
        st.header("Verificaciones")
        st.dataframe(verificaciones_impo.style.apply(highlight, axis=1), hide_index=True, use_container_width=True)
        st.header("Otros")
        st.dataframe(otros_impo.style.apply(highlight, axis=1), hide_index=True, use_container_width=True)

    with col4:
        st.header("Retiros")
        st.dataframe(retiros.style.apply(highlight, axis=1), hide_index=True, use_container_width=True)

    print('Esperando 5 para actualizar')
    time.sleep(60*5)
    print('Actualizando')
    st.rerun()

# Run the show_page function
if __name__ == "__main__":
    show_page()

