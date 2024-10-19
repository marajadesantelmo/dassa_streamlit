import streamlit as st
import pandas as pd
import gspread
from streamlit_gsheets import GSheetsConnection
import time

def fetch_data(conn):
    # Same data fetching logic
    arribos = conn.read(spreadsheet='https://docs.google.com/spreadsheets/d/1r66h7BCAu-CyG5uRsITYhuds9uGlw7k_-Xu24WNX43Q/edit?gid=0#gid=0', 
                        usecols=list(range(18)), ttl=1)
    arribos = arribos[['terminal', 'turno', 'contenedor', 'cliente', 'bookings', 'tipo_cnt', 'tiempo_transcurrido', 'Estado']]
    arribos.columns = ['Terminal', 'Turno', 'Contenedor', 'Cliente', 'Bookings', 'Tipo', 'Temp.', 'Estado']

    pendiente_desconsolidar = conn.read(spreadsheet='https://docs.google.com/spreadsheets/d/1r66h7BCAu-CyG5uRsITYhuds9uGlw7k_-Xu24WNX43Q/edit?gid=594764855#gid=594764855', 
                                        usecols=list(range(17)), ttl=1)
    pendiente_desconsolidar = pendiente_desconsolidar[['contenedor', 'Estado', 'cliente', 'Entrega', 'vto_vacio', 'tipo_cnt', 'peso']]
    pendiente_desconsolidar.columns = ['Contenedor', 'Estado', 'Cliente', 'Entrega', 'Vto. Vacio', 'Tipo', 'Peso']

    turnos = conn.read(spreadsheet='https://docs.google.com/spreadsheets/d/1aWYam7vlducK5vNiQO5lyNfWJR6dEe-WA2805URN6p0/edit?gid=1749130661#gid=1749130661', 
                       usecols=list(range(29)), ttl=1)

    verificaciones_impo = turnos[(turnos['tipo_oper'] == 'Importacion') & (turnos['destino'] == 'Verificacion')]
    verificaciones_impo = verificaciones_impo[['dia', 'cliente', 'desc_merc', 'contenedor', 'Envase', 'cantidad', 'ubicacion']]
    verificaciones_impo.columns = ['Dia', 'Cliente', 'Desc. Merc.', 'Contenedor', 'Envase', 'Cantidad', 'Ubic.']

    retiros = turnos[(turnos['tipo_oper'] == 'Importacion') & (turnos['destino'] == 'Retiro')]
    retiros = retiros[['dia', 'cliente', 'conocim1', 'contenedor', 'Envase', 'cantidad', 'ubicacion', 'Estado']]
    retiros.columns = ['Dia', 'Cliente', 'Conocim.', 'Contenedor', 'Envase', 'Cantidad', 'Ubic.', 'Estado']

    otros_impo = turnos[(turnos['tipo_oper'] == 'Importacion') & (~turnos['destino'].isin(['Retiro', 'Verificacion']))]
    otros_impo = otros_impo[['dia', 'hora', 'id', 'cliente', 'contenedor', 'Envase', 'cantidad', 'ubicacion']]
    otros_impo.columns = ['Dia', 'Hora', 'Operacion', 'Cliente', 'Contenedor', 'Envase', 'Cantidad', 'Ubic.']

    return arribos, pendiente_desconsolidar, verificaciones_impo, retiros, otros_impo

def show_page():
    # Load data
    conn = st.connection('gsheets', type=GSheetsConnection)
    arribos, pendiente_desconsolidar, verificaciones_impo, retiros, otros_impo = fetch_data(conn)

    col_logo, col_title = st.columns([2, 5])
    with col_logo:
        st.image('logo.png')
    with col_title:
        st.title("Operaciones de IMPO")

    def highlight_arribado(row):
        if "Arribado" in row['Estado']:
            return ['background-color: darkgreen; color: black' for _ in row]
        elif row['Estado'] == "Pendiente ingreso":
            return ['background-color: darkgoldenrod; color: black' for _ in row]
        else:
            return ['' for _ in row]

    def highlight_pendiente_desco(row):
        if row['Estado'] == "Vacio":
            return ['background-color: lightcoral; color: black' for _ in row]
        else:
            return ['' for _ in row]

    # Create two columns
    col1, col2 = st.columns(2)

    # Column 1: Arribos
    with col1:
        st.header("Arribos Contenedores")
        st.dataframe(arribos.style.apply(highlight_arribado, axis=1), hide_index=True)

    # Column 2: Pendiente Desconsolidar
    with col2:
        st.header("Pendiente Desconsolidar y Vacios")
        st.dataframe(pendiente_desconsolidar.style.apply(highlight_pendiente_desco, axis=1).format(precision=0), hide_index=True)

    st.header("Turnos")

    # Create two columns for the tables
    col3, col4 = st.columns(2)

    # Column 3: Verificaciones and Otros
    with col3:
        st.header("Verificaciones")
        st.dataframe(verificaciones_impo, hide_index=True)
        
        st.header("Otros")
        st.dataframe(otros_impo, hide_index=True)

    # Column 4: Retiros
    with col4:
        st.header("Retiros")
        st.dataframe(retiros, hide_index=True)

    # Refresh data every 5 minutes using query parameters for rerunning
    print('Esperando para actualizar')
    time.sleep(60*5)
    print('Actualizando')
    st.rerun()

# Run the show_page function
if __name__ == "__main__":
    show_page()

