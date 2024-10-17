import streamlit as st
import pandas as pd
def show_page():
    # Load data
    arribos_expo = pd.read_csv('data/arribos_expo.csv')
    arribos_expo_carga = arribos_expo[arribos_expo['tipo_oper'] != 'VACIO']
    arribos_expo_ctns = arribos_expo[arribos_expo['tipo_oper'] == 'VACIO']

    arribos_expo_carga = arribos_expo_carga[['fecha', 'bookings', 'cliente', 'desc_merc', 'Estado']]
    arribos_expo_carga.columns = ['Fecha', 'Bookings', 'Cliente', 'Desc. Merc.', 'Estado']

    arribos_expo_ctns = arribos_expo_ctns[['fecha', 'bookings', 'cliente', 'dimension', 'contenedor', 'precinto','Estado']]
    arribos_expo_ctns.columns = ['Fecha', 'Bookings', 'Cliente', 'Dimension', 'Contenedor', 'Precinto', 'Estado']   

    turnos= pd.read_csv('data/turnos.csv')

    verificaciones_expo = turnos[(turnos['tipo_oper'] == 'Exportacion') & (turnos['destino'] == 'Verificacion')]
    verificaciones_impo = verificaciones_impo[['dia', 'cliente', 'desc_merc', 'contenedor', 'Envase', 'cantidad', 'ubicacion']]
    verificaciones_impo.columns = ['Dia', 'Cliente', 'Desc. Merc.', 'Contenedor', 'Envase', 'Cantidad', 'Ubic.']

    retiros = turnos[(turnos['tipo_oper'] == 'Exportacion') & (turnos['destino'] == 'Retiro')]
    retiros = retiros[['dia', 'cliente', 'conocim1', 'contenedor', 'Envase', 'cantidad', 'ubicacion', 'Estado']]
    retiros.columns = ['Dia', 'Cliente', 'Conocim.', 'Contenedor', 'Envase', 'Cantidad', 'Ubic.', 'Estado']

    otros_expo = turnos[(turnos['tipo_oper'] == 'Exportacion') & (~turnos['destino'].isin(['Retiro', 'Verificacion']))]
    otros_expo = otros_expo[['dia', 'hora', 'id', 'cliente', 'contenedor', 'Envase', 'cantidad', 'ubicacion']]
    otros_expo.columns = ['Dia', 'Hora', 'Operacion', 'Cliente', 'Contenedor', 'Envase', 'Cantidad', 'Ubic.']

    remisiones = turnos[turnos['destino'] == 'Remision']
    consolidados = turnos[turnos['destino'] == 'Consolidado']

    col_logo, col_title = st.columns([2, 5])
    with col_logo:
        st.image('logo.png')
    with col_title:
        st.title("Operaciones de EXPO")

    def highlight_arribado(row):
        if "Arribado" in row['Estado']:
            return ['background-color: darkgreen; color: black' for _ in row]
        elif row['Estado'] == "Pendiente ingreso":
            return ['background-color: lightyellow' for _ in row]
        else:
            return ['' for _ in row]

    def highlight_pendiente_desco(row):
        if row['Estado'] == "Vacio":
            return ['background-color: lightcoral' for _ in row]
        else:
            return ['' for _ in row]

    # Create two columns
    col1, col2 = st.columns(2)

    # Column 1: Arribos
    with col1:
        st.header("Arribos Contenedores")
        st.dataframe(arribos_expo_carga.style.apply(highlight_arribado, axis=1), hide_index=True)

    # Column 2: Pendiente Desconsolidar
    with col2:
        st.header("Pendiente Desconsolidar y Vacios")
        st.dataframe(arribos_expo_ctns.style.apply(highlight_pendiente_desco, axis=1).format(precision=0), hide_index=True)
