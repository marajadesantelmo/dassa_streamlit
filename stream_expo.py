import streamlit as st
import pandas as pd
from datetime import datetime
from utils import highlight, rellenar_df_vacio

def fetch_data():
    arribos_expo = pd.read_csv('data/arribos_expo.csv')
    arribos_expo_carga = arribos_expo[arribos_expo['tipo_oper'] != 'VACIO']
    arribos_expo_ctns = arribos_expo[arribos_expo['tipo_oper'] == 'VACIO']

    arribos_expo_carga = arribos_expo_carga[['fecha', 'bookings', 'cliente', 'desc_merc', 'Estado']]
    arribos_expo_carga.columns = ['Fecha', 'Bookings', 'Cliente', 'Desc. Merc.', 'Estado']

    arribos_expo_ctns = arribos_expo_ctns[['fecha', 'bookings', 'cliente', 'dimension', 'contenedor', 'precinto','Estado']]
    arribos_expo_ctns.columns = ['Fecha', 'Bookings', 'Cliente', 'Dimension', 'Contenedor', 'Precinto', 'Estado']   

    turnos= pd.read_csv('data/turnos.csv')

    verificaciones_expo = turnos[(turnos['tipo_oper'] == 'Exportacion') & (turnos['destino'] == 'Verificacion')]
    verificaciones_expo = verificaciones_expo[['dia', 'cliente', 'desc_merc', 'contenedor', 'Envase', 'cantidad', 'ubicacion']]
    verificaciones_expo = rellenar_df_vacio(verificaciones_expo)
    verificaciones_expo.columns = ['Dia', 'Cliente', 'Desc. Merc.', 'Contenedor', 'Envase', 'Cantidad', 'Ubic.']

    retiros = turnos[(turnos['tipo_oper'] == 'Exportacion') & (turnos['destino'] == 'Retiro')]
    retiros = retiros[['dia', 'cliente', 'conocim1', 'contenedor', 'Envase', 'cantidad', 'ubicacion', 'Estado']]
    retiros.columns = ['Dia', 'Cliente', 'Conocim.', 'Contenedor', 'Envase', 'Cantidad', 'Ubic.', 'Estado']

    otros_expo = turnos[(turnos['tipo_oper'] == 'Exportacion') & (~turnos['destino'].isin(['Retiro', 'Verificacion']))]
    otros_expo = otros_expo[['dia', 'hora', 'id', 'cliente', 'contenedor', 'Envase', 'cantidad', 'ubicacion']]
    otros_expo.columns = ['Dia', 'Hora', 'Operacion', 'Cliente', 'Contenedor', 'Envase', 'Cantidad', 'Ubic.']

    remisiones = turnos[turnos['destino'] == 'Remision']
    consolidados = turnos[turnos['destino'] == 'Consolidado']

    arribos_expo_carga = rellenar_df_vacio(arribos_expo_carga)
    arribos_expo_ctns = rellenar_df_vacio(arribos_expo_ctns)
    verificaciones_expo = rellenar_df_vacio(verificaciones_expo)
    retiros = rellenar_df_vacio(retiros)
    otros_expo = rellenar_df_vacio(otros_expo)
    remisiones = rellenar_df_vacio(remisiones)
    consolidados = rellenar_df_vacio(consolidados)

    return arribos_expo_carga, arribos_expo_ctns, verificaciones_expo, retiros, otros_expo, remisiones, consolidados
    

def show_page():
    # Load data
    arribos_expo_carga, arribos_expo_ctns, verificaciones_expo, retiros, otros_expo, remisiones, concolidados = fetch_data()

    col_logo, col_title = st.columns([2, 5])
    with col_logo:
        st.image('logo.png')
    with col_title:
        current_day = datetime.now().strftime("%d/%m/%Y")
        st.title(f"Operaciones de EXPO a partir del {current_day}")

    # Create two columns
    col1, col2 = st.columns(2)

    # Column 1: Arribos
    with col1:
        st.header("Arribos Contenedores")
        st.dataframe(arribos_expo_carga.style.apply(highlight, axis=1), hide_index=True)

    # Column 2: Pendiente Desconsolidar
    with col2:
        st.header("Pendiente Desconsolidar y Vacios")
        st.dataframe(arribos_expo_ctns.style.apply(highlight, axis=1).format(precision=0), hide_index=True)

    st.header("Turnos")
    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Verificaciones")
        st.dataframe(verificaciones_expo, hide_index=True)

        st.subheader("Otros Exportaciones")
        st.dataframe(otros_expo, hide_index=True)

    with col4:
        st.subheader("Retiros")
        st.dataframe(retiros.style.apply(highlight, axis=1), hide_index=True)

        st.subheader("Remisiones")
        st.dataframe(remisiones.style.apply(highlight, axis=1), hide_index=True)

        st.subheader("Consolidados")
        st.dataframe(concolidados.style.apply(highlight, axis=1), hide_index=True)

    print('Esperando 5 para actualizar')
    time.sleep(60*5)
    print('Actualizando')
    st.rerun()

# Run the show_page function
if __name__ == "__main__":
    show_page()

