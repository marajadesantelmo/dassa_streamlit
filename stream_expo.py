import streamlit as st
import pandas as pd
from datetime import datetime
from utils import highlight, fetch_data
from tokens import username, password

def show_page():
    # Load data from session state
    data = st.session_state.data
    arribos, pendiente_desconsolidar, verificaciones_impo, retiros, otros_impo, arribos_expo_carga, arribos_expo_ctns, verificaciones_expo, retiros, otros_expo, remisiones, consolidados = data

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
        st.dataframe(consolidados.style.apply(highlight, axis=1), hide_index=True)

# Run the show_page function
if __name__ == "__main__":
    show_page()

