import streamlit as st
import pandas as pd
from datetime import datetime
from utils import highlight, fetch_data
from tokens import username, password
import threading
import time

# Function to fetch and cache data every 5 minutes
@st.cache_data(ttl=300)
def get_data():
    return fetch_data(username, password)

# Background function to update expo data
def update_data(update_event):
    while True:
        new_data = fetch_data(username, password)
        st.session_state.data = new_data
        update_event.set()  # Signal data has been updated
        time.sleep(300)  # Update every 5 minutes

def show_page():
    # Check if data is in session state, load if not
    if 'data' not in st.session_state:
        with st.spinner('Loading initial data...'):
            st.session_state.data = get_data()
        update_event = threading.Event()
        # Start background thread to update data
        threading.Thread(target=update_data, args=(update_event,), daemon=True).start()
    else:
        update_event = threading.Event()

    # Load data from session state
    data = st.session_state.data
    arribos, pendiente_desconsolidar, verificaciones_impo, retiros, otros_impo, arribos_expo_carga, arribos_expo_ctns, verificaciones_expo, retiros, otros_expo, remisiones, consolidados = data

    # Layout setup
    col_logo, col_title = st.columns([2, 5])
    with col_logo:
        st.image('logo.png')
    with col_title:
        current_day = datetime.now().strftime("%d/%m/%Y")
        st.title(f"Operaciones de IMPO a partir del {current_day}")

    # Columns for displaying data
    col1, col2 = st.columns(2)
    with col1:
        st.header("Arribos Contenedores")
        st.dataframe(arribos.style.apply(highlight, axis=1).set_properties(subset=['Cliente'], **{'width': '20px'}), hide_index=True, use_container_width=True)

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

    # Check if update_event was triggered, then rerun if data updated
    if update_event.is_set():
        st.experimental_rerun()

# Run the show_page function
if __name__ == "__main__":
    show_page()
