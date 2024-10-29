import streamlit as st
import stream_impo
import stream_expo
import stream_impo_historico
import stream_expo_historico
import threading
import time
from utils import fetch_data
from tokens import username, password

st.set_page_config(page_title="Operativa DASSA", 
                   page_icon="📊", 
                   layout="wide", 
                   initial_sidebar_state="expanded")

# Sidebar Navigation
st.sidebar.title("Menú de Navegación")

# Initialize session state for page selection
if 'page_selection' not in st.session_state:
    st.session_state.page_selection = "IMPO Orden del día"

# Sidebar radio button for page selection
page_selection = st.sidebar.radio('Seleccionar', 
                                  ["IMPO Orden del día", "EXPO Orden del día", "IMPO Histórico", "EXPO Histórico"],
                                  index=["IMPO Orden del día", "EXPO Orden del día", "IMPO Histórico", "EXPO Histórico"].index(st.session_state.page_selection))

# Update session state based on user selection
st.session_state.page_selection = page_selection

# Background function to update data
def update_data(update_event):
    while True:
        new_data = fetch_data(username, password)
        st.session_state.data = new_data
        update_event.set()  # Signal data has been updated
        time.sleep(150)  # Update every 3 minutes


# Initialize session state for data
if 'data' not in st.session_state:
    with st.spinner('Cargando datos del SQL de DEPOFS. Esta acción puede tardar unos minutos...'):
        st.session_state.data = fetch_data(username, password)
    update_event = threading.Event()
    # Start background thread to update data
    threading.Thread(target=update_data, args=(update_event,), daemon=True).start()


# Apply custom styles
st.markdown(
    """
    <style>
        /* Solarized Dark background and text colors */
        body {
            background-color: #002b36;
            color: #839496;
        }
        h1, h2, h3, h4, h5, h6 {
            color: #93a1a1;  /* Solarized headers */
        }
        .stApp {
            background-color: #002b36;
        }
        
        /* Table customization */
        .dataframe {
            background-color: #D3D3D3; /* Gray background for the entire table */
            color: black;  /* Black text for headers and cells */
        }
        .dataframe table {
            width: auto;
            table-layout: auto;
            background-color: #D3D3D3; /* Ensure table background is gray */
        }
        .dataframe th {
            background-color: #B0B0B0; /* Darker gray for headers */
            color: black;  /* Black text for headers */
        }
        .dataframe td {
            background-color: #D3D3D3;  /* Gray background for table cells */
            color: black;  /* Black text for table cells */
            font-size: 6px;  /* Smaller font size */
        }
        .dataframe td, .dataframe th {
            padding: 0.1rem;  /* Thinner padding */
            text-align: left;
            word-wrap: break-word;
            white-space: nowrap;
            border: 1px solid #586e75;  /* Subtle border for table */
        }
        
        /* Sidebar styling */
        .css-1d391kg {  /* Sidebar */
            background-color: #073642;
            color: #839496;
            font-size: 8px;  /* Smaller font size */
            width: 20px;  /* Adjust the width to make the sidebar smaller */
        }
        
        /* Streamlit buttons and input fields */
        .stButton button, .stTextInput input {
            background-color: #073642;
            color: #93a1a1;
            border: 1px solid #586e75;
        }
        .stButton button:hover {
            background-color: #586e75;
        }
    </style>
    """, 
    unsafe_allow_html=True
)

# Display the selected page content
if st.session_state.page_selection == "IMPO Orden del día":
    stream_impo.show_page()
elif st.session_state.page_selection == "EXPO Orden del día":
    stream_expo.show_page()
elif st.session_state.page_selection == "IMPO Histórico":
    stream_impo_historico.show_page()
elif st.session_state.page_selection == "EXPO Histórico":
    stream_expo_historico.show_page()