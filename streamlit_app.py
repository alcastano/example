import streamlit as st
from openai import OpenAI
import pandas as pd
import numpy
import lib
#import pip
#pip.main(["install", "openpyxl"])
st.set_page_config(layout="wide")
# Show title and description.
st.title("(GenReMur) Buscador Recursivo de antepasados Murcia")
st.markdown(
    "Descarga el archivos Excel del pueblo que te interese [aquí](https://onedrive.live.com/?authkey=%21AI%2DjU1MqxB9G8oM&id=BF237BB486352469%21510525&cid=BF237BB486352469). Una vez lo tengas súbelo a esta web. A continuación introduce los datos de una persona que aparece en ese Excel y la aplicación buscará los datos de sus padres, abuelos, bisabuelos, etc."
)

import streamlit.components.v1 as components



st.markdown(
        r"""
        <style>
        .stAppHeader {
        display:None} 
        .stMainBlockContainer {
               padding-top: 2rem;
               padding-left:0rem;
               padding-right:0rem;
               padding-bottom:1rem;
            }
        .stMarkdown p {
        padding-right:2em;
        }
        .stMainBlockContainer > div > div > .stVerticalBlock > div {
               padding-left:2rem;
               padding-right:2em;
        }
        .stElementContainer:has(> .stIFrame) {
        padding-left:0rem !important;
               padding-right:0.1rem !important;
        }
        </style>
        
        """, unsafe_allow_html=True
    )
css="""
<style>

[data-testid="stFileUploaderDropzone"] div div::before {
content:"Sube un Excel de indexaciones"}
[data-testid="stFileUploaderDropzone"] div div span {
display:none;}

[data-testid="stFileUploader"]>section[data-testid="stFileUploaderDropzone"]>button[data-testid="stBaseButton-secondary"] {
       color: rgba(0, 0, 0, 0);
    }
    [data-testid="stFileUploader"]>section[data-testid="stFileUploaderDropzone"]>button[data-testid="stBaseButton-secondary"]::after {
        content: "Subir";
        color:green;
        display: block;
        position: absolute;
    }
[data-testid="stFileUploaderDropzone"] div div::after { font-size: .8em; content:"Límite 200MB"}
[data-testid="stFileUploaderDropzone"] div div small{display:none;}
</style>
"""


def set_key(k,v):
    st.session_state[k] = v


st.markdown(css, unsafe_allow_html=True)

@st.fragment
def bar():
    # Let the user upload a file via `st.file_uploader`.
    uploaded_file = st.file_uploader(
        "Sube un Excel de indexaciones (.xlsx)", type=("xlsx"),
        help="Algunos tips"
    )

    if uploaded_file:
        with st.spinner('(1) Cargando y limpiando Excels....'):
            baut_all,matr_all,defu_all = lib.load_all_sheets_in_colab(uploaded_file.read())
            print(f"Bautizos: {len(baut_all)}")
            print(f"Matrimonios: {len(matr_all)}")
            print(f"Defunciones: {len(defu_all)}")
        with st.spinner('(2) Separando nombre y apellidos, agrupando datos por año... '):
            from collections import defaultdict
            defu_by_year: dict[int, list] = defaultdict(list)
            for year, group in defu_all.groupby('Año'):
                for _, row in group.iterrows():
                    if x := lib.Defuncion.defu_from_series(row):
                        defu_by_year[year].append(x)
            baut_by_year: dict[int, list] = defaultdict(list)
            for year, group in baut_all.groupby('Año'):
                for _, row in group.iterrows():
                    if x := lib.Bautizo.baut_from_series(row):
                        baut_by_year[year].append(x)

            matr_by_year = {year: group.to_dict('records') for year, group in matr_all.groupby('Año')}
            sheets = lib.Sheets(baut_by_year=baut_by_year, matr_by_year=matr_by_year, defu_by_year=defu_by_year)
            set_key("sheets",sheets)
            st.markdown("Excel procesado con éxito")

@st.fragment
def foo():
    with st.form(key='columns_in_form'):
        col1, col2, col3 = st.columns(3)
        with col1:
            nombre_val = st.text_input("Nombre")
            nombre_padre_val = st.text_input("Nombre Padre")
        with col2:
            apellido_1_val = st.text_input("Apellido 1")
            nombre_madre_val = st.text_input("Nombre Madre")
        with col3:
            apellido_2_val = st.text_input("Apellido 2",)
            st.write('<div style="height: 28px;"></div>', unsafe_allow_html=True)
            submitted = st.form_submit_button("Buscar",use_container_width=True)
        
    if submitted:
        if "sheets" in st.session_state:
            error = ""
            n_missing = 0
            if not nombre_val:
                error += "Campo 'Nombre' vacío.  \n"
                n_missing += 1
            if not apellido_1_val:
                error += "Campo 'Apellido 1' vacío.  \n"
                n_missing += 1
            if not apellido_2_val:
                error += "Campo 'Apellido 2' vacío.  \n"    
                n_missing += 1
            if not nombre_padre_val:
                error += "Campo 'Nombre Padre' vacío.  \n"  
                n_missing += 1
            if not nombre_madre_val:
                error += "Campo 'Nombre Madre' vacío.  \n"  
                n_missing += 1
            if n_missing > 1:
                error += "**Todos los campos son obligatorios**.  \n"
                st.markdown(error)
            else:
                with st.spinner('(3) Buscando antepasados...'):
                    g = lib.Gen(sheets=st.session_state['sheets'])
                    z = g.get_ancestors(lib.SearchInfo(
                        nombre_val,
                        apellido_1_val,
                        apellido_2_val,
                        nombre_padre_val,
                        nombre_madre_val,
                    ))
                    #z = g.get_ancestors(lib.SearchInfo("Aurelio","Castaño","Molina","Antonio","Trinidad"))
                    webpage = lib.get_webpage(z)
                    size = lib.get_tree_size(z)
                    if size <= 3:
                        st.markdown("**No se ha encontrado a esta persona en el Excel**")
                    else:
                        st.markdown(f"Deducido árbol con {size} miembros")
                    components.html(webpage, height=700, scrolling=True)
        else:
            st.markdown("**Antes de continuar debes subir un Excel en el que buscar**.")

bar()
foo()