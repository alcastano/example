import streamlit as st
from openai import OpenAI
import pandas as pd
import numpy
#import pip
#pip.main(["install", "openpyxl"])

# Show title and description.
st.title("📄 Document question answering")
st.write(
    "HI HIH IHI Upload a document below and ask a question about it – GPT will answer! "
    "To use this app, you need to provide an OpenAI API key, which you can get [here](https://platform.openai.com/account/api-keys). "
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

# Let the user upload a file via `st.file_uploader`.
uploaded_file = st.file_uploader(
    "Sube un Excel de indexaciones (.xlsx)", type=("xlsx"),
    help="Algunos tips"
)

st.markdown(css, unsafe_allow_html=True)


col1, col2 = st.columns(2)

with st.form("my_form"):
    with col1:
        st.write("Inside the form 1")
        nombre_val = st.text_input("Nombre")
        apellido_1_val = st.text_input("Apellido 1")
        apellido_2_val = st.text_input("Apellido 2")
    with col2:
        st.write("Inside the form 2")
        nombre_padre_val = st.text_input("Nombre Padre")
        nombre_madre_val = st.text_input("Nombre Madre")
     # Every form must have a submit button.
    submitted = st.form_submit_button("Buscar")
    if submitted:
        st.write("nombre_val", nombre_val, "nombre_padre_val", nombre_padre_val)





if uploaded_file and submitted:
    defu: pd.DataFrame = pd.read_excel(uploaded_file.read())
    st.write(str(defu.head()))

