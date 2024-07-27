import logging

import streamlit as st

logger = logging.getLogger(__name__)


st.set_page_config(layout="wide")
st.title("Spartid Public Transport")

st.write("Using Entur data to visualize public transport in Norway")
