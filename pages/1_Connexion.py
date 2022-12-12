# app.py
import pandas as pd
import asyncio
import streamlit as st
# from streamlit_multipage import MultiPage

import numpy as np
from sqlalchemy import create_engine
import snowflake
import snowflake.connector as snow
import sys
import io
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
from tqdm import tqdm
import os
from os import listdir
from os.path import isfile, join

from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
# use snowflka python connector
import snowflake.connector
import toolbox as tb


st.set_page_config(layout="wide")
####################################################

st.markdown("# 	🔒 PAGE DE CONNEXION ")

# FONCTIONS



####################################################


# main
account_info = pd.read_csv('account_infos.csv', index_col=0)
if st.session_state.con_state:
    st.info("Vous êtes déjà connecté")

else:
    st.title('Choix de la connexion')
    list_connexion = ['Creer une nouvelle connexion']
    list_connexions = account_info.Nom_connexion.values
    if len(list_connexions) > 0:
        for i in range(len(list_connexions)):
            list_connexion.append(list_connexions[i])

    methode_connexion = st.selectbox('Liste des connexions disponibles',
                                     list_connexion)
    if methode_connexion == 'Creer une nouvelle connexion':
        form = st.form(key='auth_form')
        nom_connexion = form.text_input('Nom de la connexion')
        # Snowflake connexion
        form.title("Connexion Snowflake")
        account_snow = form.text_input('ACCOUNT')
        user_snow = form.text_input('USERNAME')
        mdp_snow = form.text_input('PASSWORD', type='password')

        save_button = form.form_submit_button(
            label='Enrengistrer la connexion')
        if save_button:
            if nom_connexion in account_info.Nom_connexion.values:
                pass
            else:
                df = pd.DataFrame(columns=account_info.columns)
                values = [nom_connexion, account_snow, user_snow, mdp_snow]
                df.loc[0] = values
                account_info = pd.concat([account_info, df])
                account_info.to_csv('account_infos.csv')
        submit_button = form.form_submit_button(label='Connexion')
        st.session_state.submit_button_validate = submit_button

    else:
        informations = account_info[
            account_info['Nom_connexion'] == methode_connexion]
        form = st.form(key='auth_form')
        # Snowflake connexion
        form.title("Connexion Snowflake")
        account_snow = form.text_input(
            'ACCOUNT', value=informations['account'].values[0])
        user_snow = form.text_input(
            'USERNAME', value=informations['nom_utilisateur_snow'].values[0])
        mdp_snow = form.text_input(
            'PASSWORD', value=informations['mdp_snow'].values[0],
            type='password')
        submit_button = form.form_submit_button(label='Connexion')
        st.session_state.submit_button_validate = submit_button

submit_button_validated = st.session_state.submit_button_validate

if submit_button_validated:
    if st.session_state.load_state:
        pass
    else:
        st.title('Tentative de connexion')
        try:
            con = snowflake.connector.connect(
                user=user_snow,
                account=account_snow,
                password=mdp_snow)
            st.success('Connexion a SNOWFLAKE REUSSIE')
            st.session_state.account_snow = account_snow
            st.session_state.user_snow = user_snow
            st.session_state.mdp_snow = mdp_snow
            st.session_state.con_state = True
            con.close()
            st.session_state.load_state = True

            st.session_state.role = tb.get_role(user_snow,account_snow,mdp_snow)

        except Connexion.Error:
            st.error('Erreur de connexion à SNOWFLAKE')
            for key in st.session_state.keys():
                st.session_state[key] = False
            st.experimental_rerun()

if st.session_state.load_state:
    if st.button('Deconnexion'):
        for key in st.session_state.keys():
            st.session_state[key] = False
        st.experimental_rerun()
