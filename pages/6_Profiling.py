#app.py
import pandas as pd
import streamlit as st

st.markdown("# 	📊 PROFILING ")
st.sidebar.markdown("# 📊 PROFILING")
import streamlit.components.v1 as components



import numpy as np
import streamlit as st
from pandas_profiling import ProfileReport
from streamlit_pandas_profiling import st_profile_report
#app.py
import pandas as pd
import toolbox_cache as tb
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
import re
import snowflake.connector
from collections import Counter
import json

import random

# Compute recipe outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

# Web App Title
st.markdown('''
# **Analyse de donnée d'une table SNOWFLAKE**
''')

# Upload CSV data



def aggrid_interactive_table(df, selection_mode,key):
    """Creates an st-aggrid interactive table based on a dataframe.

    Args:
        df (pd.DataFrame]): Source dataframe

    Returns:
        dict: The selected row
    """

    options = GridOptionsBuilder.from_dataframe(
        df, enableRowGroup=True, enableValue=True, enablePivot=True
    )

    options.configure_side_bar()
    options.configure_selection(selection_mode=selection_mode, rowMultiSelectWithClick=True, use_checkbox=True)
    options.configure_default_column(min_column_width=35)
    options.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        update_mode=GridUpdateMode.MODEL_CHANGED,key=key
    )

    return selection


if not st.session_state.load_state:
    st.error("Veuillez vous connecter a la page d'accueil")

else:


            account_snow = st.session_state.account_snow
            user_snow = st.session_state.user_snow
            mdp_snow = st.session_state.mdp_snow

            role_list_select = tb.list_roles(user_snow, account_snow, mdp_snow)
            role_selected = st.sidebar.selectbox(
                "ROLE Utilisé",
                role_list_select
            )
            st.session_state.role = role_selected

            list_dbs = tb.get_dbs_in_account(st.session_state.user_snow, st.session_state.account_snow,
                                             st.session_state.mdp_snow, role_selected)

            st.header('Choisir la base dans laquelle se trouve la table que vous voulez enrichir')
            db = st.selectbox('Liste des databases', (list_dbs))

            if db == '':
                st.error('Selectionner une database')

            else:
                list_table = tb.get_list_table_with_info_schema_memo(db, st.session_state.user_snow, st.session_state.account_snow,
                                                                st.session_state.mdp_snow, role_selected)
                st.header('Choix de la table à analyser')
                with st.expander("Choix de la table"):
                    selection = aggrid_interactive_table(list_table, 'single', 'choice1')

                if len(selection['selected_rows'])==0:
                    st.error('Choisir une table ')

                else:
                    num_rows = selection['selected_rows'][0]['ROW_COUNT']
                    percent = st.slider("% de la table pour analyse", 10, 100, step=1)
                    num_rows_sample = (num_rows * percent) / 100
                    if num_rows_sample > 10000:
                        st.error('Nombre de ligne trop élevé. La limite de 10 000 lignes est appliqués')
                        num_rows_sample = 10000
                    st.write(f"Nombre de ligne pour l'echantillon {str(int(num_rows_sample))}")

                    schema = selection['selected_rows'][0]['TABLE_SCHEMA']
                    table_name = selection['selected_rows'][0]['TABLE_NAME']
                    df_table = tb.get_table_from_db_sch_with_sample_limit(db, schema, table_name, percent, 10000, st.session_state.user_snow, st.session_state.account_snow,
                                                            st.session_state.mdp_snow, role_selected)

                    randomlist = []
                    for i in range(0, 20):
                        n = random.randint(0, len(df_table.columns)-1)
                        if n in randomlist:
                            pass
                        else:
                            randomlist.append(n)

                    randomlist.append(-1)
                    list_cols = ['LotArea', 'LotConfig', 'OverallCond','YearRemodAdd', 'OpenPorchSF', 'PoolArea',
                                         'YearBuilt', 'YrSold', 'MasVnrArea', 'SalePrice']
                    df_table = df_table[list_cols]
                    #df_table = df_table[df_table.columns[randomlist]]

                    st.header('Echantillon de la table selectionée')
                    st.table(df_table.head(5))

                    if st.button("Lancer l'analyse"):
                        pr = tb.launch_profile_report(df_table)
                        st.write('-------------------')

                        st.header('**Rapport de Pandas Profiling**')
                        st_profile_report(pr)
                        st.session_state.report_state = True
                        st.session_state.profile = pr

                    if st.button('Réafficher le raport'):
                        pr = st.session_state.profile
                        st_profile_report(pr)

                    if st.session_state.report_state:

                        nom_rapport = st.text_input('Nom du rapport')
                        pr = st.session_state.profile

                        if st.button('Exporter le rapport au format HTML'):
                            name = nom_rapport + ".html"
                            pr.to_file(name)
                            pr.to_file(f"{nom_rapport}.json")

                    st.experimental_memo.clear()




