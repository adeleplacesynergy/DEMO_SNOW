import pandas as pd
import streamlit as st
import snowflake
import snowflake.connector as snow
from snowflake.snowpark.functions import udf
import pandas as pd
import numpy as np
import requests
from snowflake.snowpark import Session
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
import toolbox_cache as tb

st.markdown("# 	Enrichissement INSEE ")

st.markdown("Dans cette page nous allons enrichir une table grâce à des tables INSEE. L'idée est de récupérer des features"
            "d'une table INSEE via le code GEO")

def aggrid_interactive_table_1(df, selection_mode):
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
    options.configure_selection(use_checkbox=True, selection_mode=selection_mode)
    options.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        update_mode=GridUpdateMode.MODEL_CHANGED
    )

    return selection

def aggrid_interactive_table_2(df, selection_mode):
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
    options.configure_selection(use_checkbox=True, selection_mode=selection_mode)
    options.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        update_mode=GridUpdateMode.MODEL_CHANGED
    )

    return selection

def aggrid_interactive_table_3(df, selection_mode):
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
    options.configure_selection(use_checkbox=True, selection_mode=selection_mode)
    options.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        update_mode=GridUpdateMode.MODEL_CHANGED
    )

    return selection


def aggrid_interactive_table_multiple(df: pd.DataFrame):
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
    options.configure_selection(selection_mode="multiple", rowMultiSelectWithClick=True, use_checkbox=True)
    options.configure_default_column(min_column_width=35)
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        update_mode=GridUpdateMode.MODEL_CHANGED
    )

    return selection

if not st.session_state.load_state:
    st.error("Veuillez vous connecter a la page d'accueil")

else:
    role_list_select = tb.list_roles(st.session_state.user_snow, st.session_state.account_snow,
                                     st.session_state.mdp_snow)
    role_selected = st.sidebar.selectbox(
        "ROLE Utilisé",
        role_list_select
    )
    st.session_state.role = role_selected

    list_dbs = tb.get_dbs_in_account(st.session_state.user_snow, st.session_state.account_snow,
                                     st.session_state.mdp_snow, role_selected)

    if st.button('Mise à jour de la liste des tables'):
        st.experimental_memo.clear()

    st.header('Choisir la base dans laquelle se trouve la table que vous voulez enrichir')
    db = st.selectbox('Liste des databases', (list_dbs))

    if db == '':
        st.error('Selectionner une database')

    else:
        st.session_state.dbs_insee = db

        list_table = tb.get_list_table_with_info_schema_memo(db, st.session_state.user_snow, st.session_state.account_snow,
                                                        st.session_state.mdp_snow, role_selected)

        st.header('Choisir la table à enrichir')

        selection = aggrid_interactive_table_1(list_table, 'single')

        if len(selection['selected_rows']) > 0:
            dbs = selection['selected_rows'][0]['TABLE_CATALOG']
            schemas = selection['selected_rows'][0]['TABLE_SCHEMA']
            table = selection['selected_rows'][0]['TABLE_NAME']

            table_enrichissement_insee = tb.get_table_from_db_sch(dbs, schemas, table, st.session_state.user_snow,
                                                            st.session_state.account_snow,
                                                            st.session_state.mdp_snow, role_selected)

            st.header('Echantillon de la table selectionnée:')
            st.table(table_enrichissement_insee.head(5))

            if 'city_code' not in table_enrichissement_insee.columns:
                st.error("Veuillez choisir une autre table ou enrichir cette table via l'onglet 'Enrichissement Geocoding'")

            else:

                st.header('Selectionner la table qui va enrichir la table')
                selection2 = aggrid_interactive_table_2(list_table, 'single')

                if len(selection2['selected_rows']) > 0:
                    dbs2 = selection2['selected_rows'][0]['TABLE_CATALOG']
                    schemas2 = selection2['selected_rows'][0]['TABLE_SCHEMA']
                    table2 = selection2['selected_rows'][0]['TABLE_NAME']

                    table_insee = tb.get_table_from_db_sch(dbs2, schemas2, table2, st.session_state.user_snow,
                                                                          st.session_state.account_snow,
                                                                          st.session_state.mdp_snow, role_selected)

                    st.header('Echantillon de la table selectionnée:')
                    st.table(table_insee.head(5))

                    if 'city_code' not in table_insee.columns:
                        st.error(
                            "Veuillez choisir une table INSEE")

                    else:
                        insee_colonnes = pd.DataFrame(table_insee.columns, columns=['Feature'])
                        st.header('Choisir les features à ajouter a la table')
                        selection3 = aggrid_interactive_table_3(insee_colonnes, 'multiple')
                        query_for_view = tb.query_view_creation(table_enrichissement_insee, selection3)
                        name_view = st.text_input('Nom de la vue')
                        view_creation = f'CREATE OR REPLACE VIEW  {name_view} AS SELECT {query_for_view} ' \
                                        f'FROM {dbs}.{schemas}.{table} a,' \
                                        f'{dbs2}.{schemas2}.{table2} b WHERE a."city_code"=b."city_code";'

                        st.code(view_creation, language='sql')

                        if st.button('Créer la vue'):
                            con = snowflake.connector.connect(
                                user=st.session_state.user_snow,
                                account=st.session_state.account_snow,
                                password=st.session_state.mdp_snow, role=role_selected)

                            con.cursor().execute(f'use database {dbs}')
                            con.cursor().execute(view_creation)
                            con.close()
                            st.success('Vue créee')
                            st.snow()





