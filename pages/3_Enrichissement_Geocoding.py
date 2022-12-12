import pandas as pd
import streamlit as st
from snowflake.snowpark.functions import udf
import pandas as pd
import snowflake
import snowflake.connector as snow
import numpy as np
import requests
from snowflake.snowpark import Session
import json
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
import toolbox_cache as tb


def df_to_geojson(df, properties, lat='latitude', lon='longitude'):
    # create a new python dict to contain our geojson data, using geojson format
    geojson = {'type': 'FeatureCollection', 'features': []}

    # loop through each row in the dataframe and convert each row to geojson format
    for _, row in df.iterrows():
        # create a feature template to fill in
        feature = {'type': 'Feature',
                   'properties': {},
                   'geometry': {'type': 'Point',
                                'coordinates': []}}

        # fill in the coordinates
        feature['geometry']['coordinates'] = [row[lon], row[lat]]

        # for each column, get the value and add it as a new feature property
        for prop in properties:
            feature['properties'][prop] = row[prop]

        # add this feature (aka, converted dataframe row) to the list of features inside our dict
        geojson['features'].append(feature)

    return geojson

def aggrid_interactive_table(df, selection_mode):
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


def aggrid_interactive_table_not_unique(df):
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
    options.configure_selection(use_checkbox=True, selection_mode='multiple')
    options.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        update_mode=GridUpdateMode.MODEL_CHANGED
    )

    return selection


st.markdown("# 	Enrichissement Geocoding ")
st.markdown("Dans cette page nous allons enrichir une table contenant des informations liés à une localisation. "
            "L'enrichissement permet d'obtenir des features tels que la lattiutde ou la longitude. L'enrichissement "
            "à pour but également de construire une table qui va pouvoir être enrichie avec des données INSEE")

# Variable

lists_adresse_tags = ['VILLE', 'CODE_POSTAL', 'PAYS', 'REGION/DEPARTEMENT', 'RUE']

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
    if st.button('Mise à jour de la liste des tables'):
        st.experimental_memo.clear()

    list_dbs = tb.get_dbs_in_account(st.session_state.user_snow, st.session_state.account_snow,
                                     st.session_state.mdp_snow, role_selected)

    st.header('Choisir la base dans laquelle se trouve la table que vous voulez enrichir')
    db = st.selectbox('Liste des databases', (list_dbs))




    if db == '':
        st.error('Selectionner une database')

    else:
        st.session_state.dbs_geocoding = db

        list_table = tb.get_list_table_with_info_schema_memo(db, st.session_state.user_snow, st.session_state.account_snow,
                                                        st.session_state.mdp_snow, role_selected)

        selection = aggrid_interactive_table(list_table, 'single')

        if len(selection['selected_rows']) > 0:
            dbs = selection['selected_rows'][0]['TABLE_CATALOG']
            schemas = selection['selected_rows'][0]['TABLE_SCHEMA']
            table = selection['selected_rows'][0]['TABLE_NAME']

            table_enrichissement = tb.get_table_from_db_sch(dbs, schemas, table, st.session_state.user_snow,
                                                            st.session_state.account_snow,
                                                            st.session_state.mdp_snow, role_selected)

            st.header('Echantillon de la table selectionnée:')
            st.table(table_enrichissement.head(5))

            if len(st.session_state.adress_tags) == 0:
                if st.session_state.tags_all_deleted:
                    adress_tags_df = st.session_state.adress_tags

                else:
                    tags_infos = tb.get_tags_informations_from_table(dbs, schemas, table, st.session_state.user_snow,
                                                                     st.session_state.account_snow,
                                                                     st.session_state.mdp_snow, role_selected)

                    adress_tags = tags_infos[tags_infos['TAG_VALUE'].isin(lists_adresse_tags)]
                    adress_tags_df = adress_tags[['TAG_NAME', 'TAG_VALUE', 'COLUMN_NAME']]
                    st.header('Liste des colonnes Taggées comme faisant partie de l adresse')
                    adress_tags_df = adress_tags_df.reset_index(drop=True)
                    st.session_state.adress_tags = adress_tags_df

            else:

                adress_tags_df = st.session_state.adress_tags

            selection_adress = aggrid_interactive_table(adress_tags_df, 'single')

            col1, col2, col3 = st.columns(3)
            with col2:
                if not st.session_state.add_button:
                    if st.button("Ajouter des colonnes liées à l'adresse"):
                        st.session_state.add_button = True

                else:
                    cols_df = pd.DataFrame(table_enrichissement.columns, columns=['Colonnes'])
                    cols_df = cols_df[~cols_df['Colonnes'].isin(adress_tags_df['COLUMN_NAME'].values)]

                    selection_cols = aggrid_interactive_table(cols_df, 'multiple')
                    if len(selection_cols['selected_rows']) == 0:
                        st.error('Cocher au moins une case')

                    else:
                        all_cols = []
                        new_cols = []
                        for i in adress_tags_df['COLUMN_NAME'].values:
                            all_cols.append(i)
                        for i in range(len(selection_cols['selected_rows'])):
                            col = selection_cols['selected_rows'][i]['Colonnes']
                            all_cols.append(col)
                            new_cols.append(col)


                    if st.button('Ajouter ces colonnes'):
                        st.session_state.cols_adress = all_cols
                        temp = pd.DataFrame()
                        temp['COLUMN_NAME'] = new_cols
                        temp['TAG_NAME'] = 'RGPD_STATUS'
                        temp['TAG_VALUE'] = 'ADRESSE'
                        st.session_state.adress_tags = pd.concat([adress_tags_df, temp])
                        st.session_state.add_button = False


            with col3:
                if st.button("Supprimer les colonnes selectionnées"):
                    st.session_state.add_button = False
                    if len(selection_adress['selected_rows']) == 0:
                        st.error('Cocher au moins une case')
                    else:
                        indexes_to_drop = []
                        for i in range(len(selection_adress['selected_rows'])):
                            index = adress_tags_df[
                                adress_tags_df['COLUMN_NAME'] == selection_adress['selected_rows'][i][
                                    'COLUMN_NAME']].index[0]
                            indexes_to_drop.append(index)

                        adress_tags_df = adress_tags_df.drop(indexes_to_drop)
                        if len(adress_tags_df) == 0:
                            st.session_state.tags_all_deleted = True
                        st.session_state.adress_tags = adress_tags_df
                        st.session_state.cols_adress = adress_tags_df['COLUMN_NAME'].values

                with col1:
                    if st.button("Valider les colonnes"):
                        st.session_state.add_button = False
                        st.session_state.cols_adress = adress_tags_df['COLUMN_NAME'].values



            if len(st.session_state.cols_adress) > 0:
                st.header('Liste des colonnes identifiée comme adresses')
                st.write(st.session_state.cols_adress)
                table_enrichissement['id'] = np.arange(len(table_enrichissement))
                cols = st.session_state.cols_adress

                nom_table_enrichie = st.text_input('Nom de la table enrichie')
                if st.button('Créer la table enrichie'):
                    with st.spinner('Lancement du processus'):
                        table_enrichissement.to_csv('requete.csv')
                        files = []
                        files.append(('data', open('requete.csv', 'rb')))
                        for col in cols:
                            files.append(('columns', (None, col)))
                    with st.spinner('Récupération de la table via l API'):
                        response = requests.post('https://api-adresse.data.gouv.fr/search/csv/', files=files)
                        retour = pd.DataFrame(response.text.split('\n'))

                        row_to_add = []
                        city_codes = []

                        for i, row in retour[1:-1].iterrows():
                            if '"' in row.values[0]:

                                part_one = row.values[0].split('"')[0].split(',')

                                part_two = [row.values[0].split('"')[1]]
                                part_three = row.values[0].split('"')[2].split(',')


                                row_prepare = part_one[:-1] + part_two + part_three[1:]

                            else:

                                row_prepare = row.values[0].split(',')

                            row_to_add.append(row_prepare)


                        df_retour = pd.DataFrame(row_to_add)


                        st.write(retour.values[0][0].split(',')[0])
                        #if retour.values[0][0].split(',')[0] == "":
                        #    df_retour.columns = retour.values[0][0].split(',')[1:]
                        #else:
                        df_retour.columns = retour.values[0][0].split(',')


                        st.markdown("Echantillon de la table retournée par l'API")
                        st.table(df_retour.head(5))
                        city_codes = df_retour['result_citycode'].values
                        columns_to_geojson = df_retour.columns[len(table_enrichissement.columns) + 1:].values
                        geojson = df_to_geojson(df_retour, columns_to_geojson)

                        table_enrichissement_copy = table_enrichissement.copy()
                        table_enrichissement_copy['geojson'] = geojson['features']
                        table_enrichissement_copy['city_code'] = city_codes

                    with st.spinner('Processus de création de la table'):
                        con = tb.connection(st.session_state.user_snow, st.session_state.account_snow,
                                            st.session_state.mdp_snow, role_selected)
                        use_db = f"use database {dbs};"
                        use_schema = f"use schema {schemas};"

                        creation_stage = "create or replace stage my_int_stage file_format = 'my_parquet_format';"

                        table_enrichissement_copy.to_parquet('table_pour_enrichissement.parquet')
                        sql_put_text = f"put file://table_pour_enrichissement.parquet @MY_INT_STAGE;"
                        con.execute_string(
                            use_db + use_schema + creation_stage + sql_put_text)
                        create_table_sql = f"""create or replace table {dbs}.{schemas}.{nom_table_enrichie} using template(select ARRAY_AGG(OBJECT_CONSTRUCT(*)) from TABLE(INFER_SCHEMA(LOCATION=> '@MY_INT_STAGE', FILE_FORMAT => 'MY_PARQUET_FORMAT')));"""
                        con.execute_string(
                            use_db + use_schema + create_table_sql)
                        sql_load_table = f"""copy into {dbs}.{schemas}.{nom_table_enrichie} from @MY_INT_STAGE MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE FILE_FORMAT= '{dbs}.{schemas}.MY_PARQUET_FORMAT'"""
                        con.execute_string(use_db + use_schema + sql_load_table)
                        sql_rm_stage = f"rm @MY_INT_STAGE;"

                        con.execute_string(use_db + use_schema + sql_rm_stage)

                        con.close()

                        st.snow()
                        st.markdown('Table crée sur SNOW')
                        st.table(table_enrichissement_copy.head())

                        # st.session_state.table_pour_geocoding = ''
                        # st.session_state.list_dbs_geocoding = ''
                        # st.session_state.dbs_geocoding = ''
                        # st.session_state.selection_geocoding = False
                        # st.session_state.get_table_geocoding = False
                        # st.session_state.cols_for_enrichissement_state = False
                        # st.session_state.cols_for_enrichissement = ''
                        # st.session_state.creation_state_geocoding = False
                        # st.experimental_rerun()
