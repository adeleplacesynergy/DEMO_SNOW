import pandas as pd
import streamlit as st
import snowflake
import snowflake.connector as snow
import numpy as np
import requests
from snowflake.snowpark import Session
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
import json

#          VARIABLEinitialiser S

# Page de connexion

if "con_state" not in st.session_state:
    st.session_state.con_state = False

if "load_state" not in st.session_state:
    st.session_state.load_state = False

if "submit_button_validate" not in st.session_state:
    st.session_state.submit_button_validate = False

if "con_snow" not in st.session_state:
    st.session_state.con_snow = False

if "account_snow" not in st.session_state:
    st.session_state.account_snow = False

if "user_snow" not in st.session_state:
    st.session_state.user_snow = False

if "mdp_snow" not in st.session_state:
    st.session_state.mdp_snow = False

# Exploration

if "applied_tags" not in st.session_state:
    st.session_state.applied_tags = False

if "applied_tags_state" not in st.session_state:
    st.session_state.applied_tags_state = False


# Identification

if "list_dbs_semantics" not in st.session_state:
    st.session_state.list_dbs_semantics = ''

if "dbs_semantics" not in st.session_state:
    st.session_state.dbs_semantics = ''

if "list_table_semantics" not in st.session_state:
    st.session_state.list_table_semantics = ''

if "choix_table_semantic" not in st.session_state:
    st.session_state.choix_table_semantic = ''

if "choix_schema_semantic" not in st.session_state:
    st.session_state.choix_schema_semantic = ''

if "echantillon_table_semantic" not in st.session_state:
    st.session_state.echantillon_table_semantic = ''

if "echantillon_table_semantic_state" not in st.session_state:
    st.session_state.echantillon_table_semantic_state = False

if "categories" not in st.session_state:
    st.session_state.categories = False

if "categories_state" not in st.session_state:
    st.session_state.categories_state = False

if "semantics" not in st.session_state:
    st.session_state.semantics = False

if "semantics2" not in st.session_state:
    st.session_state.semantics2 = False

if "option_choice" not in st.session_state:
    st.session_state.option_choice = False

if "tags_in_account" not in st.session_state:
    st.session_state.tags_in_account = False

# Enrichissement geocoding

if "adress_tags" not in st.session_state:
    st.session_state.adress_tags = pd.DataFrame(columns=['TAG_NAME','TAG_VALUE','COLUMN_NAME'])

if "add_button" not in st.session_state:
    st.session_state.add_button = False

if "cols_adress" not in st.session_state:
    st.session_state.cols_adress = ''

if "tags_all_deleted" not in st.session_state:
    st.session_state.tags_all_deleted = False

if "list_dbs_geocoding" not in st.session_state:
    st.session_state.list_dbs_geocoding = ''

if "dbs_geocoding" not in st.session_state:
    st.session_state.dbs_geocoding = ''

if "choix_table_geocoding" not in st.session_state:
    st.session_state.choix_table_geocoding = ''

if "choix_schema_geocoding" not in st.session_state:
    st.session_state.choix_schema_geocoding = ''

if "get_table_geocoding" not in st.session_state:
    st.session_state.get_table_geocoding = False

if "selection_geocoding" not in st.session_state:
    st.session_state.selection_geocoding = False

if "cols_for_enrichissement" not in st.session_state:
    st.session_state.cols_for_enrichissement = False

if "cols_for_enrichissement_state" not in st.session_state:
    st.session_state.cols_for_enrichissement_state = False

if "table_pour_geocoding" not in st.session_state:
    st.session_state.table_pour_geocoding = ''

if "creation_state_geocoding" not in st.session_state:
    st.session_state.creation_state_geocoding = False

# Enrichissement INSEE

if "list_dbs_insee" not in st.session_state:
    st.session_state.list_dbs_insee = ''

if "dbs_insee" not in st.session_state:
    st.session_state.dbs_insee = ''

if "choix_table_insee" not in st.session_state:
    st.session_state.choix_table_insee = ''

if "choix_schema_insee" not in st.session_state:
    st.session_state.choix_schema_insee = ''

if "selection_insee" not in st.session_state:
    st.session_state.selection_insee = False

if "get_table_insee" not in st.session_state:
    st.session_state.get_table_insee = False

if "table_pour_insee" not in st.session_state:
    st.session_state.table_pour_insee = ''

if "choix_table_insee2" not in st.session_state:
    st.session_state.choix_table_insee2 = ''

if "choix_schema_insee2" not in st.session_state:
    st.session_state.choix_schema_insee2 = ''

if "selection_insee2" not in st.session_state:
    st.session_state.selection_insee2 = False


# Profiling
if "report_state" not in st.session_state:
    st.session_state.report_state = False

if "db_snow_profiling" not in st.session_state:
    st.session_state.db_snow_profiling = False

if "sch_snow_profiling" not in st.session_state:
    st.session_state.sch_snow_profiling = False

if "table_snow_profiling" not in st.session_state:
    st.session_state.table_snow_profiling = False

if "num_rows_profiling" not in st.session_state:
    st.session_state.num_rows_profiling = False

if "table_to_analyse" not in st.session_state:
    st.session_state.table_to_analyse = False

if "table_to_analyse_state" not in st.session_state:
    st.session_state.table_to_analyse_state = False

if "profile" not in st.session_state:
    st.session_state.profile = False

if "profile_state" not in st.session_state:
    st.session_state.profile_state = False

#CREATE ALTER MASKING POLICIES

if "query_executor" not in st.session_state:
    st.session_state.query_executor = False

if "list_masking_policies" not in st.session_state:
    st.session_state.list_masking_policies = False

if "list_masking_policies_state" not in st.session_state:
    st.session_state.list_masking_policies_state = False

if "creation_var" not in st.session_state:
    st.session_state.creation_var = False

if "sql_generated_creation_state" not in st.session_state:
    st.session_state.sql_generated_creation_state = False
if "sql_generated_creation" not in st.session_state:
    st.session_state.sql_generated_creation = False

if "list_role_masking1" not in st.session_state:
    st.session_state.list_role_masking1 = False

if "list_role_masking2" not in st.session_state:
    st.session_state.list_role_masking2 = False

if "list_role_masking3" not in st.session_state:
    st.session_state.list_role_masking3 = False


# Association masking tag
if "masking_policies_7" not in st.session_state:
    st.session_state.masking_policies_7 = False

if "masking_policies_7_table" not in st.session_state:
    st.session_state.masking_policies_7_table = False

if "masking_policies_7_state" not in st.session_state:
    st.session_state.masking_policies_7_state = False

if "dbs_schemas_7_state" not in st.session_state:
    st.session_state.dbs_schemas_7_state = False

if "dbs_schemas_7" not in st.session_state:
    st.session_state.dbs_schemas_7 = False

if "tags_7" not in st.session_state:
    st.session_state.tags_7 = False

if "tags_7_state" not in st.session_state:
    st.session_state.tags_7_state = False

if "associe_tag_7" not in st.session_state:
    st.session_state.associe_tag_7 = False

if "associe_tag_7_state" not in st.session_state:
    st.session_state.associe_tag_7_state = False

# POlicy creation

if "policy_creation_state" not in st.session_state:
    st.session_state.policy_creation_state = False

if "policy_del_state" not in st.session_state:
    st.session_state.policy_del_state = False

if "policy_edit_state" not in st.session_state:
    st.session_state.policy_edit_state = False

if "masking_policies_state" not in st.session_state:
    st.session_state.masking_policies_state = False

if "masking_policies_save" not in st.session_state:
    st.session_state.masking_policies_save = False

if "tags_save" not in st.session_state:
    st.session_state.tags_save = False

st.markdown("# 	Présentation de l'application")

st.markdown(
    "Cette application permet d'effectuer différentes opérations sur une table présente sur ❄ SNOWFLAKE \n"
    "### Utilisation\n")
st.markdown("Pour pouvoir utiliser l'application tout d'abord il faut se connecter via la page connexion\n")
st.markdown("## L'application permet d'effectuer les actions suivantes:\n")
st.markdown("1- Explorer les informations liées aux tags et aux métadonnées d'une base ou d'une table\n")
st.markdown(
    "2- Identifier les categories semantiques des colonnes d'une table et appliquer les tags ainsi que les poilitiques de masking de données\n")
st.markdown(
    "3- Pour une table avec des colonnes liées à une adresse, rajouter des informations de localisation (Latitude, longitude)\n")
st.markdown("4- Pour une table enrichie via le point 3, on peut ajouter des données INSEE via le code GEO")
st.markdown("5- Exploration de données d'une table présente sur ❄ SNOWFLAKE, rapport interactif et téléchargeable")
