import pandas as pd
import streamlit as st
import pandas as pd
import snowflake
import snowflake.connector as snow
import numpy as np
import requests
import json
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
import re
from collections import Counter


def aggrid_interactive_table(df,selection_type,checkbox_bool):
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
    options.configure_selection(use_checkbox=checkbox_bool, selection_mode=selection_type)
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        update_mode=GridUpdateMode.MODEL_CHANGED
    )

    return selection


def get_list_databases(user_snow, account_snow, mdp_snow):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow)
    sql_show_db = """show databases;"""
    list_dbs = []
    list_dbs.append('')
    list_db = pd.read_sql(sql_show_db, con)['name'].values
    con.close()
    for d in list_db:
        list_dbs.append(d)
    st.session_state.list_dbs_geocoding = list_dbs
    db = st.selectbox('Liste des databases', (list_dbs))
    return db

def get_list_schemas_in_db(user_snow, account_snow, mdp_snow,db):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow)
    sql_show_db = """show databases;"""
    list_dbs = []
    list_dbs.append('')
    list_db = pd.read_sql(sql_show_db, con)['name'].values
    con.close()
    for d in list_db:
        list_dbs.append(d)
    st.session_state.list_dbs_geocoding = list_dbs
    db = st.selectbox('Liste des databases', (list_dbs))
    return db


def list_table_from_db(user_snow, account_snow, mdp_snow, db,selection_type,checkbox_bool):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow)

    sql_show_table = "select * from " + db + ".information_schema.tables;"
    list_table = pd.read_sql(sql_show_table, con)
    con.close()
    list_table = list_table[
        ['TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME', 'TABLE_OWNER', 'TABLE_TYPE', 'ROW_COUNT', 'BYTES',
         'RETENTION_TIME', 'CREATED', 'LAST_ALTERED']]
    list_table = list_table[list_table['TABLE_TYPE'] == 'BASE TABLE']
    selection = aggrid_interactive_table(list_table,selection_type,checkbox_bool)

    if len(selection['selected_rows']) > 0:
        num_rows = selection['selected_rows'][0]['ROW_COUNT']
        dbs = selection['selected_rows'][0]['TABLE_CATALOG']
        schemas = selection['selected_rows'][0]['TABLE_SCHEMA']
        table = selection['selected_rows'][0]['TABLE_NAME']

    return dbs, schemas, table, num_rows



def infos_from_tables(user_snow, account_snow, mdp_snow):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow)

    sql_show_table = "show tables;"
    list_table = pd.read_sql(sql_show_table, con)
    con.close()
    selection = aggrid_interactive_table(list_table,'single',True)


    if len(selection['selected_rows']) > 0:
        dbs = selection['selected_rows'][0]['database_name']
        schemas = selection['selected_rows'][0]['schema_name']
        table = selection['selected_rows'][0]['name']


    else:
        dbs = ''
        schemas = ''
        table = ''
    return dbs, schemas, table


def create_table_from_df(user_snow, account_snow, mdp_snow, df,sch,db,table_name):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow,schema = sch,database=db)


    use_db = "use database " + db + ";"
    con.cursor().execute(use_db)

    use_schema = "use schema " + sch + ";"
    con.cursor().execute(use_schema)

    creation_stage = "create or replace stage my_int_stage file_format = 'my_parquet_format'"
    con.cursor().execute(creation_stage)

    df.to_parquet('df.parquet')
    sql_put_text = "put file://df.parquet @" + db + "." + sch + ".MY_INT_STAGE;"
    con.cursor().execute(sql_put_text)

    create_table_sql = """create or replace table """ + db + """.""" + sch + """.""" + table_name + """ using template(select ARRAY_AGG(OBJECT_CONSTRUCT(*)) from TABLE(INFER_SCHEMA(LOCATION=> '@MY_INT_STAGE', FILE_FORMAT => 'MY_PARQUET_FORMAT')));"""
    con.cursor().execute(create_table_sql)
    sql_load_table = """copy into """ + db + """.""" + sch + """.""" + table_name + """ from @MY_INT_STAGE MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE FILE_FORMAT= '""" + db + """.""" + sch + """.MY_PARQUET_FORMAT'"""
    con.cursor().execute(sql_load_table)

    sql_rm_stage = "rm @" + db + "." + sch + ".MY_INT_STAGE;"
    con.cursor().execute(sql_rm_stage)
    con.close()
    return 0


def formulaire_connexion_snowflake():
    account_info = pd.read_csv('account_infos.csv', index_col=0)
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

    if submit_button or button_recall:
        st.title('Tentative de connexion')
        try:
            con = snowflake.connector.connect(
                user=user_snow,
                account=account_snow,
                password=mdp_snow)
            st.success('Connexion a SNOWFLAKE REUSSIE')
            con.close()

        except :
            st.error('Erreur de connexion à SNOWFLAKE')


    return user_snow, account_snow, mdp_snow

def get_table_from_snow(db,sch,table_name,user_snow,account_snow,mdp_snow):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow)

    table_request = "select * from " + db + "." + sch + "." + table_name
    table = pd.read_sql(table_request, con)
    con.close()

    return table


def get_table_from_snow_percent(db,sch,table_name,user_snow,account_snow,mdp_snow,percent):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow)

    table_request = f"select * from {db}.{sch}.{table_name} sample ({str(percent)}) LIMIT 10000;"
    table = pd.read_sql(table_request, con)
    con.close()

    return table


def regex_test_labelisation_cols(df_bilan,df):


    regex_url = '((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*'
    regex_mail = '[0-9a-zA-Z._%+-]+@[0-9a-zA-Z.-]+\\.[A-Za-z]{2,4}'
    iban_regex = '^(?:(?:IT|SM)\d{2}[A-Z]\d{22}|CY\d{2}[A-Z]\d{23}|NL\d{2}[A-Z]{4}\d{10}|LV\d{2}[A-Z]{4}\d{13}|(?:BG|BH|GB|IE)\d{2}[A-Z]{4}\d{14}|GI\d{2}[A-Z]{4}\d{15}|RO\d{2}[A-Z]{4}\d{16}|KW\d{2}[A-Z]{4}\d{22}|MT\d{2}[A-Z]{4}\d{23}|NO\d{13}|(?:DK|FI|GL|FO)\d{16}|MK\d{17}|(?:AT|EE|KZ|LU|XK)\d{18}|(?:BA|HR|LI|CH|CR)\d{19}|(?:GE|DE|LT|ME|RS)\d{20}|IL\d{21}|(?:AD|CZ|ES|MD|SA)\d{22}|PT\d{23}|(?:BE|IS)\d{24}|(?:FR|MR|MC)\d{25}|(?:AL|DO|LB|PL)\d{26}|(?:AZ|HU)\d{27}|(?:GR|MU)\d{28})$'
    date_regex = '([1-9]|1[0-9]|2[0-9]|3[0-1]|0[0-9])(.|-|\/)([1-9]|1[0-2]|0[0-9])(.|-|\/)(20[0-9][0-9])'
    telephone_regex = '^(?:(?:\+|00)33[\s.-]{0,3}(?:\(0\)[\s.-]{0,3})?|0)[1-9](?:(?:[\s.-]?\d{2}){4}|\d{2}(?:[\s.-]?\d{3}){2})$'
    bool_type = type(np.bool_(True))

    l = []
    p = []
    for col in df_bilan.Colonne:
        temp = df[col]
        temp = temp.dropna()
        if len(temp)==0:
            p.append(1)
            l.append('None')
        else:
            if type(temp.values[0]) == bool_type:
                label = 'Boolean'
                m = 1

            elif type(temp.values[0]) == str:

                if 'siren' in col:
                    label = 'Siren'
                    m = 1
                elif 'postalcode' in col:
                    label = 'CP'
                    m = 1
                else:
                    mapped_result_url = temp.map(lambda i: bool(re.match(regex_url, i)))
                    url_nb = Counter(mapped_result_url)[True]

                    mapped_result_mail = temp.map(lambda i: bool(re.match(regex_mail, i)))
                    mail_nb = Counter(mapped_result_mail)[True]
                    mapped_result_iban = temp.map(lambda i: bool(re.match(iban_regex, i)))
                    iban_nb = Counter(mapped_result_iban)[True]
                    mapped_result_date = temp.map(lambda i: bool(re.match(date_regex, i)))
                    date_nb = Counter(mapped_result_date)[True]
                    mapped_result_tel = temp.map(lambda i: bool(re.match(telephone_regex, i)))
                    tel_nb = Counter(mapped_result_tel)[True]

                    percentage = [url_nb / len(temp), mail_nb / len(temp), iban_nb / len(temp), date_nb / len(temp),
                                  tel_nb / len(temp)]
                    m = max(percentage)
                    if m == percentage[0] and m > 0.3:
                        label = 'URL'
                    elif m == percentage[1] and m > 0.3:
                        label = 'mail'
                    elif m == percentage[2] and m > 0.3:
                        label = 'IBAN'
                    elif m == percentage[3] and m > 0.3:
                        label = 'Date'

                    elif m == percentage[4] and m > 0.3:
                        label = 'TELEPHONE'
                    else:
                        label = 'To_define'

            elif temp.values[0].dtype == np.float64:
                label = 'Float'
                m = 1

            elif temp.values[0].dtype == np.int64:
                label = 'Int'
                m = 1

            else:
                label = 'Date'
                m = 1

            p.append(m)
            l.append(label)

    df_bilan['Label'] = l
    df_bilan['Label_Percentage'] = p

    return df_bilan


def get_role(user_snow,account_snow,mdp_snow):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow)

    get_role = "select current_role();"
    role = pd.read_sql(get_role, con)
    con.close()
    role = role['CURRENT_ROLE()'].values[0]

    return role

def get_role_list(user_snow,account_snow,mdp_snow):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow)

    show_role = "show roles;"
    list_role = pd.read_sql(show_role, con)
    con.close()
    list_role = list_role['name'].values

    return list_role

def list_roles(user_snow,account_snow,mdp_snow):
    role = st.session_state.role
    list_role = get_role_list(user_snow, account_snow, mdp_snow)
    role_list_select = [role]
    for r in list_role:
        role_list_select.append(r)

    return role_list_select

def get_tags_informations(user_snow,account_snow,mdp_snow,db):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow,
        database = db)

    show_tags = "show tags;"
    list_tags = pd.read_sql(show_tags, con)
    con.close()

    return list_tags

def get_tags_in_table(user_snow,account_snow,mdp_snow,db,sch,table):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow)

    show_tags_in_table = "select * from table("+db+".information_schema.tag_references_all_columns('"+db+"."+sch+"."+table+"','table'));"
    list_tags_in_table = pd.read_sql(show_tags_in_table, con)
    con.close()

    return list_tags_in_table

def configuration_masking_policy(db, role, mp, config, data_type):
        query = ""

        cur.execute("use {db};".format(db=db))

        if config.upper() == "TOUT CACHER" and data_type.upper() in ('TEXT', 'EMAIL'):
            query = "alter masking policy {mp} set body ->" \
                        "case    " \
                        "   when current_role() = '{role}' " \
                        "   then '{cacher}' end;".format(mp=mp, role=role, cacher='********')
        elif config.upper() == "TOUT CACHER" and data_type.upper() == 'NUMBER':
                query = "alter masking policy {mp} set body ->" \
                        "case    " \
                        "   when current_role() = '{role}' " \
                        "   then '{cacher}' end;".format(mp=mp, role=role, cacher='********')
        elif config.upper() == "TOUT CACHER" and data_type.upper() == 'DATE':
                            query = "alter masking policy {mp} set body ->" \
                                    "case    " \
                                    "   when current_role() = '{role}' " \
                                    "   then '{cacher}' end;".format(mp=mp, role=role, cacher='********')

        elif config.upper() == "VISIBLE" and data_type.upper() in ('NUMBER','TEXT', 'EMAIL','DATE'):
            query = "alter masking policy {mp} set body ->" \
                    "case    " \
                    "   when current_role() = '{role}' " \
                    "   then val end;".format(mp=mp, role=role)

        elif config.upper() == "Afficher un code".upper() and data_type.upper() in ('TEXT', 'EMAIL'):
                query = "alter masking policy {mp} set body ->" \
                        "case    " \
                        "   when current_role() = '{role}' " \
                        "   then sha2(val) end;".format(mp=mp, role=role)
        elif config.upper() == "Afficher un code".upper() and data_type.upper() == 'NUMBER':
            query = "alter masking policy {mp} set body ->" \
                        "case    " \
                        "   when current_role() = '{role}' " \
                        "   then sha2(val) end; ".format(mp=mp, role=role)

        elif config.upper() == "Domaine visible (Email)".upper() and data_type.upper() == 'EMAIL':
            query = "alter masking policy {mp} set body ->" \
                        "case when current_role() = '{role}' then regexp_replace(val,'.+\@','*****@') end;"\
                .format(mp=mp, role=role)

        elif config.upper() == "Date masking".upper() and data_type.upper() == 'DATE':
            query = "alter masking policy {mp} set body ->" \
                                    "case    " \
                                    "   when current_role() = '{role}' " \
                                    "   then date_from_parts(0001, 01, 01)::timestamp_ntz end; ".format(mp=mp, role=role)

        return query

