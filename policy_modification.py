import snowflake.connector
import streamlit as st
import pandas as pd
import numpy as np
import toolbox as tb
import ast

"""if not st.session_state.load_state:
    st.error("Veuillez vous connecter a la page d'accueil")

else:
    role_list_select = tb.list_roles(st.session_state.user_snow, st.session_state.account_snow, st.session_state.mdp_snow)
    role_selected = st.sidebar.selectbox(
        "ROLE Utilisé",
        role_list_select
    )
    st.session_state.role = role_selected
    con = snowflake.connector.connect(
        user=st.session_state.user_snow,
        account=st.session_state.account_snow,
        password=st.session_state.mdp_snow,
        role=role_selected)

    cur = con.cursor()
"""
def list_role(user_snow, account_snow, mdp_snow):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow)
    cur = con.cursor()
    query_select_role = pd.DataFrame(cur.execute("show roles;"))
    lst_role = []
    for role in query_select_role[1]:
        lst_role.append(role)
    return lst_role


def select_role(user_snow, account_snow, mdp_snow):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow)
    cur = con.cursor()
    query_select_role = pd.DataFrame(cur.execute("show roles;"))
    lst_role = []
    for role in query_select_role[1]:
        lst_role.append(role)
    select_role = st.multiselect("Selectionner un ou plusieurs rôles :", lst_role)
    return select_role

def select_masking_policies(user_snow, account_snow, mdp_snow):
    con = snowflake.connector.connect(
        user=user_snow,
        account=account_snow,
        password=mdp_snow)
    cur = con.cursor()
    query_select_mp = pd.DataFrame(cur.execute("show masking policies;"))
    lst_mp = []
    for mp in query_select_mp[1]:
        lst_mp.append(mp)
    select_mp = st.radio("Selectionner une politique de masquage à associer au(x) rôles(s) selectionné(s) :", lst_mp)
    return select_mp

def select_configuration(db, role, mp, config):
        query = ""

        #cur.execute("use {db};".format(db=db))

        if config.upper() == "TOUT CACHER":
            query = "alter masking policy {mp} set body " \
                    "-> case when current_role() = '{role}' " \
                    "then '{cacher}' end;".format(mp=mp, role=role, cacher='********')

        elif config.upper() == "VISIBLE":
            query = "alter masking policy {mp} set body " \
                    "-> case when current_role() = '{role}' " \
                    "then val end;".format(mp=mp, role=role)

        elif config.upper() == "Afficher un code".upper():
                query = "alter masking policy {mp} set body " \
                        "-> case when current_role() = '{role}'" \
                        " then sha2(val) end;".format(mp=mp, role=role)

        elif config.upper() == "Domaine visible (Email)".upper():
            query = "alter masking policy {mp} set body -> " \
                        "case when current_role() = '{role}' then regexp_replace(val,'.+\@','*****@') end;"\
                .format(mp=mp, role=role)

        elif config.upper() == "Date masking".upper() :
            query = "alter masking policy {mp} set body ->" \
                    "case when current_role() = '{role}' " \
                    "then date_from_parts(0001, 01, 01)::timestamp_ntz end; ".format(mp=mp, role=role)

        return query

def configuration(user_snow, account_snow, mdp_snow, selected_db,selected_sch):
            """selected_db = tb.get_list_databases(st.session_state.user_snow,
                                                st.session_state.account_snow,
                                                st.session_state.mdp_snow)
            query_select_sch = pd.DataFrame(
                cur.execute("show schemas in {db};".format(db=selected_db)))  # selection du schema
            lst_sch = []
            for sch in query_select_sch[1]:
                lst_sch.append(sch)

            selected_sch = st.selectbox("Selectionner un schema : ", lst_sch)

            st.code('use databse {db};'.format(db=selected_db) + 'use schema {sch}'.format(sch=selected_sch), language='sql' )
"""
            #if selected_db not in st.session_state:
                #st.session_state.selected_db = tb.get_list_databases(st.session_state.user_snow,
                                                                     #st.session_state.account_snow,
                                                                     #st.session_state.mdp_snow)
            #if st.session_state.selected_db != "":
                #if selected_sch not in st.session_state:
                    #st.session_state.selected_sch = tb.get_list_schemas_in_db(st.session_state.user_snow,
                                                                         #st.session_state.account_snow,
                                                                         #st.session_state.mdp_snow,
                                                                         #st.session_state.selected_db)"""

            con = snowflake.connector.connect(
                user=user_snow,
                account=account_snow,
                password=mdp_snow)
            cur = con.cursor()
            selected_role = select_role(user_snow, account_snow, mdp_snow)
            #col2,  col3 = st.columns(2)

            #with col2:
            selected_mp = select_masking_policies(user_snow, account_snow, mdp_snow)
            #with col3:
            select_config = st.radio("Configuration de la politique de masking en fonction "
                                             "du/des rôle(s) choisi(s) :",
                                             ("Visible", "Tout cacher", "Afficher un code",
                                              "Domaine visible (Email)", "Date masking"))


            gen_sql = st.button('Generer SQL')
            execute_sql = st.button("Exécuter")
            if "query_gen" not in st.session_state:
                st.session_state.query_gen = False
            if "query_executor" not in st.session_state:
                st.session_state.query_executor = False

            if gen_sql:
                st.session_state.query_gen = True

            if execute_sql:
                st.session_state.query_executor = True

            if len(selected_role) > 0:
                for role in selected_role:
                    if gen_sql and st.session_state.query_gen == True:
                        query = select_configuration(selected_db, role, selected_mp, select_config)

                        #st.session_state.query_gen = True
                        st.code(query, language="sql")

                        con.execute_string('use database {db};'.format(db=selected_db) +
                                           'use schema {sch}'.format(sch=selected_sch))

                        #cur.execute(query)
                        #st.success("Statement executed successfully.")

                        if st.session_state.query_executor == True:
                            cur.execute(query)
                            st.success("Statement executed successfully.")
                             #con.execute_string(query)
                    #st.write(pd.DataFrame(cur.execute("select * from TEST_MASKING;")))
            else:
                st.error('Vous devez selectionnez un ou plusieurs rôles ')

    #configuration()return 0


def generation_sql_link(type,list_role_boucle,name_policy,comment,masking_du_sous_type,selected_type,tag_selected,tag_value):
    if type=='edit':
        select_role0 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 1 :", list_role_boucle)
        masking0 = st.selectbox('Choix du Masking 1', masking_du_sous_type)
        if masking0 == '1':
            masking0 = int(masking0)
        list_role_boucle2 = []
        for s_r in list_role_boucle:
            if s_r in select_role0:
                pass
            else:
                list_role_boucle2.append(s_r)

        select_role2 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 2 :", list_role_boucle2)
        masking2 = st.selectbox('Choix du Masking 2', masking_du_sous_type)
        if masking2 == '1':
            masking2 = int(masking2)
        list_role_boucle3 = []
        for s_r in list_role_boucle2:
            if s_r in select_role2:
                pass
            else:
                list_role_boucle3.append(s_r)
        select_role3 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 3 :", list_role_boucle3)
        masking3 = st.selectbox('Choix du Masking 3', masking_du_sous_type)
        if masking3 == '1':
            masking3 = int(masking3)
        # transformation pour la requete de ['role1','role2'] => "'role1','role2'"
        string_roles0 = ""
        for x in select_role0:
            string_roles0 += "'" + x + "',"
        string_roles2 = ""
        for x in select_role2:
            string_roles2 += "'" + x + "',"
        string_roles3 = ""
        for x in select_role3:
            string_roles3 += "'" + x + "',"
        string_roles0 = string_roles0[:-1]
        string_roles2 = string_roles2[:-1]
        string_roles3 = string_roles3[:-1]
        st.write(masking0)
        #if type(masking0) == int:
        #    edit_policy_req0 = f"alter masking policy {name_policy} set body -> case when current_role() = {string_roles0} then {masking0} end SET COMMENT = {comment};"
        #else:
        edit_policy_req0 = f"alter masking policy {name_policy} set body -> case when current_role() in {string_roles0} then '{masking0}' end SET COMMENT = {comment};"

        if len(string_roles2) > 0:
            #if type(masking2) == int:
            #    edit_policy_req2 = f"alter masking policy {name_policy} set body -> case when current_role() in {string_roles2} then {masking2} end SET COMMENT = {comment};"
            #else:
            edit_policy_req2 = f"alter masking policy {name_policy} set body -> case when current_role() in {string_roles2} then '{masking2}' end SET COMMENT = {comment};"
        else:
            edit_policy_req2 = ""

        if len(string_roles3) > 0:
            #if type(masking3) == int:
            #    edit_policy_req3 = f"alter masking policy {name_policy} set body -> case when current_role() in {string_roles3} then {masking3} end SET COMMENT = {comment};"
            #else:
            edit_policy_req3 = f"alter masking policy {name_policy} set body -> case when current_role() in {string_roles3} then '{masking3}' end SET COMMENT = {comment};"
        else:
            edit_policy_req3 = ""

        edit_policy_req = edit_policy_req0 + edit_policy_req2 + edit_policy_req3

    elif type =='create':
        select_role0 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 1 :", list_role_boucle)
        masking0 = st.selectbox('Choix du Masking 1', masking_du_sous_type)
        if masking0 == '1':
            masking0 = int(masking0)
        list_role_boucle2 = []
        for s_r in list_role_boucle:
            if s_r in select_role0:
                pass
            else:
                list_role_boucle2.append(s_r)

        select_role2 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 2 :", list_role_boucle2)
        masking2 = st.selectbox('Choix du Masking 2', masking_du_sous_type)
        if masking2 == '1':
            masking2 = int(masking2)
        list_role_boucle3 = []
        for s_r in list_role_boucle2:
            if s_r in select_role2:
                pass
            else:
                list_role_boucle3.append(s_r)

        select_role3 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 3 :", list_role_boucle3)
        masking3 = st.selectbox('Choix du Masking 3', masking_du_sous_type)
        if masking3 == '1':
            masking3 = int(masking3)

        # transformation pour la requete de ['role1','role2'] => "'role1','role2'"
        string_roles0 = ""
        for x in select_role0:
            string_roles0 += "'" + x + "',"

        string_roles2 = ""
        for x in select_role2:
            string_roles2 += "'" + x + "',"

        string_roles3 = ""
        for x in select_role3:
            string_roles3 += "'" + x + "',"
        string_roles0 = string_roles0[:-1]
        string_roles2 = string_roles2[:-1]
        string_roles3 = string_roles3[:-1]

        edit_policy_req0 = f"create or replace masking policy {name_policy} as (val {selected_type}) returns {selected_type} -> case when current_role() in ({string_roles0}) then '{masking0}' end COMMENT = '{comment}';"

        if len(string_roles2) > 0:
            edit_policy_req2 = f"create or replace masking policy {name_policy} as (val {selected_type}) returns {selected_type} -> case when current_role() in ({string_roles2}) then '{masking2}' end COMMENT = '{comment}';"
        else:
            edit_policy_req2 = ""

        if len(string_roles3) > 0:
            edit_policy_req3 = f"create or replace masking policy {name_policy} as (val {selected_type}) returns {selected_type} -> case when current_role() in ({string_roles3}) then '{masking3}'end COMMENT = '{comment}';"
        else:
            edit_policy_req3 = ""

    elif type=='create_link':
        select_role0 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 1 :", list_role_boucle)
        masking0 = st.selectbox('Choix du Masking 1', masking_du_sous_type)
        if masking0 == '1':
            masking0 = int(masking0)

        elif masking0 == "regexp_replace(val,'.+\@','*****@')":
            pass
        else:
            masking0 = f"'{masking0}'"


        list_role_boucle2 = []
        for s_r in list_role_boucle:
            if s_r in select_role0:
                pass
            else:
                list_role_boucle2.append(s_r)

        select_role2 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 2 :", list_role_boucle2)
        masking2 = st.selectbox('Choix du Masking 2', masking_du_sous_type)
        if masking2 == '1':
            masking2 = int(masking2)

        elif masking2 == "regexp_replace(val,'.+\@','*****@')":
            pass
        else:
            masking2 = f"'{masking2}'"
        list_role_boucle3 = []
        for s_r in list_role_boucle2:
            if s_r in select_role2:
                pass
            else:
                list_role_boucle3.append(s_r)

        select_role3 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 3 :", list_role_boucle3)
        masking3 = st.selectbox('Choix du Masking 3', masking_du_sous_type)
        if masking3 == '1':
            masking3 = int(masking3)

        elif masking3 == "regexp_replace(val,'.+\@','*****@')":
            pass
        else:
            masking3 = f"'{masking3}'"

        # transformation pour la requete de ['role1','role2'] => "'role1','role2'"
        string_roles0 = ""
        for x in select_role0:
            string_roles0 += "'" + x + "',"

        string_roles2 = ""
        for x in select_role2:
            string_roles2 += "'" + x + "',"

        string_roles3 = ""
        for x in select_role3:
            string_roles3 += "'" + x + "',"
        string_roles0 = string_roles0[:-1]
        string_roles2 = string_roles2[:-1]
        string_roles3 = string_roles3[:-1]

        if (len(string_roles2) ==0) and (len(string_roles3)==0):
            final_request = f"create masking policy {name_policy} as (val {selected_type}) returns {selected_type} ->" \
            f" " \
            f"case when (system$get_tag_on_current_column('SNOWFLAKE.CORE.{tag_selected}') = '{tag_value}') and current_role() in ({string_roles0}) =  then {masking0} " \
            f"else val " \
            f" end COMMENT = '{comment}';"

        elif (len(string_roles2)>0) and (len(string_roles3))==0:
            final_request =f"create masking policy {name_policy} as (val {selected_type}) returns {selected_type} ->" \
            f"case when (system$get_tag_on_current_column('SNOWFLAKE.CORE.{tag_selected}')= '{tag_value}') and current_role() in ({string_roles0})  then {masking0} " \
            f" when (system$get_tag_on_current_column('SNOWFLAKE.CORE.{tag_selected}')= '{tag_value}') and current_role() in ({string_roles2})  then {masking2} " \
            f"else val " \
            f" end COMMENT = '{comment}';"

        else:
            final_request = f"create masking policy {name_policy} as (val {selected_type}) returns {selected_type} ->" \
                            f"case when (system$get_tag_on_current_column('SNOWFLAKE.CORE.{tag_selected}')= '{tag_value}') and current_role() in ({string_roles0})  then {masking0} " \
                            f" when (system$get_tag_on_current_column('SNOWFLAKE.CORE.{tag_selected}')= '{tag_value}') and current_role() in ({string_roles2})  then {masking2} " \
                            f" when (system$get_tag_on_current_column('SNOWFLAKE.CORE.{tag_selected}')= '{tag_value}') and current_role() in ({string_roles3})  then {masking3} " \
                            f"else val "\
                            f" end COMMENT = '{comment}';"



    return final_request,string_roles0



def generation_sql(type,list_role_boucle,name_policy,comment,masking_du_sous_type,selected_type):
    if type=='edit':
        select_role0 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 1 :", list_role_boucle)
        masking0 = st.selectbox('Choix du Masking 1', masking_du_sous_type)
        if masking0 == '1':
            masking0 = int(masking0)
        list_role_boucle2 = []
        for s_r in list_role_boucle:
            if s_r in select_role0:
                pass
            else:
                list_role_boucle2.append(s_r)

        select_role2 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 2 :", list_role_boucle2)
        masking2 = st.selectbox('Choix du Masking 2', masking_du_sous_type)
        if masking2 == '1':
            masking2 = int(masking2)
        list_role_boucle3 = []
        for s_r in list_role_boucle2:
            if s_r in select_role2:
                pass
            else:
                list_role_boucle3.append(s_r)
        select_role3 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 3 :", list_role_boucle3)
        masking3 = st.selectbox('Choix du Masking 3', masking_du_sous_type)
        if masking3 == '1':
            masking3 = int(masking3)
        # transformation pour la requete de ['role1','role2'] => "'role1','role2'"
        string_roles0 = ""
        for x in select_role0:
            string_roles0 += "'" + x + "',"
        string_roles2 = ""
        for x in select_role2:
            string_roles2 += "'" + x + "',"
        string_roles3 = ""
        for x in select_role3:
            string_roles3 += "'" + x + "',"
        string_roles0 = string_roles0[:-1]
        string_roles2 = string_roles2[:-1]
        string_roles3 = string_roles3[:-1]
        st.write(masking0)
        #if type(masking0) == int:
        #    edit_policy_req0 = f"alter masking policy {name_policy} set body -> case when current_role() = {string_roles0} then {masking0} end SET COMMENT = {comment};"
        #else:
        edit_policy_req0 = f"alter masking policy {name_policy} set body -> case when current_role() in {string_roles0} then '{masking0}' else val end SET COMMENT = {comment};"

        if len(string_roles2) > 0:
            #if type(masking2) == int:
            #    edit_policy_req2 = f"alter masking policy {name_policy} set body -> case when current_role() in {string_roles2} then {masking2} end SET COMMENT = {comment};"
            #else:
            edit_policy_req2 = f"alter masking policy {name_policy} set body -> case when current_role() in {string_roles2} then '{masking2}' end SET COMMENT = {comment};"
        else:
            edit_policy_req2 = ""

        if len(string_roles3) > 0:
            #if type(masking3) == int:
            #    edit_policy_req3 = f"alter masking policy {name_policy} set body -> case when current_role() in {string_roles3} then {masking3} end SET COMMENT = {comment};"
            #else:
            edit_policy_req3 = f"alter masking policy {name_policy} set body -> case when current_role() in {string_roles3} then '{masking3}' end SET COMMENT = {comment};"
        else:
            edit_policy_req3 = ""

        edit_policy_req = edit_policy_req0 + edit_policy_req2 + edit_policy_req3

    elif type =='create':
        select_role0 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 1 :", list_role_boucle)
        masking0 = st.selectbox('Choix du Masking 1', masking_du_sous_type)
        if masking0 == '1':
            masking0 = int(masking0)

        elif masking0 == "regexp_replace(val,'.+\@','*****@')":
            pass
        else:
            masking0 = f"'{masking0}'"
        list_role_boucle2 = []
        for s_r in list_role_boucle:
            if s_r in select_role0:
                pass
            else:
                list_role_boucle2.append(s_r)

        select_role2 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 2 :", list_role_boucle2)
        masking2 = st.selectbox('Choix du Masking 2', masking_du_sous_type)
        if masking2 == '1':
            masking2 = int(masking2)

        elif masking2 == "regexp_replace(val,'.+\@','*****@')":
            pass
        else:
            masking2 = f"'{masking2}'"




        list_role_boucle3 = []
        for s_r in list_role_boucle2:
            if s_r in select_role2:
                pass
            else:
                list_role_boucle3.append(s_r)

        select_role3 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 3 :", list_role_boucle3)
        masking3 = st.selectbox('Choix du Masking 3', masking_du_sous_type)
        if masking3 == '1':
            masking3 = int(masking3)


        elif masking3 == "regexp_replace(val,'.+\@','*****@')":

            pass

        else:

            masking3 = f"'{masking3}'"

        # transformation pour la requete de ['role1','role2'] => "'role1','role2'"
        string_roles0 = ""
        for x in select_role0:
            string_roles0 += "'" + x + "',"

        string_roles2 = ""
        for x in select_role2:
            string_roles2 += "'" + x + "',"

        string_roles3 = ""
        for x in select_role3:
            string_roles3 += "'" + x + "',"
        string_roles0 = string_roles0[:-1]
        string_roles2 = string_roles2[:-1]
        string_roles3 = string_roles3[:-1]

        final_request = f"create or replace masking policy {name_policy} as (val {selected_type}) returns {selected_type} -> case when current_role() in ({string_roles0}) then {masking0} else val end COMMENT = '{comment}';"

        if len(string_roles2) > 0:
            final_request = f"create or replace masking policy {name_policy} as (val {selected_type}) returns {selected_type} -> case " \
                            f" when current_role() in ({string_roles0}) then {masking0}" \
                            f" when current_role() in ({string_roles2}) then {masking2} " \
                            f" else val" \
                            f" end COMMENT = '{comment}';"


        if len(string_roles3) > 0:
            final_request = f"create or replace masking policy {name_policy} as (val {selected_type}) returns {selected_type} -> case " \
                            f" when current_role() in ({string_roles0}) then {masking0}" \
                            f" when current_role() in ({string_roles2}) then {masking2} " \
                            f" when current_role() in ({string_roles3}) then {masking3} " \
                            f" else val" \
                            f" end COMMENT = '{comment}';"



    return final_request,string_roles0


def drop_role_from_list(list_role,role_selected):
    list_roles2 = []
    for r in list_role:
        if r in role_selected:
            pass
        else:
            list_roles2.append(r)

    return list_roles2

def role_list_transformation(list_role):
    role_string = ""
    for x in list_role:
        role_string += "'" + x + "',"
    role_string = role_string[:-1]

    return role_string

def role_list_transformation_from_snow(list_role):
    list_role = ast.literal_eval(list_role)
    list_role = role_list_transformation(list_role)

    return list_role