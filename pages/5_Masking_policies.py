import snowflake.connector
import streamlit as st
import pandas as pd
import toolbox_cache as tb
import numpy as np
import policy_modification as pm
import ast
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
from random import randint


### FUNCTIONS


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


st.title('MASKING POLICIES SNOWFLAKE')
st.markdown('Cette page permet de créer modifier ou supprimer une politique de masking')

tags_values_allowed = pd.read_csv('tags_value_masking_type.csv', index_col=0)
erreur = False

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


    masking_policies = tb.get_table_from_db_sch_memo('bac_a_sable', 'masking_policies', 'MASKING_POLICY',
                                                     st.session_state.user_snow, st.session_state.account_snow,
                                                     st.session_state.mdp_snow, role_selected)

    tags = tb.show_tags_in_account(st.session_state.user_snow, st.session_state.account_snow,
                                   st.session_state.mdp_snow, role_selected)

    st.header('Liste des politiques de masking sur le compte:')
    st.markdown('Pour supprimer ou editer une policy, veuillez cocher la case ')

    selection_row = aggrid_interactive_table(masking_policies, 'multiple')


    col_but1, col_but2, col_but3 = st.columns(3)
    with col_but1:
        if st.button('Créer une policy'):
            st.session_state.policy_creation_state = True
            st.session_state.policy_edit_state = False
            st.session_state.policy_del_state = False

    with col_but2:
        if st.button('Supprimer une policy'):
            st.session_state.policy_del_state = True
            st.session_state.policy_creation_state = False
            st.session_state.policy_edit_state = False

    with col_but3:
        if st.button('Editer une policy'):
            st.session_state.policy_edit_state = True
            st.session_state.policy_del_state = False
            st.session_state.policy_creation_state = False

    if st.session_state.policy_creation_state:
        st.header("Création d'une policy")
        st.markdown('---------------------------------------------------------------------')
        row_id_to_add = 0
        while (row_id_to_add in masking_policies['ROW_ID'].values):
            row_id_to_add = randint(0, 10000)

        policy_name = st.text_input('Nom de la policy')
        if len(policy_name)==0:
            st.error('Entrer un nom de policy')
        else:

            tag_selected = st.selectbox('Choisir un tag', tags['name'].values)
            tags_table_from_selection = tags[tags['name'] == tag_selected]
            if not tags_table_from_selection['allowed_values'].values[0]:
                st.error('Choisir un autre tag')
            else:
                tags_names = ast.literal_eval(tags_table_from_selection['allowed_values'].values[0])
                tag_name = st.selectbox('Choisir une valeur de tag', tags_names)
                st.markdown('---------------------------------------------------------------------')
                table_for_selected_tag = tags_values_allowed[tags_values_allowed['valeur_fr'] == tag_name]
                data_type = table_for_selected_tag['type'].values[0]
                list_roles = pm.list_role(st.session_state.user_snow, st.session_state.account_snow,
                                          st.session_state.mdp_snow)
                select_role1 = st.multiselect("Selectionner un ou plusieurs rôles pour masking 1 :", list_roles)
                masking1 = st.selectbox("Selectionner la règle pour le masking 1 :",
                                        table_for_selected_tag['masking'].values)

                to_test = masking_policies[masking_policies['VALEUR_TAG'] == tag_name]
                list_roles_in_table = []
                for _, row in to_test.iterrows():
                    for r in ast.literal_eval(row['ROLE_GROUPE']):
                        list_roles_in_table.append(r)

                for r in select_role1:
                    if r in list_roles_in_table:
                        erreur = True

                st.markdown('---------------------------------------------------------------------')

                if erreur:
                    st.error("Cette valeur de tag et ce role sont déja liés au sein d'une policy")
                else:
                    if st.button('Ajouter la policy'):
                        insert_request = f"insert into bac_a_sable.MASKING_POLICIES.masking_policy select {row_id_to_add},'masking_tag_rgpd','{policy_name}','RGPD_STATUS', '{tag_name}', '{data_type}', 1,{select_role1}, '{masking1}' ;"
                        with st.spinner('MAJ de la table des policy'):
                            tb.insert_snowflake(insert_request, st.session_state.user_snow, st.session_state.account_snow,
                                                st.session_state.mdp_snow, role_selected)
                        with st.spinner('MAJ de la policy'):
                            tb.update_masking_policy_table(st.session_state.user_snow, st.session_state.account_snow,
                                                           st.session_state.mdp_snow, role_selected)


                        st.session_state.policy_creation_state = False
                        st.session_state.policy_del_state = False
                        st.session_state.masking_policies_state = False
                        st.session_state.masking_policies_save = False
                        st.session_state.tags_save = False
                        st.experimental_memo.clear()
                        st.experimental_rerun()




    if st.session_state.policy_del_state:
        if len(selection_row['selected_rows']) > 0:

            del_request = ""
            for i in range(len(selection_row['selected_rows'])):
                del_request = del_request + f"delete from bac_a_sable.masking_policies.MASKING_POLICY where ROW_ID = {selection_row['selected_rows'][i]['ROW_ID']};"
            with st.spinner('MAJ de la table des policy'):
                tb.delete_snowflake(del_request, st.session_state.user_snow, st.session_state.account_snow,
                                    st.session_state.mdp_snow, role_selected)
            with st.spinner('MAJ de la policy'):
                tb.update_masking_policy_table(st.session_state.user_snow, st.session_state.account_snow,
                                               st.session_state.mdp_snow, role_selected)

                st.session_state.policy_creation_state = False
                st.session_state.policy_del_state = False
                st.session_state.masking_policies_state = False
                st.session_state.masking_policies_save = False
                st.session_state.tags_save = False
                st.experimental_memo.clear()
                st.experimental_rerun()



        else:
            st.error('Choisir UNE table à supprimer')
            st.session_state.policy_del_state = False

    if st.session_state.policy_edit_state:
        st.header("Edition d'une policy")
        st.markdown('---------------------------------------------------------------------')
        st.markdown('Cochez la case correspondant à la policy que vous souhaitez éditer')
        if len(selection_row['selected_rows']) == 1:
            tag_selected = selection_row['selected_rows'][0]['NOM_TAG']
            tags_table_from_selection = tags[tags['name'] == tag_selected]
            tags_names = ast.literal_eval(tags_table_from_selection['allowed_values'].values[0])

            col1, col2 = st.columns(2)

            with col1:
                st.markdown('Valeurs à remplacer')
                valeur_tag_base = st.text_input('VALEUR_TAG', selection_row['selected_rows'][0]['VALEUR_TAG'])
                valeur_role_base = st.text_input('ROLE_GROUPE', selection_row['selected_rows'][0]['ROLE_GROUPE'])
                valeur_mask_base = st.text_input('VISIBILITE', selection_row['selected_rows'][0]['VISIBILITE'])

            with col2:
                st.markdown('Nouvelles valeurs')
                valeur_tag_edit = st.selectbox('VALEUR_TAG', tags_names)
                table_for_selected_tag = tags_values_allowed[tags_values_allowed['valeur_fr'] == valeur_tag_edit]
                data_type = table_for_selected_tag['type'].values[0]
                list_roles = pm.list_role(st.session_state.user_snow, st.session_state.account_snow,
                                          st.session_state.mdp_snow)
                select_role_edit = st.multiselect("Selectionner un ou plusieurs rôles pour masking 1 :", list_roles)
                maskingedit = st.selectbox("Selectionner la règle pour le masking 1 :",
                                           table_for_selected_tag['masking'].values)



            to_test = masking_policies[masking_policies['VALEUR_TAG'] == valeur_tag_edit]
            list_roles_in_table = []
            for _, row in to_test.iterrows():
                for r in ast.literal_eval(row['ROLE_GROUPE']):
                    list_roles_in_table.append(r)



            for r in select_role_edit:
                if r in list_roles_in_table:
                    erreur = True

            st.markdown('---------------------------------------------------------------------')

            if erreur:
                st.error("Cette valeur de tag et ce role sont déja liés au sein d'une policy")

            else:
                if st.button('Modifier la policy'):
                    with st.spinner('MAJ de la table des policy'):

                        edit_req = f" update bac_a_sable.masking_policies.masking_policy set TYPE_TAG = '{data_type}',valeur_tag = '{valeur_tag_edit}', VISIBILITE = '{maskingedit}', ROLE_GROUPE = {select_role_edit} where masking_policy.row_id = {selection_row['selected_rows'][0]['ROW_ID']}; "
                        tb.edit_snowflake(edit_req, st.session_state.user_snow, st.session_state.account_snow,
                                          st.session_state.mdp_snow, role_selected)

                    with st.spinner('MAJ de la policy'):
                        tb.update_masking_policy_table(st.session_state.user_snow, st.session_state.account_snow,
                                                       st.session_state.mdp_snow, role_selected)


                    st.session_state.policy_creation_state = False
                    st.session_state.policy_del_state = False
                    st.session_state.masking_policies_state = False
                    st.session_state.masking_policies_save = False
                    st.session_state.tags_save = False
                    st.session_state.policy_edit_state = False
                    st.experimental_memo.clear()
                    st.experimental_rerun()


        elif len(selection_row['selected_rows']) > 1:
            st.error('Choisir UNE unique table à editer')

        else:
            st.error('Choisir UNE table à editer')
