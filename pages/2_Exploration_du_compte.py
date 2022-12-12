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
import json
import toolbox_cache as tb
import plotly.express as px
from datetime import datetime
from datetime import date
import plotly.graph_objects as go
import networkx as nx
import policy_modification as pm
import ast
import itertools
from collections import Counter

import csv


def aggrid_interactive_table_for_tables(df):
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
    options.configure_pagination(
        enabled=True, paginationAutoPageSize=False,
        paginationPageSize=20)
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        update_mode=GridUpdateMode.MODEL_CHANGED
    )

    return selection


def update_point(trace, points, selector):
    for i in points.point_inds:
        st.write(i)


def aggrid_interactive_table_for_semantics(df):
    """Creates an st-aggrid interactive table based on a dataframe.

    Args:
        df (pd.DataFrame]): Source dataframe

    Returns:
        dict: The selected row
    """

    options = GridOptionsBuilder.from_dataframe(
        df, enableRowGroup=True, enableValue=True, enablePivot=True
    )
    options.configure_selection(use_checkbox=True, selection_mode='single')
    options.configure_side_bar()
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        update_mode=GridUpdateMode.MODEL_CHANGED
    )

    return selection


st.markdown("# 	Exploration Metadonnées")

if not st.session_state.load_state:
    st.error("Veuillez vous connecter a la page d'accueil")

else:
    # today = np.datetime64('today')
    today = datetime.today()
    account_snow = st.session_state.account_snow
    user_snow = st.session_state.user_snow
    mdp_snow = st.session_state.mdp_snow
    role_list_select = tb.list_roles(user_snow, account_snow, mdp_snow)
    role_selected = st.sidebar.selectbox(
        "ROLE Utilisé",
        role_list_select
    )
    st.session_state.role = role_selected

    if st.button('MAJ de la table'):
        con = snowflake.connector.connect(
            user=st.session_state.user_snow,
            account=st.session_state.account_snow,
            password=st.session_state.mdp_snow)

        schemas_in_account = pd.read_sql('show columns in account', con)
        masking_polices = pd.read_sql('select * from bac_a_sable.masking_policies.masking_policy', con)

        schemas_in_account = schemas_in_account[schemas_in_account['database_name'].isin(['SEATABLE', 'BAC_A_SABLE'])]
        schemas_in_account = schemas_in_account[['database_name', 'table_name', 'schema_name', 'column_name']]
        tags_applied = pd.read_sql(
            'select * from snowflake.account_usage.tag_references order by tag_name, domain, object_id;', con)
        con.close()
        tags_applied = tags_applied[
            ['TAG_DATABASE', 'TAG_SCHEMA', 'TAG_NAME', 'TAG_VALUE', 'OBJECT_DATABASE', 'OBJECT_SCHEMA', 'OBJECT_NAME',
             'DOMAIN', 'COLUMN_NAME']]
        tags_applied_table = tags_applied[tags_applied['DOMAIN'] == 'TABLE']
        # tags_applied_table = tags_applied_table[['']]
        tags_applied_column = tags_applied[tags_applied['DOMAIN'] == 'COLUMN']

        schemas_with_table = pd.merge(schemas_in_account, tags_applied_table[
            ['OBJECT_DATABASE', 'OBJECT_SCHEMA', 'OBJECT_NAME', 'TAG_NAME', 'TAG_VALUE']], how='left',
                                      left_on=['database_name', 'schema_name', 'table_name'],
                                      right_on=['OBJECT_DATABASE', 'OBJECT_SCHEMA', 'OBJECT_NAME'])
        schemas_with_table_and_column = pd.merge(schemas_with_table, tags_applied_column[
            ['OBJECT_DATABASE', 'OBJECT_SCHEMA', 'OBJECT_NAME', 'TAG_NAME', 'TAG_VALUE', 'COLUMN_NAME']], how='left',
                                                 left_on=['database_name', 'schema_name', 'table_name', 'column_name'],
                                                 right_on=['OBJECT_DATABASE', 'OBJECT_SCHEMA', 'OBJECT_NAME',
                                                           'COLUMN_NAME'])
        schemas_with_table_and_column = schemas_with_table_and_column[
            schemas_with_table_and_column['schema_name'] != 'INFORMATION_SCHEMA']
        schemas_with_table_and_column.to_pickle('table_infos.pkl')
        masking_polices.to_pickle('masking_polices.pkl')

    schemas_with_table_and_column = pd.read_pickle('table_infos.pkl')
    masking_polices = pd.read_pickle('masking_polices.pkl')
    col1, col2, col3 = st.columns([1, 4, 4])



    with col1:
        nb_databases = len(np.unique(schemas_with_table_and_column['database_name'].values))
        nb_schemas = 0
        nb_tables = 0
        nb_cols = 0
        for d in np.unique(schemas_with_table_and_column['database_name'].values):
            temp = schemas_with_table_and_column[schemas_with_table_and_column['database_name'] == d]
            nb_schemas += len(np.unique(temp['schema_name'].values))
            for s in np.unique(temp['schema_name'].values):
                temp2 = temp[temp['schema_name'] == s]
                nb_tables += len(np.unique(temp2['table_name']))
                for t in np.unique(temp2['table_name']):
                    temp3 = temp2[temp2['table_name'] == t]
                    nb_cols += len(np.unique(temp3['column_name'].values))

        st.markdown('**Informations globales sur le compte:**')
        st.write(f'Nombre de bases: {str(nb_databases)}')
        st.write(f'Nombre de schemas: {str(nb_schemas)}')
        st.write(f'Nombre de tables: {str(nb_tables)}')
        st.write(f'Nombre de colonnes: {str(nb_cols)}')
        st.markdown('-----------------------------------')

    state_audit = []
    nb_audit = []
    db = []
    for d in np.unique(schemas_with_table_and_column['database_name'].values):
        temp = schemas_with_table_and_column[schemas_with_table_and_column['database_name'] == d]

        temp_audit = temp[temp['TAG_NAME_x'] == 'AUDITDATE']
        temp_non_audit = temp[temp['TAG_NAME_x'] != 'AUDITDATE']
        nb_audit.append(len(np.unique(temp_audit['table_name'].values)))
        state_audit.append('audit')
        db.append(d)

        nb_audit.append(len(np.unique(temp_non_audit['table_name'].values)))
        state_audit.append('non_audit')
        db.append(d)

    df_for_chart = pd.DataFrame()
    df_for_chart['base'] = db
    df_for_chart['state_audit'] = state_audit
    df_for_chart['count'] = nb_audit

    fig = px.bar(df_for_chart, x="base", y="count", color="state_audit",
                 title="Nombre de table auditée au sain de chaque base")
    with col2:
        st.plotly_chart(fig, use_container_width=True)

    audited_table = schemas_with_table_and_column[~schemas_with_table_and_column['TAG_VALUE_x'].isna()]
    moy_date = audited_table.drop_duplicates(subset=['database_name', 'schema_name', 'table_name'])
    dates = []
    dbs = []
    for d in moy_date['TAG_VALUE_x']:
        date_in_table = datetime.strptime(d, '%d%m%Y')
        dates.append((today - date_in_table).days)
        dbs.append(d)
    moy_date['duration'] = dates

    with col3:
        fig = px.box(moy_date, x="database_name", y="duration", title='Boxplot des temps ', points="all")
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns([2, 1])

    with col1:

        role_list_select = tb.list_roles(user_snow, account_snow, mdp_snow)
        role_list_select = list(dict.fromkeys(role_list_select))
        st.header('Informations sur les rôles et politiques de maskings:')
        st.markdown(f"Nombre de rôles sur la base {str(len(role_list_select))}")
        st.markdown(f"Nombre de politiques de maskings {str(len(masking_polices))}")

        roles = []
        valeur_tag = []
        visibilite = []

        for _, row in masking_polices.iterrows():

            temp_r = ast.literal_eval(row['ROLE_GROUPE'])
            for rr in temp_r:
                roles.append(rr)
                valeur_tag.append(row['VALEUR_TAG'])
                visibilite.append(row['VISIBILITE'])

        role_df = pd.DataFrame(
            {"role_name": list(dict.fromkeys(roles))}
        )

        role_df['value_id_role'] = np.arange(len(role_df))

        valeur_tag_df = pd.DataFrame()
        valeur_tag_df['tag_value'] = list(dict.fromkeys(valeur_tag))
        valeur_tag_df['value_id_tag'] = np.arange(len(role_df), len(role_df) + len(valeur_tag_df))

        for_network = pd.DataFrame(np.transpose([roles, valeur_tag, visibilite]),
                                   columns=['role', 'valeur', 'visibilite'])
        for_network = pd.merge(for_network, role_df, how='left', left_on='role', right_on='role_name')
        for_network = pd.merge(for_network, valeur_tag_df, how='left', left_on='valeur', right_on='tag_value')
        # for_network = for_network.drop_duplicates(subset = ['role','valeur'])

        maskings_colors = pd.DataFrame()
        maskings_colors['visibilite'] = np.unique(masking_polices['VISIBILITE'])
        maskings_colors['colors'] = 'red'
        maskings_colors = maskings_colors[~maskings_colors['visibilite'].isin(['********', 'visible'])]

        masking_colors_add = pd.DataFrame(
            {
                'visibilite': ['********', 'visible'],
                'colors': ['green', 'blue']
            }
        )

        maskings_colors = pd.concat([maskings_colors, masking_colors_add])
        for_network = pd.merge(for_network, maskings_colors, how='left', left_on='visibilite', right_on='visibilite')

        G = nx.Graph()  # Use the Graph API to create an empty network graph object

        node_list = []
        # Add nodes and edges to the graph object
        for i in for_network["role_name"].unique():
            G.add_node(i)
            node_list.append(i)

        for i in for_network["tag_value"].unique():
            G.add_node(i)
            node_list.append(i)

        for i, j in for_network.iterrows():
            G.add_edges_from([(j["role_name"], j["tag_value"])])

        pos = nx.random_layout(G)

        for n, p in pos.items():
            G.nodes[n]['pos'] = p

        edge_trace1 = go.Scatter(
            x=[],
            y=[],
            line=dict(width=1, color='green'),
            hoverinfo='none', name="*****",
            mode='lines')

        edge_trace2 = go.Scatter(
            x=[],
            y=[],
            line=dict(width=1, color='red'),
            hoverinfo='none',
            mode='lines', name="autres")

        edge_trace3 = go.Scatter(
            x=[],
            y=[],
            line=dict(width=1, color='blue'),
            hoverinfo='none',
            mode='lines', name='visible')


        for i, edge in enumerate(G.edges()):
            color = for_network[for_network['role_name'] == edge[0]]
            color = color[color['tag_value'] == edge[1]]['colors'].values[0]

            x0, y0 = G.nodes[edge[0]]['pos']
            x1, y1 = G.nodes[edge[1]]['pos']
            if color == 'red':

                edge_trace2['x'] += tuple([x0, x1, None])
                edge_trace2['y'] += tuple([y0, y1, None])
            elif color == 'green':
                edge_trace1['x'] += tuple([x0, x1, None])
                edge_trace1['y'] += tuple([y0, y1, None])

            else:
                edge_trace3['x'] += tuple([x0, x1, None])
                edge_trace3['y'] += tuple([y0, y1, None])

        # Adding nodes to plotly scatter plot
        node_trace = go.Scatter(
            x=[],
            y=[],
            text=[], name='Role/Tag',
            mode='markers+text',
            hoverinfo='text',
            marker=dict(
                showscale=False,
                color=[],
                size=65
            ),
            textposition="middle center",
            line=dict(width=0))



        for node in G.nodes():
            x, y = G.nodes[node]['pos']
            node_trace['x'] += tuple([x])
            node_trace['y'] += tuple([y])

        node_adjacencies = []
        node_text = []
        symboles = []
        for node, adjacencies in enumerate(G.adjacency()):
            if node > max(for_network['value_id_role'].values):
                node_adjacencies.append('DodgerBlue')
                node_text.append(
                    f"{for_network[for_network['value_id_tag'] == node]['tag_value'].values[0]}")
                symboles.append('circle')
            else:
                node_adjacencies.append('Gainsboro')
                node_text.append(f"{for_network[for_network['value_id_role'] == node]['role_name'].values[0]}")
                symboles.append('square')

            node_trace['marker']['color'] = node_adjacencies
            node_trace['marker']['symbol'] = symboles
            node_trace['text'] = node_text
        # Plot the final figure
        fig = go.FigureWidget(data=[edge_trace1, edge_trace2, edge_trace3, node_trace],
                        layout=go.Layout(
                            title='Connection Roles -> Tags',  # title takes input from the user
                            title_x=0.45,
                            titlefont=dict(size=25),
                            showlegend=True,
                            hovermode='closest',
                            margin=dict(b=20, l=5, r=5, t=40),
                            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))

        st.plotly_chart(fig, use_container_width=True)  # Show the graph in streamlit


    with col2:

        st.markdown("Tables audité depuis trop longtemps")
        time_overdated = st.slider('Temps (en jours)', 0, 1000, 10)
        tables_over_dated = moy_date[moy_date['duration'] > time_overdated]
        for _, row in tables_over_dated.iterrows():
            st.error(f"Table {row['database_name']}.{row['schema_name']}.{row['table_name']}")


    st.header('Exploration par base de données')
    base_selected = st.selectbox('Choisir une base', np.unique(schemas_with_table_and_column['database_name'].values))
    from_selection = schemas_with_table_and_column[schemas_with_table_and_column['database_name'] == base_selected]
    from_selection_unique_table = from_selection.drop_duplicates(subset=['database_name', 'schema_name', 'table_name'])
    from_selection_audit = from_selection_unique_table.dropna(subset=['TAG_NAME_x'])
    from_selection_unique_non_adit = from_selection_unique_table[from_selection_unique_table['TAG_NAME_x'].isna()]

    from_selection_tagged = from_selection.dropna(subset=['TAG_NAME_y'])
    from_selection_non_tagged = from_selection[from_selection['TAG_NAME_y'].isna()]

    df_for_chart = pd.DataFrame()
    df_for_chart['base'] = ['table', 'table', 'column', 'column']
    df_for_chart['audited'] = ['Audité', 'Non audité', 'taggé', 'non_taggé']
    df_for_chart['count'] = [len(from_selection_unique_table), len(from_selection_unique_non_adit),
                             len(from_selection_tagged), len(from_selection_non_tagged)]

    col1, col2 = st.columns(2)

    schemas = []
    audited_list = []
    count = []
    with col1:
        for s in np.unique(from_selection['schema_name']):
            temp = from_selection[from_selection['schema_name'] == s]

            audited = temp.dropna(subset=['TAG_NAME_x'])
            audited = audited.drop_duplicates(subset=['database_name', 'schema_name', 'table_name'])
            non_adited = temp[temp['TAG_NAME_x'].isna()]
            non_adited = non_adited.drop_duplicates(subset=['database_name', 'schema_name', 'table_name'])
            tagged = temp.dropna(subset=['TAG_NAME_y'])
            non_tagged = temp[temp['TAG_NAME_y'].isna()]

            schemas.append(s + '_table')
            schemas.append(s + '_table')
            schemas.append(s + '_col')
            schemas.append(s + '_col')

            audited_list.append('audité')
            audited_list.append('non audité')
            audited_list.append('taggé')
            audited_list.append('non taggé')

            count.append(len(audited))
            count.append(len(non_adited))
            count.append(len(tagged))
            count.append(len(non_tagged))

        for_chart = pd.DataFrame()
        for_chart['schema'] = schemas
        for_chart['audite'] = audited_list
        for_chart['count'] = count

        fig = px.bar(for_chart, x="schema", y="count", title="Bar Plot schema based", color='audite')

        st.plotly_chart(fig, use_container_width=True)  # Show the graph in streamlit

    with col2:
        counters = Counter(from_selection['TAG_VALUE_y'].values)
        counters_df = pd.DataFrame.from_records(list(dict(counters).items()), columns=['tag', 'count'])
        counters_df = counters_df.dropna()
        counters_df = counters_df[~counters_df['tag'].isna()]
        counters_df = pd.merge(counters_df, from_selection[['TAG_NAME_y', 'TAG_VALUE_y']], how='left', left_on='tag',
                               right_on='TAG_VALUE_y')
        counters_df = counters_df.drop_duplicates(subset=['tag', 'count', 'TAG_NAME_y'])
        fig = px.bar(counters_df, x="TAG_VALUE_y", y="count", title="Tag au sein de la base", color='TAG_NAME_y')
        st.plotly_chart(fig, use_container_width=True)  # Show the graph in streamlit

    st.header('Exploration du schéma:')
    schema_selected = st.selectbox('Choisir un schema', np.unique(from_selection['schema_name'].values))
    schema_df = from_selection[from_selection['schema_name'] == schema_selected]
    audited = schema_df[schema_df['TAG_NAME_x'] == 'AUDITDATE']

    col1, col2,col3 = st.columns([1, 3, 3])
    with col1:
        st.header('Tables audités')
        if len(audited) == 0:
            st.error('Aucune table audité sur ce schéma')
        else:
            dfs = tb.collect_samples_from_tag(audited, base_selected, schema_selected, st.session_state.user_snow, st.session_state.account_snow,
                                     st.session_state.mdp_snow, role_selected)

        for t in np.unique(dfs['table']):
            st.text(t)

    with col2:
        audited_without_nan = audited[['table_name','TAG_VALUE_y']]
        small_audited = audited_without_nan.fillna('Non taggée')
        dfs_tags = pd.DataFrame()
        for t in np.unique(small_audited['table_name'].values):
            temp = small_audited[small_audited['table_name'] == t]
            counts_tags = Counter(temp['TAG_VALUE_y'])
            counts_tags_df = pd.DataFrame.from_records(list(dict(counts_tags).items()), columns=['tag', 'count'])
            counts_tags_df['table'] = t
            dfs_tags = pd.concat([dfs_tags,counts_tags_df])

        fig = px.bar(dfs_tags, x='table', y='count',color='tag')
        st.plotly_chart(fig, use_container_width=True)  # Show the graph in streamlit

        f_d = fig.data[0]
        f_d.on_click(update_point)


    with col3:
        final_audited = audited.dropna(subset=['TAG_VALUE_y'])
        tag_selected = st.selectbox('Chosissez une valeur de tag', np.unique(final_audited['TAG_VALUE_y']))
        col_name = final_audited[final_audited['TAG_VALUE_y'] == tag_selected][['table_name', 'column_name']]
        st.text('Extrait de la colonne taggé')
        for _, row in col_name.iterrows():
            st.text(row['table_name'])
            st.table(dfs[dfs['table']==row['table_name']][row['column_name']])

