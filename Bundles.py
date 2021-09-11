import configparser

from elasticsearch import Elasticsearch
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from prefect import task


def upload_data(_es, data_record, index_name):
    """
        This method connects with ES and upload to the needed index
        :param _es elasticsearch config object
        :param data_record: records to be uploaded: parsed receipt in our case
        :param index_name: the index
    """
    print(data_record)

    if data_record:

        _es.index(
            index=index_name,
            body=data_record)
        _es.indices.refresh(index=index_name)
        print("uploaded successfully")
        return _es
    else:
        print("No data to upload")


def scroll(_es, _index, body, _scroll, size, **kw):
    page = _es.search(index=_index, body=body, scroll=_scroll, size=size, **kw)

    scroll_id = page['_scroll_id']
    _hits = page['hits']['hits']
    while len(_hits):
        yield _hits
        page = _es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = page['_scroll_id']
        _hits = page['hits']['hits']


def create_es_client():
    config = configparser.ConfigParser()
    config.read('example.ini')

    # create a client instance of Elasticsearch
    return Elasticsearch(cloud_id=config['ELASTIC']['cloud_id'],
                         http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password']))


def get_data():
    return {
        "query": {
            "match_all": {}
        }
    }


def encode_units(x):
    if x <= 0:
        return 0
    if x >= 1:
        return 1


def build_rules(df):
    basket = (
        df.groupby(['Transaction_id', 'item_name'])['quantity'].sum().unstack().reset_index().fillna(0).set_index(
            'Transaction_id'))
    basket_sets = basket.applymap(encode_units)

    # filter by support level
    frequent_itemsets = apriori(basket_sets, min_support=0.1, use_colnames=True)
    # filter association rules by lift level
    rules = association_rules(frequent_itemsets, metric="lift", min_threshold=0)
    rules.head()

    # print(rules)
    return rules


def convert_records_to_df(_results):
    import pandas as pd
    if _results:
        receipt_table_list = []
        final_dataframes = []
        dataframes = []
        for records in _results:
            for record in records:

                key = record._source
                user_name = key.User
                total = str(key.TOTAL)
                # TODO replace the items_count
                items_count = float(total) / 10
                cash = str(key.Cash)
                paid_amount = total
                items_list = key.Items
                transaction_id = key.Transaction_id
                transaction_datetime = key.datetime
                total_quantity = 0

                df = pd.DataFrame(
                    [transaction_id, i.Item_name, i.Quantity, i.Price] for i in items_list
                )
                if not df.empty:
                    df.columns = ['Transaction_id', 'item_name', 'quantity', 'price']

                    receipt_table_list.append(df)
                    for i in items_list:
                        total_quantity = total_quantity + int(i.Quantity)
                    json_df = {
                        "cashier_name": user_name,
                        "total": total,
                        'items_count': items_count,
                        "cash": cash,
                        "paid_amount": paid_amount,
                        "items_list": items_list,
                        "Transaction_id": transaction_id,
                        "transaction_datetime": transaction_datetime,
                        'Total_quantity': total_quantity
                    }
                    temp_df = pd.json_normalize(json_df)

                    dataframes.append(temp_df)

            if receipt_table_list:

                for i in dataframes:
                    items_table_df = pd.concat(receipt_table_list)
                    final_df_temp = pd.merge(items_table_df,
                                             i, how="inner", on="Transaction_id")
                    final_dataframes.append(final_df_temp)

        df = pd.concat(final_dataframes)

        return df
    else:
        "No data found"


from types import SimpleNamespace


def get_and_structure_data(es_client, query_body, index_name):
    import json
    results_list = []
    for _hits in scroll(es_client, index_name, query_body, '2m', 70):
        results_list.append(json.loads(json.dumps(_hits, indent=4), object_hook=lambda d: SimpleNamespace(**d)))
    return results_list


@task()
def run_workflow():
    index_name = 'receipts_row_data'
    es = create_es_client()
    get_records_query = get_data()
    df = convert_records_to_df(get_and_structure_data(es, get_records_query, index_name))
    build_rules(df)
    final_df = build_rules(df)
    import json
    from datetime import datetime
    current_datetime = (datetime.now())

    current_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    current_datetime = datetime.strptime(current_datetime, '%Y-%m-%d %H:%M:%S')

    for index, row in final_df.iterrows():
        antecedents = (list(frozenset(row['antecedents'])))
        consequents = (list(frozenset(row['consequents'])))

        print(consequents, antecedents)

        json_dataframes = {
            "antecedents": antecedents,
            'consequents': consequents,
            'support': row['support'],
            'confidence': row['confidence'],
            'lift': row['lift'],
            'datetime': current_datetime.isoformat()}
        upload_data(es, json.dumps(json_dataframes), "promotions_and_bundles")
    return final_df


from prefect import Flow

flow = Flow("dashboard_promotions", tasks=[run_workflow])

import os

os.system("prefect auth login -k jCnFYiIZoczd9Lx2Ql49cw")
flow.register(project_name="'Calculations_workflow'")

flow.run_agent(token="jCnFYiIZoczd9Lx2Ql49cw")
