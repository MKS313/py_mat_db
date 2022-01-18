import os
import pandas as pd
import numpy as np
from db.dbmanager import DBManager


# query_del = query_db(co_id_list, ind_date_list)
# db.del_from_col(query_del, db_name=db_insert['db_name'], col_name=db_insert['col_name'])
# db.insert_to_col(data_to_db_list, db_name=db_insert['db_name'], col_name=db_insert['col_name'])

def create_db_manager(db_address='127.0.0.1:27017', db_user_name=None, db_password=None):
    #
    db_ip = db_address.split(':')[0]
    db_port = int(db_address.split(':')[1])
    if db_user_name in ['', ' ', '-']:
        db_user_name = None

    if db_password in ['', ' ', '-']:
        db_password = None
    return DBManager(host_address=db_ip, port=db_port, user_name=db_user_name, password=db_password)


def get_db(db_address='127.0.0.1:27017', db_user_name=None, db_password=None, db_name=None, coll=None, fields=None,
           co_ids=None, ind_dates=None, is_mat=None):

    #
    db = create_db_manager(db_address=db_address, db_user_name=db_user_name, db_password=db_password)

    if co_ids in ['', ' ', '-']:
        co_ids = None

    if ind_dates in ['', ' ', '-']:
        ind_dates = None

    fields = fields.split('__')

    # print('1')
    # print(co_ids)
    # print(ind_dates)
    # print('2')

    # fill query
    if co_ids is None and ind_dates is None:
        query = {}
        d_select = {}
    else:
        # check co_ids
        if str(co_ids) == 'all':
            co_id_q = {'$exists': True}
        elif len(co_ids) > 0:
            co_id_q = {'$in': co_ids}

        # check ind_dates
        if str(ind_dates) == 'all':
            ind_date_q = {'$exists': True}
        elif len(ind_dates) > 0:
            ind_date_q = {'$in': ind_dates}

        # create query and d_select
        if ind_dates is None:
            query = {'co_id': co_id_q}
            d_select = {'co_id': True}
        else:
            query = {'co_id': co_id_q, 'ind_date': ind_date_q}
            d_select = {'co_id': True, 'ind_date': True}

    # fill d_select
    # d_select = {'_id': False, 'update_time': False}
    for f in fields:
        d_select[f] = True

    # print(query)

    # get date from db
    data = db.get_from_col(query, db_name=db_name, col_name=coll, d_select=d_select).drop_duplicates().reset_index(
        drop=True)

    #
    if co_ids is None and ind_dates is None:
        if 'co_id' in data.columns.to_list():
            data = data.sort_values(['co_id'])
        elif 'ind_date' in data.columns.to_list():
            data = data.sort_values(['ind_date'])

        results = list()
        for f in fields:
            if 'int' in str(data[f].values.dtype) or 'float' in str(data[f].values.dtype):
                results.append(data[f].values)
            elif 'object' in str(data[f].values.dtype):
                results.append(data[f].values.tolist())

        results = tuple(results)

    else:
        data = data.sort_values(['co_id', 'ind_date'])

        max_stocks = data['co_id'].max() + 1
        max_dates = data['ind_date'].max() + 1

        if is_mat or is_mat == 1:
            d3 = len(fields)
            results = np.zeros((max_stocks, max_dates, d3)) * np.nan
            r = 0
            for f in fields:
                results[list(data['co_id']), list(data['ind_date']), r] = list(data[f])
                r += 1

    return results


# df = db_manager.get_from_collection({'co_id': {'$in': current_set}, 'date': d_date}, 'M1',
#                                         d_select={'_id': 0, 'data': 1, 'co_id': 1})

if __name__ == '__main__':
    coins, a = get_db('192.168.154.101:27017', db_name='crypto', coll='coins', fields='co_id__symbol')

    # ohlcv = get_db('192.168.154.101:27017', db_name='crypto', coll='ohlcv', fields='open__high__low__close__volume', co_ids=[0], ind_dates=[3000], is_mat=True)

    yu = 0

