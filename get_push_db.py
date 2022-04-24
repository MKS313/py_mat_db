import os
import pandas as pd
import numpy as np
from db.dbmanager import DBManager

import h5py as h5py
from scipy.io import loadmat


def create_db_manager(db_address='127.0.0.1:27017', db_user_name=None, db_password=None):
    #
    db_ip = db_address.split(':')[0]
    db_port = int(db_address.split(':')[1])
    if db_user_name in ['', ' ', '-']:
        db_user_name = None

    if db_password in ['', ' ', '-']:
        db_password = None
    return DBManager(host_address=db_ip, port=db_port, user_name=db_user_name, password=db_password)


def query_db(co_id_list=None, ind_date_list=None):

    if co_id_list is None:
        query = {'ind_date': {'$in': ind_date_list}}
    elif ind_date_list is None:
        query = {'co_id': {'$in': co_id_list}}
    elif co_id_list is not None and ind_date_list is not None:
        query = {'co_id': {'$in': co_id_list}, 'ind_date': {'$in': ind_date_list}}
    else:
        raise ValueError('please enter correct list of ind_date_list and co_id_list')
    return query


def _2D_to_st(df_in=None, time_frame=None):
    cl0 = df_in.columns.unique(0).to_list()
    df = pd.DataFrame(df_in[cl0[0]]).reset_index().melt('index')
    if len(cl0) > 1:
        for i in range(1, len(cl0)):
            df_temp = pd.DataFrame(df_in[cl0[i]]).reset_index().melt('index')
            df = pd.merge(df, df_temp, on=['index', 'variable'], how='inner')

    if time_frame is None or time_frame == 'daily':
        t = 'ind_date'
    elif time_frame == 'hourly':
        t = 'ind_h'

    columns = ['co_id']
    columns.append(t)

    for c in cl0:
        columns.append(c)

    df.columns = columns
    df.sort_values(by=columns[0:2], ignore_index=True, inplace=True)

    return df


def get_db(db_address='127.0.0.1:27017', db_user_name=None, db_password=None, db_name=None, coll=None, fields=None,
           co_ids=None, ind_dates=None, is_mat=None, is_old=None):

    #
    db = create_db_manager(db_address=db_address, db_user_name=db_user_name, db_password=db_password)

    if co_ids in ['', ' ', '-'] or len(co_ids) == 0:
        co_ids = None

    if ind_dates in ['', ' ', '-'] or len(ind_dates) == 0:
        ind_dates = None

    fields = fields.split('__')

    if is_old or is_old == 1:
        ind_date_str = 'ind_matlab_date'
    else:
        ind_date_str = 'ind_date'

    # fill query
    if co_ids is None and ind_dates is None:
        query = {}
        d_select = {}
    else:
        # check co_ids
        if co_ids is not None:
            if str(co_ids) == 'all':
                co_id_q = {'$exists': True}
            elif len(co_ids) > 0:
                co_id_q = {'$in': co_ids}

        # check ind_dates
        if ind_dates is not None:
            if str(ind_dates) == 'all':
                ind_date_q = {'$exists': True}
            elif len(ind_dates) > 0:
                ind_date_q = {'$in': ind_dates}

        # create query and d_select
        if co_ids is None and ind_dates is not None:
            query = {ind_date_str: ind_date_q}
            d_select = {ind_date_str: True}
        elif ind_dates is None and co_ids is not None:
            query = {'co_id': co_id_q}
            d_select = {'co_id': True}
        else:
            query = {'co_id': co_id_q, ind_date_str: ind_date_q}
            d_select = {'co_id': True, ind_date_str: True}

    # fill d_select
    for f in fields:
        d_select[f] = True

    # print(query)

    # get date from db
    data = db.get_from_col(query, db_name=db_name, col_name=coll, d_select=d_select).drop_duplicates().reset_index(
        drop=True)

    #
    # sort
    if 'co_id' in data.columns.to_list() and ind_date_str in data.columns.to_list():
        data = data.sort_values(['co_id', ind_date_str]).reset_index(drop=True)
    elif 'co_id' in data.columns.to_list():
        data = data.sort_values(['co_id']).reset_index(drop=True)
    elif ind_date_str in data.columns.to_list():
        data = data.sort_values([ind_date_str]).reset_index(drop=True)

    #
    results = list()
    for f in fields:
        if 'int' in str(data[f].values.dtype) or 'float' in str(data[f].values.dtype):
            results.append(data[f].values)
        elif 'object' in str(data[f].values.dtype):
            results.append(data[f].values.tolist())

    results = tuple(results)

    if co_ids is None and ind_dates is None:
        # sort
        if 'co_id' in data.columns.to_list() and ind_date_str in data.columns.to_list():
            data = data.sort_values(['co_id', ind_date_str]).reset_index(drop=True)
        elif 'co_id' in data.columns.to_list():
            data = data.sort_values(['co_id']).reset_index(drop=True)
        elif ind_date_str in data.columns.to_list():
            data = data.sort_values([ind_date_str]).reset_index(drop=True)

        #
        results = list()
        for f in fields:
            if 'int' in str(data[f].values.dtype) or 'float' in str(data[f].values.dtype):
                results.append(data[f].values)
            elif 'object' in str(data[f].values.dtype):
                results.append(data[f].values.tolist())

        results = tuple(results)

    elif co_ids is None and ind_dates is not None:
        # sort by and ind_date
        data = data.sort_values([ind_date_str]).reset_index(drop=True)

        max_dates = data[ind_date_str].max() + 1

        if is_mat or is_mat == 1:
            d3 = len(fields)
            results = np.zeros((d3, max_dates)) * np.nan
            r = 0
            for f in fields:
                results[r, list(data[ind_date_str])] = list(data[f])
                r += 1

    elif co_ids is not None and ind_dates is None:
        # sort by and co_id
        data = data.sort_values(['co_id']).reset_index(drop=True)

        max_stocks = data['co_id'].max() + 1

        if is_mat or is_mat == 1:
            d3 = len(fields)
            results = np.zeros((max_stocks, d3)) * np.nan
            r = 0
            for f in fields:
                results[list(data['co_id']), r] = list(data[f])
                r += 1

    else:
        # sort by co_id and ind_date
        data = data.sort_values(['co_id', ind_date_str]).reset_index(drop=True)

        max_stocks = data['co_id'].max() + 1
        max_dates = data[ind_date_str].max() + 1

        if is_mat or is_mat == 1:
            d3 = len(fields)
            results = np.zeros((max_stocks, max_dates, d3)) * np.nan
            r = 0
            for f in fields:
                results[list(data['co_id']), list(data[ind_date_str]), r] = list(data[f])
                r += 1

    return results


def insert_db(db_address='127.0.0.1:27017', db_user_name=None, db_password=None, db_name=None, coll=None, fields=None,
           data=None, drop_col=False):

    # print(data.shape.__len__())

    #
    db = create_db_manager(db_address=db_address, db_user_name=db_user_name, db_password=db_password)

    #
    fields = fields.split('__')

    # d1, d2, d3 = arr.shape
    # out_arr = np.column_stack((np.repeat(np.arange(d1), d2), arr.reshape(d1 * d2, -1)))
    # df = pd.DataFrame(out_arr, columns=fields)

    if data.shape.__len__() == 3:
        this_dict = {}
        r = 0
        for f in fields:
            this_dict[f.lower()] = data[:, :, r]
            r += 1

        this_data_temp = pd.concat({k: pd.DataFrame(v) for k, v in this_dict.items()}, axis=1)
        this_data = _2D_to_st(this_data_temp).dropna().reset_index(drop=True)

        query_del = query_db(this_data['co_id'].drop_duplicates().reset_index(drop=True).values.tolist(),
                             this_data['ind_date'].drop_duplicates().reset_index(drop=True).values.tolist())

    elif data.shape.__len__() == 2:
        this_dict = {}
        r = 0
        for f in fields:
            this_dict[f.lower()] = data[:, r]
            r += 1

        this_data = pd.DataFrame(this_dict, columns=fields)

        query_del = {}

    elif data.shape.__len__() == 1:
        this_dict = {}
        r = 0
        for f in fields:
            this_dict[f.lower()] = data
            r += 1

        this_data = pd.DataFrame(this_dict, columns=fields)

        query_del = {}

    # print(this_data)

    #
    if drop_col:
        db.drop_col(db_name=db_name, col_name=coll)
    else:
        db.del_from_col(query_del, db_name=db_name, col_name=coll)

    #
    db.insert_to_col(d_data=this_data, db_name=db_name, col_name=coll)

    yu = 0


if __name__ == '__main__':

    # file_path = './crypto_ohlcv.mat'
    # load_item = ['open', 'high', 'low', 'close', 'volume']
    #
    # with h5py.File(file_path, 'r') as file:
    #     arr = np.transpose(np.asarray(file['ohlcv']))
    #
    # insert_db('192.168.154.101:27017', db_name='crypto', coll='ohlcv2', fields='open__high__low__close__volume',
    #                data=arr)

    # yu = 0
    #
    adj_factor = get_db(db_address='192.168.154.107:27017', db_user_name='user', db_password='algorithm123', db_name='market', coll='adj_factor', fields='adj_factor', co_ids=list(range(457)), ind_dates=list(range(2325-1, 2325)), is_mat=True, is_old=True)

    # tepix = get_db(db_address='192.168.154.107:27017', db_user_name='user', db_password='algorithm123',
    #                     db_name='market', coll='tepix', fields='close_nw__close_real',
    #                     ind_dates=list(range(2325)), is_mat='', is_old=True)

    # coins, a = get_db('192.168.154.101:27017', db_name='crypto', coll='coins', fields='co_id__symbol')

    # ohlcv = get_db('192.168.154.101:27017', db_name='crypto', coll='ohlcv',
    # fields='open__high__low__close__volume', co_ids=[0], ind_dates=[3000], is_mat=True)

    yu = 0


