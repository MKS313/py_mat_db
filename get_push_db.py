
import os
import pandas as pd
import numpy as np
from db.dbmanager import DBManager


db = DBManager(host_address='192.168.154.101', port=27017)

# query_del = query_db(co_id_list, ind_date_list)
# db.del_from_col(query_del, db_name=db_insert['db_name'], col_name=db_insert['col_name'])
# db.insert_to_col(data_to_db_list, db_name=db_insert['db_name'], col_name=db_insert['col_name'])


def get_db(db_name=None, coll=None, fields=None, co_ids=None, ind_dates=None, is_mat=None):
    print(db_name)
    print(coll)
    print(fields)
    print(str(co_ids))
    print(ind_dates)
    print(is_mat)

    fields = fields.split('__')

    # fill query
    if co_ids is None and ind_dates is None:
        query = {}
        d_select = {}
    else:
        # check co_ids
        if str(co_ids) is 'all':
            print('ooooooooooooooo')
            co_id_q = {'$exists': True}
        elif len(co_ids) > 0:
            print('ppppppppppppppp---------')
            co_id_q = {'$in': co_ids}

        # check ind_dates
        if str(ind_dates) is 'all':
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


    print(query)

    # get date from db
    data = db.get_from_col(query, db_name=db_name, col_name=coll, d_select=d_select)
    
    # 
    if co_ids is None and ind_dates is None:
        results = tuple(len(fields))
        r = 0
        for f in fields:
            results[r] = data[f]
    
    else:
        max_stocks = data['co_id'].max() + 1
        max_dates = data['ind_date'].max() + 1
        # results_open = np.zeros((max_stocks, max_dates)) * np.nan
        # results_high = np.zeros((max_stocks, max_dates)) * np.nan
        # results_low = np.zeros((max_stocks, max_dates)) * np.nan
        # results_close = np.zeros((max_stocks, max_dates)) * np.nan
        # results_volume = np.zeros((max_stocks, max_dates)) * np.nan
        #
        # results_open[list(data['co_id']), list(data['ind_date'])] = list(data['open'])
        # results_high[list(data['co_id']), list(data['ind_date'])] = list(data['high'])
        # results_low[list(data['co_id']), list(data['ind_date'])] = list(data['low'])
        # results_close[list(data['co_id']), list(data['ind_date'])] = list(data['close'])
        # results_volume[list(data['co_id']), list(data['ind_date'])] = list(data['volume'])

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

    # coins = get_db(db_name='crypto', coll='coins', fields=['symbol'])

    ohlcv = get_db(db_name='crypto', coll='ohlcv', fields='open__high__low__close__volume', co_ids=[0],
                   ind_dates=[3000], is_mat=True)

    yu = 0

    # ohlcv = get_db(db_name='crypto', coll='ohlcv', fields=['open', 'high', 'low', 'close', 'volume'], co_ids='all', ind_dates='all', is_mat=True)

    a = 5

