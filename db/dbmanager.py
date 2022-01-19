from pymongo import MongoClient
from datetime import datetime
from pytz import timezone
import pandas as pd
import numpy as np


class DBManager:
    """
    this class is used to create connection to mongodb server and
    handling CRUD operations on a mongodb database
    Methods
    -------
    _reset_update_time:
        private method to set current system time as a last time of changing
    insert_to_col:
        write data to a collection of database
    upsert_one_to_col:
        write data to a collection if it's new
        otherwise updating existing document
    update_one_field_of_doc:
        update one field of existing document in a collection
    _encode_data_for_insert:
        private method to encode data based on data type
    get_from_col:
        read data from collection of database
    agg_col:
        impose data process pipeline on a collection of database
    del_from_col:
        delete data from collection of database
    drop_col:
        remove a collection from database
    """

    def __init__(self, host_address, user_name=None, password=None, port=27017):
        """
        Parameters
        ----------
        host_address: str required
            ip address of mongodb server
        user_name: str
            username of mongodb server
        password: str
            password of mongodb server
        port: int
            port number used to connect to mongodb server, default is 27017
        """
        if password is not None:
            password = password.replace('@', '%40')

        if user_name is None:
            self.client = MongoClient('mongodb://' + host_address + ':' + str(port) + '/')
        else:
            self.client = MongoClient(
                'mongodb://' + user_name + ':' + password + '@' + host_address + ':' + str(port) + '/')

        self.time_zone = timezone('Asia/Tehran')
        self._reset_update_time()

    def _reset_update_time(self):
        """
        private method to set current system time as a last time of changing
        store it as a python datatime object
        """
        dt = datetime.now(self.time_zone)
        self.now = datetime(year=dt.year, month=dt.month, day=dt.day, hour=dt.hour, minute=dt.minute,
                            second=dt.second)

    def insert_to_col(self, d_data, col_name, db_name, correct_encode=False,
                      remove_none=False):
        """
        inserting or writing document to a collection of database
        with the option of
        imposing custom encoder to have a proper format for storing data in mongodb
        Parameters
        ----------
        d_data: dict or list required
            document will be stored in collection
        col_name: str required
            name of collection in database
        db_name: str required
            name of database stored in mongodb server
        correct_encode: bool, optional default=False
            state of imposing of correct encoding on data
        remove_none: bool, optional default=False
            state of ignoring none values
        """
        self._reset_update_time()
        col = self.client[db_name][col_name]
        data = self._encode_data_for_insert(d_data, correct_encode, remove_none)
        if type(data) == dict:
            col.insert_one(data)
        else:
            col.insert_many(data)

    def upsert_one_to_col(self, d_data, col_name, db_name, d_filter, correct_encode=False,
                          remove_none=False):
        """
        inserting or updating document based on a filter to a collection of database
        with the option of
        imposing custom encoder to have a proper format for storing data in mongodb
        Parameters
        ----------
        d_data: dict required
            document will be stored in collection
        col_name: str required
            name of collection in database
        db_name: str required
            name of database stored in mongodb server
        d_filter:dict required
            criteria to be met
        correct_encode: bool, optional default=False
            state of imposing of correct encoding on data
        remove_none: bool, optional default=False
            state of ignoring none values
        """
        self._reset_update_time()
        if type(d_data) != dict:
            raise Exception('type of data is not supported for upsert')
        col = self.client[db_name][col_name]
        data = self._encode_data_for_insert(d_data, correct_encode, remove_none)
        col.update(d_filter, data, upsert=True)

    def update_one_field_of_doc(self, d_data, col_name, db_name, d_filter, correct_encode=False,
                                remove_none=False):
        """
        updating document from a collection of database based on a filter
        with the option of
        imposing custom encoder to have a proper format for storing data in mongodb
        Parameters
        ----------
        d_data:
            new data to update existing document
        col_name: str required
            name of collection in database
        db_name: str required
            name of database stored in mongodb server
        d_filter:dict required
            criteria to be met
        correct_encode: bool, optional default=False
            state of imposing of correct encoding on data
        remove_none: bool, optional default=False
            state of ignoring none values
        """
        self._reset_update_time()
        if type(d_data) != dict:
            raise Exception('type of data is not supported for upsert')
        col = self.client[db_name][col_name]
        data = self._encode_data_for_insert(d_data, correct_encode, remove_none)
        col.update(d_filter, {'$set': data}, upsert=True)

    def _encode_data_for_insert(self, d_data, correct_encode=False, remove_none=False):
        """
        encode data before inserting to database
        Parameters
        ----------
        d_data: dict or list or pandas.DataFrame required
            input data will be encoded before inserting to database
        correct_encode: bool, optional default=False
            state of imposing of correct encoding on data
        remove_none: bool, optional default=False
            state of ignoring none values
        """
        cor_enc = CorrectEncoding()
        if type(d_data) == list:
            data = d_data
            data = [dict(d, update_time=self.now) for d in data]
            if remove_none:
                data = [{k: v for k, v in d.items() if pd.notnull(v)} for d in data]
            if correct_encode:
                # data = [correct_encoding(d) for d in data]
                data = [cor_enc.encode_on_dtype(d) for d in data]
        elif type(d_data) == dict:
            d_data['update_time'] = self.now
            data = d_data
            if remove_none:
                data = {k: v for k, v in data.items() if pd.notnull(v)}
            if correct_encode:
                # data = correct_encoding(data)
                data = cor_enc.encode_on_dtype(data)

        else:
            d_data['update_time'] = self.now
            data = d_data.to_dict(orient='records')

            if correct_encode:
                # data = [correct_encoding(m) for m in data]
                data = [cor_enc.encode_on_dtype(m) for m in data]
            if remove_none:
                data = [{k: v for k, v in m.items() if pd.notnull(v)} for m in data]

        return data

    def get_from_col(self, d_filter, col_name, db_name, sort=None, limit=None, d_select=None,
                     return_list=False):
        """
        read or get document from a collection of database based on a filter
        Parameters
        ----------
        d_filter: dict required
            criteria to be met
        col_name: str required
            name of collection in database
        db_name: str required
            name of database stored in mongodb server
        sort: dict optional default=None
            sorting retrieved documents
        limit: int optional default=None
            specify maximum number of documents to be retrieved
        d_select:  optional default=False
            specify the fields to return in the documents that match the query filter
        return_list: bool, optional default=False
            set True if type of desired output is list, default output is pandas.DataFrame
        Returns
        -------
        d_data: pandas.DataFrame or list
            requested documents in appropriate type
        """
        col = self.client[db_name][col_name]
        if sort is None and limit is None:
            cursor = col.find(d_filter, d_select)
        elif limit is None:
            cursor = col.find(d_filter, d_select).sort(sort['by'], sort['direction'])
        elif sort is None:
            cursor = col.find(d_filter, d_select).limit(limit)
        else:
            cursor = col.find(d_filter, d_select).sort(sort['by'], sort['direction']).limit(limit)
        if return_list:
            return list(cursor)
        else:
            d_data = pd.DataFrame(list(cursor))
        return d_data

    def agg_col(self, col_name, db_name, pipeline, return_list=False):
        """
        impose data process pipeline on a collection of database
        Parameters
        ----------
        col_name: str required
            name of collection in database
        db_name: str required
            name of database stored in mongodb server
        pipeline:list required
            it defines the stages of process on documents from a collection
        return_list: bool, optional default=False
            set True if type of desired output is list, default output is pandas.DataFrame
        Returns
        -------
        d_data: pandas.DataFrame or list
            requested documents in appropriate type
        """

        cursor = self.client[db_name][col_name].aggregate(pipeline)
        if return_list:
            return list(cursor)
        else:
            d_data = pd.DataFrame(list(cursor))
        return d_data

    def del_from_col(self, d_filter, col_name, db_name):
        """
        delete document(s) from a collection of database based on a filter
        Parameters
        ----------
        d_filter:dict required
            criteria to be met
        col_name: str required
            name of collection in database
        db_name: str required
            name of database stored in mongodb server
        """
        col = self.client[db_name][col_name]
        col.remove(d_filter)

    def drop_col(self, col_name, db_name):
        """
        remove a collection from database
        Parameters
        ----------
        col_name: str required
            name of collection in database
        db_name: str required
            name of database stored in mongodb server
        """
        self.client[db_name].drop_collection(col_name)


class CorrectEncoding:
    """
    correct the encoding of python data types so they can be encoded to mongodb inps
    Methods
    -------
    encode_on_datatype:
        correct the encoding of input data based on type of input
    """

    def encode_on_dtype(self, inp):
        """
        check the data type and implement the correct encoding based on data type
        Returns
        -------
        new : new data type with (hopefully) corrected encodings
        """
        if isinstance(inp, dict):
            new = {}
            for key1, val1 in inp.items():
                new[key1] = self.encode_on_dtype(val1)
        elif isinstance(inp, list):
            new = [self.encode_on_dtype(item) for item in inp]

        elif isinstance(inp, np.bool_):
            new = bool(inp)

        elif isinstance(inp, np.int64):
            new = int(inp)

        elif isinstance(inp, np.float64):
            new = float(inp)

        elif pd.isna(inp):
            new = None

        else:
            new = inp

        return new