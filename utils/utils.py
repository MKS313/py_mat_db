import pandas as pd
from db.dbmanager import DBManager
import config


def replace_text(word):
    if type(word) == str:
        replacements = {'پ': '\u067e', 'چ': '\u0686', 'ج': '\u062c', 'ح': '\u062d', 'خ': '\u062e', 'ه': '\u0647',
                        'ع': '\u0639', 'غ': '\u063a', 'ف': '\u0641', 'ق': '\u0642', 'ث': '\u062b', 'ص': '\u0635',
                        'ض': '\u0636', 'گ': '\u06af', 'ک': '\u06a9', 'م': '\u0645', 'ن': '\u0646', 'ت': '\u062a',
                        'ا': '\u0627', 'ل': '\u0644', 'ب': '\u0628', 'ي': '\u06cc', 'س': '\u0633', 'ش': '\u0634',
                        'و': '\u0648', 'ئ': '\u0626', 'د': '\u062f', 'ذ': '\u0630', 'ر': '\u0631', 'ز': '\u0632',
                        'ط': '\u0637', 'ظ': '\u0638', 'ژ': '\u0698', 'آ': '\u0622', 'ي': '\u064a', '؟': '\u061f',
                        'ك': '\u06a9', 'ي': 'ی'}
        for src, target in replacements.items():
            word = word.replace(src, target)
    return word


def return_date(ind_date):
    db = DBManager(host_address=config.DB_ALGODB, user_name=config.DB_USER_ALGODB, password=config.DB_PASS_ALGODB)
    date = db.get_from_col({'ind_matlab_date': ind_date}, col_name='dates', db_name='market', return_list=True)[0]['date']

    return date



