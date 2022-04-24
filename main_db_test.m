clear; clc; close all; format longG;% delete(timerfind);
%% Add Path ===============================================================
% rmpath(genpath(pwd))

addpath(genpath(pwd))

clc

rng(1)

distcomp.feature( 'LocalUseMpiexec', false)

%%

clear classes
if count(py.sys.path, pwd)==0
    insert(py.sys.path, int32(0), pwd);
end
py.importlib.reload(py.importlib.import_module('get_push_db'));
py.importlib.reload(py.importlib.import_module('numpy'));


%%

[py_ver, executable, isloaded] = pyversion;

%% get from db

N = 457;
T = 2325;

% init db
query_info.db_address = '192.168.154.107:27017';
query_info.db_user = 'user';
query_info.db_pass = 'algorithm123';


% adj_factor
query_info.db_name = 'market';
query_info.coll = 'adj_factor';
query_info.fields = 'adj_factor';
query_info.fields_format = '';
query_info.co_ids = py_range(1, N);
query_info.ind_dates = py_range(1, T);
query_info.is_mat = 1;
query_info.to_struct = 0;
query_info.is_old_format = 1;
adj_factor = get_from_db(query_info);


% market_cap
query_info.db_name = 'market';
query_info.coll = 'market_cap';
query_info.fields = 'market_cap';
query_info.fields_format = '';
query_info.co_ids = py_range(1, N);
query_info.ind_dates = py_range(1, T);
query_info.is_mat = 1;
query_info.to_struct = 0;
query_info.is_old_format = 1;
market_cap = get_from_db(query_info);


% tepix
query_info.db_name = 'market';
query_info.coll = 'tepix';
query_info.fields = 'close_nw__close_real';
query_info.fields_format = {'d', 'd'};
query_info.co_ids = '-';
query_info.ind_dates = py_range(1, T);
query_info.is_mat = 0;
query_info.to_struct = 1;
tepix = get_from_db(query_info);


% ohlcv
query_info.db_name = 'market';
query_info.coll = 'daily_tse';
query_info.fields = 'open__high__low__close__volume';
query_info.fields_format = {'d', 'd', 'd', 'd', 'd'};
query_info.co_ids = py_range(1, N);
query_info.ind_dates = py_range(1, T);
query_info.is_mat = 1;
query_info.to_struct = 1;
query_info.is_old_format = 1;
ohlcv = get_from_db(query_info);


% init db
query_info.db_address = '192.168.154.122:27017';
query_info.db_user = '';
query_info.db_pass = '';

% ohlcv_improved
query_info.db_name = 'market_data';
query_info.coll = 'ohlcv_imputed_new_candle';
query_info.fields = 'open__high__low__close__volume';
query_info.fields_format = {'d', 'd', 'd', 'd', 'd'};
query_info.co_ids = py_range(1, N);
query_info.ind_dates = py_range(1, T);
query_info.is_mat = 1;
query_info.to_struct = 1;
query_info.is_old_format = 1;
ohlcv_improved = get_from_db(query_info);


% features_fuzzy
query_info.db_name = 'features';
query_info.coll = 'fuzzy_candle';
query_info.fields = 'len_upper__len_lower__len_body__body_style__open_style__close_style__trend_last';
query_info.fields_format = {'d', 'd', 'd', 'd', 'd', 'd', 'd'};
query_info.co_ids = py_range(1, N);
query_info.ind_dates = py_range(1, T);
query_info.is_mat = 1;
query_info.to_struct = 0;
query_info.is_old_format = 1;
features_fuzzy = get_from_db(query_info);



save('deep_data.mat', '', '', '', '', '')



