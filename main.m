clear; clc; close all; format longG;% delete(timerfind);
%% Add Path ===============================================================
% rmpath(genpath(pwd))

addpath(genpath(pwd))

clc

rng(1)

distcomp.feature( 'LocalUseMpiexec', false)

%%

% useful links
% https://www.mathworks.com/help/matlab/matlab_external/call-user-defined-custom-module.html
% https://www.mathworks.com/help/matlab/matlab_external/call-modified-python-module.html
% https://www.mathworks.com/help/matlab/matlab_external/handling-data-returned-from-python.html
% https://www.mathworks.com/help/matlab/matlab_external/use-python-str-type-in-matlab.html

% pyversion 'C:/Users/alg/Desktop/Alg/alg/Packages/AlgoDB/venv/Scripts/pythonw.exe'

%%
% [load_path, save_path] = load_save_path();
% 
% if count(py.sys.path, [load_path.Packages.root, '/signal_google_sheet'])==0
%     insert(py.sys.path, int32(0), [load_path.Packages.root, '/signal_google_sheet']);
% end
% 
% py.importlib.reload(py.importlib.import_module('alg_signal_to_google_sheet'));

clear classes
insert(py.sys.path, int32(0), pwd);
py.importlib.reload(py.importlib.import_module('get_push_db'));
py.importlib.reload(py.importlib.import_module('numpy'));


%%

[py_ver, executable, isloaded] = pyversion;

%% get from db

inputs.db_address = '192.168.154.101:27017';
inputs.db_user = '';
inputs.db_pass = '';
inputs.db_name = 'crypto';
inputs.coll = 'coins';
inputs.fields = 'co_id__symbol';
inputs.fields_format = {'d', 's'};
inputs.co_ids = '';
inputs.ind_dates = '';
inputs.is_mat = 0;
inputs.to_struct = 1;

coins = get_from_db(inputs);
% save('coins.mat', 'coins', '-v7.3')

%
N = 375;
T = 4323;

inputs.db_address = '192.168.154.101:27017';
inputs.db_user = '';
inputs.db_pass = '';
inputs.db_name = 'crypto';
inputs.coll = 'ohlcv';
inputs.fields = 'open__high__low__close__volume';
inputs.fields_format = '';
inputs.co_ids = py_range(1, N);
inputs.ind_dates = py_range(1, T);
inputs.is_mat = 1;
inputs.to_struct = 0;

ohlcv = get_from_db(inputs);

% save('ohlcv_crypto.mat', 'ohlcv', '-v7.3')
% save('crypto_ohlcv_stc.mat', 'ohlcv', '-v7.3')

%% insert to db

% %
% load('crypto_ohlcv.mat')
% 
% 
% %
% inputs_insert.db_address = '192.168.154.101:27017';
% inputs_insert.db_user = '';
% inputs_insert.db_pass = '';
% inputs_insert.db_name = 'crypto';
% inputs_insert.coll = 'test_3D';
% inputs_insert.fields = 'open__high__low__close__volume';
% inputs_insert.drop_col = 1;
% 
% inputs_insert.data = ohlcv;
% 
% %
% insert_to_db(inputs_insert);
% 
% 
% %%
% 
% inputs_insert.db_address = '192.168.154.101:27017';
% inputs_insert.db_user = '';
% inputs_insert.db_pass = '';
% inputs_insert.db_name = 'crypto';
% inputs_insert.coll = 'test_2D';
% inputs_insert.fields = 'f1__f2';
% inputs_insert.drop_col = 1;
% 
% inputs_insert.data = rand([2, 100])';
% 
% %
% insert_to_db(inputs_insert);
% 
% 
% %%
% inputs_insert.db_address = '192.168.154.101:27017';
% inputs_insert.db_user = '';
% inputs_insert.db_pass = '';
% inputs_insert.db_name = 'crypto';
% inputs_insert.coll = 'test_1D';
% inputs_insert.fields = 'f1';
% inputs_insert.drop_col = 1;
% 
% inputs_insert.data = {'a1', 'a2'};
% 
% %
% insert_to_db(inputs_insert);

%% get from old algo-db
% ind_matlab_stock


N = 457;
T = 2325;

query_info.db_address = '192.168.154.107:27017';
query_info.db_user = 'user';
query_info.db_pass = 'algorithm123';
query_info.db_name = 'market';
query_info.coll = 'adj_factor';
query_info.fields = 'adj_factor';
query_info.fields_format = '';
query_info.co_ids = py_range(1, N);
query_info.ind_dates = py_range(1, T);
query_info.is_mat = 1;
query_info.to_struct = 0;
query_info.is_old_format = 0;

adj_factor = get_from_db(query_info);


%%

figure(1)
for n = 1:N
    pl(1) = subplot(4, 1, [1 2 3]);
    candle1(ohlcv(n, :, 2)', ohlcv(n, :, 3)', ohlcv(n, :, 4)', ohlcv(n, :, 1)', 'k')
    grid on
    set(gca, 'YScale', 'log')

    title([num2str(n), ': ', coins.symbol{n}])

    pl(2) = subplot(4, 1, 4);
    plot(ohlcv(n, :, 5)', 'LineWidth', 1.5)
    grid on
    set(gca, 'YScale', 'log')

    linkaxes(pl, 'x')

    pause
end









