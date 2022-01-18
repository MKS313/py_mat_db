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


%%

[py_ver, executable, isloaded] = pyversion;

%%

%
N = 2; %375;
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
inputs.to_struct = 1;

ohlcv = get_data(inputs);

% ohlcv = double(temp);
% 
% 
% n = 3;
% candle1(ohlcv(n, :, 2)', ohlcv(n, :, 3)', ohlcv(n, :, 4)', ohlcv(n, :, 1)', 'k')
% grid on
% set(gca, 'YScale', 'log')
% 
% save('crypto_ohlcv.mat', 'ohlcv')



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

coins = get_data(inputs);





