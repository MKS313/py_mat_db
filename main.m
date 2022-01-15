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

co_ids = py_range(1, 10);
ind_dates = py_range(1, 4323);

temp = py.get_push_db.get_db('crypto', 'ohlcv', 'open__high__low__close__volume', co_ids, ind_dates, true);

ohlcv = double(temp);
Open = ohlcv(:,:,1);



n = 1;
candle1(ohlcv(n, :, 2)', ohlcv(n, :, 3)', ohlcv(n, :, 4)', ohlcv(n, :, 1)', 'k')
grid on
set(gca, 'YScale', 'log')



% temp = py.get_db.return_daily_data();

closing_price = double(temp{1});
closing_volume = double(temp{2});
closing_price_prev = double(temp{3});










