function out_var = get_from_db(query_info)

%
temp = py.get_push_db.get_db(query_info.db_address, query_info.db_user, query_info.db_pass, ...
    query_info.db_name, query_info.coll, query_info.fields, py.list(num2cell(int32((query_info.co_ids) - 1))), py.list(num2cell(int32((query_info.ind_dates) - 1))), query_info.is_mat, query_info.is_old_format);

%
if query_info.is_mat && query_info.to_struct
    out_var = to_struct_(double(temp), query_info);

elseif query_info.is_mat && ~query_info.to_struct
    out_var = double(temp);

    if ~query_info.fill_nan && size(out_var, 2) == query_info.ind_dates(end)
        out_var = out_var(:, query_info.ind_dates, :);
    end

else
    out_var = to_struct_(temp, query_info);

end


%
function this_struct = to_struct_(x, query_info)

fields_1 = strsplit(query_info.fields, '__');
this_struct = struct();
fc = 0;
for f = fields_1(:)'
    fc = fc + 1;

    if query_info.is_mat
        this_struct.(f{1}) = x(:, :, fc);
    else
        if strcmp(query_info.fields_format{fc}, 'd')
            this_struct.(f{1}) = double(x{fc});
        elseif strcmp(query_info.fields_format{fc}, 's')
            this_struct.(f{1}) = string(cell(x{fc}));
        end
    end

    %
    if ~query_info.fill_nan && size(this_struct.(f{1}), 2) == query_info.ind_dates(end)
        this_struct.(f{1}) = this_struct.(f{1})(:, query_info.ind_dates, :);
    end
end

