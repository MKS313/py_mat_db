function out_var = get_from_db(inputs)

%
temp = py.get_push_db.get_db(inputs.db_address, inputs.db_user, inputs.db_pass, ...
    inputs.db_name, inputs.coll, inputs.fields, inputs.co_ids, inputs.ind_dates, inputs.is_mat);

%
if inputs.is_mat && inputs.to_struct
    out_var = to_struct_(double(temp), inputs);

elseif inputs.is_mat
    out_var = double(temp);

else
    out_var = to_struct_(temp, inputs);

end


%
function this_struct = to_struct_(x, inputs)

fields_1 = strsplit(inputs.fields, '__');
this_struct = struct();
fc = 0;
for f = fields_1(:)'
    fc = fc + 1;

    if inputs.is_mat
        this_struct.(f{1}) = x(:, :, fc);
    else
        if strcmp(inputs.fields_format{fc}, 'd')
            this_struct.(f{1}) = double(x{fc});
        elseif strcmp(inputs.fields_format{fc}, 's')
            this_struct.(f{1}) = string(cell(x{fc}));
        end
    end
end

