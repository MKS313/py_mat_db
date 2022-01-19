function insert_to_db(inputs)

%
this_data = py.numpy.asarray(inputs.data);

% call python function
py.get_push_db.insert_db(inputs.db_address, inputs.db_user, inputs.db_pass, ...
    inputs.db_name, inputs.coll, inputs.fields, this_data, inputs.d_type, inputs.drop_col);



