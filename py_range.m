function ind = py_range(start, stop)

ind = py.list(num2cell(int32( (start:stop) - 1) ));
