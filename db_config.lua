local db = require('db')

local write_record = db.write_record

--- look for documentation in lua/src/export_db_lua.c
--- return two results, the record created and its key, if the key is not passed 
--- as an argument
function w_rec(file_name, data, key, number)
	if key == nil then
		return write_record(file_name,data)
	elseif key not nil then
		if key == 'base' or key == 'BASE' then
			if number == nil then 
				return -1 
			end
			return write_record(file_name,data,key,number)
	 	else
			return write_record(file_name,data,key)
		end
	end
end



