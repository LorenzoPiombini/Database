db = require("db")

name_file = "network_interface/db/name_file"

write_record = db.write_record
get_numeric_key = db.get_numeric_key
string_data_to_add_template = db.string_data_to_add_template

--- look for documentation in lua/src/export_db_lua.c
--- return two results, the record created and its key, if the key is not passed
--- as an argument
function w_rec(file_name, data, key, number)
	if key == nil then
		return write_record(file_name, data)
	elseif key == "base" or key == "BASE" then
		if number == nil then
			return -1
		end
		return write_record(file_name, data, key, number)
	else
		return write_record(file_name, data, key)
	end
end

--- return the key of the record
function write_to_name_file(data)
	local key, rec = write_record(name_file, data)
	if key == nil or rec == nil then
		return -1
	end
	return key
end

function write_customers(data)
	local k, cli_rec = w_rec("network_interface/db/customers", data)
	local name_file_data = string_data_to_add_template(name_file)
	if cli_rec == nil or k == nil then
		return -1
	end

	local key = get_numeric_key(name_file, 0)
	local f = cli_rec.fields
	local res = write_to_name_file(string.format(name_file_data, f.c_name, f.c_code, key))
	print(key, res)
	if key == res then
		return 0, key
	end

	return nil
end

--write_customers('c_code:E00001:c_name:test srl:c_addr1:via corassori 72:c_csz:Formigne Modena Italy:c_phone:+1973-609-9071:c_contact:Lorenzo')
