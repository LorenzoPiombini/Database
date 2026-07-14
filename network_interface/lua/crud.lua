local db = require("db")

local crud = {}

--- database crud functions
crud.write_record = db.write_record
crud.update_record = db.update_record
crud.get_numeric_key = db.get_numeric_key
crud.string_data_to_add_template = db.string_data_to_add_template
crud.indexing = db.save_key_at_index
crud.create_rec = db.create_record
crud.g_rec = db.get_record
crud.d_rec = db.delete_record
crud.g_all_key = db.get_all_key


--- look for documentation in lua/src/export_db_lua.c
--- return two results, the record created and its key, if the key is not passed
--- as an argument the key will be computed automatically
local function w_rec(file_name, data, key, number)
	if key == nil then
		return write_record(file_name, data)
	elseif key == "base" and number ~= nil then
		return write_record(file_name, data, key, number)
	elseif key ~= "base" then
		return write_record(file_name, data, key)
	end
end

--- NOT NEEDED AS FOR NOW
--- return the key of the name_file record
local function write_to_name_file(data)
	local key, rec = write_record(name_file, data)
	if key == nil or rec == nil then
		return -1
	end
	return key
end


crud.w_rec = w_rec
crud.write_to_name_file = write_to_name_file

return crud
