require("string_helper")
local crud = require("crud")
local system = require("db_files")

function get_item(key)
	local item,err
	if type(key) == 'string' then
		item,err = crud.g_rec(system.items,key,1)
	else
		item,err = crud.g_rec(system.items,key)
	end

	if item == nil then 
		print(err)
		return nil 
	end
	
	return rec_to_json(item)
end
