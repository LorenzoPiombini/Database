--- Lua script example:
--- write data to a file called test,
--- data -> 'name:YourName:last_name:YourLastName' 
local db = require("db")

local write_record = db.write_record
local indexing = db.save_key_at_index

--- write_record returns two values, the key that the record was saved with 
--- and the record itself, now we can use lua, to create for example, another data_to_add string,(see from line 16)
--- in the format that the database expects, to write other files, 


local key,rec = write_record('test','name:YourName:last_name:YourLastName')
if rec == nil then
	print('write record failed')
else
	print('key is '..key)

	local res = indexing('test',rec.fields.last_name,1,rec.offset)
	if res ~= 1 then 
		print("indexing failed")
	end
	
	--- example: create a string for the dasbase,
	--- this is usefull when we need to write some of the record data to the main db file (name_file)
	--- the string st can be pass to another write_record().
	
	local st = string.format("n_code:%s:n_name:%s:ref:%d",rec.fields.name,rec.fields.last_name,key)
	print(st)
end
