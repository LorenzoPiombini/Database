local db = require("db")

local create_record = db.create_record
local create_data_to_add_string = db.string_data_to_add_template

local rec = create_record('test',"name:Lorenzo:last_name:Piombini")

if rec == nil then
	print("create record failed")
else
	print(string.format('name %s\nlast name %s\n',rec.fields.name,rec.fields.last_name))
end


-- create data string based on the file schema

local data_to_add = create_data_to_add_string('test')

print(data_to_add)
print()
print(string.format(data_to_add,'Lorenzo','Piombini'))
