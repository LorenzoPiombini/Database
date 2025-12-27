db = require("db")

--- DB GLOBALS
ORDER_BASE = 100
--- database files
name_file = "db/name_file"
customers = "db/customers"
sales_orders ={} 
sales_orders['head'] = "db/sales_orders_head"
sales_orders['lines'] = "db/sales_orders_lines"

--- database crud functions
write_record = db.write_record
get_numeric_key = db.get_numeric_key
string_data_to_add_template = db.string_data_to_add_template
indexing = db.save_key_at_index
create_rec = db.create_record

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

--- return the key of the name_file record
function write_to_name_file(data)
	local key, rec = write_record(name_file, data)
	if key == nil or rec == nil then
		return -1
	end
	return key
end

function write_customers(data)
	-- TODO: write a function that look for a key in the hash table
	-- to determine if e record already exist.
	
	local key = get_numeric_key(name_file, 0)
	local data_with_c_number = string.format("%s:c_number:%d", data, key)

	local cli_rec = create_rec("db/customers", data_with_c_number)
	if cli_rec == nil then return nil end

	local f = cli_rec.fields

	-- indexing function
	-- we are saving the same record with a different key, to get better
	-- searching performance,data integrety  and user experience
	-- if this one fails, it means that we have this customer in the DB already
	local res = indexing("db/customers", f.c_name, 1, cli_rec.offset)
	if res ~= 1 then
		return -1
	end

	local k, rec = w_rec("db/customers", data_with_c_number)
	local name_file_data = string_data_to_add_template(name_file)
	if rec == nil or k == nil then
		return -1
	end

	local res = write_to_name_file(string.format(name_file_data, f.c_name, f.c_code, key))
	if key == res then
		return 0, key
	end

	return nil
end

--write_customers('c_code:E00001:c_name:test srl:c_addr1:via corassori 72:c_csz:Formigne Modena Italy:c_phone:+1973-609-9071:c_contact:Lorenzo')


function write_orders(orders_head,orders_lines)
	local kh, ord_head = w_rec(sales_orders.head, orders_head,"base",ORDER_BASE)

	if ord_head == nil then return nil end

	local f = ord_head.fields
	if f.lines_nr == 1 then
		local key_line = string.format("%d/%d",kh,f.lines_nr)
		local line = string.sub(orders_lines,2,-2)
		local data = string.sub(line,3,#line)
		local kl, ord_lines = w_rec(sales_orders.lines,data,key_line)
	else
		for i = 1, f.lines_nr do
			local key_line = string.format("%d/%d",k,i)
			local kh, ord_lines = w_rec(sales_orders.lines,orders_line,key_line)
			if ord_lines == nil then return nil end
		end
	end
	

	return kh
end

