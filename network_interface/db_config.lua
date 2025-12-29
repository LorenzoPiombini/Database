db = require("db")

--- DB GLOBALS
ORDER_BASE = 100

--- database files
name_file = "db/name_file"
customers = "db/customers"
sales_orders = {}
sales_orders["head"] = "db/sales_orders_head"
sales_orders["lines"] = "db/sales_orders_lines"

--- database crud functions
write_record = db.write_record
update_record = db.update_record
get_numeric_key = db.get_numeric_key
string_data_to_add_template = db.string_data_to_add_template
indexing = db.save_key_at_index
create_rec = db.create_record
g_rec = db.get_record

--- look for documentation in lua/src/export_db_lua.c
--- return two results, the record created and its key, if the key is not passed
--- as an argument
function w_rec(file_name, data, key, number)
	if key == nil then
		return write_record(file_name, data)
	elseif key == "base" and number ~= nil then
		return write_record(file_name, data, key, number)
	elseif key ~= "base" then
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
	local key = get_numeric_key(name_file, 0)
	local data_with_c_number = string.format("%s:c_number:%d", data, key)

	local cli_rec = create_rec(customers, data_with_c_number)
	if cli_rec == nil then
		return nil
	end

	local f = cli_rec.fields

	-- indexing function
	-- we are saving the same record with a different key, to get better
	-- searching performance,data integrety  and user experience
	-- if this one fails, it means that we have this customer in the DB already
	local res = indexing(customers, f.c_name, 1, cli_rec.offset)
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

function update_orders(orders_head, orders_lines, key)
	local head_rec = create_rec(sales_orders.head,orders_head)
	if head_rec == nil then
		return nil
	end

	local f = head_rec.fields
	if f == nil then
		return nil
	end

	-- update the lines first
	if f.lines_nr == 1 then
		local k_line = string.format("%s/%d", tostring(key), i)
		local line = string.sub(orders_lines, 2, -2)
		local data = string.sub(line, 3, #line)
		local up = update_record(sales_orders.lines, data, k_line)
		if up == nil then
			return nil
		end
	else
		for i = 1, f.lines_nr do
			local k_line = string.format("%s/%d", tostring(key), i)
			local line
			local ending, sub_str_ending = string.find(orders_lines, "],")
			if ending == nil then
				line = string.sub(orders_lines, 2, -2)
				line = string.sub(line, 3, #line)
			else
				line = string.sub(orders_lines, 1, ending)
				orders_lines = string.sub(orders_lines, sub_str_ending + 1, #orders_lines)
				line = string.sub(line, 2, -2)
				line = string.sub(line, 3, #line)
			end
			local up = update_record(sales_orders.lines, line, k_line)
			if up == nil then
				return nil
			end
		end
	end

	-- if update line succseed update head
	local up = update_record(sales_orders.head, orders_head, key)
	if up == nil then
		return nil
	end

	return 0
end

function write_orders(orders_head, orders_lines)
	local kh, ord_head = w_rec(sales_orders.head, orders_head, "base", ORDER_BASE)

	if ord_head == nil or kh == nil then
		return nil
	end

	local f = ord_head.fields
	if f.lines_nr == 1 then
		local key_line = string.format("%d/%d", kh, f.lines_nr)
		-- the lines of the orders will be a string like this:
		-- [w|item:Soccer shoes:uom:pair:qty:43:disc:2.2:unit_price:200:total:8410.80:request_date:1-8-26]
		-- string.sub(orders_lines,2,-2) return a string without []
		-- and the following string sub return the string without w|
		local line = string.sub(orders_lines, 2, -2)
		local data = string.sub(line, 3, #line)
		local kl, ord_lines = w_rec(sales_orders.lines, data, key_line)
	else
		for i = 1, f.lines_nr do
			local key_line = string.format("%d/%d", kh, i)
			local line
			local ending, sub_str_ending = string.find(orders_lines, "],")
			if ending == nil then
				line = string.sub(orders_lines, 2, -2)
				line = string.sub(line, 3, #line)
			else
				line = string.sub(orders_lines, 1, ending)
				orders_lines = string.sub(orders_lines, sub_str_ending + 1, #orders_lines)
				line = string.sub(line, 2, -2)
				line = string.sub(line, 3, #line)
			end
			local kh, ord_lines = w_rec(sales_orders.lines, line, key_line)
			if ord_lines == nil then
				return nil
			end
		end
	end

	return kh
end

local function rec_to_json(rec)
	local f = rec.fields
	local json = "{ "
	for key, value in pairs(f) do
		local sb
		if type(value) == "string" then
			sb = string.format('"%s":"%s",', key, value)
		elseif type(value) == "number" then
			if math.type(value) == "integer" then
				sb = string.format('"%s":"%d",', key, value)
			else
				sb = string.format('"%s":"%.2f",', key, value)
			end
		end
		json = string.format("%s%s", json, sb)
	end
	json = string.sub(json, 1, #json - 1)
	json = string.format("%s%s", json, "}")
	return json
end

function get_customer(key)
	local cust
	if type(key) == 'string' then
		local sanitized_key = string.gsub(key,"%%%d+"," ") -- decode URL encoding
		cust = g_rec(customers,sanitized_key,1); -- 1 is the index number in the file	
	else
		cust = g_rec(customers,key)
	end

	if cust == nil  then return nil end
	
	if cust.fields.on_credit_old ~= nil then
		if cust.fields.on_credit_old == 1 then
			return '{ message: "Customer on a credit hold , see a manager."}'
		end
	end

	return rec_to_json(cust)
end

function get_order(data)
	local ord = g_rec(sales_orders.head, data)
	if ord == nil then
		return nil
	end

	local head_json = rec_to_json(ord)

	local json = string.format('{ "sales_orders_head":%s,"sales_orders_lines":{', head_json)
	for i = 1, ord.fields.lines_nr do
		local key_line = string.format("%d/%d", data, i)
		local line = g_rec(sales_orders.lines, key_line)
		if line == nil then
			return nil
		end
		local line_title = string.format("line_%d", i)
		json = string.format('%s"%s":%s,', json, line_title, rec_to_json(line))
	end

	json = string.sub(json, 1, #json - 1)
	json = string.format("%s}}", json)
	return json
end
