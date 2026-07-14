require("string_helper")
local crud = require("crud")
local system = require("db_files")

local customers = {}

function write_customers(data)

	local cli_rec = crud.create_rec(system.customers, data)
	if cli_rec == nil then
		return nil
	end

	local f = cli_rec.fields

	-- indexing function
	-- we are saving the same record with a different key, to get better
	-- searching performance,data integrety  and user experience
	-- if this one fails, it means that we have this customer in the DB already
	-- USING INDEX 2 BECAUSE INDEX 1 and 0 ARE USED INTERNALY FROM THE DB system
	-- @@ we are creating a reference to the customer record, based on the customer name @@
	local res = crud.indexing(system.customers, f.name, 2, cli_rec.offset)
	if res ~= 2 then
		return -1
	end

	local k, rec = crud.w_rec(system.customers, data)
	if k >= 0 then
		return 0,k
	else
		return -1
	end
end


function get_customer(key)
	local cust,pr_l,err
	if type(key) == 'string' then
		local sanitized_key = string.gsub(key,"%%%d+"," ") -- decode URL encoding
		cust,err = crud.g_rec(system.customers,sanitized_key,2); -- 2 is the index number in the file	
	else
		cust,err = crud.g_rec(system.customers,key)
	end

	if cust == nil  then
		print(err)
		return nil
	end


	if cust["price_level_id"] ~= nil then
		pr_l,err = crud.g_rec(system.price_level,cust.price_level_id,1) -- 1 is the file index
		if pr_l == nil then
			print(err)
			return nil
		end
	end
	
	return rec_to_json(cust)
end

function get_customer_for_new_sales_order(key)
	local cust,pr_l,err
	cust,err = g_rec(system.customers,key,2) -- 2 is the index number in the file	

	if cust == nil  then 
		print(err)
		return nil 
	end

	if cust.fields["price_level_id"] ~= nil then
		-- get price_level data
		pr_l = g_rec(system.price_level,cust.fields.price_level_id,1) -- 1 is the file index
		if pr_l == nil then
			print(err)
			return nil
		end
		-- create a json string that will be used to populate the new_sales_order
		local json_price_level = string.gsub(rec_to_json(pr_l), "{", ",")
		local json_cust =  string.gsub(rec_to_json(cust),"}","")
		return string.format("%s%s",json_cust,json_price_level)
	end
	

	return rec_to_json(cust)
end

customers.write_customer = write_customer
customers.get_customer = get_customer
customers.get_customer_for_new_sales_order = get_customer_for_new_sales_order

return customers
