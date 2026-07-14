require("globals")
require("string_helper")
local crud = require("crud")
local system = require("db_files")

local s_orders = {}
function update_orders(orders_head, orders_lines, key)
	local head_rec = crud.create_rec(system.sales_orders.head,orders_head)
	if head_rec == nil then
		return nil
	end

	local f = head_rec.fields
	if f == nil then
		return nil
	end

	-- update the lines first
	if f.lines_nr == 1 then
		local k_line = string.format("%s/%d", tostring(key), f.lines_nr)
		local line = string.sub(orders_lines, 2, -2)
		local data = string.sub(line, 3, #line)
		local up = crud.update_record(system.sales_orders.lines, data, k_line)
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
			local up = crud.update_record(system.sales_orders.lines, line, k_line)
			if up == KEY_NOT_FOUND  then
				local r = crud.write_record(system.sales_orders.lines,line,k_line)
				if r == nil then 
					print("cannot write!") 
					return -1 
				end
			elseif up ~= 0 then
				print("error!")
				return -1
			end
		end
	end
	
	-- check if lines are different, 
	local head_on_file,err = crud.g_rec(system.sales_orders.head,key)
	if head_on_file == nil then 
		print(err)
		return -1 
	end

	if head_on_file.fields.lines_nr > f.lines_nr then
		-- we need to delete some records in sales_orders_lines
		for i = f.lines_nr + 1, head_on_file.fields.lines_nr do
			local k = string.format("%d/%d",key,i)
			--delete record with key k
			local r,message = crud.d_rec(system.sales_orders.lines,k)
			if r ~= 0 then 
				print("error in cleaning sales_order_lines");
				print(message)
				return -1
			end
		end
	end

	-- if update line succseed update head
	local up = crud.update_record(system.sales_orders.head, orders_head, key)
	if up == nil then
		return nil
	end

	return 0
end

function write_orders(orders_head, orders_lines)
	local kh, ord_head = crud.w_rec(system.sales_orders.head, orders_head, "base", ORDER_BASE)

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
		local kl, ord_lines = crud.w_rec(system.sales_orders.lines, data, key_line)
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
			local kh, ord_lines = crud.w_rec(system.sales_orders.lines, line, key_line)
			if ord_lines == nil then
				return nil
			end
		end
	end

	return kh
end

-- the get_order function has to rebuild the order!
-- the order data are spread between 
-- 	@ sales_order_head
-- 	@ sales_order_lines
-- 	@ items
-- 	@ price_level
-- this is because we want to maintain the database normalized
function get_order(data)
	local ord,pr_l,item,err
	ord,err = crud.g_rec(system.sales_orders.head, data)

	if ord == nil then
		print(err)
		return nil 
	end

	local disc = 0
	if ord.fields.price_level_id ~= nil then
		pr_l,err = crud.g_rec(system.price_level,ord.fields.price_level_id,1)
		if pr_l == nil then
			print(err)
			return nil
		end
		disc = pr_l.fields.percentage
	end

	local head_json = rec_to_json(ord)

	local json = string.format('{ "sales_orders_head":%s,"sales_orders_lines":{', head_json)
	for i = 1, ord.fields.lines_nr do
		local key_line = string.format("%d/%d", data, i)
		local line,err = crud.g_rec(sales_orders.lines, key_line)

		if line == nil then 
			print(err)
			return nil
		end

		-- change the date format to conform with what the browser expect  
		if line.fields.request_date ~= nil then
			m,d,y = string.match(line.fields.request_date,"(%d+)-(%d+)-(%d+)")
			y = tonumber(y) + 2000
			line.fields.request_date = string.format('%s-%s-%s',y,m,d)
		end

		local item, err= crud.g_rec(system.items,line.fields.item_id,1)
		if item == nil then
			print(err)
			return nil
		end

		local total = item.fields.unit_price * line.fields.qty
		if disc ~= 0 then
			total = total * ((100 - disc)/100)
		end
		local str_to_add_to_line = string.format('"uom":"%s","unit_price":"%.2f","disc":"%.2f","total":"%.2f"',
								item.fields.uom,
								item.fields.unit_price,
								disc,
								total)

		local line_title = string.format("line_%d", i)
		local line_from_file = rec_to_json(line)
		line_from_file = string.sub(line_from_file,2,-1)
		json = string.format('%s"%s":{%s,%s,', json, line_title,str_to_add_to_line,line_from_file)
	end

	json = string.sub(json,1,#json-1)
	json = string.format("%s}}", json)
	return json
end

-- helper function for reports on open orders
local function get_orders_total(keys_head)
	local totals = {}
	local orders_tot = 0.0
	for n in string.gmatch(keys_head,"%d+") do
		local k = tonumber(n)
		local h,err = crud.g_rec(system.sales_orders.head,k)

		if h == nil then
			print(err)
			return -1 
		end

		local disc = 0
		if h.fields.price_level_id ~= nil then
			local pr_l,err = crud.g_rec(system.price_level,h.fields.price_level_id,1)
			if pr_l == nil then
				print(err)
				return -2 
			end
			disc = pr_l.fields.percentage
		end

		local total = 0
		for i = 1, h.fields.lines_nr do
			local k_lines = string.format("%d/%d",k,i)
			local line,err_l = crud.g_rec(system.sales_orders.lines,k_lines)
			if line == nil then 
				print(err_l)
				return -3 
			end

			local item,err_i = crud.g_rec(system.items,line.fields.item_id,1)
			if item == nil then print(err_i) return -4 end

			total = total + item.fields.unit_price * line.fields.qty
			if disc ~= 0 then
				total = total * ((100-disc)/ 100)
			end
		end
		orders_tot = orders_tot + total
		totals[n] = string.format('"%s":"%s","%s":%.2f',"customer_id",h.fields.customer_id,"total",total)
	end
	totals["orders_total"] = string.format('%.2f',orders_tot)
	return totals
end

s_orders.get_orders_total = get_orders_total

return s_orders
