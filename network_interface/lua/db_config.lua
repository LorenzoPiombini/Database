require("customers")
require("sales_orders")
require("items")


TEST = true



-- function signature 
-- this will be use from work_process.c to execute the function open_orders()
-- this way we won't need to recompile the C back end all the time 
-- when adding a new report

open_orders_s = ">s"
sales_orders_week_s = ">s"
overdue_orders_s = ">s"
-----------------------------------------------------------------------

function open_orders()
	local all_head_k = g_all_key(sales_orders.head,0)
	if type(all_head_k) ~= "string" then return -1 end

	local totals = get_orders_total(all_head_k)
	if totals == -1 then 
		print("cannot get sales_order_head data!")
		return '{message: "server error"}'
	elseif totals == -2 then 
		print("cannot get price_level data!")
		return '{message: "server error"}'
	elseif totals == -3 then 
		print("cannot get sales_order_line data!")
		return '{message: "server error"}'
	elseif totals == -4 then 
		print("cannot get item data!")
		return '{message: "server error"}'
	end

	local json = "{"
	for n in string.gmatch(all_head_k,"%d+") do
		json = string.format('%s"%d":{%s},',json,n,totals[n])
	end
	json = string.format('%s"orders_total":%.2f}',json,totals['orders_total'])
	return json
end


-- return begining of the current week in seconds (since january 1 1970) 
local function get_week_start()
	today_second = os.time()
	start = os.date('%w',today_second)
	if start == 0 then
		return today_second
	end
	return today_second - (start *(60*60*24))
end

-- return the end of the current week in seconds (since january 1 1970) 
local function get_week_end()
	today_second = os.time()
	finish = os.date('%w',today_second)
	if finish == 6 then
		return today_second
	end
	return today_second + ((finish +(6 - finish)) *(60*60*24))
end

local function convert_date(date)
	if date == nil then return {year  = 1987 ,month = 3, day = 2} end
	m,d,y = string.match(date,"(%d+)-(%d+)-(%d+)")
	y = tonumber(y) + 2000
	date = {year  = y ,month = m, day = d}
	return os.time(date)
end

local function is_date_this_week(date_second)
	start = get_week_start()
	finish = get_week_end()
	return (start <= date_second) and (finish >= date_second)
end

local function get_orders_by_week_range(head_keys)
	local result = '{"message":['
	local exit = false
	for k,v in pairs(head_keys) do
		local h,err_h = g_rec(sales_orders.head,v)

		if h == nil then print(err_h) return -1 end

		for i = 1, h.fields.lines_nr do
			local k_lines = string.format("%d/%d",v,i)
			local line,err_l = g_rec(sales_orders.lines,k_lines)
			if line == nil then print(err_l) return -1 end
			
			if is_date_this_week(convert_date(line.fields.request_date)) == true then
				if string.sub(result,-1) == '[' then
					result = string.format('%s%d',result,v)
				else
					result = string.format('%s,%d',result,v)
				end
			elseif convert_date(line.fields.request_date) > get_week_end() then
				exit = true
				break
			end
		end
		if exit then
			break
		end
	end
	
	if string.sub(result,-1) == '[' then
		return '{"message":[]}'
	end

	if string.sub(result,-1) == ',' then
		result = string.sub(result, 1, #result - 1)
		return string.format('%s]}',result)
	else
		return string.format('%s]}',result)
	end
	
end

function sales_orders_week()
	local all_head_k = g_all_key(sales_orders.head,0)
	if type(all_head_k) ~= "string" then return -1 end

	-- we sort the keys, to increase performance (NOTE: FOR NOW!)
	-- we need to come up with better solutions
	-- this patch run on the assumption that the sales order file contains only the open orders
	-- which will be the case in real life, but the open orders could be thousands
	local ordered_keys = {}
	for n in string.gmatch(all_head_k,"%d+") do
		table.insert(ordered_keys,tonumber(n))
	end
	table.sort(ordered_keys)

	return get_orders_by_week_range(ordered_keys)
end

local function is_date_in_the_past(date)
	one_day_seconds = 60*60*24
	now = os.time()
	if now - date < one_day_seconds then
		-- TODO: convert to table and check the date
	end
	return (now - date) > one_day_seconds
end

local function get_overdue_order(head_keys)
	local result = '{"message":['
	local exit = false
	now = os.time()
	for k,v in pairs(head_keys) do
		local h,err_h = g_rec(sales_orders.head,v)

		if h == nil then print(err_h) return -1 end

		for i = 1, h.fields.lines_nr do
			local k_lines = string.format("%d/%d",v,i)
			local line,err_l = g_rec(sales_orders.lines,k_lines)
			if line == nil then print(err_l) return -1 end
			
			if is_date_in_the_past(convert_date(line.fields.request_date)) == true then
				if i == 1 then
					if string.sub(result,-1) == '[' then
						result = string.format('%s"%s"',result,v)
					else
						result = string.format('%s,"%s"',result,v)
					end
				end
			end
		end
	end
	
	if string.sub(result,-1) == '[' then
		return '{"message":[]}'
	end

	if string.sub(result,-1) == ',' then
		result = string.sub(result, 1, #result - 1)
		return string.format('%s]}',result)
	else
		return string.format('%s]}',result)
	end
	
end

-- helper function for reports on open orders
local function get_orders_total(keys_head)
	local totals = {}
	local orders_tot = 0.0
	for n in string.gmatch(keys_head,"%d+") do
		local k = tonumber(n)
		local h,err = g_rec(sales_orders.head,k)

		if h == nil then
			print(err)
			return -1 
		end

		local disc = 0
		if h.fields.price_level_id ~= nil then
			local pr_l,err = g_rec('/root/db/price_level',h.fields.price_level_id,1)
			if pr_l == nil then
				print(err)
				return -2 
			end
			disc = pr_l.fields.percentage
		end

		local total = 0
		for i = 1, h.fields.lines_nr do
			local k_lines = string.format("%d/%d",k,i)
			local line,err_l = g_rec(sales_orders.lines,k_lines)
			if line == nil then 
				print(err_l)
				return -3 
			end

			local item,err_i = g_rec('/root/db/item',line.fields.item_id,1)
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

function overdue_orders()
	local all_head_k = g_all_key(sales_orders.head,0)
	if type(all_head_k) ~= "string" then return -1 end

	local ordered_keys = {}
	for n in string.gmatch(all_head_k,"%d+") do
		table.insert(ordered_keys,tonumber(n))
	end
	table.sort(ordered_keys)
	local r = get_overdue_order(ordered_keys);
	local k = string.match(r,"%[.-%]")

	local totals = get_orders_total(k)

	local json = "{"
	for n in string.gmatch(k,"%d+") do
		json = string.format('%s"%d":{%s},',json,n,totals[n])
	end

	json = string.format('%s"orders_total":%.2f}',json,totals['orders_total'])
	return json
end
