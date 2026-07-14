
local system = {}
--- database files
-- name_file = "db/name_file" /* i do not need it for now */
system.customers = "/root/db/customer"
system.price_level = "/root/db/price_level"
system.items = "/root/db/item"
system.sales_orders = {}
system.sales_orders["head"] = "/root/db/sales_orders_head"
system.sales_orders["lines"] = "/root/db/sales_orders_lines"

return system
