#ifndef _END_POINTS_H
#define _END_POINTS_H

/*file system*/
#define SALES_ORDERS_H "/root/db/sales_orders_head"
#define SALES_ORDERS_L "/root/db/sales_orders_lines"
#define CUSTOMER_FILE "/root/db/customer"
#define NAME_FILE "/root/db/name_file"
#define ITEM_FILE "/root/db/item"

/*****/

/*ENDPOINTS*/
enum endp{
	NEW_SORD,
	NEW_SORD_LINES,
	CUSTOMER_GET,
	S_ORD_CUSTOMER_GET, /*this is for new sales orders menu*/
	S_ORD,
	S_ORD_GET,
	UPDATE_SORD,
	CUSTOMER_GET_ALL,
	NEW_CUST,
	ITEM_GET_ALL,
	NEW_ITEM,
	UPDATE_ITEM,
	ITEM_GET
};


#define NEW_S_ORDER "/new_sales_order"
#define CUSTOMERS "/customers"
#define SALES_NEW_ORDER_CUSTOMERS "/sales_new_order_customers"
#define NEW_CUSTOMER "/new_customer"
#define SALES_ORDERS "/sales_orders"
#define UPDATE_ORDERS "/update_orders/sales"
#define ITEMS "/items"
#define NEW_ITEM "/new_item"
#define UPDATE_ITEM "/update_item/item"


int map_end_point(char *end_point);

#endif

