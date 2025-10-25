#ifndef _END_POINTS_H
#define _END_POINTS_H


enum endp{
	NEW_SORD,
	NEW_SORD_LINES,
	CUSTOMER_GET,
	S_ORD,
	S_ORD_GET,
	UPDATE_SORD,
	CUSTOMER_GET_ALL
};

#define NEW_S_ORDER "/new_sales_order"
#define CUSTOMERS "/customers"
#define SALES_ORDERS "/sales_orders"
#define UPDATE_ORDERS "/update_orders/sales"


int map_end_point(char *end_point);

#endif

