#ifndef _END_POINTS_H
#define _END_POINTS_H


enum endp{
	NEW_SORD,
	NEW_SORD_LINES,
	CUSTOMER,
	S_ORD
};


#define NEW_S_ORDER "/new_sales_order"
#define UPDATE_S_ORDER "/update_sales_order"
#define CUSTOMERS "/customers"
#define SALES_ORDERS "/sales_orders"


int map_end_point(char *end_point);

#endif

