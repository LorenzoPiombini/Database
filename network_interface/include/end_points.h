#ifndef _END_POINTS_H
#define _END_POINTS_H


enum endp{
	NEW_SORD,
	CUSTOMER
};


#define NEW_S_ORDER "/new_sales_order"
#define CUSTOMERS "/customers"


int map_end_point(char *end_point);

#endif

