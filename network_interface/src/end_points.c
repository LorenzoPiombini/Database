#include <string.h>
#include <stdio.h>
#include "end_points.h"


int map_end_point(char *end_point)
{
	size_t s = strlen(end_point);
	size_t so_s = strlen(NEW_S_ORDER);
	if(s == so_s)
		if(strncmp(end_point,NEW_S_ORDER,so_s) == 0) return NEW_SORD;

	s = strlen(end_point);
	so_s = strlen(CUSTOMERS);
	if(s == so_s)
		if(strncmp(end_point,CUSTOMERS,so_s) == 0) return CUSTOMER;

	s = strlen(end_point);
	so_s = strlen(SALES_ORDERS);
	if(s == so_s)
		if(strncmp(end_point,SALES_ORDERS,so_s) == 0) return S_ORD;

	return -1;
}
