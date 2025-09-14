#include <string.h>
#include <stdio.h>
#include "end_points.h"


int map_end_point(char *end_point)
{
	size_t s,so_s; 
	if(strstr(end_point,NEW_S_ORDER)){
		s = strlen(end_point);
		so_s = strlen(NEW_S_ORDER);
		if(s == so_s)
			if(strncmp(end_point,NEW_S_ORDER,so_s) == 0) return NEW_SORD;

	}

	if(strstr(end_point,CUSTOMERS)){
	s = strlen(end_point);
	so_s = strlen(CUSTOMERS);
	if(s == so_s)
		if(strncmp(end_point,CUSTOMERS,so_s) == 0) return CUSTOMER;
	}

	if(strstr(end_point,SALES_ORDERS)){
		s = strlen(end_point);
		so_s = strlen(SALES_ORDERS);
		if(s == so_s){
			if(strncmp(end_point,SALES_ORDERS,so_s) == 0) return S_ORD;
		}else if(s > so_s){
			/*if contains sales order return */
			if(strncmp(end_point,SALES_ORDERS,so_s) == 0) return S_ORD_GET;
		}
	}

	
	if(strstr(end_point,UPDATE_ORDERS)){
		s = strlen(end_point);
		so_s = strlen(UPDATE_ORDERS);
		if(s == so_s || s > so_s){
			if(strncmp(end_point,UPDATE_ORDERS,so_s) == 0) return UPDATE_SORD;
		}
	}

	return -1;
}
