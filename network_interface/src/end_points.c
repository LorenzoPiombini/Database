#include "end_points.h"
#include "freestand.h"


int map_end_point(char *end_point)
{ 
	size_t s,so_s; 
	if(find_needle(end_point,NEW_S_ORDER)){
		s = string_length(end_point);
		so_s = string_length(NEW_S_ORDER);
		if(s == so_s)
			if(string_compare(end_point,NEW_S_ORDER,so_s,-1) == 0) return NEW_SORD;

	}

	if(find_needle(end_point,NEW_CUSTOMER)){
		so_s = string_length(NEW_CUSTOMER);
		if(string_compare(end_point,NEW_CUSTOMER,so_s,-1) == 0) return NEW_CUST;
	}


	if(find_needle(end_point,SALES_ORDERS)){
		s = string_length(end_point);
		so_s = string_length(SALES_ORDERS);
		if(s == so_s){
			if(string_compare(end_point,SALES_ORDERS,so_s,-1) == 0) return S_ORD;
		}else if(s > so_s){
			/*if contains sales order return */
			if(string_compare(end_point,SALES_ORDERS,so_s,COMPARE_CONTAIN) == 0) return S_ORD_GET;
		}
	}

	if(find_needle(end_point,CUSTOMERS)){
		s = string_length(end_point);
		so_s = string_length(CUSTOMERS);
		if(s == so_s){
			if(string_compare(end_point,CUSTOMERS,so_s,-1) == 0) return CUSTOMER_GET_ALL;
		}else if(s > so_s){
			if(string_compare(end_point,CUSTOMERS,so_s,COMPARE_CONTAIN) == 0) return CUSTOMER_GET;
		}

	}


	if(find_needle(end_point,SALES_ORDERS)){
		s = string_length(end_point);
		so_s = string_length(SALES_ORDERS);
		if(s == so_s){
			if(string_compare(end_point,SALES_ORDERS,so_s,-1) == 0) return S_ORD;
		}else if(s > so_s){
			/*if contains sales order return */
			if(string_compare(end_point,SALES_ORDERS,so_s,COMPARE_CONTAIN) == 0) return S_ORD_GET;
		}
	}

	
	if(find_needle(end_point,UPDATE_ORDERS)){
		s = string_length(end_point);
		so_s = string_length(UPDATE_ORDERS);
		if(s == so_s){
			if(string_compare(end_point,UPDATE_ORDERS,so_s,-1) == 0) return UPDATE_SORD;
		}else if(s > so_s){
			if(string_compare(end_point,UPDATE_ORDERS,so_s,COMPARE_CONTAIN) == 0) return UPDATE_SORD;
		}
	}

	return -1;
}
