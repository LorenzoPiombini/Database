#ifndef __LOAD_H_
#define __LOAD_H_

#include "request.h"

#define MAX_CONT_SZ 1024*8 /* 8 Kib */
#define SALES_ORDERS_H "./db/sales_orders_head"
#define SALES_ORDERS_L "./db/sales_orders_lines"
struct Content{
	char cnt_st[MAX_CONT_SZ];
	char *cnt_dy;
	size_t size;
	uint64_t capacity; 
};

int load_resource(struct Request *req, struct Content *cont, int data_sock);
void clear_content(struct Content *cont);

#endif
