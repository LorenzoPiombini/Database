#ifndef DTYPE_H
#define DTYPE_H

#include <stdint.h>

#define DEF_STR 1024 /*1 kb*/
#define SET_ON 1
#define SET_OFF 0

struct String{
	uint8_t allocated : 1;
	char base[DEF_STR];
	char *str;	
	size_t size;
	int (*append)(struct String*,const char *);
	uint8_t (*is_empty)(struct String*);
	void (*close)(struct String*);
};


int init(struct String* str,const char *val);
#endif
