#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "dtype.h"

static char prog[] = "db";
static void free_str(struct String *str);
static uint8_t empty(struct String *str);
static int append(struct String *str, const char *str_to_appen);

static int append(struct String *str, const char *str_to_appen)
{
	if(!str_to_appen) return -1;

	size_t nl = strlen(str_to_appen);
	if(str->size < DEF_STR){
		if((str->size + nl) < DEF_STR){
			strncat(str->base,str_to_appen,nl);	
			str->size += nl;
			return 0;
		}
		goto allocate_new_mem;
	}

allocate_new_mem:
	if(str->str){
		errno = 0;
		char *n = realloc(str->str,(str->size + nl) * sizeof(char));
		if(!n){
			fprintf(stderr,"(%s): realloc failed with '%s', %s:%d\n",prog,strerror(errno), __FILE__,__LINE__-2);	
			return -1;
		}
		str->str = n;
		if(str->allocated != SET_OFF){
			strncat(str->str,str_to_appen,nl);
		}else{
			strncpy(str->str,str->base,str->size);
			strncat(str->str,str_to_appen,nl);
			memset(str->base,0,DEF_STR);
		}
			
		str->size += nl;
		return 0;
	}

	errno = 0;
	str->str =  calloc(str->size + nl,sizeof(char));
	if(!str->str){
		fprintf(stderr,"(%s): calloc failed with '%s', %s:%d\n",prog,strerror(errno), __FILE__,__LINE__-2);	
		return -1;
	}

	if(str->allocated != SET_OFF){
		strncat(str->str,str_to_appen,nl);
	}else{
		strncpy(str->str,str->base,str->size);
		strncat(str->str,str_to_appen,nl);
		memset(str->base,0,DEF_STR);
	}

	str->size += nl;

	return 0;
}

int init(struct String *str,const char *val)
{
	if(!str) return -1;
	if(str->base[0] != '\0' || str->str){
		fprintf(stderr,"(%s): string has been a;ready initialized",prog);
		return -1;
	}
	
	if(!val){
		memset(str->base,0,DEF_STR);
		str->str = NULL;
		str->append = append;
		str->is_empty = empty;
		str->close = free_str;
		str->allocated |= SET_OFF;
		str->size = 0;
		return 0;
	}

	size_t l = strlen(val);
	if(l >= DEF_STR){
		errno = 0;
		str->str = calloc(l+1,sizeof(char));
		if(!str->str){
			fprintf(stderr,"(%s): calloc failed with '%s', %s:%d\n",prog,strerror(errno), __FILE__,__LINE__-2);	
			return -1;
		}
		strncpy(str->str,val,l);
		str->append = append;
		str->is_empty = empty;
		str->allocated |= SET_ON;
		str->size = l;
		str->close = free_str;
		memset(str->base,0,DEF_STR);
		return 0;
	}
	strncpy(str->base,val,l);
	str->allocated |= SET_OFF;
	str->size = l;
	str->append = append;
	str->is_empty = empty;
	str->close = free_str;
	return 0;
}

static uint8_t empty(struct String *str)
{
	if(str->str) return *str->str == '\0';
	return str->base[0] == '\0';
}

static void free_str(struct String *str)
{
	if(str->allocated){
		free(str->str);
		str->allocated |= SET_OFF;
		str->append = NULL;
		return;
	}	

	memset(str->base,0,DEF_STR);
	str->allocated |= SET_OFF;
	str->size = 0;
}
