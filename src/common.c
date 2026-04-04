#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "common.h"

void *array_init(size_t size, int type)
{
	switch(type){
	case INT:
	{
		struct Metadata *head = malloc(sizeof(int)*size +sizeof(struct Metadata));
		if(!head)
			return NULL;

		memset(head,0,sizeof(int)*size+sizeof(struct Metadata));
		head->capacity = (long long)size;
		head->elements = (long long) 0;
		head->type = type;
		return (void*)(head +1);
	}
	case LONG:
	{
		struct Metadata *head = malloc(sizeof(long)*size +sizeof(struct Metadata));
		if(!head)
			return NULL;

		memset(head,0,sizeof(long)*size+sizeof(struct Metadata));
		head->capacity = (long long)size;
		head->elements = (long long) 0;
		head->type = type;
		
		return (void*)(head +1);
	}
	case BYTE:
	{
		struct Metadata *head = malloc(sizeof(unsigned char)*size +sizeof(struct Metadata));
		if(!head)
			return NULL;

		memset(head,0,sizeof(unsigned char)*size+sizeof(struct Metadata));
		head->capacity = (long long)size;
		head->elements = (long long) 0;
		head->type = type;
		return (void*)(head +1);
	}
	case STRING:
	{
		struct Metadata *head = malloc(sizeof(char*)*size +sizeof(struct Metadata));
		if(!head)
			return NULL;

		memset(head,0,sizeof(char*)*size+sizeof(struct Metadata));
		head->capacity = (long long)size;
		head->elements = (long long) 0;
		head->type = type;
		return (void*)(head +1);
	}
	case DOUBLE:
	{
		struct Metadata *head = malloc(sizeof(double)*size +sizeof(struct Metadata));
		if(!head)
			return NULL;

		memset(head,0,sizeof(double)*size+sizeof(struct Metadata));
		head->capacity = (long long)size;
		head->elements = (long long) 0;
		head->type = type;
		return (void*)(head +1);
	}	
	case FLOAT:
	{
		struct Metadata *head = malloc(sizeof(double)*size +sizeof(struct Metadata));
		if(!head)
			return NULL;

		memset(head,0,sizeof(float)*size+sizeof(struct Metadata));
		head->capacity = (long long)size;
		head->elements = (long long) 0;
		head->type = type;
		return (void*)(head +1);
	}	
	default:
		return NULL;
	}
}

void array_insert_at(int i, void *arr, void *el)
{
	struct Metadata *h = (struct Metadata*)arr - 1;
	switch(h->type){
	case LONG:
		if(h->capacity < i){
			/*TODO: realloc?*/
			assert(0);
		}else{
			long *a = (long*)arr;
			a[i] = *(long*)el;
			arr = (void*)a;
			h->elements++;
		}
		return;
	case INT:
		if(h->capacity < i){
			/*realloc*/
			assert(0);
		}else{
			int *a = (int*)arr;
			a[i] = *(int*)el;
			arr = (void*)a;
			h->elements++;
		}
		return; 
	case DOUBLE:
		if(h->capacity < i){
			/*realloc*/
		}else{
			double *a = (double*)arr;
			a[i] = *(double*)el;
			arr = (void*)a;
			h->elements++;
		}
		return;
	case FLOAT:
		if(h->capacity < i){
			/*realloc*/
		}else{
			float *a = (float*)arr;
			a[i] = *(float*)el;
			arr = (void*)a;
			h->elements++;
		}
		return;
	case BYTE:
		if(h->capacity < i){
			/*realloc*/
		}else{
			unsigned char *a = (unsigned char*)arr;
			a[i] = *(unsigned char*)el;
			arr = (void*)a;
			h->elements++;
		}
		return;
	case STRING:
		if(h->capacity < i){
			/*realloc*/
		}else{
			char **a = (char **)arr;
			a[i] = (char*)malloc(strlen((char*)el)+1);
			a[i][strlen((char*) el)] = '\0';
			memcpy(a[i],(char*) el,strlen((char*)el));
			h->elements++;
			arr = (void*)a;
		}
		return;
	default:
		return;
	}
}

void array_push(void *arr, void *el)
{

	struct Metadata *h = (struct Metadata*)arr - 1;
	switch(h->type){
	case LONG:
		if(h->elements == h->capacity){
			/*realloc*/
		}else{
			long *a = (long*)arr;
			a[h->elements] = *(long*)el;
			arr = (void*)a;
			h->elements++;
		}
		return;
	case INT:
		if(h->elements == h->capacity){
			/*realloc*/
		}else{
			int *a = (int*)arr;
			a[h->elements] = *(int*)el;
			arr = (void*)a;
			h->elements++;
		}
		return; 
	case DOUBLE:
		if(h->elements == h->capacity){
			/*realloc*/
		}else{
			double *a = (double*)arr;
			a[h->elements] = *(double*)el;
			arr = (void*)a;
			h->elements++;
		}
		return;
	case FLOAT:
		if(h->elements == h->capacity){
			/*realloc*/
		}else{
			float *a = (float*)arr;
			a[h->elements] = *(float*)el;
			arr = (void*)a;
			h->elements++;
		}
		return;
	case BYTE:
		if(h->elements == h->capacity){
			/*realloc*/
		}else{
			unsigned char *a = (unsigned char*)arr;
			a[h->elements] = *(unsigned char*)el;
			arr = (void*)a;
			h->elements++;
		}
		return;
	case STRING:
		if(h->elements == h->capacity){
			/*realloc*/
		}else{
			char **a = (char **)arr;
			a[h->elements] = (char*)malloc(strlen((char*)el)+1);
			a[h->elements][strlen((char*) el)] = '\0';
			memcpy(a[h->elements],(char*) el,strlen((char*)el));
			h->elements++;
			arr = (void*)a;
		}
		return;
	default:
		return;
	}
}

void array_free(void*arr)
{
	struct Metadata *h = (struct Metadata *)arr - 1;
	switch(h->type){
	case INT:
	case LONG:
	case DOUBLE:
	case FLOAT:
	case BYTE:
		free(h);
		return;
	case STRING:
	{
		char **c = (char**)arr;
		int i;
		for(i = 0; i < h->elements; i++){
			if(c[i])
				free(c[i]);
		}
		free(h);
		return;
	}
	default:
		return;
	}
}


