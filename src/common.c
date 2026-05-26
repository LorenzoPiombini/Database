/*
 * this is the implementation of a dynamic array
 * */
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
		struct Metadata *head = malloc(sizeof(float)*size +sizeof(struct Metadata));
		if(!head)
			return NULL;

		memset(head,0,sizeof(float)*size+sizeof(struct Metadata));
		head->capacity = (long long)size;
		head->elements = (long long) 0;
		head->type = type;
		return (void*)(head +1);
	}	
	case VOID:
	{
		struct Metadata *head = malloc(sizeof(struct Mix_t)*size +sizeof(struct Metadata));
		if(!head)
			return NULL;

		memset(head,0,sizeof(struct Mix_t)*size+sizeof(struct Metadata));
		head->capacity = (long long)size;
		head->elements = (long long) 0;
		head->type = type;
		return (void*)(head +1);
	}
	default:
		return NULL;
	}
}

int array_mix_element(int type,struct Mix_t *el, void* value)
{
	switch(type){
	case INT:
	{ 
		el->type = type;
		el->v = (void*)malloc(sizeof(int));
		if(!el->v){
			return -1;
		}
		el->v = value;	
		return 0;
	}
	case LONG:
	{
		el->type = type;
		el->v = (void*)malloc(sizeof(long));
		if(!el->v){
			return -1;
		}
		*(long*)el->v = *(long*)value;	
		return 0;
	}
	case BYTE:
	{
		el->type = type;
		el->v = (void*)malloc(sizeof(unsigned char));
		if(!el->v){
			return -1;
		}
		el->v = value;	
		return 0;
	}
	case FLOAT:
	{
		el->type = type;
		el->v = (void*)malloc(sizeof(float));
		if(!el->v){
			return -1;
		}
		el->v = value;	
		return 0;
	}
	case DOUBLE:
	{
		el->type = type;
		el->v = (void*)malloc(sizeof(double));
		if(!el->v){
			return -1;
		}
		el->v = value;	
		return 0;
	}
	case STRING:
	{
		size_t size = strlen((char*)value);
		el->type = type;
		el->v = (void*)malloc(sizeof(char*)*(size+1));
		if(!el->v){
			return -1;
		}
		memcpy(el->v,value,size);
		return 0;
	}
	default:
		return -1;
	}

}
/*
 * insert at, should always expand the memory allocation at least one
 * 
 * */
int array_insert_at(int i, void **arr, void *el)
{
	struct Metadata *h = (struct Metadata*)*arr - 1;
	switch(h->type){
	case LONG:
	{
		if(h->capacity <= i){
			int new_size = (i - h->capacity) + 1;
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(long)*(h->capacity + new_size)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((long*)*arr)[h->capacity],0,sizeof(long) * new_size);
			h->capacity += new_size;
		}else {
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(long)*(h->capacity + 1)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((long*)*arr)[h->capacity],0,sizeof(long) * 1);
			h->capacity++;
		}

		/*move the data over to the right*/
		long *a = (long*)*arr;
		int j;
		for(j = h->capacity - 1; j != i; j--){
				a[j-1] = a[j] ^ a[j-1];
				a[j] = a[j - 1] ^ a[j];
				a[j-1] = a[j] ^ a[j-1];
		}

		a[i] = *(long*)el;
		*arr = (void*)a;
		h->elements++;
		return 0;
	}
	case INT:
	{
		if(h->capacity <= i){
			/*realloc*/
			int new_size = (i - h->capacity) + 1;
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(int)*(h->capacity + new_size)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((int*)*arr)[h->capacity],0,sizeof(int) * new_size);
			h->capacity += new_size;
		}else {
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(int)*(h->capacity + 1)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((int*)*arr)[h->capacity],0,sizeof(int) * 1);
			h->capacity++;
		}

		/*move the data over to the right*/
		int *a = (int*)*arr;
		int j;
		for(j = h->capacity - 1; j != i; j--){
				a[j-1] = a[j] ^ a[j-1];
				a[j] = a[j - 1] ^ a[j];
				a[j-1] = a[j] ^ a[j-1];
		}

		a[i] = *(int*)el;
		*arr = (void*)a;
		h->elements++;
		return 0; 
	}
	case DOUBLE:
	{
		if(h->capacity <= i){
			/*realloc*/
			int new_size = (i - h->capacity) + 1;
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(double)*(h->capacity + new_size)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((double*)*arr)[h->capacity],0,sizeof(double) * new_size);
			h->capacity += new_size;
		}else {
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(double)*(h->capacity + 1)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((double*)*arr)[h->capacity],0,sizeof(double) * 1);
			h->capacity++;
		}

		/*move the data over to the right*/
		double *a = (double*)*arr;
		int j;
		for(j = h->capacity - 1; j != i; j--){
			double temp = a[j-1];
			a[j-1] = a[j];
			a[j] = temp;
		}
		a[i] = *(double*)el;
		*arr = (void*)a;
		h->elements++;
		return 0;
	}
	case FLOAT:
	{
		if(h->capacity < i){
			/*realloc*/
			int new_size = (i - h->capacity) + 1;
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(float)*(h->capacity + new_size)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((float*)*arr)[h->capacity],0,sizeof(float) * new_size);
			h->capacity += new_size;
		}else {
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(float)*(h->capacity + 1)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((float*)*arr)[h->capacity],0,sizeof(long) * 1);
			h->capacity++;
		}

		/*move the data over to the right*/
		float *a = (float*)*arr;
		int j;
		for(j = h->capacity - 1; j != i; j--){
			float temp = a[j-1];
			a[j-1] = a[j];
			a[j] = temp;
		}
		a[i] = *(float*)el;
		*arr = (void*)a;
		h->elements++;
		return 0;
	}
	case BYTE:
	{
		if(h->capacity < i){
			/*realloc*/
			int new_size = (i - h->capacity) + 1;
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(unsigned char)*(h->capacity + new_size)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((unsigned char*)*arr)[h->capacity],0,sizeof(unsigned char) * new_size);
			h->capacity += new_size;
		}else {
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(unsigned char)*(h->capacity + 1)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((unsigned char*)*arr)[h->capacity],0,sizeof(unsigned char) * 1);
			h->capacity++;
		}

		/*move the data over to the right*/
		unsigned char *a = (unsigned char*)*arr;
		int j;
		for(j = h->capacity - 1; j != i; j--){
				a[j-1] = a[j] ^ a[j-1];
				a[j] = a[j - 1] ^ a[j];
				a[j-1] = a[j] ^ a[j-1];
		}
		a[i] = *(unsigned char*)el;
		*arr = (void*)a;
		h->elements++;
		return 0;
	}
	case STRING:
	{
		if(h->capacity < i){
			/*realloc*/
			int new_size = (i - h->capacity) + 1;
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(char*)*(h->capacity + new_size)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((char**)*arr)[h->capacity],0,sizeof(char*) * new_size);
			h->capacity += new_size;
		}else {
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(char*)*(h->capacity + 1)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((char**)*arr)[h->capacity],0,sizeof(char*) * 1);
			h->capacity++;
		}

		char **a = (char **)*arr;
		int j;
		for(j= h->capacity -1; j != i; j--){
			if(a[j-1] && !a[j]){
				int size = (int) strlen(a[j-1]);
				a[j] = (char*) malloc(size+1);
				if(!a[j])
					return -1;
				memset(a[j], 0,size+1);
				strncpy(a[j],a[j-1],size);
				free(a[j-1]);
				a[j-1] = NULL;
			}
		}
		a[i] = (char*)malloc(strlen((char*)el)+1);
		a[i][strlen((char*) el)] = '\0';
		memcpy(a[i],(char*) el,strlen((char*)el));
		h->elements++;
		*arr = (void*)a;
		return 0;
	}
	case VOID:
	{
		if(h->capacity < i){
			/*realloc*/
			int new_size = (i - h->capacity) + 1;
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(struct Mix_t)*(h->capacity + new_size)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((struct Mix_t*)*arr)[h->capacity],0,sizeof(struct Mix_t) * new_size);
			h->capacity += new_size;
		}else {
			void *n = realloc((struct Metadata*)*arr - 1, (sizeof(struct Mix_t)*(h->capacity + 1)) + sizeof(struct Metadata));
			if(!n){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__ -2);
				return -1;
			}
			h = (struct Metadata*)n;
			*arr = (struct Metadata*) n + 1;
			memset(&((struct Mix_t*)*arr)[h->capacity],0,sizeof(struct Mix_t) * 1);
			h->capacity++;
		}

		struct Mix_t *a = (struct Mix_t*)*arr;
		int j;
		for(j= h->capacity -1; j != i; j--){
			if(a[j-1].type != -1 && a[j].type == -1){
				struct Mix_t *temp = &a[j-1];
				a[j] = *temp;
				a[j-1].type = -1;
				a[j-1].v = NULL;
			}
		}
		a[i] = *(struct Mix_t*) el;
		h->elements++;
		*arr = (void*)a;
		return 0;
	}
	default:
		return -1;
	}
}

int array_push(void **arr, void *el)
{
	struct Metadata *h = (struct Metadata*)*arr - 1;
	switch(h->type){
	case LONG:
	{
		if(h->elements == h->capacity){
			/*realloc*/
			void *r = realloc((struct Metadata*)*arr-1,(sizeof(long)*(h->capacity * 2))+ sizeof(struct Metadata));
			if(!r){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__);
				return -1;
			}
			h = (struct Metadata*)r;
			/*zeroed out the new alloc*/
			*arr = (struct Metadata*) r + 1;
			memset(&((long*)*arr)[h->capacity],0,sizeof(long) * h->capacity);
			h->capacity = h->capacity * 2;
		}
			long *a = (long*)*arr;
			a[h->elements] = *(long*)el;
			*arr = (void*)a;
			h->elements++;
		return 0;
	}
	case INT:
	{
		if(h->elements == h->capacity){
			/*realloc*/
			void *r = realloc((struct Metadata*)*arr-1,(sizeof(int)*(h->capacity * 2))+ sizeof(struct Metadata));
			if(!r){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__);
				return -1;
			}
			h = (struct Metadata*)r;
			*arr = (struct Metadata*)r + 1;
			memset(&((int*)*arr)[h->capacity],0,sizeof(int) * h->capacity);
			h->capacity = h->capacity * 2;
		}
		int *a = (int*)arr;
		a[h->elements] = *(int*)el;
		*arr = (void*)a;
		h->elements++;
		return 0; 
	}
	case DOUBLE:
	{
		if(h->elements == h->capacity){
			/*realloc*/
			void *r = realloc((struct Metadata*)*arr-1,(sizeof(double)*(h->capacity * 2))+ sizeof(struct Metadata));
			if(!r){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__);
				return -1;
			}
			h = (struct Metadata*)r;
			*arr = (struct Metadata*)r + 1;
			memset(&((double*)*arr)[h->capacity],0,sizeof(double) * h->capacity);
			h->capacity = h->capacity * 2;

		}
		double *a = (double*)*arr;
		a[h->elements] = *(double*)el;
		*arr = (void*)a;
		h->elements++;
		return 0;
	}
	case FLOAT:
	{
		if(h->elements == h->capacity){
			/*realloc*/
			void *r = realloc((struct Metadata*)*arr-1,(sizeof(float)*(h->capacity * 2))+ sizeof(struct Metadata));
			if(!r){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__);
				return -1;
			}
			h = (struct Metadata*)r;
			*arr = (struct Metadata*)r + 1;
			memset(&((float*)*arr)[h->capacity],0,sizeof(float) * h->capacity);
			h->capacity = h->capacity * 2;
		}
		float *a = (float*)*arr;
		a[h->elements] = *(float*)el;
		*arr = (void*)a;
		h->elements++;
		return 0;
	}
	case BYTE:
	{
		if(h->elements == h->capacity){
			/*realloc*/
			void *r = realloc((struct Metadata*)*arr-1,(sizeof(unsigned char)*(h->capacity * 2))+ sizeof(struct Metadata));
			if(!r){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__);
				return -1;
			}
			h = (struct Metadata*)r;
			h->capacity = h->capacity * 2;
			*arr = (struct Metadata*)r + 1;
			memset(&((unsigned char*)*arr)[h->capacity],0,sizeof(unsigned char) * h->capacity);
		}
		unsigned char *a = (unsigned char*)*arr;
		a[h->elements] = *(unsigned char*)el;
		*arr = (void*)a;
		h->elements++;
		return 0;
	}
	case STRING:
	{
		if(h->elements == h->capacity){
			/*realloc*/
			void *r = realloc((struct Metadata*)*arr-1,(sizeof(char*)*(h->capacity * 2))+ sizeof(struct Metadata));
			if(!r){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__);
				return -1;
			}
			h = (struct Metadata*)r;
			h->capacity = h->capacity * 2;
			*arr = (struct Metadata*)r + 1;
			memset(&((char**)*arr)[h->capacity],0,sizeof(char*) * h->capacity);
		}
		char **a = (char **)*arr;
		a[h->elements] = (char*)malloc(strlen((char*)el)+1);
		a[h->elements][strlen((char*) el)] = '\0';
		memcpy(a[h->elements],(char*) el,strlen((char*)el));
		h->elements++;
		*arr = (void*)a;
		return 0;
	}
	case VOID:
	{
		if(h->elements == h->capacity){
			/*realloc*/
			void *r = realloc((struct Metadata*)*arr-1,(sizeof(struct Mix_t)*(h->capacity * 2))+ sizeof(struct Metadata));
			if(!r){
				fprintf(stderr,"realloc() failed. %s:%d\n",__FILE__,__LINE__);
				return -1;
			}
			h = (struct Metadata*)r;
			h->capacity = h->capacity * 2;
			*arr = (struct Metadata*)r + 1;
			memset(&((struct Mix_t*)*arr)[h->capacity],0,sizeof(struct Mix_t) * h->capacity);
		}
		struct Mix_t *a = (struct Mix_t*)*arr;
		a[h->elements] = *(struct Mix_t*)el;
		*arr = (void*)a;
		h->elements++;
		return 0;

	}
	default:
		return -1;
	}
}

void array_free(void*arr)
{
	if(!arr)
		return;

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
		for(i = 0; i < h->capacity; i++){
			if(c[i])
				free(c[i]);
		}
		free(h);
		return;
	}
	case VOID:
	{
		struct Mix_t *v = (struct Mix_t*)arr;
		int i;
		for(i = 0; i < h->capacity; i++){
			if(v[i].v)
				free(v[i].v);
		}
		free(h);
		return;
	}
	default:
		return;
	}
}


