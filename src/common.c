#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "common.h"

static int BST_node_init(struct BSTnode **node, struct BSTnode **v);

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
	/* this was more of a challenge that i want to accomplish
	 * I hardly find the need to have an array of mixed type, 
	 * but was fun to implement*/
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
		*(int*)el->v = *(int *)value;	
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

static int BST_node_init(struct BSTnode **node, struct BSTnode **v)
{
	*node = malloc(sizeof **node);
	if(!(*node)){
		return -1;
	}
	memset(*node,0,sizeof(**node));
	(*node)->value = malloc(sizeof(struct Mix_t));
	if(!(*node)->value){
		return -1;
	}
	memset((*node)->value,0,sizeof(struct Mix_t));
	(*node)->value = (*v)->value;
	return 0;
}
int comparison(void *src, void *dest)
{
    struct Mix_t *s = (struct Mix_t*)src;
    struct Mix_t *d = (struct Mix_t*)dest;
	if(s->type != d->type){
		return ERR;	
	}

	switch(s->type){
	case BYTE:
		if((unsigned char*)s->v > (unsigned char*)d->v) return LEFT;
		if((unsigned char*)s->v == (unsigned char*)d->v) return 0;
		return RIGHT;
	case INT:
		if((int*)s->v > (int*)d->v) return LEFT;
		if((int*)s->v == (int*)d->v) return 0;
		return RIGHT;
	case LONG:
		if((long*)s->v > (long*)d->v) return LEFT;
		if((long*)s->v == (long*)d->v) return 0;
		return RIGHT;
	default:
		return ERR;
	}
}
int BST_insert(struct BSTnode **root, struct BSTnode *node,int (*comparison)(void*,void*))
{
	if((*root) && (*root)->value)
		return BST_insert(root,node,comparison);

    if(BST_node_init(root,&node) == -1)
		return -1;
	else 
		return 0;

	int result = comparison((*root)->value,node->value);
	switch(result){
	case LEFT:
		/*GO LEFT*/
		if((*root)->left)
			return BST_insert(&(*root)->left,node,comparison);
		if(BST_node_init(&(*root)->left, &node) == -1)
			return -1;
		(*root)->left->value = (void*)malloc(sizeof(struct Mix_t));
		if(!(*root)->left->value){
			return -1;
		}	
		memcpy((struct Mix_t*)(*root)->left->value, (struct Mix_t*)node->value,sizeof(struct Mix_t));
	case RIGHT:
		/*GO right*/
		if((*root)->right)
			return BST_insert(&(*root)->right,node,comparison);
		if(BST_node_init(&(*root)->right,&node) == -1)
			return -1;
		(*root)->right->value = (void*)malloc(sizeof(struct Mix_t));
		if(!(*root)->right->value){
			return -1;
		}	
		memcpy((struct Mix_t*)(*root)->right->value, (struct Mix_t*)node->value,sizeof(struct Mix_t));
	case ERR:
		/*CLEANUP*/
		fprintf(stderr,"comparison() failed, check the types\n");
		return -1;
	default:
		return 0;	
	}
	return 0;
}
