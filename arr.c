#include <stdio.h>
#include <stdlib.h>
#include "common.h"


int main()
{
	int (*comp)(void*,void*) = NULL; 
	comp = comparison;

	struct BSTnode node = {0};
	int number = 10;
	if(mix_type_init(INT,(struct Mix_t**)&node.value,&number) == -1)
		return -1;

	struct BSTnode *root = NULL;
	if(BST_insert(&root,&node,comp) == -1)
		return -1;

	FREE_MIX_TYPE(node.value);
	number = 9;
	if(mix_type_init(INT,(struct Mix_t**)&node.value,&number) == -1)
		return -1;

	if(BST_insert(&root,&node,comp) == -1)
		return -1;

	FREE_MIX_TYPE(node.value);
	number = 11;
	if(mix_type_init(INT,(struct Mix_t**)&node.value,&number) == -1)
		return -1;

	if(BST_insert(&root,&node,comp) == -1)
		return -1;

	FREE_MIX_TYPE(node.value);

	struct Mix_t* mixed_array = array_init(5,VOID);
	if(!mixed_array){
		return -1;
	}

	struct Mix_t *el = NULL;
	char value[] ="this is a string";
	if(mix_type_init(STRING,&el,&value) == -1){
		return -1;
	}

	if(array_push((void**)&mixed_array,el) == -1){
		array_free(mixed_array);
		return -1;
	}

	long num = 9;
	FREE_MIX_TYPE(el);
	free(el);
	el = NULL;
	if(mix_type_init(LONG,&el,&num) == -1){
		return -1;
	}

	if(array_push((void**)&mixed_array,&el) == -1){
		array_free(mixed_array);
		return -1;
	}

	printf("print mixed type array:\n[ ");
	for(int i = 0; i < ((struct Metadata*)mixed_array -1)->capacity;i++){
		switch(mixed_array[i].type){
		case LONG:
			printf("%ld, ",*(long*)((struct Mix_t*)mixed_array)[i].v);
			break;
		case STRING:
			printf("\"%s\", ",(char*)((struct Mix_t*)mixed_array)[i].v);
			break;
		}
	}
	FREE_MIX_TYPE(el);
	free(el);
	el = NULL;
	printf("]\n");
	array_free((void*)mixed_array);
	void* a = array_init(5,LONG);
	for(long i = 0,*pi = &i; i < 7; i++)
		if(array_push(&a,(void*)pi) == -1){
			array_free(a);
			return -1;
		}

	long *vector = a;
	for(int i=0; i < ((struct Metadata*)a-1)->capacity;i++)
		printf("%3ld",vector[i]);

	printf("\n");
	long n = 8, *j=&n;
	array_insert_at(2, &a,(void*)j);

	n = 8; j=&n;
	array_insert_at(2, &a,(void*)j);

	n = 8; j=&n;
	array_insert_at(20, &a,(void*)j);

	n = 8; j=&n;
	array_insert_at(16, &a,(void*)j);

	vector = a;
	for(int i=0; i < ((struct Metadata*)a-1)->capacity;i++)
		printf("%3ld",vector[i]);

	printf("\n");
	array_free(a);
	printf("======================================\n\n");

	a = array_init(5,STRING);
	if(!a){
		return -1;
	}
	
	char b[] = "this is a string";
	for(int i=0; i < ((struct Metadata*)a-1)->capacity;i++){
		if(array_push(&a,(void*)b) == -1){
			array_free(a);
			return -1;
		}
	}

	printf("[");
	char **v = a;
	for(int i=0; i < ((struct Metadata*)a-1)->capacity;i++){
		if(((((struct Metadata*) a - 1))->capacity - i) > 1)
			printf(" %s,",v[i]);
		else 
			printf(" %s ]\n",v[i]);
	}

	char c[] = "inserted";
	array_insert_at(2, &a,(void*)c);

	printf("[");
	v = a;
	for(int i=0; i < ((struct Metadata*)a-1)->capacity;i++){
		if(((((struct Metadata*) a - 1))->capacity - i) > 1)
			printf(" %s,",v[i]);
		else 
			printf(" %s ]\n",v[i]);
	}

	array_insert_at(8, &a,(void*)c);

	printf("[");
	v = a;
	for(int i=0; i < ((struct Metadata*)a-1)->capacity;i++){
		if(((((struct Metadata*) a - 1))->capacity - i) > 1){
			printf(" %s,",v[i] == NULL ? "null" : v[i]);
		} else {
			printf(" %s ]\n",v[i] == NULL ? "null" : v[i]);
		}
	}
	array_free(a);
}
