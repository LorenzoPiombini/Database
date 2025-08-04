#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <record.h>
#include <hash_tbl.h>
#include <file.h>
#include <str_op.h>
#include "key.h"
#include "end_points.h"


#define ORDER_BASE 100

uint32_t generate_numeric_key(int *fds, int end_point)
{
	uint32_t key = 0;
	switch(end_point){
	case NEW_SORD: 
	{
		HashTable ht = {0};
		HashTable *p_ht = &ht;
		if(!read_index_nr(0,fds[0],&p_ht)){
			/*log failure*/
			return 0;
		}

		int elements = len(ht);
		key = elements + ORDER_BASE;

		destroy_hasht(p_ht);
		break;
	}
	case NEW_SORD_LINES:
	{
		HashTable ht = {0};
		HashTable *p_ht = &ht;
		if(!read_index_nr(0,fds[0],&p_ht)){
			/*log failure*/
			return 0;
		}

		int elements = len(ht);
		key = ++elements;

		destroy_hasht(p_ht);
		break;
	}
	default:
		return 0;
	}

	return key;
}

char *get_all_keys_for_file(int *fds,int index)
{
	HashTable ht;
	memset(&ht,0,sizeof(HashTable));
	HashTable *p_ht = &ht;

	struct Keys_ht all_keys;
	memset(&all_keys,0,sizeof(struct Keys_ht));

	if (index <= 0){
		if(!read_index_nr(0,fds[0],&p_ht)){
			/*log failure*/
			return NULL;
		}
	} else{
		if(!read_index_nr(index,fds[0],&p_ht)){
			/*log failure*/
			return NULL;
		}
	}

	if(keys(&ht,&all_keys)){
		/*log failure*/
		destroy_hasht(&ht);
		return NULL;
	}

	destroy_hasht(&ht);
	assert(all_keys.keys != NULL);

	/*stringfy the data */
	size_t str_size = 0;
	for(int i = 0; i < all_keys.length; i++){
		switch(all_keys.keys[i].type){
		case STR:
		{
			if(all_keys.keys[i].k.s){
				str_size += strlen(all_keys.keys[i].k.s);
			}
			break;
		}
		case UINT:
		{
			if(all_keys.keys[i].size == 16){
				str_size += number_of_digit(all_keys.keys[i].k.n16);
			}else{
				str_size += number_of_digit(all_keys.keys[i].k.n);
			}
			break;
		}
		default:
			free_keys_data(&all_keys);
			return NULL;
		}
	}

	/* 2 is for '[' and ']' */
	str_size += (2 + all_keys.length);
	char *str_keys = calloc(str_size + 1,sizeof(char));
	if(!str_keys){
		/*log failure*/
		free_keys_data(&all_keys);
		return NULL;
	}
	
	uint32_t ind_str = 0;
	strncpy(str_keys,"[",2);

	ind_str= 2 -1;
	for(int i = 0; i < all_keys.length; i++){
		switch(all_keys.keys[i].type){
		case STR:
		{
			size_t l = strlen(all_keys.keys[i].k.s);
			if(all_keys.keys[i].k.s){
				strncpy(&str_keys[ind_str],all_keys.keys[i].k.s,l);
				ind_str += l;
				if(all_keys.length - i > 1){
					str_keys[ind_str] = ',';
					ind_str++;
				}
			}
			break;
		}
		case UINT:
		{
			if(all_keys.keys[i].size == 16){
				size_t n = number_of_digit(all_keys.keys[i].k.n16);
				if(snprintf(&str_keys[ind_str],n+1,"%d",all_keys.keys[i].k.n16) == -1){
					/*log failure*/						
					free(str_keys);
					free_keys_data(&all_keys);
					return NULL;
				}
				ind_str += n;
				if(all_keys.length - i > 1){
					str_keys[ind_str] = ',';
					ind_str++;
				}
			}else{
				size_t n = number_of_digit(all_keys.keys[i].k.n);
				if(snprintf(&str_keys[ind_str],n+1,"%d",all_keys.keys[i].k.n) == -1){
					/*log failure*/						
					free(str_keys);
					free_keys_data(&all_keys);
					return NULL;
				}
				ind_str += n;
				if(all_keys.length - i > 1){
					str_keys[ind_str] = ',';
					ind_str++;
				}
		
			}
			break;
		}
		default:
			free_keys_data(&all_keys);
			free(str_keys);	
			return NULL;
		}
	}

	strncpy(&str_keys[ind_str],"]",2);
	ind_str += 1;
	free_keys_data(&all_keys);
	return str_keys;
}
