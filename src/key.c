#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <record.h>
#include <hash_tbl.h>
#include <file.h>
#include <str_op.h>
#include "key.h"
#include "memory.h"
#include "freestand.h"

/*order starting number*/
#define ORDER_BASE 100

/*mode select the type of numbering of the key*/
i64 generate_numeric_key(int *fds, int mode, int base)
{
	i64 key = 0;
	switch(mode){
	case REG:
	{
		HashTable ht;
		set_memory(&ht,0,sizeof(HashTable));
		HashTable *p_ht = &ht;
		if(!read_index_nr(0,fds[0],&p_ht)){
			/*log failure*/
			return -1;
		}

		key = len(ht);
		destroy_hasht(p_ht);
		break;
	}
	case BASE: 
	{
		HashTable ht;
		set_memory(&ht,0,sizeof(HashTable));
		HashTable *p_ht = &ht;
		if(!read_index_nr(0,fds[0],&p_ht)){
			/*log failure*/
			return -1;
		}

		key = len(ht) + base;

		destroy_hasht(p_ht);
		break;
	}
	case INCREM:
	{
		HashTable ht;
		memset(&ht,0,sizeof(HashTable));
		HashTable *p_ht = &ht;
		if(!read_index_nr(0,fds[0],&p_ht)){
			/*log failure*/
			return -1;
		}

		int elements = len(ht);
		key = ++elements;

		destroy_hasht(p_ht);
		break;
	}
	default:
		return -1;
	}

	return key;
}

char *get_all_keys_for_file(int *fds,int index)
{
	HashTable ht;
	set_memory(&ht,0,sizeof(HashTable));
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
	int i;
	for(i = 0; i < all_keys.length; i++){
		switch(all_keys.keys[i].type){
		case STR:
		{
			if(all_keys.keys[i].k.s){
				str_size += string_length(all_keys.keys[i].k.s);
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
	char *str_keys = (char *)ask_mem(str_size + 1);
	if(!str_keys){
		/*log failure*/
		free_keys_data(&all_keys);
		return NULL;
	}
	
	ui32 ind_str = 0;
	string_copy(str_keys,"[",1);

	ind_str= 2 -1;
	for(i = 0; i < all_keys.length; i++){
		switch(all_keys.keys[i].type){
		case STR:
		{
			size_t l = string_length(all_keys.keys[i].k.s);
			if(all_keys.keys[i].k.s){
				string_copy(&str_keys[ind_str],all_keys.keys[i].k.s,l);
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
				if(copy_to_string(&str_keys[ind_str],n+1,"%d",all_keys.keys[i].k.n16) == -1){
					/*log failure*/						
					cancel_memory(NULL,str_keys,str_size + 1);	
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
				if(copy_to_string(&str_keys[ind_str],n+1,"%d",all_keys.keys[i].k.n) == -1){
					/*log failure*/						
					cancel_memory(NULL,str_keys,str_size + 1);	
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
			cancel_memory(NULL,str_keys,str_size + 1);	
			return NULL;
		}
	}

	string_copy(&str_keys[ind_str],"]",1);
	ind_str += 1;
	free_keys_data(&all_keys);
	return str_keys;
}
