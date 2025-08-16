#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <math.h>
#include "hash_tbl.h"
#include "str_op.h"
#include "debug.h"
#include "endian.h"
#include "memory.h"
#include "types.h"


void print_hash_table(HashTable tbl)
{
	int i = 0;
	for (i = 0; i < tbl.size; i++)
	{
		printf("%d: \n", i);
		Node *node = tbl.data_map[i];

		while (node)
		{
			switch (node->key.type)
			{
			case STR:
			{
				if (node->key.k.s)
				{
					printf("\t{ %s,%ld }\n",
						   node->key.k.s, node->value);
				}

				node = node->next;
				break;
			}
			case UINT:
			{
				printf("\t{ %u,%ld}\n", node->key.k.n, node->value);
				node = node->next;
				break;
			}
			default:
				break;
			}
		}

		if (i > 0 && (i % 30 == 0))
			printf("\npress any key . . ."), getchar();
	}
}

int write_ht(int fd, HashTable *ht)
{
	int i = 0;

	uint32_t sz_n = swap32(ht->size);
	if (write(fd, &sz_n, sizeof(sz_n)) == -1) {
		perror("writing index file");
		return 0;
	}

	uint32_t ht_ln = swap32(len(*ht));
	if (write(fd, &ht_ln, sizeof(ht_ln)) == -1) {
		perror("write ht length");
		return 0;
	}

	if (len(*ht) == 0) {
		return 1;
	}

	for (i = 0; i < ht->size; i++) {
		if (ht->data_map[i] == NULL)
			continue;

		Node *current = ht->data_map[i];
		while (current != NULL) {
			switch (current->key.type)
			{
			case STR:
			{
				uint32_t type = swap32(current->key.type);
				uint64_t key_l = swap64(strlen(current->key.k.s));
				uint64_t value = swap64(current->value);

				if (write(fd, &type, sizeof(type)) == -1 ||
					write(fd, &key_l, sizeof(key_l)) == -1 ||
					write(fd, current->key.k.s, strlen(current->key.k.s) + 1) == -1 ||
					write(fd, &value, sizeof(value)) < 0){
					perror("write index:");
					return 0; /*false*/
				}
				current = current->next;
				break;
			}
			case UINT:
			{
				uint32_t type = swap32(current->key.type);
				uint8_t size = (uint8_t)current->key.size;
				uint64_t value = swap64(current->value);
				
				uint32_t k = 0;
				uint16_t k16 = 0;
				if(current->key.size == 32)
					k = swap32(current->key.k.n);
				else
					k16 = swap16(current->key.k.n16);

				if (write(fd, &type, sizeof(type)) == -1 ||
						write(fd, &size, sizeof(size)) == -1) {
					perror("write index:");
					return 0; /*false*/
				}

				if(current->key.size == 16){
					if (write(fd, &k16, sizeof(k16)) == -1) {
						perror("write index:");
						return 0; /*false*/
					}
				}else{
					if (write(fd, &k, sizeof(k)) == -1) {
						perror("write index:");
						return 0; /*false*/

					}
				}
				if (write(fd, &value, sizeof(value)) == -1) {
					perror("write index:");
					return 0; /*false*/
				}
				current = current->next;
				break;
			}
			default:
				fprintf(stderr, "key type not supported.\n");
				return 0;
			}
		}
	}

	return 1;
}

int hash(void *key, int size, int key_type)
{

	uint32_t integer_key = 0;
	char *string_key = NULL;
	int hash = 0;
	int number = 78;

	/*compute the prime number */
	uint32_t prime = (uint32_t)(pow(2, 31) - 1);
	switch (key_type) {
	case UINT:
	{
		integer_key = *(uint32_t *)key;
		hash = ((COPRIME * integer_key + number) % prime) % size;
		break;
	}
	case STR:
		string_key = (char *)key;
		for (; *string_key != '\0'; string_key++, number = COPRIME * number % (size - 1)){
			hash = (number * hash + *string_key) % size;
		}
		break;
	default:
		fprintf(stderr, "key type not supported");
		return -1;
	}

	return hash;
}

off_t get(void *key, HashTable *tbl, int key_type)
{
	int index = hash(key, tbl->size, key_type);
	Node *temp = tbl->data_map[index];
	while (temp != NULL)
	{
		switch (key_type) {
		case STR:
			if(temp->key.type != key_type) return -1;
			if(!temp->key.k.s)return -1;
			if (strcmp(temp->key.k.s, (char *)key) == 0)
				return temp->value;

			temp = temp->next;
			break;
		case UINT:
		{
			if(temp->key.size == 16 && *(uint16_t*)key < USHRT_MAX){
				if(temp->key.k.n16 == *(uint16_t *)key)
					return temp->value;

			}else if (temp->key.size == 32 && *(uint32_t*)key < UINT_MAX){
				if(temp->key.k.n == *(uint32_t*)key)		
					return temp->value;
			}

			temp = temp->next;
			break;
		}
		default:
			fprintf(stderr, "key type not supported.\n");
			return -1;
		}
	}
	return -1;
}

int set(void *key, int key_type, off_t value, HashTable *tbl)
{

	int index = hash(key, tbl->size, key_type);
	Node *new_node = (Node*)ask_mem(sizeof(Node));
	if (!new_node){
		fprintf(stderr,"ask_mem() failed, %s:%d.\n",F, L - 2);
		return 0;
	}

	switch (key_type){
	case UINT:
	{
		if(*(uint32_t*)key < USHRT_MAX){ 
			new_node->key.k.n16 = *(uint16_t *)key;
			new_node->key.size = 16;
		}else{ 
			new_node->key.k.n = *(uint32_t *)key;
			new_node->key.size = 32;
		}
		break;
	}
	case STR:
	{
		new_node->key.k.s = duplicate_str((char *)key);
		if (!new_node->key.k.s) {
			fprintf(stderr, "duplicate_str() failed, %s:%d",F,L-2);
			return 0;
		}
		break;
	}
	default:
		fprintf(stderr, "key type not supported.\n");
		return 0;
	}

	new_node->key.type = key_type;
	new_node->value = value;
	new_node->next = NULL;

	if (tbl->data_map[index] == NULL) {
		tbl->data_map[index] = new_node;
	}else if (key_type == STR){
		/*for key strings*/
		/*
		 * check if the key already exists at
		 * the base  element of the index
		 * */
		size_t key_len = 0;
		if ((key_len = strlen(tbl->data_map[index]->key.k.s)) == strlen(new_node->key.k.s))
		{
			if (strncmp(tbl->data_map[index]->key.k.s, new_node->key.k.s, ++key_len) == 0)
			{
				printf("key %s, already exist.\n", new_node->key.k.s);
				cancel_memory(NULL,new_node->key.k.s,strlen(new_node->key.k.s)+1);
				cancel_memory(NULL,new_node,sizeof(Node));
				return 0;
			}
		}

		/*
		 * check all the nodes in the index
		 * for duplicates keys
		 * */
		Node *temp = tbl->data_map[index];
		while (temp->next != NULL) {
			if (temp->next->key.type != STR){
				temp = temp->next;
				continue;
			}
			if ((key_len = strlen(temp->next->key.k.s)) == strlen(new_node->key.k.s)) {
				if (strncmp(temp->next->key.k.s, new_node->key.k.s, ++key_len) == 0) {
					printf("could not insert new node \"%s\"\n", new_node->key.k.s);
					printf("key already exist. Choose another key value.\n");
					cancel_memory(NULL,new_node->key.k.s,strlen(new_node->key.k.s)+1);
					cancel_memory(NULL,new_node,sizeof(Node));
					return 0;
				}
			}
			temp = temp->next;
		}
		/*
		 * the key is unique
		 * we add the node to the list
		 * */
		temp->next = new_node;
	}
	else if (key_type == UINT) {
		if (tbl->data_map[index]->key.size == new_node->key.size){
			if(new_node->key.size == 16){
				if(tbl->data_map[index]->key.k.n16 == new_node->key.k.n16) {
					printf("could not insert new node '%u'\n", new_node->key.k.n16);
					printf("key already exist. Choose another key value.\n");
					cancel_memory(NULL,new_node,sizeof(Node));
					return 0;
				}
			}else{
				if (tbl->data_map[index]->key.k.n == new_node->key.k.n) {
					printf("could not insert new node '%u'\n", new_node->key.k.n);
					printf("key already exist. Choose another key value.\n");
					cancel_memory(NULL,new_node,sizeof(Node));
					return 0;
				}
			}
		}

		Node *temp = tbl->data_map[index];
		while (temp->next) {
			if(temp->key.size == new_node->key.size){
				if(new_node->key.size == 16){
					if(temp->key.k.n16 == new_node->key.k.n16) {
						printf("could not insert new node '%u'\n", new_node->key.k.n16);
						printf("key already exist. Choose another key value.\n");
						cancel_memory(NULL,new_node,sizeof(Node));
						return 0;
					}
				}else{
					if (temp->key.k.n == new_node->key.k.n) {
						printf("could not insert new node '%u'\n", new_node->key.k.n);
						printf("key already exist. Choose another key value.\n");
						cancel_memory(NULL,new_node,sizeof(Node));
						return 0;
					}
				}

				temp = temp->next;
			}
		}
		temp->next = new_node;
	}

	return 1; /* succseed!*/
}

Node *delete(void *key, HashTable *tbl, int key_type)
{
	int index = hash(key, tbl->size, key_type);
	Node *current = tbl->data_map[index];
	Node *previous = NULL;

	while (current != NULL)
	{
		switch (key_type)
		{
		case STR:
			if (strcmp(current->key.k.s, (char *)key) == 0)
			{
				if (previous == NULL){
					tbl->data_map[index] = current->next;
				}else {
					previous->next = current->next;
				}

				return current;
			}
			previous = current;
			current = current->next;
			break;
		case UINT:
		{

			if(current->key.size == 16){
				if (current->key.k.n16 == *(uint16_t *)key)
				{
					if (previous == NULL)
						tbl->data_map[index] = current->next;
					else
						previous->next = current->next;

					return current;
				}
			}else{
				if (current->key.k.n == *(uint32_t *)key)
				{
					if (previous == NULL)
						tbl->data_map[index] = current->next;
					else
						previous->next = current->next;

					return current;
				}


			}
			previous = current;
			current = current->next;
			break;
		}
		default:
			fprintf(stderr, "key type not supported.\n");
			return NULL;
		}
	}

	return NULL;
}

void free_ht_array(HashTable *ht, int l)
{
	if (!ht)
		return;

	int i = 0;
	for (i = 0; i < l; i++)
		destroy_hasht(&ht[i]);

	free(ht);
}
void destroy_hasht(HashTable *tbl)
{

	int i = 0;
	for (i = 0; i < tbl->size; i++) {
		Node *current = tbl->data_map[i];
		while (current != NULL) {
			switch (current->key.type) {
			case STR:
			{
				Node *next = current->next;
				cancel_memory(NULL,current->key.k.s,strlen(current->key.k.s)+1);
				cancel_memory(NULL,current,sizeof(Node));
				current = next;
				break;
			}
			case UINT:
			{
				Node *next = current->next;
				cancel_memory(NULL,current,sizeof(Node));
				current = next;
				break;
			}
			default:
				fprintf(stderr, "key type not supported.\n");
				break;
			}
		}
	}
}

int keys(HashTable *ht, struct Keys_ht *all_keys)
{
	int elements = len(*ht);

	struct Key *keys = (struct Key*)ask_mem(elements * sizeof(struct Key));
	if (!keys){
		fprintf(stderr,"ask_mem() failed, %s:%d.\n",F, L - 2);
		return -1;
	}

	int i = 0;
	int index = 0;
	for (i = 0; i < ht->size; i++)
	{
		Node *temp = ht->data_map[i];
		while (temp != NULL)
		{
			if (temp->key.type == STR)
			{

				keys[index].k.s = duplicate_str(temp->key.k.s);
				if (!keys[index].k.s){
					fprintf(stderr, "duplicate_str() failed, %s:%d",F,L-2);
					return -1;
				}
				keys[index].type = STR;
			}
			else
			{
				if(temp->key.size == 16){
					keys[index].k.n16 = temp->key.k.n16;
					keys[index].type = UINT;
					keys[index].size = 16;
					pack(keys[index].k.n16,keys[index].paked_k);
				}else{
					keys[index].k.n = temp->key.k.n;
					keys[index].type = UINT;
					keys[index].size = 32;
					pack(keys[index].k.n,keys[index].paked_k);
					
				}
			}
			temp = temp->next;
			++index;
			
		}
	}

	all_keys->keys = keys;
	all_keys->length = elements;
	return 0;
}

int len(HashTable tbl)
{
	int counter = 0;

	if (tbl.size == 0) return 0;

	int i;
	for (i = 0; i < tbl.size; i++){
		Node *temp = tbl.data_map[i];
		while (temp != NULL)
		{
			counter++;
			temp = temp->next;
		}
	}

	return counter;
}

void free_nodes(Node **data_map, int size)
{
	int i = 0;
	for (i = 0; i < size; i++) {
		Node *current = (*data_map);
		while (current) {
			switch (current->key.type)
			{
			case STR:
			{
				Node *next = current->next;
				cancel_memory(NULL,current->key.k.s,strlen(current->key.k.s)+1);
				cancel_memory(NULL,current,sizeof(Node));
				current = next;
				break;
			}
			case UINT:
			{
				Node *next = current->next;
				cancel_memory(NULL,current,sizeof(Node));
				current = next;
				break;
			}
			default:
				fprintf(stderr, "key type not supported.\n");
				break;
			}
		}
	}

}

void free_ht_node(Node *node)
{
	if (!node)
		return;

	switch (node->key.type)
	{
	case STR:
		cancel_memory(NULL,node->key.k.s,strlen(node->key.k.s)+1);
		break;
	case UINT:
		break;
	default:
		fprintf(stderr, "key type not supported");
		return;
	}

	cancel_memory(NULL,node,sizeof(Node));
}

void free_keys_data(struct Keys_ht *data)
{
	int i;
	for (i = 0; i < data->length; i++)
	{
		if (data->keys[i].type == STR)
			cancel_memory(NULL,data->keys[i].k.s,strlen(data->keys[i].k.s)+1);
	}

	cancel_memory(NULL,data->keys,data->length * sizeof(struct Key));
}

/*if mode is set to 1 it will overwrite the content of dest
	if mode is 0 then the src HashTable will be added to the dest, maintaing whatever is in dest*/
unsigned char copy_ht(HashTable *src, HashTable *dest, int mode)
{
	if (!src)
	{
		printf("no data to copy.\n");
		return 0;
	}

	if (mode == 1)
	{
		destroy_hasht(dest);

		(*dest).size = src->size;
		memset((*dest).data_map,0,sizeof(Node*)*MAX_HT_BUCKET);
	}

	int i;
	for (i = 0; i < src->size; i++)
	{
		if (src->data_map[i])
		{
			switch (src->data_map[i]->key.type){
			case STR:
			{
				set((void *)src->data_map[i]->key.k.s,
					src->data_map[i]->key.type,
					src->data_map[i]->value, dest);
				Node *next = src->data_map[i]->next;
				while (next){
					set(next->key.k.s,
						next->key.type,
						next->value, dest);
					next = next->next;
				}
				break;
			}
			case UINT:
			{
				if(src->data_map[i]->key.size == 16){
					set((void *)&src->data_map[i]->key.k.n16,
							src->data_map[i]->key.type,
							src->data_map[i]->value, dest);
				}else{
					set((void *)&src->data_map[i]->key.k.n,
							src->data_map[i]->key.type,
							src->data_map[i]->value, dest);
				}

				Node *next = src->data_map[i]->next;
				while (next)
				{
					if(src->data_map[i]->key.size == 16){
						set((void *)&src->data_map[i]->key.k.n16,
							src->data_map[i]->key.type,
							src->data_map[i]->value, dest);
					}else{
						set((void *)&src->data_map[i]->key.k.n,
								src->data_map[i]->key.type,
								src->data_map[i]->value, dest);
					}
					next = next->next;
				}
				break;
			}
			default:
				fprintf(stderr, "key type not supported");
			}
		}
	}

	return 1;
}
