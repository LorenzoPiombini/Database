#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <byteswap.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <math.h>
#include "hash_tbl.h"
#include "str_op.h"
#include "debug.h"


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

	uint32_t sz_n = htonl(ht->size);
	if (write(fd, &sz_n, sizeof(sz_n)) == -1) {
		perror("writing index file");
		return 0;
	}

	uint32_t ht_ln = htonl((uint32_t)len(*ht));
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
				uint32_t type = htonl((uint32_t)current->key.type);
				uint64_t key_l = bswap_64((uint64_t)strlen(current->key.k.s));
				uint64_t value = bswap_64((uint64_t)current->value);

				if (write(fd, &type, sizeof(type)) == -1 ||
					write(fd, &key_l, sizeof(key_l)) == -1 ||
					write(fd, current->key.k.s, strlen(current->key.k.s) + 1) == -1 ||
					write(fd, &value, sizeof(value)) < 0){
					perror("write index:");
					return 0; // false
				}
				current = current->next;
				break;
			}
			case UINT:
			{
				uint32_t type = htonl((uint32_t)current->key.type);
				uint64_t value = bswap_64((uint64_t)current->value);
				uint32_t k = htonl(current->key.k.n);

				if (write(fd, &type, sizeof(type)) == -1) {
					perror("write index:");
					return 0; // false
				}

				if (write(fd, &k, sizeof(k)) == -1) {
					perror("write index:");
					return 0; // false
				}

				if (write(fd, &value, sizeof(value)) == -1) {
					perror("write index:");
					return 0; // false
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
			if(!temp->key.k.s)return -1;
			if (strcmp(temp->key.k.s, (char *)key) == 0)
				return temp->value;

			temp = temp->next;
			break;
		case UINT:
		{
			if (temp->key.k.n == *(uint32_t *)key)
				return temp->value;

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
	Node *new_node = calloc(1, sizeof(Node));
	if (!new_node)
	{
		__er_calloc(F, L - 2);
		return 0;
	}

	switch (key_type)
	{
	case UINT:
	{
		new_node->key.k.n = *(uint32_t *)key;
		break;
	}
	case STR:
	{
		new_node->key.k.s = strdup((char *)key);
		if (!new_node->key.k.s) {
			fprintf(stderr, "strdup() failed");
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
				free(new_node->key.k.s);
				free(new_node);
				return 0;
			}
		}

		/*
		 * check all the nodes in the index
		 * for duplicates keys
		 * */
		Node *temp = tbl->data_map[index];
		while (temp->next != NULL) {
			if ((key_len = strlen(temp->next->key.k.s)) == strlen(new_node->key.k.s)) {
				if (strncmp(temp->next->key.k.s, new_node->key.k.s, ++key_len) == 0) {
					printf("could not insert new node \"%s\"\n", new_node->key.k.s);
					printf("key already exist. Choose another key value.\n");
					free(new_node->key.k.s);
					free(new_node);
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
		if (tbl->data_map[index]->key.k.n == new_node->key.k.n) {
			printf("could not insert new node \"%u\"\n", new_node->key.k.n);
			printf("key already exist. Choose another key value.\n");
			free(new_node);
			return 0;
		}

		Node *temp = tbl->data_map[index];
		while (temp->next) {
			if (temp->next->key.k.n == new_node->key.k.n) {
				printf("could not insert new node \"%u\"\n", new_node->key.k.n);
				printf("key already exist. Choose another key value.\n");
				free(new_node);
				return 0;
			}
			temp = temp->next;
		}
		temp->next = new_node;
	}

	return 1; // succseed!
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

			if (current->key.k.n == *(uint32_t *)key)
			{
				if (previous == NULL)
					tbl->data_map[index] = current->next;
				else
					previous->next = current->next;

				return current;
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
				free(current->key.k.s);
				free(current);
				current = next;
				break;
			}
			case UINT:
			{
				Node *next = current->next;
				free(current);
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

struct Keys_ht *keys(HashTable *ht)
{
	int elements = len(*ht);

	void **keys_arr = calloc(elements, sizeof(void *));
	if (!keys_arr)
	{
		__er_calloc(F, L - 2);
		return NULL;
	}

	int *types = calloc(elements, sizeof(int));
	if (!types)
	{
		__er_calloc(F, L - 2);
		return NULL;
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
				keys_arr[index] = (void *)strdup(temp->key.k.s);
				if (!keys_arr[index])
				{
					fprintf(stderr, "strdup() failed.");
					return NULL;
				}
				types[index] = STR;
			}
			else
			{
				keys_arr[index] = (void *)&temp->key.k.n;
				types[index] = UINT;
			}
			temp = temp->next;
			++index;
		}
	}

	struct Keys_ht *keys = calloc(1, sizeof(struct Keys_ht));
	if (!keys)
	{
		__er_calloc(F, L - 2);
		return NULL;
	}

	keys->k = keys_arr;
	keys->types = types;
	keys->length = elements;
	return keys;
}

int len(HashTable tbl)
{
	int counter = 0;

	if (tbl.size == 0) return 0;

	for (int i = 0; i < tbl.size; i++)
	{
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
				free(current->key.k.s);
				free(current);
				current = next;
				break;
			}
			case UINT:
			{
				Node *next = current->next;
				free(current);
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
		free(node->key.k.s);
		break;
	case UINT:
		break;
	default:
		fprintf(stderr, "key type not supported");
		return;
	}

	free(node);
}

void free_keys_data(struct Keys_ht *data)
{
	for (int i = 0; i < data->length; i++)
	{
		if (data->types[i] == STR)
			free(data->k[i]);
	}

	free(data->k);
	free(data->types);
	free(data);
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

	for (register int i = 0; i < src->size; i++)
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
				set((void *)&src->data_map[i]->key.k.n,
					src->data_map[i]->key.type,
					src->data_map[i]->value, dest);
				Node *next = src->data_map[i]->next;
				while (next)
				{
					set((void *)&next->key.k.n,
						next->key.type,
						next->value, dest);
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
