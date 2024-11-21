#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <byteswap.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <unistd.h>
#include "hash_tbl.h"
#include "debug.h"

void print_hash_table(HashTable tbl)
{
	int i = 0;
	for (i = 0; i < tbl.size; i++)
	{
		printf("%d: \n", i);
		Node *node = tbl.dataMap[i];

		while (node)
		{
			if (node->key)
			{
				printf("\t{ %s,%ld }\n", node->key, node->value);
			}
			node = node->next;
		}
		if (i > 0 && (i % 30 == 0))
			printf("\npress any key . . ."), getchar();
	}
}

int write_ht(int fd, HashTable *ht)
{
	int i = 0;

	uint32_t sz_n = htonl(ht->size);
	if (write(fd, &sz_n, sizeof(sz_n)) == -1)
	{
		perror("writing index file");
		return 0;
	}
	uint32_t ht_ln = htonl((uint32_t)len(*ht));
	if (write(fd, &ht_ln, sizeof(ht_ln)) == -1)
	{
		perror("write ht length");
		return 0;
	}

	if (len(*ht) == 0)
	{
		return 1;
	}

	for (i = 0; i < ht->size; i++)
	{
		if (ht->dataMap[i] == NULL)
			continue;

		Node *current = ht->dataMap[i];
		while (current != NULL)
		{
			uint64_t key_l = bswap_64((uint64_t)strlen(current->key));
			uint64_t value = bswap_64((uint64_t)current->value);

			if (write(fd, &key_l, sizeof(key_l)) < 0 ||
				write(fd, current->key, strlen(current->key) + 1) < 0 ||
				write(fd, &value, sizeof(value)) < 0)
			{
				perror("write index:");
				return 0; // false
			}
			current = current->next;
		}
	}

	return 1;
}

int hash(char *key, int size)
{
	unsigned long hash = 0;
	int i = 0;

	int len = strlen(key);
	for (i = 0; i < len; i++)
	{
		int asciiValue = (int)key[i];
		hash = (hash + asciiValue * 23);
	}

	return hash % size;
}

off_t get(char *key, HashTable *tbl)
{
	int index = hash(key, tbl->size);
	Node *temp = tbl->dataMap[index];
	while (temp != NULL)
	{
		if (strcmp(temp->key, key) == 0)
			return temp->value;

		temp = temp->next;
	}

	return -1l;
}

int set(char *key, off_t value, HashTable *tbl)
{
	int index = hash(key, tbl->size);
	Node *newNode = malloc(sizeof(Node));
	if (!newNode)
	{
		printf("failed to allocate memory while setting the HashTable.\n");
		return 0;
	}

	newNode->key = strdup(key);
	newNode->value = value;
	newNode->next = NULL;

	if (tbl->dataMap[index] == NULL)
	{
		tbl->dataMap[index] = newNode;
	}
	else
	{
		size_t key_len = 0;
		if ((key_len = strlen(tbl->dataMap[index]->key)) == strlen(key))
		{
			if (strncmp(tbl->dataMap[index]->key, key, ++key_len) == 0)
			{
				printf("key %s, already exist.\n", key);
				free(newNode->key);
				free(newNode);
				return 0;
			}
		}
		Node *temp = tbl->dataMap[index];
		while (temp->next != NULL)
		{
			if ((key_len = strlen(tbl->dataMap[index]->key)) == strlen(key))
			{
				if (strncmp(temp->next->key, key, ++key_len) == 0)
				{
					printf("could not insert new node \"%s\"\n", key);
					printf("key already exist. Choose another key value.");
					free(newNode->key);
					free(newNode);
					return 0;
				}
			}
			temp = temp->next;
		}
		temp->next = newNode;
	}

	return 1; // succseed!
}

Node *delete(char *key, HashTable *tbl)
{
	int index = hash(key, tbl->size);
	Node *current = tbl->dataMap[index];
	Node *previous = NULL;

	while (current != NULL)
	{
		if (strcmp(current->key, key) == 0)
		{
			if (previous == NULL)
			{
				tbl->dataMap[index] = current->next;
			}
			else
			{
				previous->next = current->next;
			}

			return current;
		}
		previous = current;
		current = current->next;
	}

	return NULL;
}

void free_ht_array(HashTable *ht, int l)
{
	if (!ht)
		return;

	int i = 0;
	for (i = 0; i < l; i++)
	{
		destroy_hasht(&ht[i]);
	}

	free(ht);
}
void destroy_hasht(HashTable *tbl)
{

	if (!tbl->dataMap)
		return;

	int i = 0;
	for (i = 0; i < tbl->size; i++)
	{
		Node *current = tbl->dataMap[i];
		while (current != NULL)
		{
			Node *next = current->next;
			free(current->key);
			free(current);
			current = next;
		}
	}

	if (tbl->dataMap)
		free(tbl->dataMap);
}

char **keys(HashTable *ht)
{
	int elements = len(*ht);
	char **keys_arr = calloc(elements, sizeof(char *));

	int i = 0;
	int index = 0;
	for (i = 0; i < ht->size; i++)
	{
		Node *temp = ht->dataMap[i];
		while (temp != NULL)
		{
			keys_arr[index] = strdup(temp->key);
			temp = temp->next;
			++index;
		}
	}

	return keys_arr;
}

int len(HashTable tbl)
{
	int i = 0;
	int counter = 0;

	if (tbl.dataMap == NULL)
	{
		return 0;
	}

	for (i = 0; i < tbl.size; i++)
	{
		Node *temp = tbl.dataMap[i];
		while (temp != NULL)
		{
			counter++;
			temp = temp->next;
		}
	}

	return counter;
}

void free_nodes(Node **dataMap, int size)
{
	if (!dataMap)
		return;

	int i = 0;
	for (i = 0; i < size; i++)
	{
		Node *current = dataMap[i];
		while (current)
		{
			Node *next = current->next;
			free(current->key);
			free(current);
			current = next;
		}
	}

	free(dataMap);
}
/*if mode is set to 1 it will overwrite the condtent of dest
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
		Node **data_map = calloc(src->size, sizeof(Node *));
		if (!data_map)
		{
			printf("calloc failed,%s:%d.\n", F, L - 2);
			return 0;
		}

		(*dest).size = src->size;
		(*dest).dataMap = data_map;
	}

	register int i = 0;
	for (i = 0; i < src->size; i++)
	{
		if (src->dataMap[i])
		{
			set(src->dataMap[i]->key, src->dataMap[i]->value, dest);
			Node *next = src->dataMap[i]->next;
			while (next)
			{
				set(next->key, next->value, dest);
				next = next->next;
			}
		}
	}

	return 1;
}
