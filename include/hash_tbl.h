#ifndef HASH_TBL_H
#define HASH_TBL_H

#include <sys/types.h>
#include <stdint.h>

#define MAX_KEY 4294967296

#define STR 1
#define UINT 2

typedef struct Node
{
	int key_type;
	union
	{
		uint32_t n;
		char *s;
	} key;
	off_t value;
	struct Node *next;
} Node;

typedef struct HashTable
{
	int size;
	Node **dataMap;
	int (*write)(int fd, struct HashTable *ht);
} HashTable;

/*this will contains all the keys of the hash table*/
struct Keys_ht
{
	void **k;
	int *types;
	int length;
};

/*
 * COPRIME is a prime number needed for the hash function
 * I simply pick a prime number between 1 and 3000
 * this will be used along with a very large prime number ( 2^t - 1 )
 * the decison of using such a large number is mainly because i want to be able
 * to handle a very large system anbd the integer key for the hash table
 * will be of type uint32_t wich give us more than 4 billions of key for each hash table
 * this hash table will be used as index file in a file db,
 * so you will have the possibility to store billions of records.
 * */
#define COPRIME 2837

unsigned char hash_tbl_init(int bucket, HashTable *ht);
void print_hash_table(HashTable tbl);
int write_ht(int fd, HashTable *ht);
int hash(void *key, int size, int key_type);
Node *delete(void *key, HashTable *tbl, int key_type);
int set(void *key, int key_type, off_t value, HashTable *tbl);
void free_ht_array(HashTable *ht, int l);
void destroy_hasht(HashTable *tbl); /*free memory*/
off_t get(void *key, HashTable *tbl, int key_type);
struct Keys_ht *keys(HashTable *ht);
int len(HashTable tbl);
void free_nodes(Node **dataMap, int size);
void free_ht_node(Node *node);
void free_keys_data(struct Keys_ht *data);
unsigned char copy_ht(HashTable *src, HashTable *dest, int mode);

#endif /* hash_tbl.h */
