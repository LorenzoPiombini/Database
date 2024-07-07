#ifndef HASH_TBL_H
#define HASH_TBL_H

#include <sys/types.h>

typedef struct Node{
	char *key;
	off_t value;
        struct Node* next;
}Node;

typedef struct HashTable HashTable; //forward declaration

struct HashTable {
	int size;
	Node** dataMap;
	int(*write)(int fd, HashTable *ht);
};

void print_hash_table(HashTable tbl);

int write_ht(int fd, HashTable *ht);

int hash(char *key, int size);


Node* delete(char *key, HashTable *tbl);

int set(char *key, off_t value, HashTable *tbl);

void destroy_hasht(HashTable *tbl); //free memory

off_t get(char *key, HashTable *tbl);

int len(HashTable tbl);

char** keys(HashTable *ht);
int len(HashTable tbl);
void free_nodes(Node** dataMap, int size);
#endif
