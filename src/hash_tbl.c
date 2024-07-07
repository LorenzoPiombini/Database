#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include "hash_tbl.h"


void print_hash_table(HashTable tbl){
        int i = 0;
        for(i; i < tbl.size;i++ ){
                printf("%d: \n",i);
                Node *node = tbl.dataMap[i];

                        while(node) {
                                if(node->key){
                                printf("\t{ %s,%ld }\n",node->key,node->value);
                                }
                                node = node->next;
                        }

        }
}


int write_ht(int fd, HashTable *ht){
        int i= 0;

	if(write(fd, &ht->size, sizeof(ht->size)) == -1){
		perror("writing index file");
		return 0;
	}
		int ht_l = len(*ht);

		if(write(fd, &ht_l, sizeof(ht_l)) == -1){
			perror("write ht length");
			return 0;
		}

		for(i; i < ht->size; i++){
	    		Node* current = ht->dataMap[i];
					while(current != NULL){
						size_t key_l = strlen(current->key);

						if(write(fd, &key_l, sizeof(key_l)) < 0 ||
						   write(fd, current->key, key_l ) < 0 	||
		   				   write(fd, &current->value, sizeof(current->value)) < 0){
							perror("write index:");	
							return 0; //false	
						}
						current = current->next;
					}
		}
		
	return 1;
}


int hash(char *key, int size){
	unsigned long hash = 0;
	int i = 0;

	int len = strlen(key);
	for (i ; i < len; i++){
		int asciiValue = (int)key[i];
		hash = (hash + asciiValue * 23);
	}

	return hash % size;
}

off_t get(char *key, HashTable *tbl){
	int index = hash(key, tbl->size);
	Node* temp = tbl->dataMap[index];
	while(temp != NULL){
		if (strcmp(temp->key, key) == 0) 
			return temp->value;

		temp = temp->next;
	}

	return -1l;
}


int set(char *key, off_t value, HashTable* tbl){
	int index = hash(key, tbl->size);
	Node* newNode = malloc(sizeof(Node));
	if(!newNode){
		printf("failed to allocate memory while setting the HashTable.\n");
		return 0;
	}

	newNode->key = strdup(key);
	newNode->value= value;
	newNode->next = NULL;

	if(tbl->dataMap[index] == NULL){
		tbl->dataMap[index]= newNode;
	}else {

		if(strcmp(tbl->dataMap[index]->key, key) == 0){
			printf("key %s, already exist.\n",key);
			free(newNode->key);
			free(newNode);
			return 0 ;
		}

		Node* temp = tbl->dataMap[index];
		while(temp->next != NULL){
			if(strcmp(temp->next->key, key ) == 0){
				printf("could not insert new node \"%s\"\n", key);
			        printf("key already exist. Choose another key value.");
				free(newNode->key);
				free(newNode);
				return 0;
			}
			temp= temp->next;
		}
		temp->next = newNode;
	}

	return 1;//succseed!
}




Node* delete(char *key, HashTable *tbl){
	int index = hash(key,tbl->size);
	Node *current = tbl->dataMap[index];
	Node *previous = NULL;

	while(current != NULL){
		if(strcmp(current->key, key) == 0){
			if (previous == NULL){
				tbl->dataMap[index] = current->next;
			}else{
				previous->next = current->next;
			}

			return current;	
		}
		previous = current;
		current = current->next;
	}

	return NULL;
}
void destroy_hasht(HashTable *tbl){
	int i = 0;
	for(i; i < tbl->size; i++){
		Node* current = tbl->dataMap[i];
		while( current != NULL){
			Node* next = current->next;
			free(current->key);
			free(current);
			current = next;

		}
	}

	free(tbl->dataMap);

}

char** keys(HashTable *ht){
	int elements = len(*ht);
	char** keys_arr = calloc(elements, sizeof(char*));

	int i = 0;
	int index = 0;
	for(i = 0; i < ht->size; i++){
		Node* temp = ht->dataMap[i];
		while(temp != NULL){
			keys_arr[index++] = temp->key;
			temp = temp->next;
		}
	}	

	return keys_arr;
}

int len(HashTable tbl){
	int i=0;
	int counter = 0;

	for(i=0; i < tbl.size; i++){
		Node* temp = tbl.dataMap[i];
		while(temp != NULL){
		    counter++;
		    temp = temp->next;
		}
	}

	return counter;

}

void free_nodes(Node** dataMap, int size){
	if(!dataMap)
		return;

	int i = 0 ;
	for(i; i < size ; i++){
		Node* current = dataMap[i];
		while(current){
			Node* next = current->next;
		      	free(current->key);
	       	       	free(current);
			current= next;	       
		}
	}

	free(dataMap);

}
