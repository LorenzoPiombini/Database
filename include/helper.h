#ifndef HELPER_H
#define HELPER_H

#include "hash_tbl.h"
#include "str_op.h"

#if defined(_WIN32)
	#include <windows.h>
unsigned char create_empty_file(HANDLE fd_schema, HANDLE fd_index, int bucket_ht);
unsigned char append_to_file(HANDLE *fds, char *file_path, char *key,char files[][MAX_FILE_PATH_LENGTH],char *data_to_add, HashTable *ht);
int create_file_with_schema(HANDLE fd_schema, HANDLE fd_index, char *schema_def, int bucket_ht, int indexes, int file_field);
#else
unsigned char create_empty_file(int fd_schema, int fd_index, int bucket_ht);
unsigned char append_to_file(int *fds, char *file_path, char *key,char files[][MAX_FILE_PATH_LENGTH],char *data_to_add, HashTable *ht);
int create_file_with_schema(int fd_schema,  int fd_index, char *schema_def, int bucket_ht, int indexes, int file_field);
#endif



#endif
