#ifndef HELPER_H
#define HELPER_H

#include "hash_tbl.h"

unsigned char create_empty_file(int fd_data, int fd_index, int bucket_ht);
unsigned char append_to_file(int fd_data, int fd_index, char *file_path, char *key, char *data_to_add, char **files, HashTable *ht);
unsigned char create_file_with_schema(int fd_data, int fd_index, char *schema_def, int bucket_ht, int indexes);
#endif
