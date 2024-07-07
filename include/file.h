#ifndef FILE_H
#define FILE_H

#include "record.h"

typedef struct HashTable HashTable;

int open_file(char *fileName, int use_trunc);
int create_file(char *fileName);
void close_file(int count, ...);
off_t get_file_offset(int fd);
off_t go_to_EOF(int fd);
off_t find_record_position(int fd,off_t offset );
off_t begin_in_file(int fd);
int  read_index_file(int fd, HashTable *ht);
int write_file(int fd, Record_f *rec);
ssize_t compute_record_size(Record_f *rec);
Record_f* read_file(int fd, char* file_name);
int file_error_handler(int count, ...);
int padding_file(int fd, int bytes);

#endif
