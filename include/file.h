#ifndef FILE_H
#define FILE_H

#include "record.h"

typedef struct HashTable HashTable;

int open_file(char *fileName, int use_trunc);
int create_file(char *fileName);
void close_file(int count, ...);
off_t get_file_offset(int fd);
off_t go_to_EOF(int fd);
off_t find_record_position(int fd, off_t offset);
void delete_file(unsigned short count, ...);
off_t begin_in_file(int fd);
off_t move_in_file_bytes(int fd, off_t offset);
unsigned char write_index_file_head(int fd, int index_num);
unsigned char write_index_body(int fd, int i, HashTable *ht);
unsigned char read_index_nr(int i_num, int fd, HashTable **ht);
unsigned char read_all_index_file(int fd, HashTable **ht, int *p_index);
unsigned char read_index_file(int fd, HashTable *ht);
int write_file(int fd, Record_f *rec, off_t update_off_t, unsigned char update);
ssize_t compute_record_size(Record_f *rec, unsigned char update);
off_t get_update_offset(int fd);
Record_f *read_file(int fd, char *file_name);
int file_error_handler(int count, ...);
int padding_file(int fd, int bytes, size_t hd_st);
ssize_t get_record_size(int fd);
unsigned char indexes_on_file(int fd, int *p_i_nr);
unsigned char nr_bucket(int fd, int *p_buck);
#endif
