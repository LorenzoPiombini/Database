#ifndef FILE_H
#define FILE_H

#include "record.h"
#include "hash_tbl.h"
#include "parse.h"



#if defined(__linux__)

#define STD_RAM_FILE 4096*1000 /* 4 MiB */  

struct Ram_file{
	uint8_t *mem; /* memory */
	uint64_t capacity; /*memory size*/
	uint64_t size; /* size of the data written to memory*/
	uint64_t offset; /* the place where we are in the file in memory */
};

/*API end points*/
int open_file(char *fileName, int use_trunc);
int create_file(char *fileName);
void close_file(int count, ...);
file_offset get_file_offset(int fd);
file_offset go_to_EOF(int fd);
file_offset find_record_position(int fd, file_offset offset);
void delete_file(unsigned short count, ...);
file_offset begin_in_file(int fd);
file_offset move_in_file_bytes(int fd, file_offset offset);
unsigned char write_index_file_head(int fd, int index_num);
unsigned char write_index_body(int fd, int i, HashTable *ht);
unsigned char read_index_nr(int i_num, int fd, HashTable **ht);
unsigned char read_all_index_file(int fd, HashTable **ht, int *p_index);
unsigned char read_index_file(int fd, HashTable *ht);
size_t record_size_on_disk(void *rec_f);
int write_file(int fd, struct Record_f *rec, file_offset update_file_offset, unsigned char update);
int buffered_write(int *fd, struct Record_f *rec, int update, file_offset rec_ram_file_pos, file_offset offset);
file_offset get_update_offset(int fd);
int read_file(int fd, char *file_name, struct Record_f *rec, struct Schema sch);
int file_error_handler(int count, ...);
int padding_file(int fd, int bytes, size_t hd_st);
unsigned char indexes_on_file(int fd, int *p_i_nr);
unsigned char nr_bucket(int fd, int *p_buck);
file_offset get_file_size(int fd, char *file_name);
int add_index(int index_nr, char *file_name, int bucket);
int write_ram_record(struct Ram_file *ram, struct Record_f *rec, int update, size_t init_ram_size, file_offset offset);
file_offset read_ram_file(char* file_name, struct Ram_file *ram, struct Record_f *rec, struct Schema sch);
int get_all_record(int fd, struct Ram_file *ram);
void clear_ram_file(struct Ram_file *ram);
void close_ram_file(struct Ram_file *ram);

#elif defined(_WIN32)

HANDLE open_file(char *fileName, uint32_t use_trunc)

#endif /* os if*/
#endif /* ifndef */
