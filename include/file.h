#ifndef FILE_H
#define FILE_H 1

#include "db_types.h"
#include "record.h"
#include "hash_tbl.h"
#include "parse.h"

#define STD_RAM_FILE 4096*1000 /* 4 MiB */  



struct Ram_file{
	ui8 *mem; /* memory */
	ui64 capacity; /*memory size*/
	ui64 size; /* size of the data written to memory*/
	ui64 offset; /* the place where we are in the file in memory */
};

#define FILE_IS_CACHED -2 /*error value to define the presence of a file in the cache*/
struct Cache{
	HashTable *index_file;
	struct Ram_file data_file;
};/*16 bytes*/


/* Key,value pair the key will be the file name in the cache, 
 *  value is the time_stamp since the file has been cached
 * */
extern HashTable cache_register;

#if defined(__linux__) || defined(__APPLE__)

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
long long read_ram_file(char* file_name, struct Ram_file *ram, struct Record_f *rec, struct Schema sch);
int get_all_record(int fd, struct Ram_file *ram);
void clear_ram_file(struct Ram_file *ram);
void close_ram_file(struct Ram_file *ram);
int init_ram_file(struct Ram_file *ram, size_t size);
int cache_file(int *fds,char *file_name,struct Cache *c,HashTable *cache_register);
#elif defined(_WIN32)

HANDLE open_file(char *fileName, ui32 use_trunc);
HANDLE create_file(char *file_name);
void close_file(int count, ...);
int cache_file(HANDLE file_handle,char *file_name,struct Cache *c,HashTable *cache_register)
int delete_file(int count,...);
file_offset begin_in_file(HANDLE file_handle);
file_offset get_file_offset(HANDLE file_handle);
file_offset find_record_position(HANDLE file_handle, long long offset);
file_offset go_to_EOF(HANDLE file_handle);
file_offset move_in_file_bytes(HANDLE file_handle, file_offset offset);
DWORD get_file_size(HANDLE file_handle);
#endif /* os if*/
#endif /* ifndef */
