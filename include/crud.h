#ifndef _CRUD_H_
#define _CRUD_H_

#define ONLY_SCHEMA 1
#define ONLY_INDEX 2
#define CREATE_FILE 3
#define ONLY_DATA 4
#define CREATE_ONLY_DATA 5
#define CREATE_ONLY_SCHEMA 6

#define IMPORT 0
#define RAM_FILE 1

#include "common.h"
#include "hash_tbl.h"
#include "str_op.h"
#include "file.h"


extern HashTable *g_ht;
extern int g_index;
extern int *p_gi;
extern struct Ram_file ram;


#if defined(__linux__) || defined(__APPLE__)
int open_files(char *file_name, int *fds, char files[3][MAX_FILE_PATH_LENGTH], int option);
int get_record(int mode,char *file_name,struct Record_f *rec, void *key, int key_type, struct Header_d hd, int *fds, int index);
int check_data(char *file_path,char *data_to_add,
		int *fds, 
		char files[][MAX_FILE_PATH_LENGTH], 
		struct Record_f *rec,
		struct Header_d *hd,
		int *lock,
		int option,
		int update);
int write_record(int *fds,
		void *key,
		int key_type,
		struct Record_f *rec, 
		int update,
		char files[3][MAX_FILE_PATH_LENGTH],
		int *lock_f,
		int mode,
		struct Schema *sch);
int get_all_records(char *file_name,int *fds,struct Record_f ***recs,struct Header_d hd);
int is_db_file(struct Header_d *hd, int *fds);
int write_index(int *fds, int index, HashTable *ht, char *file_name);
int update_rec(	char *file_path,
				int *fds,
				void *key,
				int key_type,
				struct Record_f *rec,
				struct Header_d hd,
				int check,
				int *lock_f, 
				char *options, 
				int index);
#else
int open_files(char *file_name, HANDLE *fds, char files[3][MAX_FILE_PATH_LENGTH], int option);
int get_record(int mode,char *file_name,struct Record_f *rec, void *key, int key_type, struct Header_d hd, HANDLE *fds, int index);
int check_data(char *file_path,char *data_to_add,
		HANDLE *fds, 
		char files[][MAX_FILE_PATH_LENGTH], 
		struct Record_f *rec,
		struct Header_d *hd,
		HANDLE *lock,
		int option,
		int update);
int write_record(HANDLE *fds,
		void *key,
		int key_type,
		struct Record_f *rec, 
		int update,
		char files[3][MAX_FILE_PATH_LENGTH],
		HANDLE *lock_f,
		int mode,
		struct Schema *sch);
int get_all_records(char *file_name,HANDLE *fds,struct Record_f ***recs,struct Header_d hd);
int is_db_file(struct Header_d *hd, HANDLE *fds);
int write_index(HANDLE *fds, int index, HashTable *ht, char *file_name);
int update_rec(	char *file_path,
				HANDLE *fds,
				void *key,
				int key_type,
				struct Record_f *rec,
				struct Header_d hd,
				int check,
				HANDLE *lock_f, 
				char *options, 
				int index);
#endif/*OS if*/

int set_tbl(struct HashTable *ht, void *key, file_offset offset, int key_type,int indexing);
int check_const_unique(struct Schema *sch, struct Record_f *rec, HashTable **ht, file_offset eof);
int write_cache_to_disk(struct Cache *c);

#endif /*crud.h*/
