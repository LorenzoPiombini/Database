#ifndef _CRUD_H_
#define _CRUD_H_

#define ONLY_SCHEMA 1

#define IMPORT 0
#define RAM_FILE 1


extern HashTable *g_ht;
extern int g_index;
extern int *p_gi;
extern struct Ram_file ram;

int open_files(char *file_name, int *fds, char files[3][MAX_FILE_PATH_LENGTH], int option);
int get_record(int mode,char *file_name,struct Record_f *rec, void *key, struct Header_d hd, int *fds);

int check_data(char *file_path,char *data_to_add,
		int *fds, 
		char files[][MAX_FILE_PATH_LENGTH], 
		struct Record_f *rec,
		struct Header_d *hd,
		int *lock);
int write_record(int *fds,void *key,
		int key_type, 
		struct Record_f *rec, 
		int update,
		char files[3][MAX_FILE_PATH_LENGTH],
		int *lock,
		int mode);
int is_db_file(struct Header_d *hd, int *fds);
int write_index(int *fds, int index, HashTable *ht, char *file_name);
int update_rec(char *file_path,int *fds,void *key,struct Record_f *rec,struct Header_d hd,int check,int *lock_f);

#endif
