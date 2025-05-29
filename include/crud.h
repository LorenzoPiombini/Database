#ifndef _CRUD_H_
#define _CRUD_H_

#define ONLY_SCHEMA 1

int open_files(char *file_name, int *fds, char files[3][MAX_FILE_PATH_LENGTH], int option);
int get_record(char *file_name,struct Record_f *rec, void *key, struct Header_d hd, int *fds);
int check_schema(char *file_path,char *data_to_add,
		int *fds, 
		char files[][MAX_FILE_PATH_LENGTH], 
		struct Record_f *rec,
		struct Header_d *hd,
		int *lock);
int write_record(int *fds,void *key, struct Record_f *rec, int update, char files[3][MAX_FILE_PATH_LENGTH]);
int is_db_file(struct Header_d *hd, int *fds);

#endif
