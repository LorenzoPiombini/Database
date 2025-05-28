#ifndef _CRUD_H_
#define _CRUD_H_

#define ONLY_SCHEMA 1

int open_files(char *file_name, int *fds, char files[3][MAX_FILE_PATH_LENGTH], int option);
int get_record(char *file_name,struct Record_f *rec, void *key, struct Header_d hd, int *fds);
int is_db_file(struct Header_d *hd, int *fds);

#endif
