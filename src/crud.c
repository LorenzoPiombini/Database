#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "file.h"
#include "record.h"
#include "lock.h"
#include "str_op.h"
#include "common.h"
#include "debug.h"
#include "hash_tbl.h"
#include "crud.h"

static char prog[] = "db";
static off_t get_rec_position(struct HashTable *ht, char *key);

int get_record(char *file_name,struct Record_f *rec, void *key, struct Header_d hd, int *fds)
{

	HashTable ht = {0};
	HashTable *p_ht = &ht;
	if (!read_index_nr(0, fds[0], &p_ht)) {
		printf("reading index file failed, %s:%d.\n", F, L - 1);
		return STATUS_ERROR;
	}

	off_t offset = 0;
	if((offset = get_rec_position(p_ht,(char *)key)) == -1) return STATUS_ERROR;

	destroy_hasht(p_ht);
	if (find_record_position(fds[1], offset) == -1) {
		__er_file_pointer(F, L - 1);
		return STATUS_ERROR;
	}

	if(read_file(fds[1], file_name, rec, hd.sch_d) == -1) {
		printf("read record failed, %s:%d.\n",__FILE__, __LINE__ - 1);
		return STATUS_ERROR;
	}

	off_t update_rec_pos = 0; 
	struct Record_f *temp = NULL;
	temp = rec;
	while ((update_rec_pos = get_update_offset(fds[1])) > 0) {
		struct Record_f *n = calloc(1, sizeof(struct Record_f));
		if(!n){		
			__er_calloc(F,L-2);
			return STATUS_ERROR;
		}

		if (find_record_position(fds[1], update_rec_pos) == -1) {
			__er_file_pointer(F, L - 1);
			free(n);
			return STATUS_ERROR;
		}

		if(read_file(fds[1], file_name,n,hd.sch_d) == -1) {
			printf("read record failed, %s:%d.\n",__FILE__, __LINE__ - 2);
			return STATUS_ERROR;
		}

		temp->next = n;
		while(temp->next) temp = temp->next; 
		rec->count++;
	}

	if (update_rec_pos == -1) {
		return STATUS_ERROR;
	}

	return 0;
}

int is_db_file(struct Header_d *hd, int *fds)
{

	while((is_locked(3,fds[0],fds[1],fds[2])) == LOCKED);

	if (!read_header(fds[2], hd)) return STATUS_ERROR;

	return 0;
}
	
int write_record(int *fds)
{
	off_t eof = go_to_EOF(fds[1]);
	if (eof == -1) {
		__er_file_pointer(F, L - 1);
		return -1;
	}

	HashTable *ht = NULL;
	int index = 0;
	int *p_index = &index;
	/* load al indexes in memory */
	if (!read_all_index_file(fds[0], &ht, p_index)) {
		fprintf(stderr,"read index file failed. %s:%d.\n", F, L - 2);
		return 1;
	}




}
int open_files(char *file_name, int *fds, char files[3][MAX_FILE_PATH_LENGTH], int option)
{
	if(three_file_path(file_name, files) == EFLENGTH){
		fprintf(stderr,"(%s): file name or path '%s' too long",prog,file_name);
		return STATUS_ERROR;
	}
	
	int err = 0;
	int fd_index = -1; 
	int fd_data = -1; 
	int fd_schema = -1; 
	switch(option){
	case ONLY_SCHEMA:
		fd_schema = open_file(files[2], 0);
		if ((err = file_error_handler(1,fd_schema)) != 0) {
			if(err == ENOENT)
				fprintf(stderr,"(%s): File '%s' doesn't exist.\n",prog,file_name);
			else
				printf("(%s): Error in creating or opening files, %s:%d.\n",prog, F, L - 2);

			return STATUS_ERROR;
		}

		break;
	default:
		fd_index = open_file(files[0], 0);
		fd_data = open_file(files[1], 0);
		fd_schema = open_file(files[2], 0);

		/* file_error_handler will close the file descriptors if there are issues */
		if ((err = file_error_handler(3, fd_index, fd_data,fd_schema)) != 0) {
			if(err == ENOENT)
				fprintf(stderr,"(%s): File '%s' doesn't exist.\n",prog,file_name);
			else
				printf("(%s): Error in creating or opening files, %s:%d.\n",prog, F, L - 2);

			return STATUS_ERROR;
		}
		break;
	}

	fds[0] = fd_index;
	fds[1] = fd_data;
	fds[2] = fd_schema;
	return 0;
}


static off_t get_rec_position(struct HashTable *ht, char *key)
{
	off_t offset = 0;
	int key_type = 0;
	void *key_conv = key_converter(key, &key_type);
	if (key_type == UINT && !key_conv) {
		fprintf(stderr, "(%s): error to convert key",prog);
		return STATUS_ERROR;
	} else if (key_type == UINT) {
		if (key_conv) {
			offset = get(key_conv, ht, key_type); /*look for the key in the ht */
			free(key_conv);
		}
	} else if (key_type == STR) {
		offset = get((void *)key, ht, key_type); /*look for the key in the ht */
	}
	return offset;
}
