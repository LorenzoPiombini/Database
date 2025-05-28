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
static int set_rec(struct HashTable *ht, char *key);

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
	
int write_record(int *fds,void *key, struct Record_f *rec, int update, char files[3][MAX_FILE_PATH_LENGTH])
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

	off_t offset = 0;
	if(set_rec(ht,(char *)key) == -1) return STATUS_ERROR;

	if(!write_file(fds[1], rec, 0, update)) {
		printf("write to file failed, main.c l %d.\n", __line__ - 1);
		return -1;
	}

	close_file(1, fds[0]);
	fds[0] = open_file(files[0], 1); // opening with o_trunc

	/* write the new indexes to file */
	if (!write_index_file_head(fds[0], index)) {
		printf("write to file failed, %s:%d", f, l - 2);
		return -1;	
	}

	for (int i = 0; i < index; i++) {

		if (!write_index_body(fd_index, i, &ht[i])) {
			printf("write to file failed. %s:%d.\n", F, L - 2);
			free_ht_array(ht, index);
			return -1;
		}
		destroy_hasht(&ht[i]);
	}

	free(ht);

	close_file(1, fds[0]);
	fds[0] = open_file(files[0], 0); // opening in regular mode
	return 0;
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

static int set_rec(struct HashTable *ht, char *key)
{
	int key_type = 0;
	void *key_conv = key_converter(key, &key_type);
	if (key_type == UINT && !key_conv) {
		fprintf(stderr, "error to convert key");
		goto clean_on_error;
	} else if (key_type == UINT) {
		if (key_conv) {
			if (!set(key_conv, key_type, eof, &ht[0])){
				free(key_conv);
				return -1;
			}
			free(key_conv);
		}
	} else if (key_type == STR) {
		/*create a new key value pair in the hash table*/
		if (!set((void *)key, key_type, eof, &ht[0])) {
			return -1;
		}
	}
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
