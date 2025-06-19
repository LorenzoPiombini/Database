#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <byteswap.h>
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
static int set_rec(struct HashTable *ht, void *key, off_t offset, int key_type);


HashTable *g_ht = NULL;
int g_index = 0;
int *p_gi = &g_index;
struct Ram_file ram = {0};

int get_record(int mode,char *file_name,struct Record_f *rec, void *key, struct Header_d hd, int *fds)
{
	int e = 0;
	if(mode == RAM_FILE){
		e = 1;
		if(get_all_record(fds[1],&ram) == -1){
			e = 0;
			fprintf(stderr,"cannot get all the record from '%s'.\n",file_name);
		}
	}

	HashTable ht = {0};
	HashTable *p_ht = &ht;
	if (!read_index_nr(0, fds[0], &p_ht)) {
		printf("reading index file failed, %s:%d.\n", F, L - 1);
		return STATUS_ERROR;
	}

	off_t offset = 0;
	if((offset = get_rec_position(p_ht,(char *)key)) == -1) {
		fprintf(stderr,"(%s): record not found.\n",prog);
		destroy_hasht(p_ht);
		return STATUS_ERROR;
	}

	destroy_hasht(p_ht);
	if(!e){
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
	} else{
		off_t pos_after_read = 0;
		if(( pos_after_read = read_ram_file(file_name,&ram, (size_t)offset, rec,hd.sch_d)) == -1){
			fprintf(stderr,"cannot read from ram file '%s'.\n",file_name);
			clear_ram_file(&ram);
			return STATUS_ERROR;
		}

		uint64_t up_r_pos_ne = 0; 
		struct Record_f *temp = NULL;
		temp = rec;
		do{
			memcpy(&up_r_pos_ne,&ram.mem[pos_after_read],sizeof(uint64_t));
			if(up_r_pos_ne == 0) break;

			struct Record_f *n = calloc(1, sizeof(struct Record_f));
			if(!n){		
				__er_calloc(F,L-2);
				clear_ram_file(&ram);
				return STATUS_ERROR;
			}

			off_t update_rec_pos = bswap_64(up_r_pos_ne);
			if(( pos_after_read = read_ram_file(file_name,&ram, (size_t)update_rec_pos, n,hd.sch_d)) == -1){
				fprintf(stderr,"cannot read from ram file '%s'.\n",file_name);
				clear_ram_file(&ram);
				return STATUS_ERROR;
			}

			temp->next = n;
			while(temp->next) temp = temp->next; 
			rec->count++;
			
		}while(up_r_pos_ne != 0); 

		


	}
	clear_ram_file(&ram);
	return 0;
}

int is_db_file(struct Header_d *hd, int *fds)
{

	while((is_locked(3,fds[0],fds[1],fds[2])) == LOCKED);

	if (!read_header(fds[2], hd)) return STATUS_ERROR;

	return 0;
}

int check_data(char *file_path,char *data_to_add,
		int *fds, 
		char files[][MAX_FILE_PATH_LENGTH], 
		struct Record_f *rec,
		struct Header_d *hd,
		int *lock_f)
{
	int fields_count = 0;
	unsigned char check = 0;
	int pos[200];
	memset(pos,-1,sizeof(int)*200);

   	find_double_delim("::",data_to_add,pos); 

	int mode = check_handle_input_mode(data_to_add, FWRT) | WR;

	/*check schema*/
	if(mode == TYPE_WR){
		fields_count = count_fields(data_to_add,NULL);
		if(fields_count == 0){
			fprintf(stderr,"(%s):check input syntax.\n",prog);
			return STATUS_ERROR;
		}
		if (fields_count > MAX_FIELD_NR) {
			printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
			return STATUS_ERROR;
		}
		if(pos[0] != -1){
			int i = 0;
			while(pos[i] != -1){
				data_to_add[pos[i]] = ':';
				i++;
			}
		}

		char *buffer = strdup(data_to_add);
		if(!buffer){
			fprintf(stderr,"(%s): strdup() failed, %s:%d.\n",prog,F,L-5);
			return STATUS_ERROR;
		}

		check = perform_checks_on_schema(mode,buffer, fields_count,file_path, rec, hd);
		free(buffer);

	} else {
		check = perform_checks_on_schema(mode,data_to_add, -1,file_path, rec, hd);
	}

	if (check == SCHEMA_ERR || check == 0) return STATUS_ERROR;

	/*
	 * save the schema file for 
	 * SCHEMA_NW 
	 * SCHEMA_EQ_NT 
	 * SCHEMA_NW_NT 
	 * SCHEMA_CT_NT
	 * */	
	int r = 0;
	if (check == SCHEMA_NW ||
			check == SCHEMA_NW_NT ||
			check == SCHEMA_CT_NT ||
			check == SCHEMA_EQ_NT){
		/*
		 * if the schema is one between 
		 * SCHEMA_NW 
		 * SCHEMA_EQ_NT 
		 * SCHEMA_NW_NT 
		 * SCHEMA_CT_NT
		 * we update the header
		 * */

		/* aquire lock */
		while(is_locked(3,fds[0],fds[1],fds[2]) == LOCKED);
		while((r = lock(fds[0],WLOCK)) == WTLK);
		if(r == -1){
			fprintf(stderr,"can't acquire or release proper lock.\n");
			return STATUS_ERROR;
		}
		*lock_f = 1;
		close_file(1,fds[2]);
		fds[2] = open_file(files[2],1); /*open with O_TRUNCATE*/

		if(file_error_handler(1,fds[2]) != 0) return STATUS_ERROR;

		if (!write_header(fds[2], hd)) {
			__er_write_to_file(F, L - 1);
			return STATUS_ERROR;
		}

		close_file(1,fds[2]);
		fds[2] = open_file(files[2],0); /*open in regular mode*/
	} 

	return check;
}

int write_record(int *fds,void *key,
		int key_type,
		struct Record_f *rec, 
		int update,
		char files[3][MAX_FILE_PATH_LENGTH],
		int *lock_f,
		int mode)
{
	if(mode == IMPORT){
		/*allocate memory buffer for index and data file*/
		/*don't write to disk all the time*/
		if(!g_ht){
			/* load al indexes in memory */
			if (!read_all_index_file(fds[0], &g_ht, p_gi)) {
				fprintf(stderr,"read index file failed. %s:%d.\n", F, L - 2);
				return 1;
			}
		}

		if(set_rec(g_ht,key,(off_t)ram.size,key_type) == -1) return 0;

		if(write_ram_record(&ram,rec) == -1){
			fprintf(stderr,"write_ram_record() failed. %s:%d.\n",__FILE__,__LINE__-1);
			free_ht_array(g_ht,g_index);
			return STATUS_ERROR;
		}
	
		return 0;
	}

	if(!(*lock_f)){
		*lock_f = 1;
		int r = 0;
		/* aquire lock */
		while(is_locked(3,fds[0],fds[1],fds[2]) == LOCKED);
		while((r = lock(fds[0],WLOCK)) == WTLK);
		if(r == -1){
			fprintf(stderr,"can't acquire or release proper lock.\n");
			return STATUS_ERROR;
		}
	}
	
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

	if(set_rec(ht,key,eof,key_type) == -1){
		free_ht_array(ht, index);
		return STATUS_ERROR;
	}

	if(!write_file(fds[1], rec, 0, update)) {
		fprintf(stderr,"write to file failed, %s:%d.\n", F,L - 1);
		return -1;
	}

	if(write_index(fds,index,ht,files[0]) == -1) return -1;

	if(*lock_f) while(lock(fds[0],UNLOCK) == WTLK);
	return 0;
}

int write_index(int *fds, int index, HashTable *ht, char *file_name)
{
	close_file(1, fds[0]);
	fds[0] = open_file(file_name, 1); // opening with o_trunc

	/* write the new indexes to file */
	if (!write_index_file_head(fds[0], index)) {
		fprintf(stderr,"write index head to file failed, %s:%d.\n", F,L - 1);
		return -1;	
	}

	for (int i = 0; i < index; i++) {

		if (!write_index_body(fds[0], i, &ht[i])) {
			printf("write to file failed. %s:%d.\n", F, L - 2);
			free_ht_array(ht, index);
			return -1;
		}
		destroy_hasht(&ht[i]);
	}

	free(ht);

	close_file(1, fds[0]);
	fds[0] = open_file(file_name, 0); // opening in regular mode
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

static int set_rec(struct HashTable *ht, void *key, off_t offset, int key_type)
{
	if(key_type == UINT){
		if(!set(key, key_type, offset, &ht[0])) return -1;

		return 0;
	} else if (key_type == STR) {
		if(!set((void *)key, key_type, offset, &ht[0])) return -1;

		return 0;
	}

	void *key_conv = key_converter(key, &key_type);
	if (key_type == UINT && !key_conv) {
		fprintf(stderr, "error to convert key");
		return -1;
	} else if (key_type == UINT) {
		if (key_conv) {
			if (!set(key_conv, key_type, offset, &ht[0])){
				free(key_conv);
				return -1;
			}
			free(key_conv);
		}
	} else if (key_type == STR) {
		/*create a new key value pair in the hash table*/
		if (!set((void *)key, key_type, offset, &ht[0])) return -1;
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
