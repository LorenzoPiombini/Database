#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "helper.h"
#include "record.h"
#include "parse.h"
#include "common.h"
#include "file.h"
#include "str_op.h"
#include "debug.h"

static char prog[] = "db";
unsigned char create_empty_file(int fd_schema, int fd_index, int bucket_ht)
{

	struct Schema sch = {0};
	struct Header_d hd = {HEADER_ID_SYS, VS, sch};

	if (!write_empty_header(fd_schema, &hd)) {
		printf("helper.c l %d.\n", __LINE__ - 1);
		return 0;
	}

	int bucket = bucket_ht > 0 ? bucket_ht : 7;
	HashTable ht = {0};
	ht.size = bucket;
	ht.write = write_ht;

	if (!ht.write(fd_index, &ht)) {
		printf("write to index file failed, helper.c l %d.\n", __LINE__ - 1);
		return 0;
	}

	destroy_hasht(&ht);
	return 1;
}

unsigned char append_to_file(int fd_data, int *fd_schema, char *file_path, char *key,
		char files[][MAX_FILE_PATH_LENGTH],char *data_to_add, HashTable *ht)
{

	int fields_count = 0; 
	
	struct Record_f rec ={0};
	struct Schema sch = {0};
	struct Header_d hd = {0, 0, sch};
	unsigned char check = 0;

	int mode = check_handle_input_mode(data_to_add, FWRT);
	if(mode == 1){
		
		fields_count = count_fields(data_to_add, NULL);
		if (fields_count > MAX_FIELD_NR) {
			printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
			return 0;
		}

		char *buffer = strdup(data_to_add);
		if(!buffer) return 0;
		check = perform_checks_on_schema(mode,buffer, fields_count, file_path, &rec, &hd);

		free(buffer);
	
	}else{
		check = perform_checks_on_schema(mode,data_to_add, -1, file_path, &rec, &hd);
	}

	begin_in_file(fd_data);
	if (check == SCHEMA_ERR || check == 0) {
		return 0;
	}

	if (check == SCHEMA_NW || 
		check == SCHEMA_NW_NT ||
		check == SCHEMA_CT_NT ||
		check == SCHEMA_EQ_NT ) { 
		/*if the schema is new we update the header*/
		close_file(1,*fd_schema);
		*fd_schema = open_file(files[2],1);

		if(file_error_handler(1,*fd_schema) != 0) {
			free_record(&rec, rec.fields_num);
			return 0;
		}

		if (!write_header(*fd_schema, &hd))
		{
			printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
			free_record(&rec, rec.fields_num);
			return 0;
		}
	}

	off_t eof = go_to_EOF(fd_data);
	if (eof == -1) {
		printf("file pointer failed, helper.c l %d.\n", __LINE__ - 2);
		free_record(&rec, rec.fields_num);
		return 0;
	}

	int key_type = is_num(key);
	if (!set((void *)key, key_type, eof, ht)) {
		free_record(&rec, rec.fields_num);
		return ALREADY_KEY;
	}

	if (!write_file(fd_data, &rec, 0, 0)) {
		printf("write to file failed, helper.c l %d.\n", __LINE__ - 1);
		free_record(&rec, rec.fields_num);
		return 0;
	}

	free_record(&rec, rec.fields_num);
	return 1;
}

int create_file_with_schema(int fd_schema,  int fd_index, char *schema_def, int bucket_ht, int indexes, int file_field)
{

	int mode = check_handle_input_mode(schema_def, FCRT) | DF;
	int fields_count = 0; 
	char *buf_sdf = NULL; 
	char *buf_t = NULL;
	/* init he Schema structure*/
	struct Schema sch = {0};
	sch.fields_num = fields_count;
	memset(sch.types,-1,sizeof(int)*MAX_FIELD_NR);

	switch(mode){
	case TYPE_DF:
	{
		fields_count = count_fields(schema_def,NULL);

		if (fields_count == 0) {
			fprintf(stderr,"(%s): type syntax might be wrong.\n",prog);
			return -1;
		}

		if (fields_count > MAX_FIELD_NR) {
			fprintf(stderr,"(%s): too many fields, max %d fields each file definition.\n"
					,prog, MAX_FIELD_NR);
			return -1;
		}

		buf_sdf = strdup(schema_def);
		buf_t = strdup(schema_def);
		if(!buf_t || !buf_sdf){
			fprintf(stderr,"(%s): strdup failed, %s:%d.\n",prog,__FILE__,__LINE__);
			if(buf_sdf) free(buf_sdf);	
			if(buf_t) free(buf_t);	
			return -1;
		}
		if (!create_file_definition_with_no_value(mode,fields_count, buf_sdf, buf_t, &sch)) {
			fprintf(stderr,"(%s): can't create file definition %s:%d.\n",prog, F, L - 1);
			free(buf_sdf);
			free(buf_t);
			return -1;
		}
		free(buf_sdf);
		free(buf_t);
		break;
	}
	case HYB_DF:
	case NO_TYPE_DF	:	
	{
		if (!create_file_definition_with_no_value(mode,fields_count, schema_def,NULL, &sch)) {
			fprintf(stderr,"(%s): can't create file definition %s:%d.\n",prog, F, L - 1);
			return -1;
		}
		break;
	}
	default:
		fprintf(stderr,"(%s):invalid input.\n",prog);
		return -1;
	}

	struct Header_d hd = {0, 0, sch};

	if (!create_header(&hd)) return -1;

	if (!write_header(fd_schema, &hd)) {
		fprintf(stderr,"(%s): write schema failed, %s:%d.\n",prog, F, L - 1);
		return -1;
	}

	if(file_field) return 0;

	/*  write the index file */
	int bucket = bucket_ht > 0 ? bucket_ht : 7;
	int index_num = indexes > 0 ? indexes : 5;

	if (!write_index_file_head(fd_index, index_num)) {
		printf("write to file failed, %s:%d", F, L - 2);
		return -1;
	}

	int i = 0;
	for (i = 0; i < index_num; i++)
	{
		HashTable ht = {0}; 
		ht.size = bucket;
		ht.write = write_ht;

		if (!write_index_body(fd_index, i, &ht))
		{
			printf("write to file failed. %s:%d.\n", F, L - 2);
			destroy_hasht(&ht);
			return -1;
		}

		destroy_hasht(&ht);
	}

	return 0;
}
