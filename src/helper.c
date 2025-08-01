#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "helper.h"
#include "record.h"
#include "parse.h"
#include "common.h"
#include "file.h"
#include "str_op.h"
#include "crud.h"
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

unsigned char append_to_file(int *fds, char *file_path, char *key,
		char files[][MAX_FILE_PATH_LENGTH],char *data_to_add, HashTable *ht)
{

	struct Record_f rec ={0};
	struct Schema sch = {0};
	struct Header_d hd = {0, 0, sch};
	int lock_f = 0;
	if(check_data(file_path,data_to_add,fds,files, &rec,&hd,&lock_f) == -1) return -1;

	off_t eof = go_to_EOF(fds[1]);
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

	if(!lock_f)
	if (!write_file(fds[1], &rec, 0, 0)) {
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
	/* init the Schema structure*/
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
