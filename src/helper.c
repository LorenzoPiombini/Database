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

unsigned char append_to_file(int fd_data, int fd_schema, char *file_path, char *key, char *data_to_add, HashTable *ht)
{
	int fields_count = count_fields(data_to_add, TYPE_) + count_fields(data_to_add, T_);

	if (fields_count > MAX_FIELD_NR)
	{
		printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
		return 0;
	}

	char *buffer = strdup(data_to_add);
	char *buf_t = strdup(data_to_add);
	char *buf_v = strdup(data_to_add);

	struct Record_f *rec = NULL;
	struct Schema sch = {0};
	struct Header_d hd = {0, 0, sch};

	begin_in_file(fd_data);
	unsigned char check = perform_checks_on_schema(buffer, buf_t, buf_v, fields_count,
						fd_data, file_path, &rec, &hd);

	free(buffer), free(buf_t), free(buf_v);
	if (check == SCHEMA_ERR || check == 0) {
		return 0;
	}

	if (!rec) {
		printf("error creating record, helper.c l %d\n", __LINE__ - 1);
		return 0;
	}

	if (check == SCHEMA_NW)
	{ /*if the schema is new we update the header*/
		// check the header size
		// printf("header size is: %ld",compute_size_header(hd));
		close_file(1,fd_schema);
		open_file(fd_schema,1);

		if(file_error_handler(1,fd_schema) != 0) {
			free_record(rec, rec->fields_num);
			return 0;
		}

		if (!write_header(fd_schema, &hd))
		{
			printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
			free_record(rec, rec->fields_num);
			return 0;
		}
	}

	off_t eof = go_to_EOF(fd_data);
	if (eof == -1)
	{
		printf("file pointer failed, helper.c l %d.\n", __LINE__ - 2);
		free_record(rec, rec->fields_num);
		return 0;
	}

	int key_type = is_num(key);
	if (!set((void *)key, key_type, eof, ht))
	{
		free_record(rec, rec->fields_num);
		return ALREADY_KEY;
	}

	if (!write_file(fd_data, rec, 0, 0))
	{
		printf("write to file failed, helper.c l %d.\n", __LINE__ - 1);
		free_record(rec, rec->fields_num);
		return 0;
	}

	free_record(rec, rec->fields_num);
	return 1;
}

unsigned char create_file_with_schema(int fd_schema, int fd_data, int fd_index, char *schema_def, int bucket_ht, int indexes)
{
	int fields_count = count_fields(schema_def, TYPE_) + count_fields(schema_def, T_);

	if (fields_count > MAX_FIELD_NR)
	{
		printf("Too many fields, max %d fields each file definition.\n", MAX_FIELD_NR);
		return 0;
	}

	char *buf_sdf = strdup(schema_def);
	char *buf_t = strdup(schema_def);

	struct Schema sch = {0};
	sch.fields_num = fields_count;
	if (!create_file_definition_with_no_value(fields_count, buf_sdf, buf_t, &sch))
	{
		free(buf_sdf), free(buf_t);
		printf("can't create file definition %s:%d.\n", __FILE__, __LINE__ - 1);
		return 0;
	}

	free(buf_sdf), free(buf_t);
	struct Header_d hd = {0, 0, sch};

	if (!create_header(&hd)) return 0;

	// print_size_header(hd);

	if (!write_header(fd_schema, &hd)) return 0;

	/*  write the index file */
	int bucket = bucket_ht > 0 ? bucket_ht : 7;
	int index_num = indexes > 0 ? indexes : 5;

	if (!write_index_file_head(fd_index, index_num)) {
		printf("write to file failed, %s:%d", F, L - 2);
		return 0;
	}

	int i = 0;
	for (i = 0; i < index_num; i++)
	{
		HashTable ht = {0}; 
		ht.size = bucket;
		ht.write = write_ht;

		if (write_index_body(fd_index, i, &ht) == -1)
		{
			printf("write to file failed. %s:%d.\n", F, L - 2);
			destroy_hasht(&ht);
			return 0;
		}

		destroy_hasht(&ht);
	}

	return 1;
}
