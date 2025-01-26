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

unsigned char create_empty_file(int fd_data, int fd_index, int bucket_ht)
{

	struct Schema sch = {0, NULL, NULL};
	struct Header_d hd = {HEADER_ID_SYS, VS, sch};

	size_t hd_st = compute_size_header((void *)&hd);
	if (hd_st >= MAX_HD_SIZE)
	{
		printf("File definition is bigger than the limit.\n");
		return 0;
	}

	if (!write_empty_header(fd_data, &hd))
	{
		printf("helper.c l %d.\n", __LINE__ - 1);
		return 0;
	}

	if (!padding_file(fd_data, MAX_HD_SIZE, hd_st))
	{
		printf("padding failed. helper.c:%d.\n", __LINE__ - 1);
		return 0;
	}

	int bucket = bucket_ht > 0 ? bucket_ht : 7;
	Node **dataMap = calloc(bucket, sizeof(Node *));
	HashTable ht = {bucket, dataMap, write_ht};

	if (!ht.write(fd_index, &ht))
	{
		printf("write to index file failed, helper.c l %d.\n", __LINE__ - 1);
		return 0;
	}

	destroy_hasht(&ht);
	return 1;
}

unsigned char append_to_file(int fd_data, int fd_index, char *file_path, char *key, char *data_to_add, char **files, HashTable *ht)
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
	struct Schema sch = {0, NULL, NULL};
	struct Header_d hd = {0, 0, sch};

	begin_in_file(fd_data);
	unsigned char check = perform_checks_on_schema(buffer, buf_t, buf_v, fields_count,
												   fd_data, file_path, &rec, &hd);

	if (check == SCHEMA_ERR || check == 0)
	{
		free_schema(&hd.sch_d);
		free(buffer), free(buf_t), free(buf_v);
		return 0;
	}

	if (!rec)
	{
		printf("error creating record, helper.c l %d\n", __LINE__ - 1);
		free(buffer), free(buf_t), free(buf_v);
		free_schema(&hd.sch_d);
		return 0;
	}

	if (check == SCHEMA_NW)
	{ /*if the schema is new we update the header*/
		// check the header size
		// printf("header size is: %ld",compute_size_header(hd));
		if (compute_size_header((void *)&hd) >= MAX_HD_SIZE)
		{
			printf("File definition is bigger than the limit.\n");
			free(buffer), free(buf_t), free(buf_v);
			free_schema(&hd.sch_d);
			free_record(rec, rec->fields_num);
			return 0;
		}

		if (begin_in_file(fd_data) == -1)
		{ /*set the file pointer at the start*/
			printf("file pointer failed, helper.c l %d.\n", __LINE__ - 1);
			free(buffer), free(buf_t), free(buf_v);
			free_schema(&hd.sch_d);
			free_record(rec, rec->fields_num);
			return 0;
		}

		if (!write_header(fd_data, &hd))
		{
			printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
			free(buffer), free(buf_t), free(buf_v);
			free_schema(&hd.sch_d);
			free_record(rec, rec->fields_num);
			return 0;
		}
	}

	free_schema(&hd.sch_d);
	off_t eof = go_to_EOF(fd_data);
	if (eof == -1)
	{
		printf("file pointer failed, helper.c l %d.\n", __LINE__ - 2);
		free(buffer), free(buf_t), free(buf_v);
		free_record(rec, rec->fields_num);
		return 0;
	}

	int key_type = is_num(key);
	if (!set((void *)key, key_type, eof, ht))
	{
		free_record(rec, rec->fields_num);
		free(buffer), free(buf_t), free(buf_v);
		return ALREADY_KEY;
	}

	if (!write_file(fd_data, rec, 0, 0))
	{
		printf("write to file failed, helper.c l %d.\n", __LINE__ - 1);
		free(buffer), free(buf_t), free(buf_v);
		free_record(rec, rec->fields_num);
		return 0;
	}

	free(buffer), free(buf_t), free(buf_v);
	// fd_index = open_file(files[0],1); //opening with O_TRUNC
	free_record(rec, rec->fields_num);
	return 1;
}

unsigned char create_file_with_schema(int fd_data, int fd_index, char *schema_def, int bucket_ht, int indexes)
{
	int fields_count = count_fields(schema_def, TYPE_) + count_fields(schema_def, T_);

	if (fields_count > MAX_FIELD_NR)
	{
		printf("Too many fields, max %d fields each file definition.\n", MAX_FIELD_NR);
		return 0;
	}

	char *buf_sdf = strdup(schema_def);
	char *buf_t = strdup(schema_def);

	struct Schema sch = {fields_count, NULL, NULL};
	if (!create_file_definition_with_no_value(fields_count, buf_sdf, buf_t, &sch))
	{
		free(buf_sdf), free(buf_t);
		printf("can't create file definition %s:%d.\n", __FILE__, __LINE__ - 1);
		return 0;
	}

	struct Header_d hd = {0, 0, sch};

	if (!create_header(&hd))
	{
		free(buf_sdf), free(buf_t);
		free_schema(&sch);
		return 0;
	}

	// print_size_header(hd);
	size_t hd_st = compute_size_header((void *)&hd);
	if (hd_st >= MAX_HD_SIZE)
	{
		printf("File definition is bigger than the limit.\n");
		free(buf_sdf), free(buf_t);
		free_schema(&sch);
		return 0;
	}

	if (!write_header(fd_data, &hd))
	{
		printf("write to file failed, %s:%d.\n", __FILE__, __LINE__ - 1);
		free(buf_sdf), free(buf_t);
		free_schema(&sch);
		return 0;
	}

	free_schema(&sch);

	if (!padding_file(fd_data, MAX_HD_SIZE, hd_st))
	{
		printf("padding failed. %s:%d.\n", __FILE__, __LINE__ - 1);
		free(buf_sdf), free(buf_t);
		return 0;
	}

	/*  write the index file */
	int bucket = bucket_ht > 0 ? bucket_ht : 7;
	int index_num = indexes > 0 ? indexes : 5;

	if (!write_index_file_head(fd_index, index_num))
	{
		printf("write to file failed, %s:%d", F, L - 2);
		free(buf_sdf), free(buf_t);
		return 0;
	}

	int i = 0;
	for (i = 0; i < index_num; i++)
	{
		Node **dataMap = calloc(bucket, sizeof(Node *));
		if (!dataMap)
		{
			printf("calloc failed. %s:%d.\n", F, L - 3);
			free(buf_sdf), free(buf_t);
			return 0;
		}

		HashTable ht = {bucket, dataMap, write_ht};

		if (write_index_body(fd_index, i, &ht) == -1)
		{
			printf("write to file failed. %s:%d.\n", F, L - 2);
			free(buf_sdf), free(buf_t);
			destroy_hasht(&ht);
			return 0;
		}

		destroy_hasht(&ht);
	}

	free(buf_sdf), free(buf_t);
	return 1;
}
