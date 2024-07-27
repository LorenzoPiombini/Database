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
#include "hash_tbl.h"

unsigned char create_empty_file(int fd_data, int fd_index, int bucket_ht)
{

	Schema sch = {0, NULL, NULL};
	Header_d hd = {HEADER_ID_SYS, VS, sch};

	size_t hd_st = compute_size_header(hd);
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

	Record_f *rec = NULL;
	Schema sch = {0, NULL, NULL};
	Header_d hd = {0, 0, sch};

	begin_in_file(fd_data);
	unsigned char check = perform_checks_on_schema(buffer, buf_t, buf_v, fields_count,
												   fd_data, file_path, &rec, &hd);

	if (check == SCHEMA_ERR || check == 0)
	{
		clean_schema(&hd.sch_d);
		free(buffer), free(buf_t), free(buf_v);
		return 0;
	}

	if (!rec)
	{
		printf("error creating record, helper.c l %d\n", __LINE__ - 1);
		free(buffer), free(buf_t), free(buf_v);
		clean_schema(&hd.sch_d);
		return 0;
	}

	if (check == SCHEMA_NW)
	{ /*if the schema is new we update the header*/
		// check the header size
		// printf("header size is: %ld",compute_size_header(hd));
		if (compute_size_header(hd) >= MAX_HD_SIZE)
		{
			printf("File definition is bigger than the limit.\n");
			free(buffer), free(buf_t), free(buf_v);
			clean_schema(&hd.sch_d);
			clean_up(rec, rec->fields_num);
			return 0;
		}

		if (begin_in_file(fd_data) == -1)
		{ /*set the file pointer at the start*/
			printf("file pointer failed, helper.c l %d.\n", __LINE__ - 1);
			free(buffer), free(buf_t), free(buf_v);
			clean_schema(&hd.sch_d);
			clean_up(rec, rec->fields_num);
			return 0;
		}

		if (!write_header(fd_data, &hd))
		{
			printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
			free(buffer), free(buf_t), free(buf_v);
			clean_schema(&hd.sch_d);
			clean_up(rec, rec->fields_num);
			return 0;
		}
	}

	clean_schema(&hd.sch_d);
	off_t eof = go_to_EOF(fd_data);
	if (eof == -1)
	{
		printf("file pointer failed, helper.c l %d.\n", __LINE__ - 2);
		free(buffer), free(buf_t), free(buf_v);
		clean_up(rec, rec->fields_num);
		return 0;
	}

	if (!set(key, eof, ht))
	{
		clean_up(rec, rec->fields_num);
		free(buffer), free(buf_t), free(buf_v);
		return ALREADY_KEY;
	}

	if (!write_file(fd_data, rec, 0, 0))
	{
		printf("write to file failed, helper.c l %d.\n", __LINE__ - 1);
		free(buffer), free(buf_t), free(buf_v);
		clean_up(rec, rec->fields_num);
		return 0;
	}

	free(buffer), free(buf_t), free(buf_v);
	// fd_index = open_file(files[0],1); //opening with O_TRUNC
	clean_up(rec, rec->fields_num);
	return 1;
}
