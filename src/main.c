#include <stdio.h>
#include <getopt.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include "common.h"
#include "file.h"
#include "record.h"
#include "input.h"
#include "hash_tbl.h"
#include "str_op.h"
#include "lock.h"
#include "parse.h"
#include "debug.h"

int main(int argc, char *argv[])
{
	if (argc < 2)
	{
		print_usage(argv);
		return 1;
	}
	/* file descriptors */
	int fd_index = -1, fd_data = -1;

	/*----------- bool values-------------------*/

	unsigned char new_file = 0;
	unsigned char del = 0;
	unsigned char update = 0;
	unsigned char list_def = 0;
	unsigned char del_file = 0;

	/*------------------------------------------*/
	int c = 0;
	char *file_path = NULL;
	char *record_id = NULL;
	char *data_to_add = NULL;
	char *fileds_and_type = NULL;
	char *key = NULL;
	char *schema_def = NULL;

	while ((c = getopt(argc, argv, "ntf:r:d:a:k:D:R:ule")) != -1)
	{
		switch (c)
		{
		case 'a':
			data_to_add = optarg;
			break;
		case 'n':
			new_file = 1; // true
			break;
		case 'f':
			file_path = optarg;
			break;
		case 'r':
			record_id = optarg;
			break;
		case 'd':
			fileds_and_type = optarg;
			break;
		case 'k':
			key = optarg;
			break;
		case 'D':
			del = 1, record_id = optarg; // 1 is marking del as true
			break;
		case 't':
			print_types();
			return 0;
		case 'R':
			schema_def = optarg;
			break;
		case 'u':
			update = 1;
			break;
		case 'l':
			list_def = 1;
			break;
		case 'e':
			del_file = 1;
			break;
		default:
			printf("Unknow option -%c\n", c);
			return 1;
		}
	}

	if (!check_input_and_values(file_path, data_to_add, fileds_and_type, key,
								argv, del, list_def, new_file, update, del_file))
	{
		return 1;
	}

	if (new_file)
	{
		/*creates two name from the file_path  "str_op.h" */
		char **files = two_file_path(file_path);

		fd_index = create_file(files[0]);
		fd_data = create_file(files[1]);

		/* file_error_handler will close the file descriptors if there are issues */
		/* -------------- and print error messages to the console ----------------*/
		if (file_error_handler(2, fd_index, fd_data) != 0)
		{
			printf("main.c l %d.\n", __LINE__ - 6);
			free(files[0]), free(files[1]), free(files);
			return 1;
		}

		if (schema_def)
		{ /* case when user creates a file with only file definition*/
			int fields_count = count_fields(schema_def);

			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d fields each file definition.\n", MAX_FIELD_NR);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			char *buf_sdf = strdup(schema_def);
			char *buf_t = strdup(schema_def);

			Schema sch = {fields_count, NULL, NULL};
			if (!create_file_definition_with_no_value(fields_count, buf_sdf, buf_t, &sch))
			{
				printf("can't create file definition main.c l %d.\n", __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			Header_d hd = {0, 0, sch};

			if (!create_header(&hd))
			{
				free(buf_sdf), free(buf_t);
				clean_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			// print_size_header(hd);
			size_t hd_st = compute_size_header(hd);
			if (hd_st >= MAX_HD_SIZE)
			{
				printf("File definition is bigger than the limit.\n");
				free(buf_sdf), free(buf_t);
				clean_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!write_header(fd_data, &hd))
			{
				printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
				free(buf_sdf), free(buf_t);
				clean_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			clean_schema(&sch);

			if (!padding_file(fd_data, MAX_HD_SIZE, hd_st))
			{
				printf("padding failed. main.c l %d.\n", __LINE__ - 1);
				free(buf_sdf), free(buf_t);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			Node **dataMap = calloc(7, sizeof(Node *));
			HashTable ht = {7, dataMap, write_ht};

			if (!ht.write(fd_index, &ht))
			{
				printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
				free(buf_sdf), free(buf_t);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			printf("File created successfully!\n");

			free(buf_sdf), free(buf_t);
			free(files[0]), free(files[1]), free(files);
			close_file(2, fd_index, fd_data);
			destroy_hasht(&ht);
			return 0;
		}

		if (fileds_and_type)
		{ /* creates a file with full definitons (fields and value)*/

			int fields_count = count_fields(fileds_and_type);

			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
			}

			char *buffer = strdup(fileds_and_type);
			char *buf_t = strdup(fileds_and_type);
			char *buf_v = strdup(fileds_and_type);

			Schema sch = {0, NULL, NULL};
			Record_f *rec = parse_d_flag_input(file_path, fields_count, buffer, buf_t, buf_v, &sch, 0);

			if (!rec)
			{
				printf("error creating the record, main.c l %d.\n", __LINE__ - 1);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			Header_d hd = {0, 0, sch};
			//	print_schema(sch);
			if (!create_header(&hd))
			{
				printf("main.c l %d.\n", __LINE__ - 1);
				free(buffer), free(buf_t), free(buf_v);
				clean_up(rec, fields_count), clean_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}
			// print_size_header(hd);

			size_t hd_st = compute_size_header(hd);
			if (hd_st >= MAX_HD_SIZE)
			{
				printf("File definition is bigger than the limit.\n");
				free(buffer), free(buf_t), free(buf_v);
				clean_up(rec, fields_count), clean_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!write_header(fd_data, &hd))
			{
				printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				clean_up(rec, fields_count), clean_schema(&sch);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!padding_file(fd_data, MAX_HD_SIZE, hd_st))
			{
				printf("padding failed. main.c l %d.\n", __LINE__ - 1);
				free(buffer), free(buf_t), free(buf_v);
				clean_up(rec, fields_count), clean_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			clean_schema(&sch);

			Node **dataMap = calloc(7, sizeof(Node *));
			HashTable ht = {7, dataMap, write_ht};

			off_t offset = get_file_offset(fd_data);

			set(key, offset, &ht); /*create a new key value pair in the hash table*/

			// print_hash_table(ht);

			if (!write_file(fd_data, rec, 0, update))
			{
				printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
				clean_up(rec, fields_count);
				free(buffer), free(buf_t), free(buf_v);
				destroy_hasht(&ht);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!ht.write(fd_index, &ht))
			{
				printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
				clean_up(rec, fields_count);
				free(buffer), free(buf_t), free(buf_v);
				destroy_hasht(&ht);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			printf("File created successfully.\n");
			clean_up(rec, fields_count); // this free the memory allocated for the record
			free(buffer), free(buf_t), free(buf_v);
			destroy_hasht(&ht);
			free(files[0]), free(files[1]), free(files);
			close_file(2, fd_index, fd_data);
			return 0;
		}
		else
		{
			printf("no data to write to file %s.\n", file_path);
			printf("%s has been created, you can add to the file using option -a.\n", file_path);
			print_usage(argv);

			Schema sch = {0, NULL, NULL};
			Header_d hd = {HEADER_ID_SYS, VS, sch};

			size_t hd_st = compute_size_header(hd);
			if (hd_st >= MAX_HD_SIZE)
			{
				printf("File definition is bigger than the limit.\n");
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!write_empty_header(fd_data, &hd))
			{
				printf("main.c l %d.\n", __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!padding_file(fd_data, MAX_HD_SIZE, hd_st))
			{
				printf("padding failed. main.c l %d.\n", __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			Node **dataMap = calloc(7, sizeof(Node *));
			HashTable ht = {7, dataMap, write_ht};

			if (!ht.write(fd_index, &ht))
			{
				printf("write to index file failed, main.c l %d.\n", __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			printf("File created successfully.\n");

			destroy_hasht(&ht);
			free(files[0]), free(files[1]), free(files);
			close_file(2, fd_index, fd_data);
			return 0;
		}
	}
	else
	{ /*file already exist. we can perform CRUD operation*/

		/*creates two name from the file_path => from "str_op.h" */
		char **files = two_file_path(file_path);

		fd_index = open_file(files[0], 0);
		fd_data = open_file(files[1], 0);

		/* file_error_handler will close the file descriptors if there are issues */
		if (file_error_handler(2, fd_index, fd_data) != 0)
		{
			printf("Error in creating or opening files, main.c l %d.\n", __LINE__ - 5);
			free(files[0]), free(files[1]), free(files);
			return 1;
		}

		if (del_file)
		{ /*delete file */
			delete_file(2, files[0], files[1]);
			printf("file %s, deleted.\n", file_path);
			free(files[0]), free(files[1]), free(files);
			close_file(2, fd_index, fd_data);
			return 0;
		}

		if (del)
		{ /* delete the record specified by the -D option, in the index file*/
			HashTable ht = {0, NULL, write_ht};
			read_index_file(fd_index, &ht);

			Node *record_del = delete (record_id, &ht);
			if (!record_del)
			{
				printf("record %s not found.\n", record_id);
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
				destroy_hasht(&ht);
				return 1;
			}

			printf("record %s deleted!.\n", record_id);
			print_hash_table(ht);
			close_file(1, fd_index);
			fd_index = open_file(files[0], 1); // opening with O_TRUNC

			if (!ht.write(fd_index, &ht))
			{
				printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
				destroy_hasht(&ht);
				return 1;
			}

			free(files[0]), free(files[1]), free(files);
			close_file(2, fd_index, fd_data);
			destroy_hasht(&ht);
			return 0;
		}

		if (!update && data_to_add)
		{ /* append data to the specified file*/
			int fields_count = count_fields(data_to_add);

			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			char *buffer = strdup(data_to_add);
			char *buf_t = strdup(data_to_add);
			char *buf_v = strdup(data_to_add);

			Record_f *rec = NULL;
			Schema sch = {0, NULL, NULL};
			Header_d hd = {0, 0, sch};

			unsigned char check = perform_checks_on_schema(buffer, buf_t, buf_v, fields_count,
														   fd_data, file_path, &rec, &hd);

			if (check == SCHEMA_ERR || check == 0)
			{
				clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
				free(buffer), free(buf_t), free(buf_v);
				clean_up(rec, fields_count);
				return 1;
			}

			if (!rec)
			{
				printf("error creating record, main.c l %d\n", __LINE__ - 1);
				free(buffer), free(buf_t), free(buf_v);
				clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
				return 1;
			}

			if (check == SCHEMA_NW)
			{ /*if the schema is new we update the header*/
				// check the header size
				// printf("header size is: %ld",compute_size_header(hd));
				if (compute_size_header(hd) >= MAX_HD_SIZE)
				{
					printf("File definition is bigger than the limit.\n");
					free(buffer), free(buf_t), free(buf_v);
					clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
					clean_up(rec, fields_count);
					return 1;
				}

				if (begin_in_file(fd_data) == -1)
				{ /*set the file pointer at the start*/
					printf("file pointerfailed,  main.c l %d.\n", __LINE__ - 1);
					free(buffer), free(buf_t), free(buf_v);
					clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
					clean_up(rec, fields_count);
					return 1;
				}

				if (!write_header(fd_data, &hd))
				{
					printf("write to fiel failed, main.c l %d.\n", __LINE__ - 1);
					free(buffer), free(buf_t), free(buf_v);
					clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
					clean_up(rec, fields_count);
					return 1;
				}
			}

			clean_schema(&hd.sch_d);
			off_t eof = go_to_EOF(fd_data);
			if (eof == -1)
			{
				printf("file pointer failed, main.c l %d.\n", __LINE__ - 2);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
			}

			HashTable ht = {0, NULL, write_ht};
			read_index_file(fd_index, &ht);

			if (!set(key, eof, &ht))
			{
				close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
				free(buffer), free(buf_t), free(buf_v);
				destroy_hasht(&ht);
				return 1;
			}

			if (!write_file(fd_data, rec, 0, update))
			{
				printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
				destroy_hasht(&ht);
				return 1;
			}

			free(buffer), free(buf_t), free(buf_v);
			close_file(1, fd_index);

			fd_index = open_file(files[0], 1); // opening with O_TRUNC

			free(files[0]), free(files[1]), free(files);
			if (!ht.write(fd_index, &ht))
			{
				printf("could not write index file. main.c l %d.\n", __LINE__ - 1);
				clean_up(rec, fields_count);
				close_file(2, fd_index, fd_data);
				destroy_hasht(&ht);
				return 1;
			}

			clean_up(rec, fields_count);
			close_file(2, fd_index, fd_data);
			destroy_hasht(&ht);
			return 0;
		}

		free(files[0]), free(files[1]), free(files);
		if (update && data_to_add && key)
		{ /* updating an existing record */
			// 1 - check the schema with the one on file
			int fields_count = count_fields(data_to_add);

			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			char *buffer = strdup(data_to_add);
			char *buf_t = strdup(data_to_add);
			char *buf_v = strdup(data_to_add);

			Record_f *rec = NULL;
			Schema sch = {0, NULL, NULL};
			Header_d hd = {0, 0, sch};
			// printf("before perform check in update paths");
			unsigned char check = perform_checks_on_schema(buffer, buf_t, buf_v, fields_count,
														   fd_data, file_path, &rec, &hd);
			if (check == SCHEMA_ERR || check == 0)
			{
				clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
				free(buffer), free(buf_t), free(buf_v);
				return 1;
			}

			/* updating th schema if it is new */

			if (check == SCHEMA_NW)
			{
				size_t hd_st = compute_size_header(hd);
				if (hd_st > MAX_HD_SIZE)
				{
					printf("header is bigger than the limit, main.c l %d\n", __LINE__ - 2);
					clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
					free(buffer), free(buf_t), free(buf_v);
					clean_up(rec, rec->fields_num);
					return 1;
				}

				if (begin_in_file(fd_data) == -1)
				{
					printf("file pointer failed, main.c l %d.\n", __LINE__ - 1);
					clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
					free(buffer), free(buf_t), free(buf_v);
					clean_up(rec, rec->fields_num);
					return 1;
				}

				if (!write_header(fd_data, &hd))
				{
					printf("can`t write header, main.c l %d.\n", __LINE__ - 1);
					clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
					free(buffer), free(buf_t), free(buf_v);
					clean_up(rec, rec->fields_num);
					return 1;
				}

				/* setting the file position back to the top*/
				if (begin_in_file(fd_data) == -1)
				{
					printf("file pointer failed, main.c l %d.\n", __LINE__ - 1);
					free(buffer), free(buf_t), free(buf_v);
					clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					return 1;
				}
			}
			/* from here you have to change all error printf statements*/
			free(buf_t), free(buf_v);
			HashTable ht = {0, NULL};

			// 2 - if schema is good load the old record into memory and update the fields if any
			// -- at this point you already checked the header, and you have an updated header,
			//	an updated record in memory

			if (!read_index_file(fd_index, &ht))
			{
				printf("index file reading failed. main.c l %d.\n", __LINE__ - 1);
				free(buffer), close_file(2, fd_index, fd_data);
				clean_up(rec, rec->fields_num);
				clean_schema(&hd.sch_d);
				return 1;
			}

			off_t offset = get(key, &ht); /*look for the key in the ht */

			if (offset == -1)
			{
				printf("record not found.\n");
				free(buffer), close_file(2, fd_index, fd_data);
				clean_up(rec, rec->fields_num);
				clean_schema(&hd.sch_d);
				destroy_hasht(&ht);
				return 1;
			}

			destroy_hasht(&ht);
			if (find_record_position(fd_data, offset) == -1)
			{
				printf("file pointer failed, main.c l %d.\n", __LINE__ - 1);
				free(buffer), close_file(2, fd_index, fd_data);
				clean_schema(&hd.sch_d);
				clean_up(rec, rec->fields_num);
				return 1;
			}

			Record_f *rec_old = read_file(fd_data, file_path);
			if (!rec_old)
			{
				printf("reading record failed main.c l %d.\n", __LINE__ - 2);
				free(buffer), close_file(2, fd_index, fd_data);
				clean_schema(&hd.sch_d);
				clean_up(rec, rec->fields_num);
				return 1;
			}

			off_t update_p_position = get_file_offset(fd_data);
			off_t updated_rec_pos = get_update_offset(fd_data);
			if (updated_rec_pos == -1)
			{
				printf("file pointer failed, main.c l %d.\n", __LINE__ - 2);
				free(buffer), close_file(2, fd_index, fd_data);
				clean_up(rec, rec->fields_num);
				clean_up(rec_old, rec_old->fields_num);
				clean_schema(&hd.sch_d);
				return 1;
			}

			Record_f **recs_old = NULL;
			off_t *pos_u = NULL;
			if (updated_rec_pos > 0)
			{
				int index = 2;
				int pos_i = 2;
				recs_old = calloc(index, sizeof(Record_f *));
				free(buffer);
				if (!recs_old)
				{
					printf("calloc failed main.c l %d.\n", __LINE__ - 3);
					close_file(2, fd_index, fd_data);
					clean_schema(&hd.sch_d);
					clean_up(rec_old, rec_old->fields_num);
					clean_up(rec, rec->fields_num);
					return 1;
				}

				pos_u = calloc(pos_i, sizeof(off_t));
				if (!pos_u)
				{
					printf("calloc failed, main.c l %d.\n", __LINE__ - 2);
					close_file(2, fd_index, fd_data);
					clean_schema(&hd.sch_d);
					clean_up(rec_old, rec_old->fields_num);
					clean_up(rec, rec->fields_num);
					if (recs_old)
					{
						int i = 0;
						for (i = 0; i < index; i++)
						{
							if (recs_old[i])
								clean_up(recs_old[i], recs_old[i]->fields_num);
						}
						free(recs_old);
					}
					return 1;
				}
				pos_u[0] = offset;			/*first record position*/
				pos_u[1] = updated_rec_pos; /*first updated record position*/

				if (find_record_position(fd_data, updated_rec_pos) == -1)
				{
					printf("can`t find offset, main.c l %d.\n", __LINE__ - 1);
					close_file(2, fd_index, fd_data);
					free(pos_u);
					clean_schema(&hd.sch_d);
					clean_up(rec, rec->fields_num);
					if (recs_old)
					{
						int i = 0;
						for (i = 0; i < index; i++)
						{
							if (recs_old[i])
								clean_up(recs_old[i], recs_old[i]->fields_num);
						}
						free(recs_old);
					}
					return 1;
				}

				Record_f *rec_old_s = read_file(fd_data, file_path);
				if (!rec_old_s)
				{
					printf("error reading file, main.c l %d.\n", __LINE__ - 2);
					close_file(2, fd_index, fd_data);
					clean_schema(&hd.sch_d);
					clean_up(rec, rec->fields_num);
					free(pos_u);
					if (recs_old)
					{
						int i = 0;
						for (i = 0; i < index; i++)
						{
							clean_up(recs_old[i], recs_old[i]->fields_num);
						}
						free(recs_old);
					}
					return 1;
				}

				recs_old[0] = rec_old;
				recs_old[1] = rec_old_s;

				while ((updated_rec_pos = get_update_offset(fd_data)) > 0)
				{
					index++, pos_i++;
					Record_f **recs_old_n = realloc(recs_old, index * sizeof(Record_f *));

					if (!recs_old_n)
					{
						printf("realloc failed, main.c l %d.\n", __LINE__ - 2);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						clean_schema(&hd.sch_d);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
						return 1;
					}
					recs_old = recs_old_n;

					off_t *pos_u_n = realloc(pos_u, pos_i * sizeof(off_t));
					if (!pos_u_n)
					{
						printf("realloc failed for positions, main.c l %d.\n", __LINE__ - 2);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						clean_schema(&hd.sch_d);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
						return 1;
					}
					pos_u = pos_u_n;
					pos_u[pos_i - 1] = updated_rec_pos;

					Record_f *rec_old_new = read_file(fd_data, file_path);
					if (!rec_old_new)
					{
						printf("error reading file, main.c l %d.\n", __LINE__ - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						clean_schema(&hd.sch_d);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
						return 1;
					}

					recs_old[index - 1] = rec_old_new;
				}

				/* check which record we have to update*/

				char *positions = calloc(index, sizeof(char));

				if (!positions)
				{
					printf("calloc failed, main.c l %d.\n", __LINE__ - 1);
					close_file(2, fd_index, fd_data);
					free(pos_u);
					clean_up(rec, rec->fields_num);
					if (recs_old)
					{
						int i = 0;
						for (i = 0; i < index; i++)
						{
							clean_up(recs_old[i], recs_old[i]->fields_num);
						}
						free(recs_old);
					}
					return 1;
				}
				/* this function check all records from the file
				   against the new record setting the values that we have to update
				   and populate the char array positions, if an element contain 'y'
				   you have to update the record at that index position. */

				find_fields_to_update(recs_old, positions, rec, index);

				if (positions[0] != 'n' && positions[0] != 'y')
				{
					printf("check on fields failed,  main.c l %d.\n", __LINE__ - 1);
					close_file(2, fd_index, fd_data);
					free(pos_u), free(positions);
					clean_schema(&hd.sch_d);
					clean_up(rec, rec->fields_num);
					if (recs_old)
					{
						int i = 0;
						for (i = 0; i < index; i++)
						{
							clean_up(recs_old[i], recs_old[i]->fields_num);
						}
						free(recs_old);
					}
					return 1;
				}

				/* write the update records to file */
				int i = 0;
				unsigned short updates = 0; /* bool value if 0 no updates*/
				for (i = 0; i < index; i++)
				{
					if (positions[i] == 'n')
						continue;

					++updates;
					if (find_record_position(fd_data, pos_u[i]) == -1)
					{
						printf("error file pointer, main.c l %d.\n", __LINE__ - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						clean_up(rec, rec->fields_num);
						clean_schema(&hd.sch_d);
						free(positions);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
						return 1;
					}

					off_t right_update_pos = 0;
					if ((index - i) > 1)
						right_update_pos = pos_u[i + 1];

					if (index - i == 1 && check == SCHEMA_NW)
					{
						right_update_pos = go_to_EOF(fd_data);
						if (find_record_position(fd_data, pos_u[i]) == -1 ||
							right_update_pos == -1)
						{
							printf("error file pointer, main.c l %d.\n", __LINE__ - 2);
							close_file(2, fd_index, fd_data);
							free(pos_u);
							clean_up(rec, rec->fields_num);
							clean_schema(&hd.sch_d);
							free(positions);
							if (recs_old)
							{
								int i = 0;
								for (i = 0; i < index; i++)
								{
									clean_up(recs_old[i], recs_old[i]->fields_num);
								}
								free(recs_old);
							}
							return 1;
						}
					}

					if (!write_file(fd_data, recs_old[i], right_update_pos, update))
					{
						printf("error write file, main.c l %d.\n", __LINE__ - 1);
						close_file(2, fd_index, fd_data);
						clean_schema(&hd.sch_d);
						free(pos_u);
						free(positions);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
						return 1;
					}
				}

				if (check == SCHEMA_NW && updates)
				{
					Record_f *new_rec = NULL;
					if (!create_new_fields_from_schema(recs_old, rec, &hd.sch_d,
													   index, &new_rec, file_path))
					{
						printf("create new fileds failed,  main.c l %d.\n", __LINE__ - 1);
						close_file(2, fd_index, fd_data);
						clean_schema(&hd.sch_d);
						free(pos_u);
						free(positions);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
						return 1;
					}

					off_t eof = go_to_EOF(fd_data); /* file pointer to the end*/
					if (eof == -1)
					{
						printf("file pointer failed, main.c l %d.\n", __LINE__ - 2);
						close_file(2, fd_index, fd_data);
						clean_schema(&hd.sch_d);
						free(pos_u);
						free(positions);
						clean_up(new_rec, new_rec->fields_num);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
						return 1;
					}
					/*writing the new part of the schema to the file*/
					if (!write_file(fd_data, new_rec, 0, 0))
					{
						printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
						close_file(2, fd_index, fd_data);
						clean_schema(&hd.sch_d);
						clean_up(new_rec, new_rec->fields_num);
						free(pos_u);
						free(positions);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
						return 1;
					}

					clean_up(new_rec, new_rec->fields_num);
					/*the position of this new record in the old part of the record
					 was already set at line 846*/
				}

				if (check == SCHEMA_NW && !update)
				{
					/* store the EOF value*/
					off_t eof = 0;
					if ((eof = go_to_EOF(fd_data)) == -1)
					{
						printf("file pointer failed, main.c l %d", __LINE__ - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						free(positions);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
					}

					/*here you have to find the position of the last piece of the record*/
					if (find_record_position(fd_data, pos_u[index - 1]) == -1)
					{
						printf("file pointer failed, main.c l %d.\n", __LINE__ - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						free(positions);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
					}
					/*read the record to move the pointer down */
					Record_f *temp = read_file(fd_data, file_path);
					if (!temp)
					{
						printf("read file failed, main.c l %d.\n", __LINE__ - 2);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						free(positions);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
					}
					/*update the position for the next piece of record*/
					if (write(fd_data, &eof, sizeof(eof)) == -1)
					{
						printf("write file failed, main.c l %d.\n", __LINE__ - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						free(positions);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
					}

					/*move to EOF*/
					if ((eof = go_to_EOF(fd_data)) == -1)
					{
						printf("file pointer failed, main.c l %d", __LINE__ - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						free(positions);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
					}
					/*create the new record*/
					Record_f *new_rec = NULL;
					if (!create_new_fields_from_schema(recs_old, rec, &hd.sch_d,
													   index, &new_rec, file_path))
					{
						printf("create new fields failed,  main.c l %d.\n", __LINE__ - 2);
						close_file(2, fd_index, fd_data);
						clean_schema(&hd.sch_d);
						free(pos_u);
						free(positions);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
						return 1;
					}

					/*write the actual new data*/
					if (!write_file(fd_data, new_rec, 0, 0))
					{
						printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
						close_file(2, fd_index, fd_data);
						clean_schema(&hd.sch_d);
						clean_up(new_rec, new_rec->fields_num);
						free(pos_u);
						free(positions);
						clean_up(rec, rec->fields_num);
						if (recs_old)
						{
							int i = 0;
							for (i = 0; i < index; i++)
							{
								clean_up(recs_old[i], recs_old[i]->fields_num);
							}
							free(recs_old);
						}
						return 1;
					}

					clean_up(new_rec, new_rec->fields_num);
				}

				clean_schema(&hd.sch_d);
				printf("record %s updated!\n", key);
				close_file(2, fd_index, fd_data);
				free(pos_u);
				free(positions);
				clean_up(rec, rec->fields_num);
				if (recs_old)
				{
					int i = 0;
					for (i = 0; i < index; i++)
					{
						clean_up(recs_old[i], recs_old[i]->fields_num);
					}
					free(recs_old);
				}
				return 0;
			}

			clean_schema(&hd.sch_d);
			/*updated_rec_pos is 0, THE RECORD IS ALL IN ONE PLACE */
			Record_f *new_rec = NULL;
			unsigned char comp_rr = compare_old_rec_update_rec(&rec_old, rec, &new_rec, file_path, check, buffer, fields_count);
			free(buffer);
			if (comp_rr == 0)
			{
				printf(" compare records failed,  main.c l %d.\n", __LINE__ - 4);
				close_file(2, fd_index, fd_data);
				clean_up(rec, rec->fields_num);
				clean_up(rec_old, rec_old->fields_num);
				if (new_rec)
				{
					clean_up(new_rec, new_rec->fields_num);
				}
				return 1;
			}

			if (updated_rec_pos == 0 && comp_rr == UPDATE_OLD)
			{
				// set the position back to the record
				if (find_record_position(fd_data, offset) == -1)
				{
					printf("error lseek record, main.c l %d.\n", __LINE__ - 1);
					close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					clean_up(rec_old, rec_old->fields_num);
					return 1;
				}

				// write the updated record to the file
				if (!write_file(fd_data, rec_old, 0, update))
				{
					printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
					close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					clean_up(rec_old, rec_old->fields_num);
					return 1;
				}

				printf("record %s updated!\n", key);
				close_file(2, fd_index, fd_data);
				clean_up(rec, rec->fields_num);
				clean_up(rec_old, rec_old->fields_num);
				return 0;
			}

			/*updating the record but we need to write some data in another place in the file*/
			if (updated_rec_pos == 0 && comp_rr == UPDATE_OLDN)
			{
				// get EOF position (update record position)
				off_t eof = go_to_EOF(fd_data);
				if (eof == -1)
				{
					printf("error file pointer, main.c l %d.\n", __LINE__ - 2);
					close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					clean_up(rec_old, rec_old->fields_num);
					clean_up(new_rec, new_rec->fields_num);
					return 1;
				}

				// the position back to the record
				if (find_record_position(fd_data, offset) == -1)
				{
					printf("file pointer failed, main.c l %d.\n", __LINE__ - 1);
					close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					clean_up(rec_old, rec_old->fields_num);
					clean_up(new_rec, new_rec->fields_num);
					return 1;
				}

				// update the old record :
				if (!write_file(fd_data, rec_old, eof, update))
				{
					printf("can't write record, main.c l %d.\n", __LINE__ - 1);
					close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					clean_up(rec_old, rec_old->fields_num);
					clean_up(new_rec, new_rec->fields_num);
					return 1;
				}

				// set position at EOF
				eof = go_to_EOF(fd_data);
				// printf("eof is %ld\n", eof);
				if (eof == -1)
				{
					printf("file pointer failed, main.c l %d.\n", __LINE__ - 1);
					close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					clean_up(rec_old, rec_old->fields_num);
					clean_up(new_rec, new_rec->fields_num);
					return 1;
				}
				/*passing update as 0 becuase is a "new_rec", (right most paramaters) */
				if (!write_file(fd_data, new_rec, 0, 0))
				{
					printf("can't write record, main.c l %d.\n", __LINE__ - 1);
					close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					clean_up(rec_old, rec_old->fields_num);
					clean_up(new_rec, new_rec->fields_num);
					return 1;
				}

				printf("record %s updated!\n", key);
				close_file(2, fd_index, fd_data);
				clean_up(rec, rec->fields_num);
				clean_up(rec_old, rec_old->fields_num);
				clean_up(new_rec, new_rec->fields_num);
				return 0;
			}

			return 0;
		}

		/*	 reading the file to show data  	*/
		Schema sch = {0, NULL, NULL};
		Header_d hd = {0, 0, sch};

		begin_in_file(fd_data); /* ensure starts at the 1st position  in the file*/
		if (!read_header(fd_data, &hd))
		{
			printf("failed to read header, main.c l %d.\n", __LINE__ - 1);
			close_file(2, fd_index, fd_data);
			clean_schema(&sch); // it might not be neccessery to free schema, but the function is safe
			return 1;
		}
		if (list_def)
		{ /* show file definitions */
			print_schema(hd.sch_d);
			close_file(2, fd_index, fd_data);
			clean_schema(&hd.sch_d);
			return 0;
		}

		clean_schema(&hd.sch_d);

		if (record_id)
		{
			HashTable ht = {0, NULL};

			if (!read_index_file(fd_index, &ht))
			{
				printf("reading index file failed, main.c l %d.\n", __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			off_t offset = get(record_id, &ht); /*look for the key in the ht */

			if (offset == -1)
			{
				printf("record not found.\n");
				destroy_hasht(&ht);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			destroy_hasht(&ht);
			if (find_record_position(fd_data, offset) == -1)
			{
				printf("file pointer failed, main.c l %d.\n", __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			Record_f *rec = read_file(fd_data, file_path);

			if (!rec)
			{
				printf("read record failed, main.c l %d.\n", __LINE__ - 1);
				close_file(2, fd_data, fd_index);
				return 1;
			}
			off_t offt_rec_up_pos = get_file_offset(fd_data);
			off_t update_rec_pos = get_update_offset(fd_data);
			if (update_rec_pos == -1)
			{
				printf("file pointer failed, main.c l %d.\n", __LINE__ - 1);
				clean_up(rec, rec->fields_num);
				close_file(2, fd_data, fd_index);
				return 1;
			}

			int counter = 1;
			Record_f **recs = NULL;

			if (update_rec_pos > 0)
			{
				off_t cp_urc = update_rec_pos;
				recs = calloc(counter, sizeof(Record_f *));
				if (!recs)
				{
					printf("calloc failed, main.c l %d.\n", __LINE__ - 2);
					clean_up(rec, rec->fields_num);
					close_file(2, fd_data, fd_index);
					return 1;
				}

				recs[0] = rec;

				// set the file pointer back to update_rec_pos (we need to read it)
				//  again for the reading process to be successful
				if (find_record_position(fd_data, offt_rec_up_pos) == -1)
				{
					printf("file pointer failed, main.c l %d.\n", __LINE__ - 1);
					int i = 0;
					for (i = 0; i < counter; i++)
					{
						if (recs[i])
							clean_up(recs[i], recs[i]->fields_num);
					}
					free(recs);
					close_file(2, fd_data, fd_index);
					return 1;
				}

				while ((update_rec_pos = get_update_offset(fd_data)) > 0)
				{
					counter++;

					recs = realloc(recs, counter * sizeof(Record_f *));
					if (!recs)
					{
						printf("realloc failed, main.c l %d.\n", __LINE__ - 2);
						int i = 0;
						for (i = 0; i < counter; i++)
						{
							if (recs[i])
								clean_up(recs[i], recs[i]->fields_num);
						}
						free(recs);
						close_file(2, fd_data, fd_index);
						return 1;
					}

					if (find_record_position(fd_data, update_rec_pos) == -1)
					{
						printf("file pointer failed, main.c l %d.\n", __LINE__ - 1);
						int i = 0;
						for (i = 0; i < counter; i++)
						{
							if (recs[i])
								clean_up(recs[i], recs[i]->fields_num);
						}
						free(recs);
						close_file(2, fd_index, fd_data);
						return 1;
					}

					Record_f *rec_n = read_file(fd_data, file_path);
					if (!rec_n)
					{
						printf("read record failed, main.c l %d.\n", __LINE__ - 1);
						int i = 0;
						for (i = 0; i < counter; i++)
						{
							if (recs[i])
								clean_up(recs[i], recs[i]->fields_num);
						}
						free(recs);
						close_file(2, fd_data, fd_index);
						return 1;
					}

					recs[counter - 1] = rec_n;
				}
			}
			if (counter == 1)
			{
				print_record(1, &rec);
				clean_up(rec, rec->fields_num);
			}
			else
			{
				print_record(counter, recs);
				int i = 0;
				for (i = 0; i < counter; i++)
				{
					if (recs[i])
						clean_up(recs[i], recs[i]->fields_num);
				}
				free(recs);
			}
		}
	}

	close_file(2, fd_index, fd_data);
	return 0;
}
