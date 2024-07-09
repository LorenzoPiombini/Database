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

	int fd_index = -1, fd_data = -1;
	int new_file = 0; // bool value
	int del = 0;	  // bool value
	int c = 0;
	char *file_path = NULL;
	char *record_id = NULL;
	char *data_to_add = NULL;
	char *fileds_and_type = NULL;
	char *key = NULL;

	while ((c = getopt(argc, argv, "ntf:r:d:a:k:D:")) != -1)
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
			del = 1, record_id = optarg;
			break;
		case 't':
			print_types();
			return 0;
		default:
			printf("Unknow option -%c\n", c);
			return 1;
		}
	}

	if (!check_input_and_values(file_path, data_to_add, fileds_and_type, key, argv, del))
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
		if (file_error_handler(2, fd_index, fd_data) != 0)
		{
			printf("Error in creating or opening the files\n");
			free(files[0]), free(files[1]), free(files);
			return 1;
		}

		if (fileds_and_type)
		{

			int fields_count = count_fields(fileds_and_type);

			char *buffer = strdup(fileds_and_type);
			char *buf_t = strdup(fileds_and_type);
			char *buf_v = strdup(fileds_and_type);

			Schema sch = {0, NULL, NULL};
			Record_f *rec = parse_d_flag_input(file_path, fields_count, buffer, buf_t, buf_v, &sch, 0);

			if (!rec)
			{
				printf("error creating the record");
				free(buffer), free(buf_t), free(buf_v);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			Header_d hd = {0, 0, sch};

			if (!create_header(&hd))
			{
				free(buffer), free(buf_t), free(buf_v);
				clean_up(rec, fields_count), clean_schema(&sch);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			// print_size_header(hd);

			if (!write_header(fd_data, &hd))
			{
				free(buffer), free(buf_t), free(buf_v);
				clean_up(rec, fields_count), clean_schema(&sch);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!padding_file(fd_data, MAX_HD_SIZE))
			{
				printf("padding failed!\n");
				free(buffer), free(buf_t), free(buf_v);
				clean_up(rec, fields_count), clean_schema(&sch);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			clean_schema(&hd.sch_d);

			Node **dataMap = calloc(7, sizeof(Node));
			HashTable ht = {7, dataMap, write_ht};

			off_t offset = get_file_offset(fd_data);

			set(key, offset, &ht); /*create a new key value pair i the hash table*/

			// print_hash_table(ht);

			if (write_file(fd_data, rec))
				printf("File %s written!\n", files[1]);
			else
				printf("error, could not write to the file %s\n", file_path);

			if (!ht.write(fd_index, &ht))
				printf("could not write the file.\n");

			clean_up(rec, fields_count); // this free the memory allocated for the record
			free(buffer), free(buf_t), free(buf_v);
			destroy_hasht(&ht);
			free(files[0]), free(files[1]), free(files);
		}
		else
		{
			printf("no data to write to file %s.\n", file_path);
			printf("%s has been created, you can add to the file using option -a.\n", file_path);
			print_usage(argv);

			Schema sch = {0, NULL, NULL};
			Header_d hd = {HEADER_ID_SYS, VS, sch};

			if (!write_empty_header(fd_data, &hd))
			{
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			if (!padding_file(fd_data, MAX_HD_SIZE))
			{
				printf("padding failed!\n");
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			Node **dataMap = calloc(7, sizeof(Node));
			HashTable ht = {7, dataMap, write_ht};

			if (!ht.write(fd_index, &ht))
				printf("could not write the file.\n");

			destroy_hasht(&ht);
			close_file(2, fd_index, fd_data);
			return 0;
		}
	}
	else
	{ /*file already exist. we can add, display and delete records*/

		/*creates two name from the file_path => from "str_op.h" */
		char **files = two_file_path(file_path);

		fd_index = open_file(files[0], 0);
		fd_data = open_file(files[1], 0);

		/* file_error_handler will close the file descriptors if there are issues */
		if (file_error_handler(2, fd_index, fd_data) != 0)
		{
			printf("Error in creating or opening the files\n");
			free(files[0]), free(files[1]), free(files);
			return 1;
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

			if (ht.write(fd_index, &ht))
				printf("index written!\n");
			else
				printf("could not write index file.\n");

			free(files[0]), free(files[1]), free(files);
			close_file(2, fd_index, fd_data);
			destroy_hasht(&ht);
			return 0;
		}

		if (data_to_add)
		{ /* append data to the specified file*/

			int fields_count = count_fields(data_to_add);

			char *buffer = strdup(data_to_add);
			char *buf_t = strdup(data_to_add);
			char *buf_v = strdup(data_to_add);

			/* TODO */
			// check if the schema on the file is equal to the Schema just passed from the user.
			//  if it is good but with new fields you have to update the header and pass a new Schema
			//  struct to the fuction instead of NULL
			Schema sch = {0, NULL, NULL};
			Header_d hd = {0, 0, sch};

			if (!read_header(fd_data, &hd))
			{
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				return 1;
			}

			Record_f *rec = NULL;

			if (hd.sch_d.fields_num != 0)
			{
				int check = check_schema(fields_count, buffer, buf_t, hd);

				// printf("check schema is %d",check);
				switch (check)
				{
				case SCHEMA_EQ:
					rec = parse_d_flag_input(file_path, fields_count, buffer, buf_t,
											 buf_v, &hd.sch_d, SCHEMA_EQ);
					break;
				case SCHEMA_ERR:
					free(files[0]), free(files[1]), free(files);
					free(buffer), free(buf_t), free(buf_v);
					close_file(2, fd_index, fd_data);
					clean_schema(&hd.sch_d);
					return 1;
				case SCHEMA_NW:
					rec = parse_d_flag_input(file_path, fields_count, buffer, buf_t,
											 buf_v, &hd.sch_d, SCHEMA_NW);

					break;
				case SCHEMA_CT:
					printf("schema is not complete but valid.\n");
					return 1;
				default:
					printf("no processable option for the SCHEMA. main.c l 237");
					free(files[0]), free(files[1]), free(files);
					free(buffer), free(buf_t), free(buf_v);
					close_file(2, fd_index, fd_data);
					return 1;
				}
			}
			else
			{

				rec = parse_d_flag_input(file_path, fields_count, buffer, buf_t, buf_v, &hd.sch_d, 0);
			}

			if (!rec)
			{
				printf("error creating the record");
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
				return 1;
			}

			// check the header size
			printf("header size is: %ld", compute_size_header(hd));
			if (compute_size_header(hd) >= MAX_HD_SIZE)
			{
				printf("File definition is bigger than the limit.\n");
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
				return 1;
			}

			if (begin_in_file(fd_data) == -1)
			{
				printf("failed to set file pointer (main.c l 279).\n");
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
				return 1;
			}

			if (!write_header(fd_data, &hd))
			{
				printf("failed to update the header.(main.c l 287)\n");
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
				return 1;
			}
			clean_schema(&hd.sch_d);
			off_t eof = go_to_EOF(fd_data);

			if (eof == -1)
			{
				printf("could not reach EOF for %s", files[1]);
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
			}

			HashTable ht = {0, NULL, write_ht};
			read_index_file(fd_index, &ht);

			if (!set(key, eof, &ht))
			{
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
				destroy_hasht(&ht);
				return 1;
			}

			if (write_file(fd_data, rec))
			{
				printf("File %s written!", files[1]);
			}
			else
			{
				printf("could not write to %s", files[1]);
			}

			free(buffer), free(buf_t), free(buf_v);

			close_file(1, fd_index);

			fd_index = open_file(files[0], 1); // opening with O_TRUNC

			if (ht.write(fd_index, &ht))
				printf("index written!\n");
			else
				printf("could not write the file.\n");

			clean_up(rec, fields_count);
			close_file(2, fd_index, fd_data);
			free(files[0]), free(files[1]), free(files);
			destroy_hasht(&ht);
			return 0;
		}

		HashTable ht = {0, NULL};

		if (!read_index_file(fd_index, &ht))
		{
			printf("index file reading failed.");
			free(files[0]), free(files[1]), free(files);
			close_file(2, fd_index, fd_data);
			return 1;
		}

		if (record_id)
		{

			off_t offset = get(record_id, &ht); /*look for the key in the ht */
			// printf("the off set is %ld\n",offset);
			if (offset == -1)
			{
				printf("record not found");
				free(files[0]), free(files[1]), free(files);
				destroy_hasht(&ht);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			off_t find_record = find_record_position(fd_data, offset);
			// printf("record pos is %ld\n",find_record);

			if (find_record == -1)
			{
				perror("error looking for record in file");
				free(files[0]), free(files[1]), free(files);
				destroy_hasht(&ht);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			Record_f *rec = read_file(fd_data, file_path);

			if (!rec)
			{
				printf("no memory for record.\n");
				free(files[0]), free(files[1]), free(files);
				destroy_hasht(&ht);
				return 1;
			}

			print_record(rec);

			clean_up(rec, rec->fields_num);
		}

		free(files[0]), free(files[1]), free(files);
		destroy_hasht(&ht);
	}

	close_file(2, fd_index, fd_data);
	return 0;
}
