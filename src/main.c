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
	/*----------- bool values-------------------*/

	unsigned char new_file = 0;
	unsigned char del = 0;
	unsigned char update = 0;
	unsigned char list_def = 0;

	/*------------------------------------------*/
	int c = 0;
	char *file_path = NULL;
	char *record_id = NULL;
	char *data_to_add = NULL;
	char *fileds_and_type = NULL;
	char *key = NULL;
	char *schema_def = NULL;

	while ((c = getopt(argc, argv, "ntf:r:d:a:k:D:R:ul")) != -1)
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
		default:
			printf("Unknow option -%c\n", c);
			return 1;
		}
	}

	if (!check_input_and_values(file_path, data_to_add, fileds_and_type, key,
								argv, del, list_def, new_file, update))
	{
		return 1;
	}
	printf("%s\n", file_path);
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

		if (schema_def)
		{ /* case when user creates a file with only file definition*/
			int fields_count = count_fields(schema_def);

			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
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
				printf("can't create file definition");
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
				printf("could not write the file.\n");
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
				printf("error creating the record");
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
				free(buffer), free(buf_t), free(buf_v);
				clean_up(rec, fields_count), clean_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!write_header(fd_data, &hd))
			{
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				clean_up(rec, fields_count), clean_schema(&sch);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!padding_file(fd_data, MAX_HD_SIZE, hd_st))
			{
				printf("padding failed!\n");
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

			if (!write_file(fd_data, rec))
			{
				printf("error, could not write to the file %s\n", file_path);
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
				printf("could not write the file.\n");
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
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!padding_file(fd_data, MAX_HD_SIZE, hd_st))
			{
				printf("padding failed!\n");
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			Node **dataMap = calloc(7, sizeof(Node *));
			HashTable ht = {7, dataMap, write_ht};

			if (!ht.write(fd_index, &ht))
			{
				printf("could not write the file.\n");
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

			if (!ht.write(fd_index, &ht))
			{
				printf("could not write index file.\n");
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

		if (update && data_to_add && key)
		{
			printf("updating the record . . .\n");
			// 1 - check the schema with the one on file
			int fields_count = count_fields(data_to_add);

			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
				free(files[0]), free(files[1]), free(files);
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
				free(files[0]), free(files[1]), free(files);
				clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
				free(buffer), free(buf_t), free(buf_v);
				if (rec)
					clean_up(rec, fields_count);

				return 1;
			}
			// 2 - if schema is good load the record into memory and update the fields if any
			// -- at this point you already checked the header, and you have an updated header,
			//	an updated record in memory

			HashTable ht = {0, NULL};

			if (!read_index_file(fd_index, &ht))
			{
				printf("index file reading failed.\n");
				free(buffer), free(buf_t), free(buf_v);
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
				clean_schema(&hd.sch_d);
				return 1;
			}

			off_t offset = get(key, &ht); /*look for the key in the ht */

			if (offset == -1)
			{
				printf("record not found.\n");
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
				clean_schema(&hd.sch_d);
				destroy_hasht(&ht);
				free(buffer), free(buf_t), free(buf_v);
				return 1;
			}

			off_t find_record = find_record_position(fd_data, offset);
			// printf("record pos is %ld\n",find_record);

			if (find_record == -1)
			{
				perror("error looking for record in file.\n");
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
				clean_schema(&hd.sch_d);
				destroy_hasht(&ht);
				return 1;
			}
			Record_f *rec_old = read_file(fd_data, file_path);

			if (!rec_old)
			{
				printf("no memory for record.\n");
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
				clean_schema(&hd.sch_d);
				destroy_hasht(&ht);
				return 1;
			}

			off_t updated_rec_pos = get_update_offset(fd_data);

			if (updated_rec_pos == -1)
			{
				perror("error reading update record position (main.c l 467).\n");
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
				clean_schema(&hd.sch_d);
				destroy_hasht(&ht);
			}

			unsigned char comp_rr = compare_old_rec_update_rec(rec_old, rec);
			printf("comp_rr is %u.\n", comp_rr);
			printf("update rec pos is %d.\n", updated_rec_pos);

			if (updated_rec_pos == 0 && comp_rr == UPDATE_OLD)
			{
				// simply write the new record to the file
				//  1st set the position back to the record
				find_record = find_record_position(fd_data, offset);
				if (!write_file(fd_data, rec))
				{
					printf("could not write to %s", files[1]);
					free(files[0]), free(files[1]), free(files);
					free(buffer), free(buf_t), free(buf_v);
					close_file(2, fd_index, fd_data);
					clean_schema(&hd.sch_d);
					clean_up(rec, fields_count);
					clean_up(rec_old, fields_count);
					destroy_hasht(&ht);
					return 1;
				}

				printf("record %s updated!\n", record_id);
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				clean_schema(&hd.sch_d);
				clean_up(rec, fields_count);
				clean_up(rec_old, fields_count);
				destroy_hasht(&ht);
				return 0;
			}

			return 0;
			// udate any fields presents in the old record,
			// and move to the end of the file to write the other fields,
			// come back to the record and update the EOF

			// 3 - write the data that were not wrote to disk, at the end of the file and store
			//     .the offset
			//     .for example if the data on disk do not contain a specific field,
			//     .write the new filed at
			//     .the end of the file and save the offset in a variable.
			// 4 - update the old record if needed, update the offset at the end with the offset from
			//     .the previous file saved (EOF before that writing)
			// 5 - read the current record size on file
			//  -
		}

		if (!update && data_to_add)
		{ /* append data to the specified file*/
			printf("i real should not be here\n");
			int fields_count = count_fields(data_to_add);

			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
				free(files[0]), free(files[1]), free(files);
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

				free(files[0]), free(files[1]), free(files);
				clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
				free(buffer), free(buf_t), free(buf_v);
				if (rec)
					clean_up(rec, fields_count);

				return 1;
			}

			if (!rec)
			{
				printf("error creating the record.\n");
				free(files[0]), free(files[1]), free(files);
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

			if (!write_file(fd_data, rec))
			{
				printf("could not write to %s", files[1]);
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				clean_up(rec, fields_count);
				destroy_hasht(&ht);
				return 1;
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
		/*	 reading the file to show data  	*/
		Schema sch = {0, NULL, NULL};
		Header_d hd = {0, 0, sch};

		if (!read_header(fd_data, &hd))
		{
			free(files[0]), free(files[1]), free(files);
			close_file(2, fd_index, fd_data);
			clean_schema(&sch); // it might not be neccessery to free schema, but the function is safe
			return 1;
		}
		if (list_def)
		{
			print_schema(hd.sch_d);
			free(files[0]), free(files[1]), free(files);
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
				printf("index file reading failed.");
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			off_t offset = get(record_id, &ht); /*look for the key in the ht */

			if (offset == -1)
			{
				printf("record not found.\n");
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
			destroy_hasht(&ht);
		}

		free(files[0]), free(files[1]), free(files);
	}

	close_file(2, fd_index, fd_data);
	return 0;
}
