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
#include "build.h"

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
	unsigned char build = 0;
	unsigned char list_keys = 0;
	unsigned char create = 0;
	unsigned char options = 0;
	/*------------------------------------------*/
	int c = 0;
	char *file_path = NULL;
	char *data_to_add = NULL;
	char *key = NULL;
	char *schema_def = NULL;
	char *txt_f = NULL;
	char *option = NULL;
	int bucket_ht = 0;
	int indexes = 0;
	int index_nr = 0;

	while ((c = getopt(argc, argv, "ntf:a:k:D:R:uleb:s:x:c:i:o:")) != -1)
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
		case 'k':
			key = optarg;
			break;
		case 'D':
			del = 1, index_nr = atoi(optarg); // 1 is marking del as true
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
		case 'b':
			build = 1, txt_f = optarg;
			break;
		case 's':
			bucket_ht = atoi(optarg);
			break;
		case 'x':
			list_keys = 1, index_nr = atoi(optarg);
			break;
		case 'c':
			create = 1, txt_f = optarg;
			break;
		case 'i':
			indexes = atoi(optarg);
			break;
		case 'o':
			options = 1, option = optarg;
			break;
		default:
			printf("Unknow option -%c\n", c);
			return 1;
		}
	}

	if (!check_input_and_values(file_path, data_to_add, key,
								argv, del, list_def, new_file, update, del_file, build, create))
	{
		return 1;
	}

	if (create)
	{
		if (!create_system_from_txt_file(txt_f))
		{
			return 1;
		}
		printf("system created!\n");
		return 0;
	}

	if (build)
	{
		if (build_from_txt_file(file_path, txt_f))
		{
			return 0;
		}

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
			printf("%s:%d.\n", __FILE__, __LINE__ - 6);
			free(files[0]), free(files[1]), free(files);
			return 1;
		}

		if (schema_def)
		{ /* case when user creates a file with only file definition*/
			int fields_count = count_fields(schema_def, TYPE_) + count_fields(schema_def, T_);

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
				printf("can't create file definition %s:%d.\n", __FILE__, __LINE__ - 1);
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
				printf("write to file failed, %s:%d.\n", __FILE__, __LINE__ - 1);
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
				printf("padding failed. %s:%d.\n", __FILE__, __LINE__ - 1);
				free(buf_sdf), free(buf_t);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}
			/*  write the index file */
			int bucket = bucket_ht > 0 ? bucket_ht : 7;
			int index_num = indexes > 0 ? indexes : 5;

			if (!write_index_file_head(fd_index, index_num))
			{
				printf("write to file failed, %s:%d", F, L - 2);
				free(buf_sdf), free(buf_t);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			int i = 0;
			for (i = 0; i < index_num; i++)
			{
				Node **dataMap = calloc(bucket, sizeof(Node *));
				if (!dataMap)
				{
					printf("calloc failed. %s:%d.\n", F, L - 3);
					free(buf_sdf), free(buf_t);
					close_file(2, fd_index, fd_data);
					delete_file(2, files[0], files[1]);
					free(files[0]), free(files[1]), free(files);
					return 1;
				}

				HashTable ht = {bucket, dataMap, write_ht};

				if (!write_index_body(fd_index, i, &ht) == -1)
				{
					printf("write to file failed. %s:%d.\n", F, L - 2);
					free(buf_sdf), free(buf_t);
					destroy_hasht(&ht);
					close_file(2, fd_index, fd_data);
					delete_file(2, files[0], files[1]);
					free(files[0]), free(files[1]), free(files);
					return 1;
				}

				destroy_hasht(&ht);
			}

			printf("File created successfully!\n");

			free(buf_sdf), free(buf_t);
			free(files[0]), free(files[1]), free(files);
			close_file(2, fd_index, fd_data);
			return 0;
		}

		if (data_to_add)
		{ /* creates a file with full definitons (fields and value)*/

			int fields_count = count_fields(data_to_add, TYPE_) + count_fields(data_to_add, T_);

			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				return 1;
			}

			char *buffer = strdup(data_to_add);
			char *buf_t = strdup(data_to_add);
			char *buf_v = strdup(data_to_add);

			Schema sch = {0, NULL, NULL};
			Record_f *rec = parse_d_flag_input(file_path, fields_count, buffer, buf_t, buf_v, &sch, 0);

			free(buffer), free(buf_t), free(buf_v);
			if (!rec)
			{
				printf("error creating the record, %s:%d.\n", __FILE__, __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			Header_d hd = {0, 0, sch};
			//	print_schema(sch);
			if (!create_header(&hd))
			{
				printf("%s:%d.\n", __FILE__, __LINE__ - 1);
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
				clean_up(rec, fields_count), clean_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!write_header(fd_data, &hd))
			{
				printf("write to file failed, %s:%d.\n", __FILE__, __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				clean_up(rec, fields_count), clean_schema(&sch);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!padding_file(fd_data, MAX_HD_SIZE, hd_st))
			{
				printf("padding failed. %s:%d.\n", __FILE__, __LINE__ - 1);
				clean_up(rec, fields_count), clean_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			clean_schema(&sch);

			/*  write the index file */
			int bucket = bucket_ht > 0 ? bucket_ht : 7;
			int index_num = indexes > 0 ? indexes : 5;

			if (!write_index_file_head(fd_index, index_num))
			{
				printf("write to file failed, %s:%d", F, L - 2);
				clean_up(rec, fields_count);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			int i = 0;
			for (i = 0; i < index_num; i++)
			{
				Node **dataMap = calloc(bucket, sizeof(Node *));
				if (!dataMap)
				{
					printf("calloc failed. %s:%d.\n", F, L - 3);
					clean_up(rec, fields_count);
					close_file(2, fd_index, fd_data);
					delete_file(2, files[0], files[1]);
					free(files[0]), free(files[1]), free(files);
					return 1;
				}

				HashTable ht = {bucket, dataMap, write_ht};

				if (i == 0)
				{
					off_t offset = get_file_offset(fd_data);
					if (offset == -1)
					{
						__er_file_pointer(F, L - 3);
						clean_up(rec, fields_count);
						close_file(2, fd_index, fd_data);
						delete_file(2, files[0], files[1]);
						free(files[0]), free(files[1]), free(files);
						return 1;
					}
					set(key, offset, &ht); /*create a new key value pair in the hash table*/

					if (!write_file(fd_data, rec, 0, update))
					{
						printf("write to file failed, %s:%d.\n", __FILE__, __LINE__ - 1);
						clean_up(rec, fields_count);
						destroy_hasht(&ht);
						close_file(2, fd_index, fd_data);
						delete_file(2, files[0], files[1]);
						free(files[0]), free(files[1]), free(files);
						return 1;
					}
				}

				if (!write_index_body(fd_index, i, &ht) == -1)
				{
					printf("write to file failed. %s:%d.\n", F, L - 2);
					clean_up(rec, fields_count);
					destroy_hasht(&ht);
					close_file(2, fd_index, fd_data);
					delete_file(2, files[0], files[1]);
					free(files[0]), free(files[1]), free(files);
					return 1;
				}

				destroy_hasht(&ht);
			}

			printf("File created successfully.\n");
			clean_up(rec, fields_count); // this free the memory allocated for the record
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
				printf("%s:%d.\n", __FILE__, __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!padding_file(fd_data, MAX_HD_SIZE, hd_st))
			{
				printf("padding failed. %s:%d.\n", __FILE__, __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			/*  write the index file */
			int bucket = bucket_ht > 0 ? bucket_ht : 7;
			int index_num = indexes > 0 ? indexes : 5;

			if (!write_index_file_head(fd_index, index_num))
			{
				printf("write to file failed, %s:%d", F, L - 2);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			int i = 0;
			for (i = 0; i < index_num; i++)
			{
				Node **dataMap = calloc(bucket, sizeof(Node *));
				if (!dataMap)
				{
					printf("calloc failed. %s:%d.\n", F, L - 3);
					close_file(2, fd_index, fd_data);
					delete_file(2, files[0], files[1]);
					free(files[0]), free(files[1]), free(files);
					return 1;
				}

				HashTable ht = {bucket, dataMap, write_ht};

				if (!write_index_body(fd_index, i, &ht) == -1)
				{
					printf("write to file failed. %s:%d.\n", F, L - 2);
					close_file(2, fd_index, fd_data);
					delete_file(2, files[0], files[1]);
					free(files[0]), free(files[1]), free(files);
					return 1;
				}

				destroy_hasht(&ht);
			}

			printf("File created successfully.\n");

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
			printf("Error in creating or opening files,%s:%d.\n", __FILE__, __LINE__ - 1);
			free(files[0]), free(files[1]), free(files);
			return 1;
		}

		/* this is to ensure the file is a db file */
		Schema sch = {0, NULL, NULL};
		Header_d hd = {0, 0, sch};
		if (!read_header(fd_data, &hd))
		{
			free(files[0]), free(files[1]), free(files);
			return 1;
		}

		/* if the file is a db file reset the file pointer at the beginning */
		if (begin_in_file(fd_data) == -1)
		{
			__er_file_pointer(F, L - 2);
			clean_schema(&hd.sch_d);
			free(files[0]), free(files[1]), free(files);
			return 1;
		}

		if (del_file)
		{ /*delete file */
			delete_file(2, files[0], files[1]);
			clean_schema(&hd.sch_d);
			printf("file %s, deleted.\n", file_path);
			free(files[0]), free(files[1]), free(files);
			close_file(2, fd_index, fd_data);
			return 0;
		}

		if (schema_def)
		{
			free(files[0]), free(files[1]), free(files);
			/*check if the fields are in limit*/
			int fields_count = count_fields(schema_def, TYPE_) + count_fields(schema_def, T_);

			if (fields_count > MAX_FIELD_NR || hd.sch_d.fields_num + fields_count > 200)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
				clean_schema(&hd.sch_d);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			/*add field provided to the schema*/
			char *buffer = strdup(schema_def);
			char *buff_t = strdup(schema_def);

			if (!add_fields_to_schema(fields_count, buffer, buff_t, &hd.sch_d))
			{
				printf("add fields to chema failed. %s:%d;\n", F, L - 2);
				clean_schema(&hd.sch_d);
				close_file(2, fd_index, fd_data);
				free(buffer), free(buff_t);
				return 1;
			}

			free(buffer), free(buff_t);
			/*here you know that the file is at the beginning*/
			size_t hd_st = compute_size_header(hd);
			if (hd_st >= MAX_HD_SIZE)
			{
				printf("File definition is bigger than the limit.\n");
				clean_schema(&hd.sch_d);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			if (!write_header(fd_data, &hd))
			{
				printf("write to file failed, %s:%d.\n", __FILE__, __LINE__ - 1);
				clean_schema(&hd.sch_d);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			printf("data added to schema!\n");
			clean_schema(&hd.sch_d);
			close_file(2, fd_index, fd_data);
			return 0;
		}

		if (del)
		{
			clean_schema(&hd.sch_d);
			if (options)
			{
				if (option)
				{
					switch (convert_options(option))
					{
					case ALL:
					{
						int ind = 0, *p_i_nr = &ind;
						if (!indexes_on_file(fd_index, p_i_nr))
						{
							printf("er index nr,%s:%d.\n", F, L - 4);
							free(files[0]);
							free(files[1]);
							free(files);
							close_file(2,
									   fd_index, fd_data);
							return 1;
						}

						int buc_t = 0, *pbuck = &buc_t;
						if (!nr_bucket(fd_index, pbuck))
						{
							printf("er index nr,%s:%d.\n", F, L - 4);
							free(files[0]);
							free(files[1]);
							free(files);
							close_file(2,
									   fd_index, fd_data);
							return 1;
						}
						/* create *p_i_nr of ht and write them to file*/
						HashTable *ht = calloc(*p_i_nr,
											   sizeof(HashTable));
						if (!ht)
						{
							printf("calloc failed ,%s:%d.\n", F, L - 4);
							free(files[0]);
							free(files[1]);
							free(files);
							close_file(2,
									   fd_index, fd_data);
							return 1;
						}
						int i = 0;
						for (i = 0; i < *p_i_nr; i++)
						{
							HashTable ht_n = {*pbuck,
											  NULL, write_ht};
							ht[i] = ht_n;
						}

						close_file(1, fd_index);
						// opening with O_TRUNC
						fd_index = open_file(files[0], 1);

						/*  write the index file */

						if (!write_index_file_head(fd_index, *p_i_nr))
						{
							printf("write to file failed, %s:%d", F, L - 2);
							free(files[0]), free(files[1]), free(files);
							close_file(2, fd_index, fd_data);
							free(ht);
							return 1;
						}

						for (i = 0; i < *p_i_nr; i++)
						{

							if (!write_index_body(fd_index, i, &ht[i]) == -1)
							{
								printf("write to file failed. %s:%d.\n", F, L - 2);
								free(files[0]), free(files[1]), free(files);
								close_file(2, fd_index, fd_data);
								free(ht);
								return 1;
							}
						}

						free(files[0]), free(files[1]), free(files);
						close_file(2, fd_index, fd_data);
						free(ht);
						printf("all record deleted from %s file.\n", file_path);
						return 0;
					}
					default:
						printf("options not valid.\n");
						free(files[0]);
						free(files[1]);
						free(files);
						close_file(2, fd_index, fd_data);
						return 1;
					}
				}
			}

			/* delete the record specified by the -D option, in the index file*/
			HashTable *ht = NULL;
			int index = 0;
			int *p_index = &index;
			/* load al indexes in memory */
			read_all_index_file(fd_index, &ht, p_index);

			Node *record_del = delete (key, &ht[index_nr]);
			if (!record_del)
			{
				printf("record %s not found.\n", key);
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
				if (ht)
				{
					int i = 0;
					for (i = 0; i < index; i++)
					{
						destroy_hasht(&ht[i]);
					}
					free(ht);
				}
				return 1;
			}

			printf("record %s deleted!.\n", key);
			free(record_del->key);
			free(record_del);
			close_file(1, fd_index);
			fd_index = open_file(files[0], 1); // opening with O_TRUNC

			/*  write the index file */

			if (!write_index_file_head(fd_index, index))
			{
				printf("write to file failed, %s:%d", F, L - 2);
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
				if (ht)
				{
					int i = 0;
					for (i = 0; i < index; i++)
					{
						destroy_hasht(&ht[i]);
					}
					free(ht);
				}
				return 1;
			}

			int i = 0;
			for (i = 0; i < index; i++)
			{

				if (!write_index_body(fd_index, i, &ht[i]) == -1)
				{
					printf("write to file failed. %s:%d.\n", F, L - 2);
					free(files[0]), free(files[1]), free(files);
					close_file(2, fd_index, fd_data);
					if (ht)
					{
						int i = 0;
						for (i = 0; i < index; i++)
						{
							destroy_hasht(&ht[i]);
						}
						free(ht);
					}
					return 1;
				}

				destroy_hasht(&ht[i]);
			}

			free(files[0]), free(files[1]), free(files);
			close_file(2, fd_index, fd_data);
			free(ht);
			return 0;
		}

		if (!update && data_to_add)
		{ /* append data to the specified file*/
			int fields_count = count_fields(data_to_add, TYPE_) + count_fields(data_to_add, T_);

			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
				free(files[0]), free(files[1]), free(files);
				clean_schema(&hd.sch_d);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			char *buffer = strdup(data_to_add);
			char *buf_t = strdup(data_to_add);
			char *buf_v = strdup(data_to_add);

			Record_f *rec = NULL;

			unsigned char check = perform_checks_on_schema(buffer, buf_t, buf_v, fields_count,
														   fd_data, file_path, &rec, &hd);

			if (check == SCHEMA_ERR || check == 0)
			{
				free(files[0]), free(files[1]), free(files);
				clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
				free(buffer), free(buf_t), free(buf_v);
				return 1;
			}

			if (!rec)
			{
				printf("error creating record, main.c l %d\n", __LINE__ - 1);
				free(buffer), free(buf_t), free(buf_v);
				free(files[0]), free(files[1]), free(files);
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
					clean_up(rec, rec->fields_num);
					return 1;
				}

				if (begin_in_file(fd_data) == -1)
				{ /*set the file pointer at the start*/
					__er_file_pointer(F, L - 1);
					free(buffer), free(buf_t), free(buf_v);
					free(files[0]), free(files[1]), free(files);
					clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					return 1;
				}

				if (!write_header(fd_data, &hd))
				{
					printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
					free(files[0]), free(files[1]), free(files);
					free(buffer), free(buf_t), free(buf_v);
					clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					return 1;
				}
			}

			clean_schema(&hd.sch_d);
			off_t eof = go_to_EOF(fd_data);
			if (eof == -1)
			{
				__er_file_pointer(F, L - 1);
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				clean_up(rec, rec->fields_num);
				return 1;
			}

			HashTable *ht = NULL;
			int index = 0;
			int *p_index = &index;
			/* load al indexes in memory */
			if (!read_all_index_file(fd_index, &ht, p_index))
			{
				printf("read file failed. %s:%d.\n", F, L - 2);
				close_file(2, fd_index, fd_data);
				free(files[0]), free(files[1]), free(files);
				clean_up(rec, rec->fields_num);
				free(buffer), free(buf_t), free(buf_v);
				return 1;
			}

			if (!set(key, eof, &ht[0]))
			{
				close_file(2, fd_index, fd_data);
				free(files[0]), free(files[1]), free(files);
				clean_up(rec, rec->fields_num);
				free(buffer), free(buf_t), free(buf_v);
				if (ht)
				{
					int i = 0;
					for (i = 0; i < index; i++)
					{
						destroy_hasht(&ht[i]);
					}
					free(ht);
				}
				return 1;
			}

			if (!write_file(fd_data, rec, 0, update))
			{
				printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				clean_up(rec, rec->fields_num);
				if (ht)
				{
					int i = 0;
					for (i = 0; i < index; i++)
					{
						destroy_hasht(&ht[i]);
					}
					free(ht);
				}
				return 1;
			}

			free(buffer), free(buf_t), free(buf_v);
			close_file(1, fd_index);
			clean_up(rec, rec->fields_num);

			fd_index = open_file(files[0], 1); // opening with O_TRUNC

			free(files[0]), free(files[1]), free(files);
			/* write the new indexes to file */

			if (!write_index_file_head(fd_index, index))
			{
				printf("write to file failed, %s:%d", F, L - 2);
				close_file(2, fd_index, fd_data);
				if (ht)
				{
					int i = 0;
					for (i = 0; i < index; i++)
					{
						destroy_hasht(&ht[i]);
					}
					free(ht);
				}
				return 1;
			}

			int i = 0;
			for (i = 0; i < index; i++)
			{

				if (!write_index_body(fd_index, i, &ht[i]) == -1)
				{
					printf("write to file failed. %s:%d.\n", F, L - 2);
					close_file(2, fd_index, fd_data);
					if (ht)
					{
						int i = 0;
						for (i = 0; i < index; i++)
						{
							destroy_hasht(&ht[i]);
						}
						free(ht);
					}
					return 1;
				}

				destroy_hasht(&ht[i]);
			}

			close_file(2, fd_index, fd_data);
			free(ht);
			return 0;
		}

		free(files[0]), free(files[1]), free(files);

		if (update && data_to_add && key)
		{ /* updating an existing record */

			// 1 - check the schema with the one on file
			int fields_count = count_fields(data_to_add, TYPE_) + count_fields(data_to_add, T_);

			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
				clean_schema(&hd.sch_d);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			char *buffer = strdup(data_to_add);
			char *buf_t = strdup(data_to_add);
			char *buf_v = strdup(data_to_add);

			Record_f *rec = NULL;
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
					__er_file_pointer(F, L - 1);
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
					__er_file_pointer(F, L - 1);
					free(buffer), free(buf_t), free(buf_v);
					clean_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					return 1;
				}
			}
			free(buf_t), free(buf_v);
			// 2 - if schema is good load the old record into memory and update the fields if any
			// -- at this point you already checked the header, and you have an updated header,
			//	an updated record in memory

			HashTable ht = {0, NULL};
			HashTable *p_ht = &ht;
			if (!read_index_nr(0, fd_index, &p_ht))
			{
				printf("index file reading failed. %s:%d.\n", F, L - 1);
				free(buffer), close_file(2, fd_index, fd_data);
				clean_up(rec, rec->fields_num);
				clean_schema(&hd.sch_d);
				return 1;
			}

			off_t offset = get(key, p_ht); /*look for the key in the ht */

			if (offset == -1)
			{
				printf("record not found.\n");
				free(buffer), close_file(2, fd_index, fd_data);
				clean_up(rec, rec->fields_num);
				clean_schema(&hd.sch_d);
				destroy_hasht(p_ht);
				return 1;
			}

			destroy_hasht(p_ht);
			if (find_record_position(fd_data, offset) == -1)
			{
				__er_file_pointer(F, L - 1);
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
				__er_file_pointer(F, L - 1);
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
					__er_file_pointer(F, L - 1);
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

				/*read all the record's parts if any */
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
				   and populates the char array positions. If an element contain 'y'
				   you have to update the record at that index position of 'y'. */

				find_fields_to_update(recs_old, positions, rec, index);

				if (positions[0] != 'n' && positions[0] != 'y')
				{
					printf("check on fields failed, %s:%d.\n", __FILE__, __LINE__ - 1);
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
						__er_file_pointer(F, L - 1);
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

				if (check == SCHEMA_NW && updates > 0)
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
						__er_file_pointer(F, L - 1);
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
					/*the position of new_rec in the old part of the record
					 was already set at line 898*/
				}

				if (check == SCHEMA_NW && updates == 0)
				{
					/* store the EOF value*/
					off_t eof = 0;
					if ((eof = go_to_EOF(fd_data)) == -1)
					{
						__er_file_pointer(F, L - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						clean_schema(&hd.sch_d);
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

					/*find the position of the last piece of the record*/
					off_t initial_pos = 0;

					if ((initial_pos = find_record_position(fd_data, pos_u[index - 1])) == -1)
					{
						__er_file_pointer(F, L - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						clean_schema(&hd.sch_d);
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

					ssize_t rec_st = get_record_size(fd_data);
					find_record_position(fd_data, pos_u[index - 1]);
					//	printf("record size is %ld\n", rec_st);
					//	printf("initial record pos is %ld\n",initial_pos);
					// printf("the offset of the updatePos should be %ld", rec_st + (ssize_t)initial_pos);

					/*re-write the record*/
					if (write_file(fd_data, recs_old[index - 1], eof, update) == -1)
					{
						printf("write file failed, main.c l %d.\n", __LINE__ - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						clean_schema(&hd.sch_d);
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

					/*move to EOF*/
					if ((go_to_EOF(fd_data)) == -1)
					{
						__er_file_pointer(F, L - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						free(positions);
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
					__er_file_pointer(F, L - 1);
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

				off_t eof = 0;
				if ((eof = go_to_EOF(fd_data)) == -1)
				{
					__er_file_pointer(F, L - 1);
					close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					clean_up(rec_old, rec_old->fields_num);
					clean_up(new_rec, new_rec->fields_num);
					return 1;
				}

				// put the position back to the record
				if (find_record_position(fd_data, offset) == -1)
				{
					__er_file_pointer(F, L - 1);
					close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					clean_up(rec_old, rec_old->fields_num);
					clean_up(new_rec, new_rec->fields_num);
					return 1;
				}

				// update the old record :
				if (!write_file(fd_data, rec_old, eof, update))
				{
					printf("can't write record, %s:%d.\n", __FILE__, __LINE__ - 1);
					close_file(2, fd_index, fd_data);
					clean_up(rec, rec->fields_num);
					clean_up(rec_old, rec_old->fields_num);
					clean_up(new_rec, new_rec->fields_num);
					return 1;
				}

				eof = go_to_EOF(fd_data);
				if (eof == -1)
				{
					__er_file_pointer(F, L - 1);
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
		if (list_def)
		{ /* show file definitions */
			print_schema(hd.sch_d);
			close_file(2, fd_index, fd_data);
			clean_schema(&hd.sch_d);
			return 0;
		}

		if (list_keys)
		{
			HashTable ht = {0, NULL};
			HashTable *p_ht = &ht;
			if (!read_index_nr(index_nr, fd_index, &p_ht))
			{
				printf("reading index file failed, main.c l %d.\n", __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			char **key_a = keys(p_ht);
			char keyboard = '0';
			int end = len(ht), i = 0, j = 0;
			for (i = 0, j = i; i < end; i++)
			{
				printf("%d. %s\n", ++j, key_a[i]);
				if (i > 0 && (i % 20 == 0))
					printf("press return key. . .\n"
						   "enter q to quit . . .\n"),
						keyboard = (char)getc(stdin);

				if (keyboard == 'q')
					break;
			}

			destroy_hasht(p_ht);
			clean_schema(&hd.sch_d);
			close_file(2, fd_index, fd_data);
			free_strs(end, 1, key_a);
			return 0;
		}

		clean_schema(&hd.sch_d);

		if (key)
		{
			HashTable ht = {0, NULL};
			HashTable *p_ht = &ht;
			if (!read_index_nr(0, fd_index, &p_ht))
			{
				printf("reading index file failed, main.c l %d.\n", __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			off_t offset = get(key, p_ht); /*look for the key in the ht */

			if (offset == -1)
			{
				printf("record not found.\n");
				destroy_hasht(p_ht);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			destroy_hasht(p_ht);
			if (find_record_position(fd_data, offset) == -1)
			{
				__er_file_pointer(F, L - 1);
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
				close_file(2, fd_index, fd_data);
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
					__er_file_pointer(F, L - 1);
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
						__er_file_pointer(F, L - 1);
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
						printf("read record failed, main.c l %d.\n", __LINE__ - 2);
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
