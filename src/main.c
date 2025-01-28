#include <stdio.h>
#include <getopt.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <semaphore.h>
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
	unsigned char index_add = 0;
	/*------------------------------------------*/

	/* parameters populated with the flag from getopt()*/
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
	int only_dat = 0;

	while ((c = getopt(argc, argv, "nItAf:a:k:D:R:uleb:s:x:c:i:o:")) != -1)
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
		case 'A':
			index_add = 1;
			break;
		case 'I':
			only_dat = 1;
			break;
		default:
			printf("Unknow option -%c\n", c);
			return 1;
		}
	}

	if (!check_input_and_values(file_path, data_to_add, key,
								argv, del, list_def, new_file, update, del_file,
								build, create, options, index_add))
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

		if (only_dat)
		{
			fd_data = create_file(files[1]);
			/*
			 * file_error_handler will close the file descriptors if there are issues
			 *  and print error messages to the console
			 *  */
			if (file_error_handler(1, fd_data) != 0)
			{
				free_strs(2, 1, files);
				return 1;
			}
		}
		else
		{
			fd_index = create_file(files[0]);
			fd_data = create_file(files[1]);
			/*
			 * file_error_handler will close the file descriptors if there are issues
			 *  and print error messages to the console
			 *  */
			if (file_error_handler(2, fd_index, fd_data) != 0)
			{
				free_strs(2, 1, files);
				return 1;
			}
		}

		if (schema_def)
		{ /* case when user creates a file with only file definition*/
			int fields_count = count_fields(schema_def, TYPE_) + count_fields(schema_def, T_);

			if (fields_count == 0)
			{
				printf("type syntax might be wrong.\n");
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free_strs(2, 1, files);
				return 1;
			}
			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d fields each file definition.\n", MAX_FIELD_NR);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free_strs(2, 1, files);
				return 1;
			}

			char *buf_sdf = strdup(schema_def);
			char *buf_t = strdup(schema_def);

			struct Schema sch = {fields_count, NULL, NULL};

			if (!create_file_definition_with_no_value(fields_count, buf_sdf, buf_t, &sch))
			{
				printf("can't create file definition %s:%d.\n", F, L - 1);
				free(buf_sdf);
				free(buf_t);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free_strs(2, 1, files);
				return 1;
			}

			struct Header_d hd = {0, 0, sch};

			if (!create_header(&hd))
			{
				free(buf_sdf), free(buf_t);
				free_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free_strs(2, 1, files);
				return 1;
			}

			size_t hd_st = compute_size_header((void *)&hd);
			if (hd_st >= MAX_HD_SIZE)
			{
				printf("File definition is bigger than the limit.\n");
				free(buf_sdf), free(buf_t);
				free_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free_strs(2, 1, files);
				return 1;
			}

			if (!write_header(fd_data, &hd))
			{
				printf("write to file failed, %s:%d.\n", F, L - 1);
				free(buf_sdf), free(buf_t);
				free_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free_strs(2, 1, files);
				return 1;
			}

			free_schema(&sch);

			if (!padding_file(fd_data, MAX_HD_SIZE, hd_st))
			{
				printf("padding failed. %s:%d.\n", __FILE__, __LINE__ - 1);
				free(buf_sdf), free(buf_t);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (only_dat)
			{
				printf("File created successfully!\n");
				free(buf_sdf), free(buf_t);
				close_file(1, fd_data);
				free(files[0]), free(files[1]), free(files);
				return 0;
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

				if (write_index_body(fd_index, i, &ht) == -1)
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
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			char *buffer = strdup(data_to_add);
			char *buf_t = strdup(data_to_add);
			char *buf_v = strdup(data_to_add);

			struct Schema sch = {0, NULL, NULL};
			struct Record_f *rec = parse_d_flag_input(file_path, fields_count,
													  buffer, buf_t, buf_v, &sch, 0);

			free(buffer), free(buf_t), free(buf_v);
			if (!rec)
			{
				printf("error creating the record, %s:%d.\n", __FILE__, __LINE__ - 1);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				free_schema(&sch);
				return 1;
			}

			struct Header_d hd = {0, 0, sch};
			if (!create_header(&hd))
			{
				printf("%s:%d.\n", F, L - 1);
				free_record(rec, fields_count), free_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			size_t hd_st = compute_size_header((void *)&hd);
			if (hd_st >= MAX_HD_SIZE)
			{
				printf("File definition is bigger than the limit.\n");
				free_record(rec, fields_count), free_schema(&sch);
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
				free_record(rec, fields_count), free_schema(&sch);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!padding_file(fd_data, MAX_HD_SIZE, hd_st))
			{
				printf("padding failed. %s:%d.\n", __FILE__, __LINE__ - 1);
				free_record(rec, fields_count), free_schema(&sch);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			free_schema(&sch);

			if (only_dat)
			{
				printf("File created successfully!\n");
				free_record(rec, fields_count);
				close_file(1, fd_data);
				free(files[0]), free(files[1]), free(files);
				return 0;
			}

			/*  write the index file */
			int bucket = bucket_ht > 0 ? bucket_ht : 7;
			int index_num = indexes > 0 ? indexes : 5;

			if (!write_index_file_head(fd_index, index_num))
			{
				printf("write to file failed, %s:%d", F, L - 2);
				free_record(rec, fields_count);
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
					free_record(rec, fields_count);
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
						free_record(rec, fields_count);
						close_file(2, fd_index, fd_data);
						delete_file(2, files[0], files[1]);
						free(files[0]), free(files[1]), free(files);
						return 1;
					}
					int key_type = 0;
					void *key_conv = key_converter(key, &key_type);
					if (key_type == UINT && !key_conv)
					{
						fprintf(stderr, "error to convert key");
						free_record(rec, fields_count);
						close_file(2, fd_index, fd_data);
						delete_file(2, files[0], files[1]);
						free(files[0]), free(files[1]), free(files);
						return 1;
					}
					else if (key_type == UINT)
					{
						if (key_conv)
						{
							if (!set(key_conv, key_type, offset, &ht))
							{
								free_record(rec, fields_count);
								close_file(2, fd_index, fd_data);
								delete_file(2, files[0], files[1]);
								free(files[0]), free(files[1]), free(files);
								free(key_conv);
								return 1;
							}
							free(key_conv);
						}
					}
					else if (key_type == STR)
					{
						/*create a new key value pair in the hash table*/
						if (!set((void *)key, key_type, offset, &ht))
						{
							free_record(rec, fields_count);
							close_file(2, fd_index, fd_data);
							delete_file(2, files[0], files[1]);
							free(files[0]), free(files[1]), free(files);
							return 1;
						}
					}

					if (!write_file(fd_data, rec, 0, update))
					{
						printf("write to file failed, %s:%d.\n", F, L - 1);
						free_record(rec, fields_count);
						destroy_hasht(&ht);
						close_file(2, fd_index, fd_data);
						delete_file(2, files[0], files[1]);
						free(files[0]), free(files[1]), free(files);
						return 1;
					}
				}

				if (write_index_body(fd_index, i, &ht) == -1)
				{
					printf("write to file failed. %s:%d.\n", F, L - 2);
					free_record(rec, fields_count);
					destroy_hasht(&ht);
					close_file(2, fd_index, fd_data);
					delete_file(2, files[0], files[1]);
					free(files[0]), free(files[1]), free(files);
					return 1;
				}

				destroy_hasht(&ht);
			}

			printf("File created successfully.\n");
			free_record(rec, fields_count); // this free the memory allocated for the record
			free(files[0]), free(files[1]), free(files);
			close_file(2, fd_index, fd_data);
			return 0;
		}
		else
		{
			printf("no data to write to file %s.\n", file_path);
			printf("%s has been created, you can add to the file using option -a.\n", file_path);
			print_usage(argv);

			struct Schema sch = {0, NULL, NULL};
			struct Header_d hd = {HEADER_ID_SYS, VS, sch};

			size_t hd_st = compute_size_header((void *)&hd);
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
				printf("%s:%d.\n", F, L - 1);
				close_file(2, fd_index, fd_data);
				delete_file(2, files[0], files[1]);
				free(files[0]), free(files[1]), free(files);
				return 1;
			}

			if (!padding_file(fd_data, MAX_HD_SIZE, hd_st))
			{
				printf("padding failed. %s:%d.\n", F, L - 1);
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

				if (write_index_body(fd_index, i, &ht) == -1)
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

		/* check if there is a shared memory object
		   if there is, we map it to the lock_info* so we can read and write to the struct
		   data to share with the main program any lock to the files*/

		int fd_mo = shm_open(SH_ILOCK, O_RDWR, 0666);
		if (fd_mo != -1)
		{
			/*shared locks is declared as a global variable in lock.h and define as NULL
				inside lock.c */
			shared_locks = mmap(NULL, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE,
								PROT_READ | PROT_WRITE, MAP_SHARED, fd_mo, 0);
		}

		/*creates two name from the file_path => from "str_op.h" */
		char **files = two_file_path(file_path);

		/* acquire lock before opning the files (reading header)*/

		int lock_pos = 0, *plp = &lock_pos;
		int lock_pos_arr = 0, *plpa = &lock_pos_arr;
		int lock_pos_i = 0, *plp_i = &lock_pos_i;
		int lock_pos_arr_i = 0, *plpa_i = &lock_pos_arr_i;
		if (shared_locks)
		{
			int result_d = 0, result_i = 0;
			do
			{
				off_t fd_i_s = get_file_size(fd_index, files[0]);
				if (fd_i_s == -1)
				{
					free_strs(2, 1, files);
					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}

				if ((result_d = acquire_lock_smo(&shared_locks, plp, plpa, files[1], 0, MAX_HD_SIZE, RD_HEADER, fd_data)) == 0 ||
					(result_i = acquire_lock_smo(&shared_locks, plp_i, plpa_i, files[0], 0, fd_i_s, RD_IND, fd_index)) == 0)
				{
					printf("can't acquire lock, %s:%d", F, L - 2);
					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						free_strs(2, 1, files);
						return 1;
					}
					close_file(1, fd_mo);
					free_strs(2, 1, files);
					return 1;
				}
			} while (result_i == MAX_WTLK || result_i == WTLK ||
					 result_d == MAX_WTLK || result_d == WTLK);
		}

		if (list_def)
		{
			fd_data = open_file(files[1], 0);
			/* file_error_handler will close the file descriptors if there are issues */
			if (file_error_handler(1, fd_data) != 0)
			{
				printf("Error in creating or opening files,%s:%d.\n", F, L - 2);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 &lock_pos_i, &lock_pos_arr_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 &lock_pos, &lock_pos_arr)) == 0)
						{
							printf("release_lock_smo() failed , %s:%d.\n", F, L - 5);
							if (munmap(shared_locks,
									   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
						}
					} while (result_i == WTLK || result_d == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						printf("munmap() failed, %s:%d.\n", F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}

					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}
		}
		else
		{
			fd_index = open_file(files[0], 0);
			fd_data = open_file(files[1], 0);
			/* file_error_handler will close the file descriptors if there are issues */
			if (file_error_handler(2, fd_index, fd_data) != 0)
			{
				printf("Error in creating or opening files,%s:%d.\n", F, L - 2);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 &lock_pos_i, &lock_pos_arr_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 &lock_pos, &lock_pos_arr)) == 0)
						{
							printf("release_lock_smo() failed , %s:%d.\n", F, L - 5);
							if (munmap(shared_locks,
									   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
						}
					} while (result_i == WTLK || result_d == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						printf("munmap() failed, %s:%d.\n", F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}

					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}
		}

		/* ensure the file is a db file */
		struct Schema sch = {0, NULL, NULL};
		struct Header_d hd = {0, 0, sch};

		if (!read_header(fd_data, &hd))
		{
			free_schema(&hd.sch_d);
			free_strs(2, 1, files);
			if (shared_locks)
			{
				int result_i = 0, result_d = 0;
				do
				{
					if ((result_d = release_lock_smo(&shared_locks,
													 &lock_pos, &lock_pos_arr)) == 0 ||
						(result_i = release_lock_smo(&shared_locks,
													 &lock_pos_i, &lock_pos_arr_i)) == 0)
					{
						printf("release_lock_smo() failed, %s:%d.\n", F, L - 2);
						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(3, fd_index, fd_data, fd_mo);
							return 1;
						}
						close_file(3, fd_index, fd_data, fd_mo);
						return 1;
					}

				} while (result_i == WTLK || result_d == WTLK);

				if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
				{
					__er_munmap(F, L - 3);
					close_file(3, fd_index, fd_data, fd_mo);
					return 1;
				}

				close_file(3, fd_index, fd_data, fd_mo);
				return 1;
			}

			close_file(2, fd_index, fd_data);
			return 1;
		}

		/* if the file is a db file reset the file pointer at the beginning */
		if (begin_in_file(fd_data) == -1)
		{
			__er_file_pointer(F, L - 2);
			free_schema(&hd.sch_d);
			free_strs(2, 1, files);
			if (shared_locks)
			{
				int result_i = 0, result_d = 0;
				do
				{
					if ((result_d = release_lock_smo(&shared_locks,
													 &lock_pos, &lock_pos_arr)) == 0 ||
						(result_i = release_lock_smo(&shared_locks,
													 &lock_pos_i, &lock_pos_arr_i)) == 0)
					{
						printf("release_lock_smo() failed, %s:%d.\n", F, L - 2);
						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(3, fd_index, fd_data, fd_mo);
							return 1;
						}
						close_file(3, fd_index, fd_data, fd_mo);
						return 1;
					}

				} while (result_i == WTLK || result_d == WTLK);

				if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
				{
					__er_munmap(F, L - 2);
					close_file(3, fd_index, fd_data, fd_mo);
					return 1;
				}
				close_file(3, fd_index, fd_data, fd_mo);
				return 1;
			}
			close_file(2, fd_index, fd_data);
			return 1;
		}

		/*release the lock*/
		if (shared_locks)
		{
			int result_i = 0, result_d = 0;
			do
			{
				if ((result_d = release_lock_smo(&shared_locks, plp, plpa)) == 0 ||
					(result_i = release_lock_smo(&shared_locks, plp_i, plpa_i)) == 0)
				{
					printf("release_lock_smo() failed, %s:%d.\n", F, L - 2);
					if (munmap(shared_locks,
							   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						printf("munmap() failed, %s:%d.\n", F, L - 2);
						free_schema(&hd.sch_d);
						close_file(3, fd_index, fd_data, fd_mo);
						free_strs(2, 1, files);
						return 1;
					}
					free_schema(&hd.sch_d);
					close_file(3, fd_index, fd_data, fd_mo);
					free_strs(2, 1, files);
					return 1;
				}

			} while (result_i == WTLK || result_d == WTLK);
		}

		if (index_add)
		{
			free_schema(&hd.sch_d);
			close_file(2, fd_index, fd_data);
			free_strs(2, 1, files);

			/*  write the index file
			 *  if the user does not specify the indexes number
			 *  that they want to add, then only one will
			 *  be added.
			 *  */
			int bucket = bucket_ht > 0 ? bucket_ht : 7;
			int index_num = indexes > 0 ? indexes : 1;
			/* acquire lock */
			if (shared_locks)
			{
				int result_d = 0, result_i = 0;
				do
				{
					if (((result_d = acquire_lock_smo(&shared_locks, plp, plpa, files[1],
													  0, go_to_EOF(fd_data), WR_REC, fd_data)) == 0) ||
						((result_i = acquire_lock_smo(&shared_locks, plp_i, plpa_i, files[0],
													  0, go_to_EOF(fd_index), WR_IND, fd_index)) == 0))
					{
						printf("aquire_lock_smo() failed, %s:%d.\n", F, L - 3);
						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							printf("munmap() failed %s:%d.\n", F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}

				} while (result_d == MAX_WTLK || result_d == WTLK ||
						 result_i == MAX_WTLK || result_i == WTLK);
			}

			if (add_index(index_num, file_path, bucket) == -1)
			{
				fprintf(stderr, "can't add index %s:%d",
						F, L - 2);
				/*release the lock*/
				if (shared_locks)
				{
					int result_d = 0, result_i = 0;
					do
					{
						if ((result_d = release_lock_smo(&shared_locks, plp, plpa)) == 0 ||
							(result_i = release_lock_smo(&shared_locks, plp_i,
														 plpa_i) == 0))
						{
							printf("release_lock_smo() failed, %s:%d./n", F, L - 3);
							if (munmap(shared_locks,
									   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
							{
								printf("munmap() failed %s:%d.\n", F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_i == WT_RSLK || result_d == WT_RSLK);
				}

				close_file(1, fd_mo);
				return 1;
			}

			char *mes = (index_num > 1) ? "indexes" : "index";
			printf("%d %s added.\n", index_num, mes);
			/*release the lock*/
			if (shared_locks)
			{
				int result_d = 0, result_i = 0;
				do
				{
					if ((result_d = release_lock_smo(&shared_locks, plp, plpa)) == 0 ||
						(result_i = release_lock_smo(&shared_locks, plp_i,
													 plpa_i) == 0))
					{
						printf("release_lock_smo() failed, %s:%d./n", F, L - 3);
						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							printf("munmap() failed %s:%d.\n", F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}

				} while (result_i == WT_RSLK || result_d == WT_RSLK);

				if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
				{
					__er_munmap(F, L - 3);
					close_file(1, fd_mo);
					return 1;
				}

				close_file(1, fd_mo);
				return 0;
			}
			return 0;
		}

		if (del_file)
		{ /*delete file */

			free_schema(&hd.sch_d);
			/* acquire lock */
			if (shared_locks)
			{
				int result_d = 0, result_i = 0;
				do
				{
					if (((result_d = acquire_lock_smo(&shared_locks, plp, plpa, files[1],
													  0, go_to_EOF(fd_data), WR_REC, fd_data)) == 0) ||
						((result_i = acquire_lock_smo(&shared_locks, plp_i, plpa_i, files[0],
													  0, go_to_EOF(fd_index), WR_IND, fd_index)) == 0))
					{
						printf("aquire_lock_smo() failed, %s:%d.\n", F, L - 3);
						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							printf("munmap() failed %s:%d.\n", F, L - 3);
							close_file(3, fd_index, fd_data, fd_mo);
							free_strs(2, 1, files);
							return 1;
						}
						free_strs(2, 1, files);
						close_file(3, fd_index, fd_data, fd_mo);
						return 1;
					}

				} while (result_d == MAX_WTLK || result_d == WTLK ||
						 result_i == MAX_WTLK || result_i == WTLK);
			}

			/* we can safely delete the files, here, this process is the only one owning locks
				for both the index and the data file */
			delete_file(2, files[0], files[1]);
			printf("file %s, deleted.\n", file_path);
			close_file(2, fd_index, fd_data);

			/*release the lock*/
			if (shared_locks)
			{
				int result_d = 0, result_i = 0;
				do
				{
					if ((result_d = release_lock_smo(&shared_locks, plp, plpa)) == 0 ||
						(result_i = release_lock_smo(&shared_locks, plp_i,
													 plpa_i) == 0))
					{
						printf("release_lock_smo() failed, %s:%d./n", F, L - 3);
						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							printf("munmap() failed %s:%d.\n", F, L - 3);
							close_file(1, fd_mo);
							free_strs(2, 1, files);
							return 1;
						}
						free_strs(2, 1, files);
						return 1;
					}

				} while (result_i == WT_RSLK || result_d == WT_RSLK);
			}

			if (shared_locks)
			{
				if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
				{
					__er_munmap(F, L - 3);
					close_file(1, fd_mo);
					free_strs(2, 1, files);
					return 1;
				}

				close_file(1, fd_mo);
				free_strs(2, 1, files);
				return 0;
			}
			free_strs(2, 1, files);
			return 0;
		} /* end of delete file path*/

		if (schema_def)
		{ /* add field to schema */
			/* locks here are released that is why we do not have to release them if an error occurs*/
			/*check if the fields are in limit*/
			int fields_count = count_fields(schema_def, TYPE_) + count_fields(schema_def, T_);

			if (fields_count > MAX_FIELD_NR || hd.sch_d.fields_num + fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
				free_schema(&hd.sch_d);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(3, fd_index, fd_data, fd_mo);
						return 1;
					}
					close_file(3, fd_index, fd_data, fd_mo);
					return 1;
				}
				close_file(2, fd_index, fd_data);
				return 1;
			}

			/*add field provided to the schema*/
			char *buffer = strdup(schema_def);
			char *buff_t = strdup(schema_def);

			if (!add_fields_to_schema(fields_count, buffer, buff_t, &hd.sch_d))
			{
				free_schema(&hd.sch_d);
				free_strs(2, 1, files);
				free(buffer), free(buff_t);
				if (shared_locks)
				{
					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(3, fd_index, fd_data, fd_mo);
						return 1;
					}
					close_file(3, fd_index, fd_data, fd_mo);
					return 1;
				}
				close_file(2, fd_index, fd_data);
				return 1;
			}

			free(buffer), free(buff_t);

			/*here you know that the file is at the beginning*/
			size_t hd_st = compute_size_header((void *)&hd);
			if (hd_st >= MAX_HD_SIZE)
			{
				printf("File definition is bigger than the limit.\n");
				free_schema(&hd.sch_d);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(3, fd_index, fd_data, fd_mo);
						return 1;
					}
					close_file(3, fd_index, fd_data, fd_mo);
					return 1;
				}
				close_file(2, fd_index, fd_data);
				return 1;
			}

			/* acquire lock WR_HEADER */
			if (shared_locks)
			{
				int result = 0;
				do
				{
					if ((result = acquire_lock_smo(&shared_locks, plp, plpa, files[1], 0,
												   MAX_HD_SIZE, WR_HEADER, fd_data)) == 0)
					{
						printf("acquire_lock_smo() failed, %s:%d.\n", F, L - 3);
						free_schema(&hd.sch_d);
						free_strs(2, 1, files);
						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							printf("munmap() failed %s:%d.\n", F, L - 3);
							close_file(3, fd_mo, fd_index, fd_data);
							return 1;
						}
						close_file(3, fd_index, fd_data);
						return 1;
					}
				} while (result == MAX_WTLK || result == WTLK);
			}

			if (!write_header(fd_data, &hd))
			{
				printf("write to file failed, %s:%d.\n", F, L - 2);
				free_schema(&hd.sch_d);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_d = 0;
					do
					{
						if ((result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							printf("release_lock_smo() failed, %s:%d.\n", F, L - 2);
							if (munmap(shared_locks,
									   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(3, fd_index, fd_data, fd_mo);
								return 1;
							}
							close_file(3, fd_index, fd_data, fd_mo);
							return 1;
						}

					} while (result_d == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(3, fd_index, fd_data, fd_mo);
						return 1;
					}
					close_file(3, fd_index, fd_data, fd_mo);
					return 1;
				}
				close_file(2, fd_index, fd_data);
				return 1;
			}

			free_schema(&hd.sch_d);
			free_strs(2, 1, files);

			/*release WR_HEADER lock */
			if (shared_locks)
			{
				int result = 0;
				do
				{
					if ((result = release_lock_smo(&shared_locks, plp, plpa)) == 0)
					{
						printf("release_lock_smo() failed, %s:%d.\n", F, L - 2);
						free_strs(2, 1, files);
						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							printf("munmap() failed %s:%d.\n", F, L - 3);
							close_file(3, fd_index, fd_data, fd_mo);
							return 1;
						}

						close_file(3, fd_mo, fd_index, fd_data);
						return 1;
					}

				} while (result == WTLK);

				/*unmap the memory object*/
				if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
				{
					__er_munmap(F, L - 2);
					close_file(3, fd_index, fd_data, fd_mo);
					return 1;
				}

				printf("data added to schema!\n");
				close_file(3, fd_index, fd_data, fd_mo);
				return 0;
			}
			printf("data added to schema!\n");

			close_file(2, fd_index, fd_data);
			return 0;
		} /* end of add new fields to schema path*/

		if (del)
		{ /* del a record in a file or the all content in the file */

			free_schema(&hd.sch_d);
			/* acquire lock on index file and data file*/
			if (shared_locks)
			{
				int result_i = 0, result_d = 0;
				do
				{
					if (((result_i = acquire_lock_smo(&shared_locks, plp_i, plpa_i, files[0], 0,
													  go_to_EOF(fd_index), WR_IND, fd_index)) == 0) ||
						((result_d = acquire_lock_smo(&shared_locks, plp, plpa, files[1], 0,
													  go_to_EOF(fd_data), WR_REC, fd_data)) == 0))
					{
						printf("acquire_lock_smo() failed, %s:%d.\n", F, L - 2);
						free_strs(2, 1, files);
						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							printf("munmap() failed %s:%d.\n", F, L - 3);
							close_file(3, fd_index, fd_data, fd_mo);
							return 1;
						}

						close_file(3, fd_mo, fd_index, fd_data);
						return 1;
					}
				} while (result_d == MAX_WTLK || result_d == WTLK || result_i == MAX_WTLK || result_i == WTLK);
			}

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
							free_strs(2, 1, files);
							if (shared_locks)
							{
								int result_d = 0, result_i = 0;
								do
								{
									if ((result_d =
											 release_lock_smo(&shared_locks,
															  plp, plpa)) == 0 ||
										(result_i =
											 release_lock_smo(&shared_locks,
															  plp_i, plpa_i)) == 0)
									{
										__er_release_lock_smo(F, L - 4);
										if (munmap(shared_locks,
												   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
										{
											__er_munmap(F, L - 3);
											close_file(3, fd_index,
													   fd_data, fd_mo);
											return 1;
										}
										close_file(3, fd_index,
												   fd_data, fd_mo);
										return 1;
									}

								} while (result_d == WTLK ||
										 result_d == WTLK);

								if (munmap(shared_locks,
										   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 2);
									close_file(3, fd_index,
											   fd_data, fd_mo);
									return 1;
								}
								close_file(3, fd_index,
										   fd_data, fd_mo);
								return 1;
							}
							close_file(2, fd_index, fd_data);
							return 1;
						}

						int buc_t = 0, *pbuck = &buc_t;
						if (!nr_bucket(fd_index, pbuck))
						{
							printf("er index nr,%s:%d.\n", F, L - 4);
							free_strs(2, 1, files);
							if (shared_locks)
							{
								int result_d = 0, result_i = 0;
								do
								{
									if ((result_d =
											 release_lock_smo(&shared_locks,
															  plp, plpa)) == 0 ||
										(result_i =
											 release_lock_smo(&shared_locks,
															  plp_i, plpa_i)) == 0)
									{
										__er_release_lock_smo(F, L - 4);
										if (munmap(shared_locks,
												   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
										{
											__er_munmap(F, L - 3);
											close_file(3, fd_index,
													   fd_data, fd_mo);
											return 1;
										}
										close_file(3, fd_index,
												   fd_data, fd_mo);
										return 1;
									}

								} while (result_d == WTLK ||
										 result_d == WTLK);

								if (munmap(shared_locks,
										   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 2);
									close_file(3, fd_index,
											   fd_data, fd_mo);
									return 1;
								}
								close_file(3, fd_index,
										   fd_data, fd_mo);
								return 1;
							}
							close_file(2, fd_index, fd_data);
							return 1;
						}
						/* create *p_i_nr of ht and write them to file*/
						HashTable *ht = calloc(*p_i_nr, sizeof(HashTable));
						if (!ht)
						{
							__er_calloc(F, L - 4);
							free_strs(2, 1, files);
							if (shared_locks)
							{
								int result_d = 0, result_i = 0;
								do
								{
									if ((result_d =
											 release_lock_smo(&shared_locks,
															  plp, plpa)) == 0 ||
										(result_i =
											 release_lock_smo(&shared_locks,
															  plp_i, plpa_i)) == 0)
									{
										__er_release_lock_smo(F, L - 4);
										if (munmap(shared_locks,
												   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
										{
											__er_munmap(F, L - 3);
											close_file(3, fd_index,
													   fd_data, fd_mo);
											return 1;
										}
										close_file(3, fd_index,
												   fd_data, fd_mo);
										return 1;
									}

								} while (result_d == WTLK ||
										 result_d == WTLK);

								if (munmap(shared_locks,
										   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 2);
									close_file(3, fd_index,
											   fd_data, fd_mo);
									return 1;
								}
								close_file(3, fd_index,
										   fd_data, fd_mo);
								return 1;
							}
							close_file(2, fd_index, fd_data);
							return 1;
						}
						int i = 0;
						for (i = 0; i < *p_i_nr; i++)
						{
							HashTable ht_n = {*pbuck, NULL, write_ht};
							ht[i] = ht_n;
						}

						close_file(1, fd_index);
						// opening with O_TRUNC
						fd_index = open_file(files[0], 1);

						/*  write the index file */

						if (!write_index_file_head(fd_index, *p_i_nr))
						{
							printf("write to file failed,%s:%d",
								   F, L - 2);
							free_strs(2, 1, files);
							free(ht);
							if (shared_locks)
							{
								int result_d = 0, result_i = 0;
								do
								{
									if ((result_d =
											 release_lock_smo(&shared_locks,
															  plp, plpa)) == 0 ||
										(result_i =
											 release_lock_smo(&shared_locks,
															  plp_i, plpa_i)) == 0)
									{
										__er_release_lock_smo(F, L - 4);
										if (munmap(shared_locks,
												   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
										{
											__er_munmap(F, L - 3);
											close_file(3, fd_index,
													   fd_data, fd_mo);
											return 1;
										}
										close_file(3, fd_index,
												   fd_data, fd_mo);
										return 1;
									}

								} while (result_d == WTLK ||
										 result_d == WTLK);

								if (munmap(shared_locks,
										   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 2);
									close_file(3, fd_index,
											   fd_data, fd_mo);
									return 1;
								}
								close_file(3, fd_index,
										   fd_data, fd_mo);
								return 1;
							}
							close_file(2, fd_index, fd_data);
							return 1;
						}

						for (i = 0; i < *p_i_nr; i++)
						{
							if (write_index_body(fd_index, i, &ht[i]) == -1)
							{
								printf("write to file failed. %s:%d.\n", F, L - 2);
								free_strs(2, 1, files);
								free(ht);
								if (shared_locks)
								{
									int result_d = 0, result_i = 0;
									do
									{
										if ((result_d =
												 release_lock_smo(&shared_locks,
																  plp, plpa)) == 0 ||
											(result_i =
												 release_lock_smo(&shared_locks,
																  plp_i, plpa_i)) == 0)
										{
											__er_release_lock_smo(F, L - 4);
											if (munmap(shared_locks,
													   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
											{
												__er_munmap(F, L - 3);
												close_file(3, fd_index,
														   fd_data, fd_mo);
												return 1;
											}
											close_file(3, fd_index,
													   fd_data, fd_mo);
											return 1;
										}

									} while (result_d == WTLK ||
											 result_d == WTLK);

									if (munmap(shared_locks,
											   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 2);
										close_file(3, fd_index,
												   fd_data, fd_mo);
										return 1;
									}
									close_file(3, fd_index,
											   fd_data, fd_mo);
									return 1;
								}
								close_file(2, fd_index, fd_data);
								return 1;
							}
						}

						free_strs(2, 1, files);
						close_file(2, fd_index, fd_data);
						free(ht);
						printf("all record deleted from %s file.\n", file_path);
						/* release the lock */
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i =
										 release_lock_smo(&shared_locks,
														  plp_i, plpa_i)) == 0 ||
									(result_d =
										 release_lock_smo(&shared_locks,
														  plp, plpa)) == 0)
								{
									printf("release_lock_smo() failed,\
									%s:%d.\n",
										   F, L - 8);
									if (munmap(shared_locks,
											   sizeof(lock_info) *
												   MAX_NR_FILE_LOCKABLE) == -1)
									{
										printf("munmap failed, %s:%d",
											   F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);
						}
						if (shared_locks)
						{
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								printf("munmap failed, %s:%d", F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
						}
						return 0;
					}
					default:
						printf("options not valid.\n");
						free_strs(2, 1, files);
						if (shared_locks)
						{
							int result_d = 0, result_i = 0;
							do
							{
								if ((result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0 ||
									(result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0)
								{
									__er_release_lock_smo(F, L - 4);
									if (munmap(shared_locks,
											   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(3, fd_index,
												   fd_data, fd_mo);
										return 1;
									}
									close_file(3, fd_index, fd_data, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								printf("munmap failed, %s:%d", F, L - 3);
								close_file(3, fd_index, fd_data, fd_mo);
								return 1;
							}

							close_file(3, fd_index, fd_data, fd_mo);
							return 1;
						}
						close_file(2, fd_index, fd_data);
						return 1;
					}
				}
			}

			/* delete the record specified by the -D option, in the index file*/
			HashTable *ht = NULL;
			int index = 0;
			int *p_index = &index;
			/* load all indexes in memory */
			if (!read_all_index_file(fd_index, &ht, p_index))
			{
				free_strs(2, 1, files);
				close_file(2, fd_index, fd_data);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							printf("release_lock_smo() failed,%s:%d.\n", F, L - 8);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			int indexes = 0;
			if (!indexes_on_file(fd_index, &indexes))
			{
				printf("indexes_on_file() failed, %s:%d.\n", F, L - 2);
				free_strs(2, 1, files);
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
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							printf("release_lock_smo() failed,%s:%d.\n", F, L - 8);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			if (index_nr >= indexes)
			{
				printf("index out of bound.\n");
				free_strs(2, 1, files);
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
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							printf("release_lock_smo() failed,%s:%d.\n", F, L - 8);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			int key_type = 0;
			void *key_conv = key_converter(key, &key_type);
			Node *record_del = NULL;
			if (key_conv)
			{
				record_del = delete (key_conv, &ht[index_nr], key_type);
				free(key_conv);
			}
			else if (key_type == STR)
			{
				record_del = delete ((void *)key, &ht[index_nr], key_type);
			}
			else
			{
				fprintf(stderr, "error key_converter().\n");
				free_strs(2, 1, files);
				close_file(2, fd_index, fd_data);
				free_ht_array(ht, index);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							printf("release_lock_smo() failed,%s:%d.\n", F, L - 8);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			if (!record_del)
			{
				printf("record %s not found.\n", key);
				free_strs(2, 1, files);
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
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							printf("release_lock_smo() failed,%s:%d.\n", F, L - 8);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			printf("record %s deleted!.\n", key);
			free_ht_node(record_del);
			close_file(1, fd_index);
			fd_index = open_file(files[0], 1); // opening with O_TRUNC

			/*  write the index file */

			if (!write_index_file_head(fd_index, index))
			{
				printf("write to file failed, %s:%d", F, L - 2);
				free_strs(2, 1, files);
				if (ht)
				{
					int i = 0;
					for (i = 0; i < index; i++)
					{
						destroy_hasht(&ht[i]);
					}
					free(ht);
				}
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(3, fd_index, fd_data, fd_mo);
								return 1;
							}
							close_file(3, fd_index, fd_data, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);
					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(3, fd_index, fd_data, fd_mo);
						return 1;
					}
					close_file(3, fd_index, fd_data, fd_mo);
					return 1;
				}
				close_file(2, fd_index, fd_data);
				return 1;
			}

			int i = 0;
			for (i = 0; i < index; i++)
			{

				if (write_index_body(fd_index, i, &ht[i]) == -1)
				{
					printf("write to file failed. %s:%d.\n", F, L - 2);
					free_strs(2, 1, files);
					if (ht)
					{
						int i = 0;
						for (i = 0; i < index; i++)
						{
							destroy_hasht(&ht[i]);
						}
						free(ht);
					}
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(3, fd_index, fd_data, fd_mo);
									return 1;
								}
								close_file(3, fd_index, fd_data, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);
						if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(3, fd_index, fd_data, fd_mo);
							return 1;
						}
						close_file(3, fd_index, fd_data, fd_mo);
						return 1;
					}

					close_file(2, fd_index, fd_data);
					return 1;
				}

				destroy_hasht(&ht[i]);
			}

			free_strs(2, 1, files);
			close_file(2, fd_index, fd_data);
			free(ht);
			if (shared_locks)
			{
				int result_i = 0, result_d = 0;
				do
				{
					if ((result_i = release_lock_smo(&shared_locks,
													 plp_i, plpa_i)) == 0 ||
						(result_d = release_lock_smo(&shared_locks,
													 plp, plpa)) == 0)
					{
						__er_release_lock_smo(F, L - 5);
						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}

						close_file(1, fd_mo);
						return 1;
					}

				} while (result_d == WTLK || result_i == WTLK);

				if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
				{
					__er_munmap(F, L - 2);
					close_file(1, fd_mo);
					return 1;
				}
				close_file(1, fd_mo);
				return 0;
			}
			return 0;
		} /*end of del path (either del the all content or a record )*/

		if (!update && data_to_add)
		{ /* append data to the specified file*/
			int fields_count = count_fields(data_to_add, TYPE_) + count_fields(data_to_add, T_);

			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
				free_strs(2, 1, files);
				free_schema(&hd.sch_d);
				close_file(2, fd_index, fd_data);
				if (shared_locks)
				{
					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 3);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			char *buffer = strdup(data_to_add);
			char *buf_t = strdup(data_to_add);
			char *buf_v = strdup(data_to_add);

			struct Record_f *rec = NULL;

			unsigned char check = perform_checks_on_schema(buffer, buf_t, buf_v, fields_count,
														   fd_data, file_path, &rec, &hd);

			if (check == SCHEMA_ERR || check == 0)
			{
				free_strs(2, 1, files);
				free_schema(&hd.sch_d);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				if (shared_locks)
				{
					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 3);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			if (!rec)
			{
				printf("error creating record, %s:%d\n", F, L - 1);
				free(buffer), free(buf_t), free(buf_v);
				free_strs(2, 1, files);
				free_schema(&hd.sch_d);
				close_file(2, fd_index, fd_data);
				if (shared_locks)
				{
					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 3);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			if (check == SCHEMA_NW)
			{ /*if the schema is new we update the header*/
				// check the header size
				if (compute_size_header((void *)&hd) >= MAX_HD_SIZE)
				{
					printf("File definition is bigger than the limit.\n");
					free(buffer), free(buf_t), free(buf_v);
					free_strs(2, 1, files);
					free_schema(&hd.sch_d), close_file(2, fd_index, fd_data);
					free_record(rec, rec->fields_num);
					if (shared_locks)
					{
						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				/*aquire lock WR_HEADER*/
				if (shared_locks)
				{
					int result_d = 0;
					do
					{
						if ((result_d = acquire_lock_smo(&shared_locks, plp, plpa,
														 files[1], 0, MAX_HD_SIZE, WR_HEADER, fd_data)) == 0)
						{
							__er_acquire_lock_smo(F, L - 3);
							free(buffer), free(buf_t), free(buf_v);
							free_strs(2, 1, files);
							free_schema(&hd.sch_d);
							free_record(rec, rec->fields_num);
							close_file(2, fd_index, fd_data);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_d == MAX_WTLK);
				}

				/*set the file pointer at the start*/
				if (begin_in_file(fd_data) == -1)
				{
					__er_file_pointer(F, L - 1);
					free(buffer), free(buf_t), free(buf_v);
					free_strs(2, 1, files);
					free_schema(&hd.sch_d);
					close_file(2, fd_index, fd_data);
					free_record(rec, rec->fields_num);
					if (shared_locks)
					{ /*release the locks before exit*/
						int result_d = 0;
						do
						{
							if ((result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}
						} while (result_d == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				if (!write_header(fd_data, &hd))
				{
					__er_write_to_file(F, L - 1);
					free_strs(2, 1, files);
					free(buffer), free(buf_t), free(buf_v);
					free_schema(&hd.sch_d);
					close_file(2, fd_index, fd_data);
					free_record(rec, rec->fields_num);
					if (shared_locks)
					{ /*release the locks before exit*/
						int result_d = 0;
						do
						{
							if ((result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}
						} while (result_d == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}
				/*release the lock WR_HEADER*/
				if (shared_locks)
				{
					int result_d = 0;
					do
					{
						if ((result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							free_strs(2, 1, files);
							free(buffer), free(buf_t), free(buf_v);
							close_file(2, fd_index, fd_data);
							free_record(rec, rec->fields_num);
							free_schema(&hd.sch_d);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
					} while (result_d == WTLK);
				}
			} /* end of update schema branch*/

			free_schema(&hd.sch_d);

			/*aquire  the lock WR_REC*/
			if (shared_locks)
			{
				int result_i = 0, result_d = 0;
				do
				{
					off_t fd_i_s = get_file_size(fd_index, NULL);
					off_t fd_d_s = get_file_size(fd_data, NULL);
					if (fd_i_s == -1)
					{
						printf("file size invalid %s:%d", F, L - 3);
						free_strs(2, 1, files);
						free(buffer), free(buf_t), free(buf_v);
						close_file(2, fd_index, fd_data);
						free_record(rec, rec->fields_num);
						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}

					if (((result_i = acquire_lock_smo(&shared_locks, plp_i, plpa_i, files[0], 0,
													  fd_i_s, WR_IND, fd_index)) == 0) ||
						((result_d = acquire_lock_smo(&shared_locks, plp, plpa, files[1], 0,
													  fd_d_s, WR_REC, fd_data)) == 0))
					{
						__er_acquire_lock_smo(F, L - 5);
						free_strs(2, 1, files);
						free(buffer), free(buf_t), free(buf_v);
						close_file(2, fd_index, fd_data);
						free_record(rec, rec->fields_num);
						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}

						close_file(1, fd_mo);
						return 1;
					}
				} while (result_d == MAX_WTLK || result_d == WTLK || result_i == MAX_WTLK || result_i == WTLK);
			}

			off_t eof = go_to_EOF(fd_data);
			if (eof == -1)
			{
				__er_file_pointer(F, L - 1);
				free_strs(2, 1, files);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				free_record(rec, rec->fields_num);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
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
				free_record(rec, rec->fields_num);
				free(buffer), free(buf_t), free(buf_v);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}
			int key_type = 0;
			void *key_conv = key_converter(key, &key_type);
			if (key_type == UINT && !key_conv)
			{
				fprintf(stderr, "error to convert key");
				return 1;
			}
			else if (key_type == UINT)
			{
				if (key_conv)
				{
					if (!set(key_conv, key_type, eof, &ht[0]))
					{
						free(key_conv);
						close_file(2, fd_index, fd_data);
						free(files[0]), free(files[1]), free(files);
						free_record(rec, rec->fields_num);
						free(buffer), free(buf_t), free(buf_v);
						free_ht_array(ht, index);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks,
									   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

						return 1;
					}
					free(key_conv);
				}
			}
			else if (key_type == STR)
			{
				/*create a new key value pair in the hash table*/
				if (!set((void *)key, key_type, eof, &ht[0]))
				{
					close_file(2, fd_index, fd_data);
					free(files[0]), free(files[1]), free(files);
					free_record(rec, rec->fields_num);
					free(buffer), free(buf_t), free(buf_v);
					free_ht_array(ht, index);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}

					return 1;
				}
			}

			if (!write_file(fd_data, rec, 0, update))
			{
				printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
				free(files[0]), free(files[1]), free(files);
				free(buffer), free(buf_t), free(buf_v);
				close_file(2, fd_index, fd_data);
				free_record(rec, rec->fields_num);
				if (ht)
				{
					int i = 0;
					for (i = 0; i < index; i++)
					{
						destroy_hasht(&ht[i]);
					}
					free(ht);
				}
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}

				return 1;
			}

			free(buffer), free(buf_t), free(buf_v);
			close_file(1, fd_index);
			free_record(rec, rec->fields_num);

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
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}

				return 1;
			}

			int i = 0;
			for (i = 0; i < index; i++)
			{

				if (write_index_body(fd_index, i, &ht[i]) == -1)
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
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}

					return 1;
				}

				destroy_hasht(&ht[i]);
			}

			close_file(2, fd_index, fd_data);
			free(ht);
			printf("record %s wrote succesfully.\n", key);
			/*release the locks WR_REC and WR_IND*/
			if (shared_locks)
			{
				int result_i = 0, result_d = 0;
				do
				{
					if ((result_i = release_lock_smo(&shared_locks,
													 plp_i, plpa_i)) == 0 ||
						(result_d = release_lock_smo(&shared_locks,
													 plp, plpa)) == 0)
					{
						__er_release_lock_smo(F, L - 5);
						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}

				} while (result_d == WTLK || result_i == WTLK);

				if (munmap(shared_locks, sizeof(lock_info) *
											 MAX_NR_FILE_LOCKABLE) == -1)
				{
					__er_munmap(F, L - 2);
					close_file(1, fd_mo);
					return 1;
				}
				close_file(1, fd_mo);
				return 0;
			}

			return 0;
		}

		if (update && data_to_add && key)
		{ /* updating an existing record */

			// 1 - check the schema with the one on file
			int fields_count = count_fields(data_to_add, TYPE_) + count_fields(data_to_add, T_);

			if (fields_count > MAX_FIELD_NR)
			{
				printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
				free_schema(&hd.sch_d);
				free_strs(2, 1, files);
				close_file(2, fd_index, fd_data);
				return 1;
			}

			char *buffer = strdup(data_to_add);
			char *buf_t = strdup(data_to_add);
			char *buf_v = strdup(data_to_add);

			struct Record_f *rec = NULL;
			unsigned char check = perform_checks_on_schema(buffer, buf_t, buf_v, fields_count,
														   fd_data, file_path, &rec, &hd);
			if (check == SCHEMA_ERR || check == 0)
			{
				free_schema(&hd.sch_d);
				close_file(2, fd_index, fd_data);
				free(buffer), free(buf_t), free(buf_v);
				free_strs(2, 1, files);
				return 1;
			}

			/* updating the schema if it is new */

			if (check == SCHEMA_NW)
			{
				size_t hd_st = compute_size_header((void *)&hd);
				if (hd_st > MAX_HD_SIZE)
				{
					printf("header is bigger than the limit, main.c l %d\n", __LINE__ - 2);
					free_schema(&hd.sch_d);
					free_strs(2, 1, files);
					close_file(2, fd_index, fd_data);
					free(buffer), free(buf_t), free(buf_v);
					free_record(rec, rec->fields_num);
					return 1;
				}

				/*acquire WR_HEADER lock */
				if (shared_locks)
				{
					int result_d = 0;
					do
					{
						if ((result_d = acquire_lock_smo(&shared_locks, plp, plpa, files[1],
														 0, MAX_HD_SIZE, WR_HEADER, fd_data)) == 0)
						{
							__er_acquire_lock_smo(F, L - 3);
							free_schema(&hd.sch_d);
							free_strs(2, 1, files);
							close_file(2, fd_index, fd_data);
							free(buffer), free(buf_t), free(buf_v);
							free_record(rec, rec->fields_num);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_d == MAX_WTLK);
				}

				if (begin_in_file(fd_data) == -1)
				{
					__er_file_pointer(F, L - 1);
					free_schema(&hd.sch_d);
					close_file(2, fd_index, fd_data);
					free(buffer), free(buf_t), free(buf_v);
					free_strs(2, 1, files);
					free_record(rec, rec->fields_num);
					if (shared_locks)
					{
						int result_d = 0;
						do
						{
							if ((result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 3);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}
						} while (result_d == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				if (!write_header(fd_data, &hd))
				{
					printf("can`t write header, main.c l %d.\n", __LINE__ - 1);
					free_schema(&hd.sch_d);
					close_file(2, fd_index, fd_data);
					free(buffer), free(buf_t), free(buf_v);
					free_record(rec, rec->fields_num);
					free_strs(2, 1, files);
					if (shared_locks)
					{ /*release the locks before exit*/
						int result_d = 0;
						do
						{
							if ((result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 3);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}
						} while (result_d == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				/* setting the file position back to the top*/
				if (begin_in_file(fd_data) == -1)
				{
					__er_file_pointer(F, L - 1);
					free(buffer), free(buf_t), free(buf_v);
					free_schema(&hd.sch_d);
					close_file(2, fd_index, fd_data);
					free_record(rec, rec->fields_num);
					free_strs(2, 1, files);
					if (shared_locks)
					{ /*release the locks before exit*/
						int result_d = 0;
						do
						{
							if ((result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 3);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}
						} while (result_d == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}

					return 1;
				}

				/*release WR_HEADER LOCK*/
				if (shared_locks)
				{
					int result_d = 0;
					do
					{
						if ((result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 3);
							free(buffer), free(buf_t), free(buf_v);
							free_schema(&hd.sch_d);
							close_file(2, fd_index, fd_data);
							free_record(rec, rec->fields_num);
							free_strs(2, 1, files);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
					} while (result_d == WTLK);
				}

			} /*end of the update schema path in case the schema is new*/

			free(buf_t), free(buf_v);
			buf_t = NULL, buf_v = NULL;

			/* 2 - schema is good, thus load the old record into memory and update the fields if any
			 -- at this point you already checked the header, and you have an updated header,
				anid the updated record in memory*/

			/*acquire WR_REC and WR_IND locks*/
			if (shared_locks)
			{
				int result_i = 0, result_d = 0;
				do
				{
					off_t fd_i_s = get_file_size(fd_index, NULL);
					off_t fd_d_s = get_file_size(fd_data, NULL);

					if (((result_i = acquire_lock_smo(&shared_locks, plp_i, plpa_i, files[0], 0,
													  fd_i_s, WR_IND, fd_index)) == 0) ||
						((result_d = acquire_lock_smo(&shared_locks, plp, plpa, files[1], 0,
													  fd_d_s, WR_REC, fd_data)) == 0))
					{
						__er_acquire_lock_smo(F, L - 5);
						free_strs(2, 1, files);
						free(buffer);
						close_file(2, fd_index, fd_data);
						free_record(rec, rec->fields_num);
						free_schema(&hd.sch_d);
						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}

						close_file(1, fd_mo);
						return 1;
					}
				} while (result_d == MAX_WTLK || result_d == WTLK || result_i == MAX_WTLK || result_i == WTLK);
			}

			HashTable ht = {0, NULL};
			HashTable *p_ht = &ht;
			if (!read_index_nr(0, fd_index, &p_ht))
			{
				printf("index file reading failed. %s:%d.\n", F, L - 1);
				free(buffer), close_file(2, fd_index, fd_data);
				free_record(rec, rec->fields_num);
				free_schema(&hd.sch_d);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}

				return 1;
			}

			off_t offset = 0;
			int key_type = 0;
			void *key_conv = key_converter(key, &key_type);
			if (key_type == UINT && !key_conv)
			{
				fprintf(stderr, "error to convert key");
				free(buffer), close_file(2, fd_index, fd_data);
				free_record(rec, rec->fields_num);
				free_schema(&hd.sch_d);
				destroy_hasht(p_ht);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}

				return 1;
			}
			else if (key_type == UINT)
			{
				if (key_conv)
				{
					offset = get(key_conv, p_ht, key_type); /*look for the key in the ht */
					free(key_conv);
				}
			}
			else if (key_type == STR)
			{
				offset = get((void *)key, p_ht, key_type); /*look for the key in the ht */
			}

			if (offset == -1)
			{
				printf("record not found.\n");
				free(buffer), close_file(2, fd_index, fd_data);
				free_record(rec, rec->fields_num);
				free_schema(&hd.sch_d);
				destroy_hasht(p_ht);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}

				return 1;
			}

			destroy_hasht(p_ht);

			/*find record in the file*/
			if (find_record_position(fd_data, offset) == -1)
			{
				__er_file_pointer(F, L - 1);
				free(buffer), close_file(2, fd_index, fd_data);
				free_schema(&hd.sch_d);
				free_record(rec, rec->fields_num);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}

				return 1;
			}

			/*read the old record, aka the record that we want to update*/
			struct Record_f *rec_old = read_file(fd_data, file_path);
			if (!rec_old)
			{
				printf("reading record failed main.c l %d.\n", __LINE__ - 2);
				free(buffer), close_file(2, fd_index, fd_data);
				free_schema(&hd.sch_d);
				free_record(rec, rec->fields_num);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			/*after each record in the file there is the offset of the next part of the record
				(if any) in the file*/
			off_t updated_rec_pos = get_update_offset(fd_data);
			if (updated_rec_pos == -1)
			{
				__er_file_pointer(F, L - 1);
				free(buffer), close_file(2, fd_index, fd_data);
				free_record(rec, rec->fields_num);
				free_record(rec_old, rec_old->fields_num);
				free_schema(&hd.sch_d);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}

				return 1;
			}

			/*if update_rec_pos is bigger than 0, it means that this record is
				in different locations in the file,here we check for this case,
				and if the record is fragmented we read all the data
				and we store in the recs_old */

			struct Record_f **recs_old = NULL;
			off_t *pos_u = NULL;
			if (updated_rec_pos > 0)
			{
				free(buffer);
				int index = 2;
				int pos_i = 2;
				recs_old = calloc(index, sizeof(struct Record_f *));
				if (!recs_old)
				{
					__er_calloc(F, L - 2);
					close_file(2, fd_index, fd_data);
					free_schema(&hd.sch_d);
					free_record(rec_old, rec_old->fields_num);
					free_record(rec, rec->fields_num);
					free_strs(2, 1, files);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				pos_u = calloc(pos_i, sizeof(off_t));
				if (!pos_u)
				{
					__er_calloc(F, L - 2);
					close_file(2, fd_index, fd_data);
					free_schema(&hd.sch_d);
					free_record(rec_old, rec_old->fields_num);
					free_record(rec, rec->fields_num);
					free_strs(2, 1, files);
					free_record_array(index, &recs_old);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
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
					free_schema(&hd.sch_d);
					free_record(rec, rec->fields_num);
					free_strs(2, 1, files);
					free_record_array(index, &recs_old);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}

					return 1;
				}

				struct Record_f *rec_old_s = read_file(fd_data, file_path);
				if (!rec_old_s)
				{
					printf("error reading file, main.c l %d.\n", __LINE__ - 2);
					close_file(2, fd_index, fd_data);
					free_schema(&hd.sch_d);
					free_record(rec, rec->fields_num);
					free(pos_u);
					free_strs(2, 1, files);
					free_record_array(index, &recs_old);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				recs_old[0] = rec_old;
				recs_old[1] = rec_old_s;

				/*at this point we have the first two fragment of the record,
					 but potentially they could be many fragments,
					so we check with a loop that stops when the read of the
					update_rec_pos gives 0 or -1 (error)*/
				while ((updated_rec_pos = get_update_offset(fd_data)) > 0)
				{
					index++, pos_i++;
					struct Record_f **recs_old_n = realloc(recs_old,
														   index * sizeof(struct Record_f *));
					if (!recs_old_n)
					{
						printf("realloc failed, main.c l %d.\n", __LINE__ - 2);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						free_schema(&hd.sch_d);
						free_record(rec, rec->fields_num);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}

					recs_old = recs_old_n;

					off_t *pos_u_n = realloc(pos_u, pos_i * sizeof(off_t));
					if (!pos_u_n)
					{
						printf("realloc failed for positions, %s%d.\n", F, L - 2);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						free_schema(&hd.sch_d);
						free_record(rec, rec->fields_num);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}

					pos_u = pos_u_n;
					pos_u[pos_i - 1] = updated_rec_pos;

					struct Record_f *rec_old_new = read_file(fd_data, file_path);
					if (!rec_old_new)
					{
						printf("error reading file, %s:%d.\n", F, L - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						free_schema(&hd.sch_d);
						free_record(rec, rec->fields_num);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}

					recs_old[index - 1] = rec_old_new;
				}

				/* here we have the all record in memory and we have
					to  check which fields in the record we have to update*/

				char *positions = calloc(index, sizeof(char));
				if (!positions)
				{
					__er_calloc(F, L - 2);
					close_file(2, fd_index, fd_data);
					free(pos_u);
					free_record(rec, rec->fields_num);
					free_strs(2, 1, files);
					free_record_array(index, &recs_old);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}
				/* this function check all records from the file
				   against the new record setting the values that we have to update
				   and populates in  the char array positions. If an element contain 'y'
				   you have to update the record at that index position of 'y'. */

				find_fields_to_update(recs_old, positions, rec, index);

				if (positions[0] != 'n' && positions[0] != 'y')
				{
					printf("check on fields failed, %s:%d.\n", F, L - 1);
					close_file(2, fd_index, fd_data);
					free(pos_u), free(positions);
					free_schema(&hd.sch_d);
					free_record(rec, rec->fields_num);
					free_strs(2, 1, files);
					free_record_array(index, &recs_old);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
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
						free_record(rec, rec->fields_num);
						free_schema(&hd.sch_d);
						free(positions);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
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
							printf("error file pointer, %s:%d.\n", F, L - 2);
							close_file(2, fd_index, fd_data);
							free(pos_u);
							free_record(rec, rec->fields_num);
							free_schema(&hd.sch_d);
							free(positions);
							free_strs(2, 1, files);
							free_record_array(index, &recs_old);
							if (shared_locks)
							{
								int result_i = 0, result_d = 0;
								do
								{
									if ((result_i = release_lock_smo(
											 &shared_locks,
											 plp_i, plpa_i)) == 0 ||
										(result_d = release_lock_smo(
											 &shared_locks,
											 plp, plpa)) == 0)
									{
										__er_release_lock_smo(F, L - 5);
										if (munmap(shared_locks,
												   sizeof(lock_info) *
													   MAX_NR_FILE_LOCKABLE) == -1)
										{
											__er_munmap(F, L - 3);
											close_file(1, fd_mo);
											return 1;
										}
										close_file(1, fd_mo);
										return 1;
									}

								} while (result_d == WTLK || result_i == WTLK);

								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 2);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}
							return 1;
						}
					}

					if (!write_file(fd_data, recs_old[i], right_update_pos, update))
					{
						printf("error write file, %s:%d.\n", F, L - 1);
						close_file(2, fd_index, fd_data);
						free_schema(&hd.sch_d);
						free(pos_u);
						free(positions);
						free_record(rec, rec->fields_num);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}
				}

				if (check == SCHEMA_NW && updates > 0)
				{
					struct Record_f *new_rec = NULL;
					if (!create_new_fields_from_schema(recs_old, rec, &hd.sch_d,
													   index, &new_rec, file_path))
					{
						printf("create new fileds failed,  %s:%d.\n", F, L - 1);
						close_file(2, fd_index, fd_data);
						free_schema(&hd.sch_d);
						free(pos_u);
						free(positions);
						free_record(rec, rec->fields_num);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}

					off_t eof = go_to_EOF(fd_data); /* file pointer to the end*/
					if (eof == -1)
					{
						__er_file_pointer(F, L - 1);
						close_file(2, fd_index, fd_data);
						free_schema(&hd.sch_d);
						free(pos_u);
						free(positions);
						free_record(new_rec, new_rec->fields_num);
						free_record(rec, rec->fields_num);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}

					/*writing the new part of the schema to the file*/
					if (!write_file(fd_data, new_rec, 0, 0))
					{
						printf("write to file failed, %s:%d.\n", F, L - 1);
						close_file(2, fd_index, fd_data);
						free_schema(&hd.sch_d);
						free_record(new_rec, new_rec->fields_num);
						free_record(rec, rec->fields_num);
						free(pos_u);
						free(positions);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}

					free_record(new_rec, new_rec->fields_num);
					/*the position of new_rec in the old part of the record
					 was already set at line ????*/
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
						free_schema(&hd.sch_d);
						free(positions);
						free_record(rec, rec->fields_num);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}
							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}

					/*find the position of the last piece of the record*/
					off_t initial_pos = 0;
					if ((initial_pos = find_record_position(fd_data, pos_u[index - 1])) == -1)
					{
						__er_file_pointer(F, L - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						free_schema(&hd.sch_d);
						free(positions);
						free_record(rec, rec->fields_num);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}
							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}

					/*re-write the record*/
					if (write_file(fd_data, recs_old[index - 1], eof, update) == -1)
					{
						printf("write to file failed, %s:%d.\n", F, L - 1);
						close_file(2, fd_index, fd_data);
						free(pos_u);
						free_schema(&hd.sch_d);
						free(positions);
						free_record(rec, rec->fields_num);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}
							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
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
						free_schema(&hd.sch_d);
						free_record(rec, rec->fields_num);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}

					/*create the new record*/
					struct Record_f *new_rec = NULL;
					if (!create_new_fields_from_schema(recs_old, rec, &hd.sch_d,
													   index, &new_rec, file_path))
					{
						printf("create new fields failed,  main.c l %d.\n", __LINE__ - 2);
						close_file(2, fd_index, fd_data);
						free_schema(&hd.sch_d);
						free(pos_u);
						free(positions);
						free_record(rec, rec->fields_num);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}

					/*write the actual new data*/
					if (!write_file(fd_data, new_rec, 0, 0))
					{
						printf("write to file failed, %s:%d.\n", F, L - 1);
						close_file(2, fd_index, fd_data);
						free_schema(&hd.sch_d);
						free_record(new_rec, new_rec->fields_num);
						free(pos_u);
						free(positions);
						free_record(rec, rec->fields_num);
						free_strs(2, 1, files);
						free_record_array(index, &recs_old);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}
					free_record(new_rec, new_rec->fields_num);
				}

				free_schema(&hd.sch_d);
				printf("record %s updated!\n", key);
				close_file(2, fd_index, fd_data);
				free(pos_u);
				free(positions);
				free_record(rec, rec->fields_num);
				free_strs(2, 1, files);
				free_record_array(index, &recs_old);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}

				return 0;
			} /*end of if(update_pos > 0)*/

			free_schema(&hd.sch_d);

			/*updated_rec_pos is 0, THE RECORD IS ALL IN ONE PLACE */
			struct Record_f *new_rec = NULL;
			unsigned char comp_rr = compare_old_rec_update_rec(&rec_old, rec, &new_rec, file_path, check, buffer, fields_count);
			free(buffer);
			if (comp_rr == 0)
			{
				printf(" compare records failed, %s:%d.\n", F, L - 4);
				close_file(2, fd_index, fd_data);
				free_record(rec, rec->fields_num);
				free_record(rec_old, rec_old->fields_num);
				free_strs(2, 1, files);
				if (new_rec)
				{
					free_record(new_rec, new_rec->fields_num);
				}
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
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
					free_record(rec, rec->fields_num);
					free_record(rec_old, rec_old->fields_num);
					free_strs(2, 1, files);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				// write the updated record to the file
				if (!write_file(fd_data, rec_old, 0, update))
				{
					printf("write to file failed, %s:%d.\n", F, L - 1);
					close_file(2, fd_index, fd_data);
					free_record(rec, rec->fields_num);
					free_record(rec_old, rec_old->fields_num);
					free_strs(2, 1, files);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				printf("record %s updated!\n", key);
				close_file(2, fd_index, fd_data);
				free_record(rec, rec->fields_num);
				free_record(rec_old, rec_old->fields_num);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
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
					free_record(rec, rec->fields_num);
					free_record(rec_old, rec_old->fields_num);
					free_record(new_rec, new_rec->fields_num);
					free_strs(2, 1, files);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				// put the position back to the record
				if (find_record_position(fd_data, offset) == -1)
				{
					__er_file_pointer(F, L - 1);
					close_file(2, fd_index, fd_data);
					free_record(rec, rec->fields_num);
					free_record(rec_old, rec_old->fields_num);
					free_record(new_rec, new_rec->fields_num);
					free_strs(2, 1, files);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}

					return 1;
				}

				// update the old record :
				if (!write_file(fd_data, rec_old, eof, update))
				{
					printf("can't write record, %s:%d.\n", __FILE__, __LINE__ - 1);
					close_file(2, fd_index, fd_data);
					free_record(rec, rec->fields_num);
					free_record(rec_old, rec_old->fields_num);
					free_record(new_rec, new_rec->fields_num);
					free_strs(2, 1, files);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				eof = go_to_EOF(fd_data);
				if (eof == -1)
				{
					__er_file_pointer(F, L - 1);
					close_file(2, fd_index, fd_data);
					free_record(rec, rec->fields_num);
					free_record(rec_old, rec_old->fields_num);
					free_record(new_rec, new_rec->fields_num);
					free_strs(2, 1, files);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				/*passing update as 0 becuase is a "new_rec", (right most paramaters) */
				if (!write_file(fd_data, new_rec, 0, 0))
				{
					printf("can't write record, main.c l %d.\n", __LINE__ - 1);
					close_file(2, fd_index, fd_data);
					free_record(rec, rec->fields_num);
					free_record(rec_old, rec_old->fields_num);
					free_record(new_rec, new_rec->fields_num);
					free_strs(2, 1, files);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				printf("record %s updated!\n", key);
				close_file(2, fd_index, fd_data);
				free_record(rec, rec->fields_num);
				free_record(rec_old, rec_old->fields_num);
				free_record(new_rec, new_rec->fields_num);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 0;
			}

			return 0; /*this should be unreachable*/
		} /*end of update path*/

		/*	 reading the file to show data  	*/
		if (list_def)
		{ /* show file definitions */
			print_schema(hd.sch_d);
			close_file(1, fd_data);
			free_schema(&hd.sch_d);
			free_strs(2, 1, files);
			return 0;
		}

		/*display the keys in the file*/
		if (list_keys)
		{
			if (shared_locks)
			{
				int result_i = 0;
				do
				{
					off_t fd_i_s = get_file_size(fd_index, NULL);
					if ((result_i = acquire_lock_smo(&shared_locks, plp_i, plpa_i,
													 files[0], 0, fd_i_s, RD_IND, fd_index)) == 0)
					{
						__er_acquire_lock_smo(F, L - 3);
						free_strs(2, 1, files);
						close_file(2, fd_index, fd_data);
						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}

				} while (result_i == WTLK || result_i == MAX_WTLK);
			}

			HashTable ht = {0, NULL};
			HashTable *p_ht = &ht;
			if (!read_index_nr(index_nr, fd_index, &p_ht))
			{
				printf("reading index file failed, %s:%d.\n", F, L - 1);
				close_file(2, fd_index, fd_data);
				free_schema(&hd.sch_d);
				free_strs(2, 1, files);
				if (shared_locks)
				{ /*release the locks before exit*/
					int result_d = 0;
					do
					{
						if ((result_d = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0)
						{
							__er_release_lock_smo(F, L - 3);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
					} while (result_d == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 3);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			struct Keys_ht *keys_data = keys(p_ht);
			char keyboard = '0';
			int end = len(ht), i = 0, j = 0;
			for (i = 0, j = i; i < end; i++)
			{
				switch (keys_data->types[i])
				{
				case STR:
					printf("%d. %s\n", ++j, (char *)keys_data->k[i]);
					break;
				case UINT:
					printf("%d. %u\n", ++j, *(uint32_t *)keys_data->k[i]);
					break;
				default:
					break;
				}
				if (i > 0 && (i % 20 == 0))
					printf("press return key. . .\n"
						   "enter q to quit . . .\n"),
						keyboard = (char)getc(stdin);

				if (keyboard == 'q')
					break;
			}

			destroy_hasht(p_ht);
			free_schema(&hd.sch_d);
			close_file(2, fd_index, fd_data);
			free_strs(2, 1, files);
			free_keys_data(keys_data);
			if (shared_locks)
			{ /*release the locks before exit*/
				int result_d = 0;
				do
				{
					if ((result_d = release_lock_smo(&shared_locks,
													 plp_i, plpa_i)) == 0)
					{
						__er_release_lock_smo(F, L - 3);
						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
				} while (result_d == WTLK);

				if (munmap(shared_locks, sizeof(lock_info) *
											 MAX_NR_FILE_LOCKABLE) == -1)
				{
					__er_munmap(F, L - 3);
					close_file(1, fd_mo);
					return 1;
				}
				close_file(1, fd_mo);
				return 1;
			}
			return 0;
		}

		free_schema(&hd.sch_d);

		if (key) /*display record*/
		{
			/*acquire WR_REC and WR_IND locks*/
			if (shared_locks)
			{
				int result_i = 0, result_d = 0;
				do
				{
					off_t fd_i_s = get_file_size(fd_index, NULL);
					off_t fd_d_s = get_file_size(fd_data, NULL);

					if (((result_i = acquire_lock_smo(&shared_locks, plp_i, plpa_i, files[0], 0,
													  fd_i_s, RD_IND, fd_index)) == 0) ||
						((result_d = acquire_lock_smo(&shared_locks, plp, plpa, files[1], 0,
													  fd_d_s, RD_REC, fd_data)) == 0))
					{
						__er_acquire_lock_smo(F, L - 5);
						close_file(2, fd_index, fd_data);
						free_strs(2, 1, files);
						if (munmap(shared_locks,
								   sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
				} while (result_d == MAX_WTLK || result_d == WTLK || result_i == MAX_WTLK || result_i == WTLK);
			}

			HashTable ht = {0, NULL};
			HashTable *p_ht = &ht;
			if (!read_index_nr(0, fd_index, &p_ht))
			{
				printf("reading index file failed, %s:%d.\n", F, L - 1);
				close_file(2, fd_index, fd_data);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			off_t offset = 0;
			int key_type = 0;
			void *key_conv = key_converter(key, &key_type);
			if (key_type == UINT && !key_conv)
			{
				fprintf(stderr, "error to convert key");
				destroy_hasht(p_ht);
				close_file(2, fd_index, fd_data);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}

				return 1;
			}
			else if (key_type == UINT)
			{
				if (key_conv)
				{
					offset = get(key_conv, p_ht, key_type); /*look for the key in the ht */
					free(key_conv);
				}
			}
			else if (key_type == STR)
			{
				offset = get((void *)key, p_ht, key_type); /*look for the key in the ht */
			}

			if (offset == -1)
			{
				printf("record not found.\n");
				destroy_hasht(p_ht);
				close_file(2, fd_index, fd_data);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			destroy_hasht(p_ht);
			if (find_record_position(fd_data, offset) == -1)
			{
				__er_file_pointer(F, L - 1);
				close_file(2, fd_index, fd_data);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			struct Record_f *rec = read_file(fd_data, file_path);
			if (!rec)
			{
				printf("read record failed, main.c l %d.\n", __LINE__ - 1);
				close_file(2, fd_data, fd_index);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			off_t offt_rec_up_pos = get_file_offset(fd_data);
			off_t update_rec_pos = get_update_offset(fd_data);
			if (update_rec_pos == -1)
			{
				close_file(2, fd_index, fd_data);
				free_record(rec, rec->fields_num);
				close_file(2, fd_data, fd_index);
				free_strs(2, 1, files);
				if (shared_locks)
				{
					int result_i = 0, result_d = 0;
					do
					{
						if ((result_i = release_lock_smo(&shared_locks,
														 plp_i, plpa_i)) == 0 ||
							(result_d = release_lock_smo(&shared_locks,
														 plp, plpa)) == 0)
						{
							__er_release_lock_smo(F, L - 5);
							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 3);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}

					} while (result_d == WTLK || result_i == WTLK);

					if (munmap(shared_locks, sizeof(lock_info) *
												 MAX_NR_FILE_LOCKABLE) == -1)
					{
						__er_munmap(F, L - 2);
						close_file(1, fd_mo);
						return 1;
					}
					close_file(1, fd_mo);
					return 1;
				}
				return 1;
			}

			int counter = 1;
			struct Record_f **recs = NULL;

			if (update_rec_pos > 0)
			{
				recs = calloc(counter, sizeof(struct Record_f *));
				if (!recs)
				{
					printf("calloc failed, main.c l %d.\n", __LINE__ - 2);
					free_record(rec, rec->fields_num);
					close_file(2, fd_data, fd_index);
					free_strs(2, 1, files);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				recs[0] = rec;

				// set the file pointer back to update_rec_pos (we need to read it)
				//  again for the reading process to be successful
				if (find_record_position(fd_data, offt_rec_up_pos) == -1)
				{
					__er_file_pointer(F, L - 1);
					free_record_array(counter, &recs);
					close_file(2, fd_data, fd_index);
					free_strs(2, 1, files);
					if (shared_locks)
					{
						int result_i = 0, result_d = 0;
						do
						{
							if ((result_i = release_lock_smo(&shared_locks,
															 plp_i, plpa_i)) == 0 ||
								(result_d = release_lock_smo(&shared_locks,
															 plp, plpa)) == 0)
							{
								__er_release_lock_smo(F, L - 5);
								if (munmap(shared_locks, sizeof(lock_info) *
															 MAX_NR_FILE_LOCKABLE) == -1)
								{
									__er_munmap(F, L - 3);
									close_file(1, fd_mo);
									return 1;
								}
								close_file(1, fd_mo);
								return 1;
							}

						} while (result_d == WTLK || result_i == WTLK);

						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 2);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}
					return 1;
				}

				while ((update_rec_pos = get_update_offset(fd_data)) > 0)
				{
					counter++;
					recs = realloc(recs, counter * sizeof(struct Record_f *));
					if (!recs)
					{
						printf("realloc failed, main.c l %d.\n", __LINE__ - 2);
						free_record_array(counter, &recs);
						close_file(2, fd_data, fd_index);
						free_strs(2, 1, files);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}

					if (find_record_position(fd_data, update_rec_pos) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record_array(counter, &recs);
						close_file(2, fd_index, fd_data);
						free_strs(2, 1, files);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}

					struct Record_f *rec_n = read_file(fd_data, file_path);
					if (!rec_n)
					{
						printf("read record failed, main.c l %d.\n", __LINE__ - 2);
						free_record_array(counter, &recs);
						close_file(2, fd_data, fd_index);
						free_strs(2, 1, files);
						if (shared_locks)
						{
							int result_i = 0, result_d = 0;
							do
							{
								if ((result_i = release_lock_smo(&shared_locks,
																 plp_i, plpa_i)) == 0 ||
									(result_d = release_lock_smo(&shared_locks,
																 plp, plpa)) == 0)
								{
									__er_release_lock_smo(F, L - 5);
									if (munmap(shared_locks, sizeof(lock_info) *
																 MAX_NR_FILE_LOCKABLE) == -1)
									{
										__er_munmap(F, L - 3);
										close_file(1, fd_mo);
										return 1;
									}
									close_file(1, fd_mo);
									return 1;
								}

							} while (result_d == WTLK || result_i == WTLK);

							if (munmap(shared_locks, sizeof(lock_info) *
														 MAX_NR_FILE_LOCKABLE) == -1)
							{
								__er_munmap(F, L - 2);
								close_file(1, fd_mo);
								return 1;
							}
							close_file(1, fd_mo);
							return 1;
						}
						return 1;
					}
					recs[counter - 1] = rec_n;
				}
			}
			if (counter == 1)
			{
				print_record(1, &rec);
				free_record(rec, rec->fields_num);
			}
			else
			{
				print_record(counter, recs);
				free_record_array(counter, &recs);
			}

			if (shared_locks)
			{
				free_strs(2, 1, files);
				close_file(2, fd_index, fd_data);
				int result_i = 0, result_d = 0;
				do
				{
					if ((result_i = release_lock_smo(&shared_locks,
													 plp_i, plpa_i)) == 0 ||
						(result_d = release_lock_smo(&shared_locks,
													 plp, plpa)) == 0)
					{
						__er_release_lock_smo(F, L - 5);
						if (munmap(shared_locks, sizeof(lock_info) *
													 MAX_NR_FILE_LOCKABLE) == -1)
						{
							__er_munmap(F, L - 3);
							close_file(1, fd_mo);
							return 1;
						}
						close_file(1, fd_mo);
						return 1;
					}

				} while (result_d == WTLK || result_i == WTLK);

				if (munmap(shared_locks, sizeof(lock_info) *
											 MAX_NR_FILE_LOCKABLE) == -1)
				{
					__er_munmap(F, L - 2);
					close_file(1, fd_mo);
					return 1;
				}
				close_file(1, fd_mo);
				return 0;
			}

			close_file(2, fd_index, fd_data);
			free_strs(2, 1, files);
			return 0;
		}
	}

	return 0;
} /*-- program end --*/
