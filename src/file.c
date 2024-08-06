#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "file.h"
#include "debug.h" // take it out once your done
#include "common.h"
#include "str_op.h"	  //not to happy about this
#include "hash_tbl.h" // not to happy about this

int open_file(char *fileName, int use_trunc)
{
	int fd = 0;

	if (!use_trunc)
	{
		fd = open(fileName, O_RDWR | O_NOFOLLOW, S_IRWXU);
	}
	else
	{
		fd = open(fileName, O_WRONLY | O_TRUNC | O_NOFOLLOW, S_IRWXU);
	}

	if (fd == -1)
	{
		printf("file %s ", fileName);
		perror("open");
		return -1;
	}

	return fd;
}

int create_file(char *fileName)
{
	int fd = open(fileName, O_RDONLY | O_NOFOLLOW);
	if (fd != -1)
	{
		printf("File already exist.\n");
		close(fd);
		return -1;
	}

	fd = open(fileName, O_RDWR | O_CREAT | O_NOFOLLOW, S_IRWXU);

	if (fd == -1)
	{
		perror("open");
		return -1;
	}

	return fd;
}

void close_file(int count, ...)
{
	int sum = 0;
	short i = 0;

	va_list args;
	va_start(args, count);

	for (i = 0; i < count; i++)
	{
		int fd = va_arg(args, int);
		if (close(fd) != 0)
			sum++;
	}

	if (sum > 0)
		perror("close");
}

void delete_file(unsigned short count, ...)
{
	int sum = 0;
	short i = 0;
	va_list args;
	va_start(args, count);

	for (i = 0; i < count; i++)
	{
		char *file_name = va_arg(args, char *);
		if (unlink(file_name) != 0)
			sum++;
	}

	if (sum > 0)
		perror("unlink");
}

off_t begin_in_file(int fd)
{

	off_t pos = lseek(fd, 0, SEEK_SET);
	if (pos == -1)
	{
		perror("set begin in file");
		return pos;
	}

	return pos;
}
off_t get_file_offset(int fd)
{

	off_t offset = lseek(fd, 0, SEEK_CUR);

	if (offset == (off_t)-1)
	{
		perror("get offset: ");
		return (off_t)-1;
	}

	return offset;
}

off_t go_to_EOF(int fd)
{
	off_t eof = lseek(fd, 0, SEEK_END);
	if (eof == -1)
	{
		perror("could not find end of file");
		return eof;
	}

	return eof;
}

off_t find_record_position(int fd, off_t offset)
{
	off_t pos = lseek(fd, offset, SEEK_SET);
	if (pos == -1)
	{
		perror("seeking offset.");
		return pos;
	}

	return pos;
}

off_t move_in_file_bytes(int fd, off_t offset)
{
	off_t current_p = get_file_offset(fd);
	off_t move_to = current_p + offset;
	off_t pos = 0;
	if ((pos = lseek(fd, move_to, SEEK_SET)) == -1)
	{
		perror("seeking offset.");
		return pos;
	}

	return pos;
}

unsigned char write_index_file_head(int fd, int index_num)
{

	int i = 0;
	off_t pos = 0;

	if (write(fd, &index_num, sizeof(index_num)) == -1)
	{
		printf("write to file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	for (i = 0; i < index_num; i++)
	{
		if (write(fd, &pos, sizeof(pos)) == -1)
		{
			printf("write to file failed, %s:%d.\n", F, L - 2);
			return 0;
		}
	}

	return 1;
}

unsigned char write_index_body(int fd, int i, HashTable *ht)
{
	off_t pos = 0;
	if (write(fd, &i, sizeof(i)) == -1)
	{
		printf("write to file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	if ((pos = get_file_offset(fd)) == -1)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	if (!ht->write(fd, ht))
	{
		printf("write to file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	if (begin_in_file(fd) == -1)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	if (find_record_position(fd, sizeof(int)) == -1)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	if (i != 0)
	{
		if (move_in_file_bytes(fd, i * sizeof(pos)) == -1)
		{
			__er_file_pointer(F, L - 2);
			return 0;
		}
	}

	if (write(fd, &pos, sizeof(pos)) == -1)
	{
		printf("write to file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	if (go_to_EOF(fd) == -1)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}
	return 1;
}

unsigned char read_index_nr(int i_num, int fd, HashTable **ht)
{
	if (begin_in_file(fd) == -1)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	int array_size = 0;
	if (read(fd, &array_size, sizeof(array_size)) == -1)
	{
		printf("read from file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	if (array_size == 0)
	{
		printf("wrong reading from file, check position. %s:%d.\n", F, L - 8);
		return 0;
	}

	if (array_size <= i_num)
	{
		printf("index number out of bound.\n");
		return 0;
	}

	off_t move_to = i_num * sizeof(off_t);
	if (move_in_file_bytes(fd, move_to) == -1)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	off_t index_pos = 0;
	if (read(fd, &index_pos, sizeof(index_pos)) == -1)
	{
		printf("read from fiel failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	if (index_pos == 0)
	{
		printf("wrong reading from file, check position. %s:%d.\n", F, L - 8);
		return 0;
	}

	if (find_record_position(fd, index_pos) == -1)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	if (!read_index_file(fd, *ht))
	{
		printf("read from file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	return 1;
}

unsigned char read_all_index_file(int fd, HashTable **ht, int *p_index)
{
	if (begin_in_file(fd) == -1)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	int array_size = 0;
	if (read(fd, &array_size, sizeof(array_size)) == -1)
	{
		printf("read from file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	if (array_size == 0)
	{
		printf("read from file failed. check pointer file. %s:%d.\n", F, L - 2);
		return 0;
	}

	*p_index = array_size;
	*ht = calloc(array_size, sizeof(HashTable));

	if (!*ht)
	{
		printf("calloc failed. %s:%d.\n", F, L - 3);
		return 0;
	}
	int i = 0;
	for (i = 0; i < array_size; i++)
	{
		HashTable ht_n = {0, NULL, write_ht};
		(*ht)[i] = ht_n;
	}

	off_t move_to = (array_size * sizeof(off_t)) + sizeof(int);
	if (move_in_file_bytes(fd, move_to) == -1)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	off_t here = get_file_offset(fd);

	for (i = 0; i < array_size; i++)
	{
		if (read_index_file(fd, &((*ht)[i])) == -1)
		{
			printf("read from file failed. %s:%d.\n", F, L - 2);
			if (*ht)
			{
				int j = 0;
				for (j = 0; j < i; j++)
				{
					destroy_hasht(&((*ht)[j]));
				}
				free(*ht);
			}
			return 0;
		}

		if ((array_size - i) > 1)
		{
			int index = 0;
			if (read(fd, &index, sizeof(index)) == -1)
			{
				printf("read from file failed. %s:%d.\n", F, L - 2);
				if (*ht)
				{
					int j = 0;
					for (j = 0; j < i; j++)
					{
						destroy_hasht(&((*ht)[j]));
					}
					free(*ht);
				}
				return 0;
			}
		}
	}
	return 1;
}
unsigned char read_index_file(int fd, HashTable *ht)
{

	int size = 0;
	if (read(fd, &size, sizeof(size)) < 0)
	{
		perror("reading Index file");
		return 0; // false
	}

	Node **dataMap = calloc(size, sizeof(Node *));

	if (!dataMap)
	{
		perror("memory");
		return 0;
	}

	ht->size = size;
	ht->dataMap = dataMap;

	int ht_l = 0;
	if (read(fd, &ht_l, sizeof(ht_l)) == -1)
	{
		perror("reading ht length");
		free(dataMap);
		return 0;
	}

	register int i = 0;
	for (i = 0; i < ht_l; i++)
	{
		size_t key_l = 0;

		if (read(fd, &key_l, sizeof(key_l)) > 0)
		{
			char *key = malloc(key_l + 1);

			if (!key)
			{
				perror("memory for key");
				free_nodes(dataMap, size);
				return 0;
			}

			off_t value = 0l;
			if (read(fd, key, key_l) < 0 ||
				read(fd, &value, sizeof(value)) < 0)
			{

				perror("reading index file");
				free_nodes(dataMap, size);
				free(key);
				return 0;
			}

			key[key_l] = '\0';
			Node *newNode = malloc(sizeof(Node));

			if (!newNode)
			{
				perror("memory for node");
				free_nodes(dataMap, size);
				free(key);
				return 0;
			}

			newNode->key = strdup(key);
			free(key);
			newNode->value = value;
			newNode->next = NULL;

			int bucket = hash(newNode->key, ht->size);
			if (ht->dataMap[bucket])
			{
				Node *current = ht->dataMap[bucket];
				while (current->next != NULL)
				{
					current = current->next;
				}
				current->next = newNode;
			}
			else
			{
				ht->dataMap[bucket] = newNode;
			}
		}
	}

	return 1; // true
}

ssize_t compute_record_size(Record_f *rec, unsigned char update)
{
	register unsigned char i = 0;
	ssize_t sum = (ssize_t)sizeof(ssize_t);
	sum += (ssize_t)sizeof(rec->fields_num);

	for (i = 0; i < rec->fields_num; i++)
	{
		sum += (ssize_t)strlen(rec->fields[i].field_name) + (ssize_t)sizeof(size_t);
		sum += (ssize_t)sizeof(rec->fields[i].type);

		switch (rec->fields[i].type)
		{
		case TYPE_INT:
			sum += (ssize_t)sizeof(rec->fields[i].data.i);
			break;
		case TYPE_LONG:
			sum += (ssize_t)sizeof(rec->fields[i].data.l);
			break;
		case TYPE_FLOAT:
			sum += (ssize_t)sizeof(rec->fields[i].data.f);
			break;
		case TYPE_STRING:
			// account for '/0' and the size of each size_t wrote for each string
			if (!update)
				sum += (ssize_t)(strlen(rec->fields[i].data.s) * 2) + (ssize_t)1 + (ssize_t)sizeof(size_t);
			else
				sum += (ssize_t)strlen(rec->fields[i].data.s) + (ssize_t)1;

			break;
		case TYPE_BYTE:
			sum += (ssize_t)sizeof(rec->fields[i].data.b);
			break;
		case TYPE_DOUBLE:
			sum += (ssize_t)sizeof(rec->fields[i].data.d);
			break;
		default:
			printf("type not supported!\n");
			return -1;
		}
	}
	return sum;
}

int write_file(int fd, Record_f *rec, off_t update_off_t, unsigned char update)
{
	off_t go_back_to = 0;
	// ssize_t size = compute_record_size(rec, update);
	// if(size == -1){
	//	printf("cannot write to the file, unknown type(s).\n");
	//	return 0;
	//	}

	// if(write(fd,&size, sizeof(size)) == -1 ){
	//	perror("write size of record to file");
	//	return 0;
	// }

	if (write(fd, &rec->fields_num, sizeof(rec->fields_num)) < 0)
	{
		perror("could not write fields number");
		return 0;
	}

	register unsigned char i = 0;
	/* ----------these variables are used to handle the strings-------- */
	/* now each string fields can be updated regardless the string size */
	/* ----------some realities might required such a feature----------- */

	size_t lt = 0, new_lt = 0;
	off_t str_loc = 0, af_str_loc_pos = 0, eof = 0;
	size_t buff_update = 0, __n_buff_update = 0;
	off_t move_to = 0, bg_pos = 0;

	/*--------------------------------------------------*/
	char *buff_w = NULL;
	for (i = 0; i < rec->fields_num; i++)
	{
		size_t l = strlen(rec->fields[i].field_name);
		if (write(fd, &l, sizeof(l)) < 0)
		{
			perror("write size name");
			return 0;
		}

		if (write(fd, rec->fields[i].field_name, l) < 0)
		{
			perror("write field name");
			return 0;
		}

		if (write(fd, &rec->fields[i].type, sizeof(rec->fields[i].type)) < 0)
		{
			perror("could not write fields type");
			return 0;
		}

		switch (rec->fields[i].type)
		{
		case TYPE_INT:
			if (write(fd, &rec->fields[i].data.i, sizeof(rec->fields[i].data.i)) < 0)
			{
				perror("error in writing int type to file.\n");
				return 0;
			}
			break;
		case TYPE_LONG:
			if (write(fd, &rec->fields[i].data.l, sizeof(rec->fields[i].data.l)) < 0)
			{
				perror("error in writing long type to file.\n");
				return 0;
			}
			break;
		case TYPE_FLOAT:
			if (write(fd, &rec->fields[i].data.f, sizeof(rec->fields[i].data.f)) < 0)
			{
				perror("error in writing float type.\n");
				return 0;
			}
			break;
		case TYPE_STRING:
			if (!update)
			{
				lt = strlen(rec->fields[i].data.s) + 1;
				buff_update = (lt * 2);
				buff_w = calloc(buff_update, sizeof(char));

				if (!buff_w)
				{
					printf("calloc failed, %s:%d", __FILE__, __LINE__ - 3);
					return 0;
				}

				strncpy(buff_w, rec->fields[i].data.s, lt - 1);

				if (write(fd, &str_loc, sizeof(str_loc)) < 0 ||
					write(fd, &lt, sizeof(lt)) < 0 ||
					write(fd, &buff_update, sizeof(buff_update)) < 0 ||
					write(fd, buff_w, buff_update) < 0)
				{
					perror("write file failed: ");
					printf(" %s:%d", F, L - 2);
					free(buff_w);
					return 0;
				}

				free(buff_w);
			}
			else
			{
				/*save the satrting offset for the string record*/
				if ((bg_pos = get_file_offset(fd)) == -1)
				{
					__er_file_pointer(F, L - 2);
					return 0;
				}

				/*read pos of new str if any*/
				if (read(fd, &str_loc, sizeof(str_loc)) == -1)
				{
					perror("can't read string location: ");
					printf(" %s:%d", F, L - 3);
					return 0;
				}

				/*store record  beginning pos*/
				if ((af_str_loc_pos = get_file_offset(fd)) == -1)
				{
					__er_file_pointer(F, L - 2);
					return 0;
				}

				if (read(fd, &lt, sizeof(lt)) < 0 ||
					read(fd, &buff_update, sizeof(buff_update)) < 0)
				{
					perror("can't read safety buffer before writing string.\n");
					printf("%s:%d", F, L - 3);
					return 0;
				}

				/*save the end offset of the first string record */
				if ((go_back_to = get_file_offset(fd)) == -1)
				{
					__er_file_pointer(F, L - 2);
					return 0;
				}

				go_back_to += sizeof(buff_update);
				if (str_loc > 0)
				{
					if (find_record_position(fd, str_loc) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
					/* in the case of a regular buffure update we have to	*/
					/*	 to save the off_t to get back to it later	*/
					if ((move_to = get_file_offset(fd)) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}

					if (read(fd, &lt, sizeof(lt)) < 0 ||
						read(fd, &buff_update, sizeof(buff_update)) < 0)
					{
						perror("read file.\n");
						printf("%s:%d", F, L - 3);
						return 0;
					}
				}

				new_lt = strlen(rec->fields[i].data.s) + 1; /*get new str length*/

				if (new_lt > buff_update)
				{

					if ((eof = go_to_EOF(fd)) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
					__n_buff_update = buff_update;
					__n_buff_update += (new_lt - buff_update);
					buff_w = calloc(__n_buff_update, sizeof(char));
					if (!buff_w)
					{
						printf("calloc failed, %s:%d.\n", F, L - 3);
						return 0;
					}
				}
				else
				{
					buff_w = calloc(buff_update, sizeof(char));
					if (!buff_w)
					{
						printf("calloc failed, %s:%d.\n", F, L - 3);
						return 0;
					}
				}

				strncpy(buff_w, rec->fields[i].data.s, new_lt - 1);
				/* if we did not move to another position set the file pointer*/
				/*		 back to overwrite the data accordingly		*/
				if (str_loc == 0 && (__n_buff_update == 0))
				{
					if (find_record_position(fd, af_str_loc_pos) == -1)
					{
						__er_file_pointer(F, L - 3);
						free(buff_w);
						return 0;
					}
				}
				else if (str_loc > 0 && (__n_buff_update == 0))
				{
					if (find_record_position(fd, move_to) == -1)
					{
						__er_file_pointer(F, L - 3);
						free(buff_w);
						return 0;
					}
				}

				lt = new_lt;
				buff_update = __n_buff_update > 0 ? __n_buff_update : buff_update;
				if (write(fd, &lt, sizeof(lt)) < 0 ||
					write(fd, &buff_update, sizeof(buff_update)) < 0 ||
					write(fd, buff_w, buff_update) < 0)
				{
					perror("error in writing type string (char *)file.\n");
					free(buff_w);
					return 0;
				}

				free(buff_w);

				if (eof > 0)
				{
					update_off_t = get_file_offset(fd);
					/*go at the beginning of the str record*/
					if (find_record_position(fd, bg_pos) == -1)
					{
						__er_file_pointer(F, L - 2);
						free(buff_w);
						return 0;
					}

					/*update new string position*/
					if (write(fd, &eof, sizeof(eof)) == -1)
					{
						perror("write file: ");
						printf(" %s:%d", F, L - 3);
						return 0;
					}

					/*set file pointer to the end of the 1st string rec*/
					/*this step is crucial to avoid losing data        */

					if (find_record_position(fd, go_back_to) == -1)
					{
						__er_file_pointer(F, L - 2);
						free(buff_w);
						return 0;
					}
				}
				else if (str_loc > 0)
				{ /*Make sure that in all cases we go back to the end of the 1st record*/
					if (find_record_position(fd, go_back_to) == -1)
					{
						__er_file_pointer(F, L - 2);
						free(buff_w);
						return 0;
					}
				}
			}
			break;
		case TYPE_BYTE:
			if (write(fd, &rec->fields[i].data.b, sizeof(rec->fields[i].data.b)) < 0)
			{
				perror("error in writing type byte to file.\n");
				return 0;
			}
			break;
		case TYPE_DOUBLE:
			if (write(fd, &rec->fields[i].data.d, sizeof(rec->fields[i].data.d)) < 0)
			{
				perror("error in writing double to file.\n");
				return 0;
			}
			break;
		default:
			break;
		}
	}

	//	printf("the off set before writing the update pos is %ld.\n",get_file_offset(fd));

	if (write(fd, &update_off_t, sizeof(update_off_t)) == -1)
	{
		perror("writing off_t for future update");
		printf(" %s:%d", F, L - 3);
		return 0;
	}

	return 1; // write to file succssed!
}

ssize_t get_record_size(int fd)
{
	ssize_t size = 0L;

	if (read(fd, &size, sizeof(size)) < 0)
	{
		perror("could not read record size.\n");
		return -1;
	}

	return size;
}

off_t get_update_offset(int fd)
{
	off_t urec_pos = 0;
	if (read(fd, &urec_pos, sizeof(urec_pos)) == -1)
	{
		perror("could not read the update record position (file.c l 424).\n");
		return -1;
	}

	return urec_pos;
}

Record_f *read_file(int fd, char *file_name)
{
	//	ssize_t size = get_record_size(fd);
	off_t go_back_to = 0;

	// if(size == -1)
	//	return NULL;

	int fields_num_r = 0;
	if (read(fd, &fields_num_r, sizeof(fields_num_r)) < 0)
	{
		perror("could not read fields number:");
		printf(" %s:%d.\n", __FILE__, __LINE__ - 1);
		return NULL;
	}

	Record_f *rec = create_record(file_name, fields_num_r);
	if (!rec)
	{
		printf("create record failed, %s:%d.\n", __FILE__, __LINE__ - 2);
		return NULL;
	}

	register unsigned char i = 0;
	off_t str_loc = 0;
	size_t buff_update = 0; /* to get the real size of the string*/
	off_t move_to = 0;
	size_t l = 0;
	for (i = 0; i < rec->fields_num; i++)
	{
		size_t lt = 0;

		if (read(fd, &lt, sizeof(lt)) < 0)
		{
			perror("could not read size of field name, file.c l 458.\n");
			clean_up(rec, fields_num_r);
			return NULL;
		}
		//	printf("size is %ld",lt);
		rec->fields[i].field_name = malloc(lt + 1);

		if (!rec->fields[i].field_name)
		{
			printf("no memory for field name, file.cl 466.\n");
			clean_up(rec, rec->fields_num);
			return NULL;
		}

		if (read(fd, rec->fields[i].field_name, lt) < 0)
		{
			perror("could not read field name, file.c l 472");
			clean_up(rec, rec->fields_num);
			return NULL;
		}

		rec->fields[i].field_name[lt] = '\0';
		ValueType type_r;

		if (read(fd, &type_r, sizeof(type_r)) < 0)
		{
			perror("could not read type file.c l 481.");
			clean_up(rec, rec->fields_num);
			return NULL;
		}

		rec->fields[i].type = type_r;

		switch (rec->fields[i].type)
		{
		case TYPE_INT:
			if (read(fd, &rec->fields[i].data.i, sizeof(int)) < 0)
			{
				perror("could not read type int file.c 491.\n");
				clean_up(rec, rec->fields_num);
				return NULL;
			}
			break;
		case TYPE_LONG:
			if (read(fd, &rec->fields[i].data.l, sizeof(long)) < 0)
			{
				perror("could not read type long, file.c 498.\n");
				clean_up(rec, rec->fields_num);
				return NULL;
			}
			break;
		case TYPE_FLOAT:
			if (read(fd, &rec->fields[i].data.f, sizeof(float)) < 0)
			{
				perror("could not read type float, file.c l 505.\n");
				clean_up(rec, rec->fields_num);
				return NULL;
			}
			break;
		case TYPE_STRING:
			/*read pos of new str if any*/
			if (read(fd, &str_loc, sizeof(str_loc)) == -1)
			{
				perror("can't read string location: ");
				printf(" %s:%d", F, L - 3);
				return 0;
			}

			if (read(fd, &l, sizeof(l)) < 0)
			{
				perror("read from file failed: ");
				printf("%s:%d.\n", F, L - 2);
				clean_up(rec, rec->fields_num);
				return NULL;
			}

			if (read(fd, &buff_update, sizeof(buff_update)) < 0)
			{
				perror("read from file failed: ");
				printf("%s:%d.\n", F, L - 2);
				clean_up(rec, rec->fields_num);
				return NULL;
			}

			if (str_loc > 0)
			{
				/*save the offset past the buff_w*/
				move_to = get_file_offset(fd) + sizeof(buff_update);
				/* get current position, we might have to get back to this */
				if (find_record_position(fd, str_loc) == -1)
				{
					__er_file_pointer(F, L - 2);
					clean_up(rec, rec->fields_num);
					return NULL;
				}

				if (read(fd, &l, sizeof(l)) < 0)
				{
					perror("read file: ");
					printf("%s:%d", F, L - 2);
					clean_up(rec, rec->fields_num);
					return NULL;
				}

				if (read(fd, &buff_update, sizeof(buff_update)) < 0)
				{
					perror("read file: ");
					printf("%s:%d", F, L - 2);
					clean_up(rec, rec->fields_num);
					return NULL;
				}
			}
			rec->fields[i].data.s = calloc(l, sizeof(char));

			if (!rec->fields[i].data.s)
			{
				printf("calloc failed: %s:%d.\n", F, L - 3);
				clean_up(rec, rec->fields_num);
				return NULL;
			}

			char *all_buf = calloc(buff_update, sizeof(char));
			if (!all_buf)
			{
				printf("calloc failed file.c l 532.\n");
				clean_up(rec, rec->fields_num);
				return NULL;
			}

			if (read(fd, all_buf, buff_update) < 0)
			{
				perror("could not read buffer string, file.c l 539.\n");
				clean_up(rec, rec->fields_num);
				return NULL;
			}

			strncpy(rec->fields[i].data.s, all_buf, l);
			rec->fields[i].data.s[l - 1] = '\0';
			free(all_buf);

			/*set file pointer back at the end of the original str record*/
			if (str_loc > 0)
			{
				if (find_record_position(fd, move_to) == -1)
				{
					__er_file_pointer(F, L - 2);
					clean_up(rec, rec->fields_num);
					return NULL;
				}
			}

			break;
		case TYPE_BYTE:
			if (read(fd, &rec->fields[i].data.b, sizeof(unsigned char)) < 0)
			{
				perror("could not read type byte: ");
				printf(" %s:%d.\n", F, L - 2);
				clean_up(rec, rec->fields_num);
				return NULL;
			}
			break;
		case TYPE_DOUBLE:
			if (read(fd, &rec->fields[i].data.d, sizeof(double)) < 0)
			{
				perror("could not read type double:");
				printf(" %s:%d.\n", F, L - 2);
				clean_up(rec, rec->fields_num);
				return NULL;
			}
			break;
		default:
			break;
		}
	}

	return rec;
}

int file_error_handler(int count, ...)
{
	va_list args;
	va_start(args, count);
	int fds[count];

	int i = 0, j = 0;

	for (i = 0; i < count; i++)
	{
		int fd = va_arg(args, int);
		if (fd == STATUS_ERROR)
		{
			printf("Error in creating the file\n");
			close_file(1, fd);
			fds[i] = fd;
			j++;
		}

		if (j > 0 && fds[i] != fd)
		{
			close_file(1, fd);
		}
	}

	return j;
}

int padding_file(int fd, int bytes, size_t hd_st)
{

	short actual_pd = (short)(bytes - (int)hd_st);
	unsigned char padding[actual_pd];
	short i = 0;
	for (i = 0; i < (bytes - hd_st); i++)
		padding[i] = '0';

	if (write(fd, &padding, sizeof(padding)) == -1)
	{
		perror("padding file.\n");
		return 0;
	}

	return 1; // succssed
}
