#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <byteswap.h>
#include <arpa/inet.h>
#include <math.h>
#include "file.h"
#include "str_op.h"
#include "common.h"
#include "float_endian.h"
#include "debug.h"

static int is_array_last_block(int fd, int element_nr, size_t bytes_each_element);

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

	if (fd == STATUS_ERROR)
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
	if (fd != STATUS_ERROR)
	{
		printf("File already exist.\n");
		close(fd);
		return -1;
	}

	fd = open(fileName, O_RDWR | O_CREAT | O_NOFOLLOW, S_IRWXU);

	if (fd == STATUS_ERROR)
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
	if (pos == STATUS_ERROR)
	{
		perror("set begin in file");
		return pos;
	}

	return pos;
}
off_t get_file_offset(int fd)
{

	off_t offset = lseek(fd, 0, SEEK_CUR);

	if (offset == STATUS_ERROR)
	{
		perror("get offset: ");
		return offset;
	}

	return offset;
}

off_t go_to_EOF(int fd)
{
	off_t eof = lseek(fd, 0, SEEK_END);
	if (eof == STATUS_ERROR)
	{
		perror("could not find end of file");
		return eof;
	}

	return eof;
}

off_t find_record_position(int fd, off_t offset)
{
	off_t pos = lseek(fd, offset, SEEK_SET);
	if (pos == STATUS_ERROR)
	{
		perror("seeking offset.");
		return pos;
	}

	return pos;
}

/*
 * move_in_file_bytes will change the file pointer of offset bytes
 *	example:
 *		move_in_file_bytes(fd, -4); will move the file pointear backwords of 4 bytes
 *
 * */
off_t move_in_file_bytes(int fd, off_t offset)
{
	off_t current_p = get_file_offset(fd);
	off_t move_to = current_p + offset;
	off_t pos = 0;
	if ((pos = lseek(fd, move_to, SEEK_SET)) == STATUS_ERROR)
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

	uint32_t in = htonl((uint32_t)index_num);
	if (write(fd, &in, sizeof(in)) == -1)
	{
		printf("write to file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	for (i = 0; i < index_num; i++)
	{
		uint64_t p_n = bswap_64(pos);
		if (write(fd, &p_n, sizeof(p_n)) == -1)
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

	uint32_t i_n = htonl(i);
	if (write(fd, &i_n, sizeof(i_n)) == -1)
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

	uint64_t p_n = bswap_64(pos);
	if (write(fd, &p_n, sizeof(p_n)) == -1)
	{
		printf("write to file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	if (go_to_EOF(fd) == STATUS_ERROR)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}
	return 1;
}

unsigned char read_index_nr(int i_num, int fd, HashTable **ht)
{
	if (begin_in_file(fd) == STATUS_ERROR)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	uint32_t a_s = 0;
	if (read(fd, &a_s, sizeof(a_s)) == STATUS_ERROR)
	{
		printf("read from file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	int array_size = (int)ntohl(a_s);
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
	if (move_in_file_bytes(fd, move_to) == STATUS_ERROR)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	uint64_t i_p = 0;
	if (read(fd, &i_p, sizeof(i_p)) == STATUS_ERROR)
	{
		printf("read from fiel failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	off_t index_pos = (off_t)bswap_64(i_p);
	if (index_pos == 0)
	{
		printf("wrong reading from file, check position. %s:%d.\n", F, L - 8);
		return 0;
	}

	if (find_record_position(fd, index_pos) == STATUS_ERROR)
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

unsigned char indexes_on_file(int fd, int *p_i_nr)
{
	if (begin_in_file(fd) == STATUS_ERROR)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	uint32_t a_s = 0;
	if (read(fd, &a_s, sizeof(a_s)) == STATUS_ERROR)
	{
		printf("read from file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	int array_size = (int)ntohl(a_s);
	if (array_size == 0)
	{
		printf("wrong reading from file, check position. %s:%d.\n", F, L - 8);
		return 0;
	}

	*p_i_nr = array_size;
	return 1;
}

unsigned char nr_bucket(int fd, int *p_buck)
{
	HashTable ht = {0, NULL}, *pht = &ht;

	if (!read_index_nr(0, fd, &pht))
	{
		printf("read from file failed. %s:%d.\n", F, L - 2);
		if (ht.size > 0)
		{
			destroy_hasht(&ht);
		}
		return 0;
	}

	*p_buck = ht.size;
	destroy_hasht(&ht);
	return 1;
}

unsigned char read_all_index_file(int fd, HashTable **ht, int *p_index)
{
	if (begin_in_file(fd) == STATUS_ERROR)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	uint32_t a_s = 0;
	if (read(fd, &a_s, sizeof(a_s)) == STATUS_ERROR)
	{
		printf("read from file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	int array_size = (int)ntohl(a_s);
	if (array_size == 0)
	{
		printf("wrong reading from file, check position. %s:%d.\n", F, L - 8);
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
	if (move_in_file_bytes(fd, move_to) == STATUS_ERROR)
	{
		__er_file_pointer(F, L - 2);
		free(*ht);
		return 0;
	}

	for (i = 0; i < array_size; i++)
	{
		if (read_index_file(fd, &((*ht)[i])) == STATUS_ERROR)
		{
			printf("read from file failed. %s:%d.\n", F, L - 2);
			free_ht_array(*ht, i);
			return 0;
		}

		if ((array_size - i) > 1)
		{
			if (move_in_file_bytes(fd, sizeof(int)) == STATUS_ERROR)
			{
				__er_file_pointer(F, L - 2);
				free_ht_array(*ht, i);
				return 0;
			}
		}
	}
	return 1;
}
unsigned char read_index_file(int fd, HashTable *ht)
{

	uint32_t s_n = 0;
	if (read(fd, &s_n, sizeof(s_n)) < 0)
	{
		perror("reading Index file");
		return 0; // false
	}

	int size = (int)ntohl(s_n);
	Node **dataMap = calloc(size, sizeof(Node *));
	if (!dataMap)
	{
		perror("calloc failed");
		return 0;
	}

	ht->size = size;
	ht->dataMap = dataMap;

	uint32_t ht_ln = 0;
	if (read(fd, &ht_ln, sizeof(ht_ln)) == STATUS_ERROR)
	{
		perror("reading ht length");
		free(dataMap);
		return 0;
	}

	int ht_l = (int)ntohl(ht_ln);
	register int i = 0;
	for (i = 0; i < ht_l; i++)
	{

		uint32_t type = 0;
		if (read(fd, &type, sizeof(type)) == -1)
		{
			fprintf(stderr, "can't read key type, %s:%d.\n",
					F, L - 3);
			free_nodes(dataMap, size);
			return 0;
		}

		int key_type = (int)ntohl(type);

		switch (key_type)
		{
		case STR:
		{
			uint64_t key_l = 0l;
			if (read(fd, &key_l, sizeof(key_l)) > 0)
			{
				size_t size = (size_t)bswap_64(key_l);

				char *key = calloc(size + 1, sizeof(char));
				if (!key)
				{
					perror("memory for key");
					free_nodes(dataMap, size);
					return 0;
				}

				uint64_t v_n = 0l;
				if (read(fd, key, size + 1) == -1 ||
					read(fd, &v_n, sizeof(v_n)) == -1)
				{

					perror("reading index file");
					free_nodes(dataMap, size);
					free(key);
					return 0;
				}

				off_t value = (off_t)bswap_64(v_n);
				key[size] = '\0';
				Node *newNode = calloc(1, sizeof(Node));
				if (!newNode)
				{
					perror("memory for node");
					free_nodes(dataMap, size);
					free(key);
					return 0;
				}

				newNode->key.s = strdup(key);
				if (!newNode->key.s)
				{
					fprintf(stderr,
							"strdup() failed, %s:%d.\n",
							F, L - 3);
					free_nodes(dataMap, size);
					free(key);
					return 0;
				}
				free(key);
				newNode->value = value;
				newNode->next = NULL;
				newNode->key_type = key_type;

				int bucket = hash((void *)newNode->key.s, ht->size, key_type);
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
			else
			{
				fprintf(stderr,
						"read index failed, %s:%d\n", F, L - 2);
				free_nodes(dataMap, size);
				return 0;
			}
			break;
		}
		case UINT:
		{
			uint32_t k = 0;
			uint64_t value = 0;
			if (read(fd, &k, sizeof(k)) == -1)
			{
				fprintf(stderr, "read index failed.\n");
				free_nodes(dataMap, size);
				return 0;
			}

			if (read(fd, &value, sizeof(value)) == -1)
			{
				fprintf(stderr, "read index failed.\n");
				free_nodes(dataMap, size);
				return 0;
			}

			Node *new_node = calloc(1, sizeof(Node));
			if (!new_node)
			{
				__er_calloc(F, L - 2);
				free_nodes(dataMap, size);
				return 0;
			}

			new_node->key_type = key_type;
			new_node->key.n = ntohl(k);
			new_node->value = (off_t)bswap_64(value);
			new_node->next = NULL;

			int index = hash((void *)&new_node->key.n, ht->size, key_type);
			if (index == -1)
			{
				fprintf(stderr, "read index failed.%s:%d\n", F, L - 2);
				free_nodes(dataMap, size);
				free_ht_node(new_node);
				return 0;
			}
			if (ht->dataMap[index])
			{
				Node *current = ht->dataMap[index];
				while (current->next)
					current = current->next;

				current = new_node;
			}
			else
			{
				ht->dataMap[index] = new_node;
			}

			break;
		}
		default:
			fprintf(stderr, "key type not supported.\n");
			free_nodes(dataMap, size);
			return 0;
		}
	}

	return 1; // true
}

size_t record_size_on_disk(void *rec_f)
{
	size_t rec_size = 0;
	struct Record_f *rec = (struct Record_f *)rec_f;

	rec_size += sizeof(rec->fields_num);
	/*each field name length*/
	rec_size += (sizeof(uint64_t) * rec->fields_num);

	/*each field type*/
	rec_size += (sizeof(uint32_t) * rec->fields_num);

	for (int i = 0; i < rec->fields_num; i++)
	{
		/*actual name length wrote to disk*/
		rec_size += strlen(rec->fields[i].field_name);

		switch (rec->fields[i].type)
		{
		case TYPE_INT:
		case TYPE_FLOAT:
			rec_size += sizeof(uint32_t);
			break;
		case TYPE_LONG:
		case TYPE_DOUBLE:
			rec_size += sizeof(uint64_t);
			break;
		case TYPE_BYTE:
			rec_size += sizeof(uint16_t);
			break;
		case TYPE_STRING:
			rec_size += (sizeof(uint64_t) * 3);
			rec_size += (strlen(rec->fields[i].data.s) * 2) + 1;
			break;
		default:
			printf("unknown type");
			return -1;
		}
	}

	/*any eventual update position*/
	rec_size += sizeof(uint64_t);
	return rec_size;
}

int write_file(int fd, struct Record_f *rec, off_t update_off_t, unsigned char update)
{
	off_t go_back_to = 0;

	uint32_t rfn_ne = htonl((uint32_t)rec->fields_num);
	if (write(fd, &rfn_ne, sizeof(rfn_ne)) < 0)
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
		size_t str_l = strlen(rec->fields[i].field_name);
		uint64_t str_l_ne = bswap_64((uint64_t)str_l);
		if (write(fd, &str_l_ne, sizeof(str_l_ne)) < 0)
		{
			perror("write size name");
			return 0;
		}

		if (write(fd, rec->fields[i].field_name, str_l) < 0)
		{
			perror("write field name");
			return 0;
		}

		uint32_t type_ne = htonl((uint32_t)rec->fields[i].type);
		if (write(fd, &type_ne, sizeof(type_ne)) < 0)
		{
			perror("could not write fields type");
			return 0;
		}

		switch (rec->fields[i].type)
		{
		case TYPE_INT:
		{
			uint32_t i_ne = htonl(rec->fields[i].data.i);
			if (write(fd, &i_ne, sizeof(i_ne)) < 0)
			{
				perror("error in writing int type to file.\n");
				return 0;
			}
			break;
		}
		case TYPE_LONG:
		{
			uint64_t l_ne = bswap_64((uint64_t)rec->fields[i].data.l);
			if (write(fd, &l_ne, sizeof(l_ne)) < 0)
			{
				perror("error in writing long type to file.\n");
				return 0;
			}
			break;
		}
		case TYPE_FLOAT:
		{
			uint32_t f = htonf(rec->fields[i].data.f);
			if (write(fd, &f, sizeof(f)) < 0)
			{
				perror("error in writing float type.\n");
				return 0;
			}
			break;
		}
		case TYPE_STRING:
		{
			if (!update)
			{
				lt = strlen(rec->fields[i].data.s);
				buff_update = lt * 2;

				/* adding 1 for '\0'*/
				lt++, buff_update++;
				buff_w = calloc(buff_update, sizeof(char));
				if (!buff_w)
				{
					__er_calloc(F, L - 2);
					return 0;
				}

				strncpy(buff_w, rec->fields[i].data.s, lt - 1);

				uint64_t l_ne = bswap_64(lt);
				uint64_t bu_ne = bswap_64(buff_update);
				uint64_t str_loc_ne = bswap_64(str_loc);

				if (write(fd, &str_loc_ne, sizeof(str_loc_ne)) < 0 ||
					write(fd, &l_ne, sizeof(l_ne)) < 0 ||
					write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
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
				/*save the starting offset for the string record*/
				if ((bg_pos = get_file_offset(fd)) == STATUS_ERROR)
				{
					__er_file_pointer(F, L - 2);
					return 0;
				}

				/*read pos of new str if any*/
				uint64_t str_loc_ne = 0;
				if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == STATUS_ERROR)
				{
					perror("can't read string location: ");
					printf(" %s:%d", F, L - 3);
					return 0;
				}
				str_loc = (off_t)bswap_64(str_loc_ne);

				/*store record  beginning pos*/
				if ((af_str_loc_pos = get_file_offset(fd)) == STATUS_ERROR)
				{
					__er_file_pointer(F, L - 2);
					return 0;
				}

				uint64_t bu_ne = 0;
				uint64_t l_ne = 0;
				if (read(fd, &l_ne, sizeof(l_ne)) < 0 ||
					read(fd, &bu_ne, sizeof(bu_ne)) < 0)
				{
					perror("can't read safety buffer before writing string.\n");
					printf("%s:%d", F, L - 3);
					return 0;
				}

				buff_update = (off_t)bswap_64(bu_ne);
				lt = (off_t)bswap_64(l_ne);

				/*save the end offset of the first string record */
				if ((go_back_to = get_file_offset(fd)) == STATUS_ERROR)
				{
					__er_file_pointer(F, L - 2);
					return 0;
				}

				/*add the buffer size to this file off_t*/
				/*so we can reposition right after the 1st string after the writing */
				go_back_to += buff_update;
				if (str_loc > 0)
				{
					/*set the file pointer to str_loc*/
					if (find_record_position(fd, str_loc) == STATUS_ERROR)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
					/*
					 * in the case of a regular buffer update we have
					 *  to save the off_t to get back to it later
					 * */
					if ((move_to = get_file_offset(fd)) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}

					uint64_t bu_ne = 0;
					uint64_t l_ne = 0;
					if (read(fd, &l_ne, sizeof(l_ne)) < 0 ||
						read(fd, &bu_ne, sizeof(bu_ne)) < 0)
					{
						perror("read file.\n");
						printf("%s:%d", F, L - 3);
						return 0;
					}

					buff_update = (off_t)bswap_64(bu_ne);
					lt = (off_t)bswap_64(l_ne);
				}

				new_lt = strlen(rec->fields[i].data.s) + 1; /*get new str length*/

				if (new_lt > buff_update)
				{
					/*
					 * if the new length is bigger then the buffer,
					 * set the file pointer to the end of the file
					 * to write the new data
					 * */
					if ((eof = go_to_EOF(fd)) == STATUS_ERROR)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}

					/*expand the buff_update only for the bytes needed*/
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
						__er_calloc(F, L - 2);
						return 0;
					}
				}

				strncpy(buff_w, rec->fields[i].data.s, new_lt - 1);
				/*
				 * if we did not move to another position
				 * set the file pointer back to the begginning of the string record
				 * to overwrite the data accordingly
				 * */
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
					if (find_record_position(fd, move_to) == STATUS_ERROR)
					{
						__er_file_pointer(F, L - 3);
						free(buff_w);
						return 0;
					}
				}

				/*
				 * write the data to file --
				 * the file pointer is always pointing to the
				 * right position at this point */
				lt = new_lt;
				buff_update = __n_buff_update > 0 ? __n_buff_update : buff_update;
				l_ne = bswap_64(lt);
				bu_ne = bswap_64(buff_update);

				if (write(fd, &l_ne, sizeof(lt)) < 0 ||
					write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
					write(fd, buff_w, buff_update) < 0)
				{
					perror("error in writing type string (char *)file.\n");
					free(buff_w);
					return 0;
				}

				free(buff_w);

				/*
				 * if eof is bigger than 0 means we updated the string
				 * we need to save the off_t of the new written data
				 * at the start of the original.
				 * */
				if (eof > 0)
				{
					/*go at the beginning of the str record*/
					if (find_record_position(fd, bg_pos) == STATUS_ERROR)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}

					/*update new string position*/
					uint64_t eof_ne = bswap_64((uint64_t)eof);
					if (write(fd, &eof_ne, sizeof(eof_ne)) == STATUS_ERROR)
					{
						perror("write file: ");
						printf(" %s:%d", F, L - 3);
						return 0;
					}

					/*set file pointer to the end of the 1st string rec*/
					/*this step is crucial to avoid losing data        */

					if (find_record_position(fd, go_back_to) == STATUS_ERROR)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
				else if (str_loc > 0)
				{
					/*
					 * Make sure that in all cases
					 * we go back to the end of the 1st record
					 * */
					if (find_record_position(fd, go_back_to) == STATUS_ERROR)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}
			break;
		}
		case TYPE_BYTE:
		{
			uint16_t b_ne = htonb(rec->fields[i].data.b);
			if (write(fd, &b_ne, sizeof(b_ne)) < 0)
			{
				perror("error in writing type byte to file.\n");
				return 0;
			}
			break;
		}
		case TYPE_DOUBLE:
		{
			uint64_t d_ne = htond(rec->fields[i].data.d);
			if (write(fd, &d_ne, sizeof(d_ne)) < 0)
			{
				perror("error in writing double to file.\n");
				return 0;
			}
			break;
		}
		case TYPE_ARRAY_INT:
		{
			if (!update)
			{
				/*write the size of the array */
				uint32_t size_ne = htonl((uint32_t)rec->fields[i].data.v.size);
				if (write(fd, &size_ne, sizeof(size_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				uint32_t padding_ne = htonl(0);
				if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				for (int k = 0; k < rec->fields[i].data.v.size; k++)
				{
					if (!rec->fields[i].data.v.elements.i[k])
						continue;

					uint32_t num_ne = htonl(*(uint32_t *)rec->fields[i].data.v.elements.i[k]);
					if (write(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				uint64_t upd_ne = bswap_64(0);
				if (write(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}
			}
			else
			{
				/* update branch*/
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int step = 0;
				int sz = 0;
				int k = 0;
				int padding_value = 0;
				do
				{
					/* check the size */
					uint32_t sz_ne = 0;
					if (read(fd, &sz_ne, sizeof(sz_ne)) == -1)
					{
						fprintf(stderr, "can't read int array size.\n");
						return 0;
					}

					sz = (int)ntohl(sz_ne);
					if (rec->fields[i].data.v.size < sz ||
						rec->fields[i].data.v.size == sz)
						break;

					/*read the padding data*/
					uint32_t pd_ne = 0;
					if (read(fd, &pd_ne, sizeof(pd_ne)) == -1)
					{
						fprintf(stderr, "can't read padding array.\n");
						return 0;
					}

					padding_value = (int)ntohl(pd_ne);

					if (step >= sz)
					{
						int array_last = 0;
						int exit = 0;
						if ((array_last = is_array_last_block(fd, sz, sizeof(int))) == -1)
						{
							fprintf(stderr, "can't verify array last block %s:%d.\n", F, L - 1);
							return 0;
						}

						if (rec->fields[i].data.v.size < (sz + step) && array_last)
						{
							int pad_value = sz - (rec->fields[i].data.v.size - step);
							padding_value += pad_value;

							sz = rec->fields[i].data.v.size - step;
							exit = 1;

							if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
							{
								__er_file_pointer(F, L - 1);
								return 0;
							}

							/* write the updated size of the array */
							uint32_t new_sz = htonl((uint32_t)sz);
							if (write(fd, &new_sz, sizeof(new_sz)) == -1)
							{
								perror("error in writing remaining size int array.\n");
								return 0;
							}

							/* write the updated padding value */
							uint32_t new_pd = htonl((uint32_t)padding_value);
							if (write(fd, &new_pd, sizeof(new_pd)) == -1)
							{
								perror("error in writing new pading value int array.\n");
								return 0;
							}
						}
						else if (rec->fields[i].data.v.size == (sz + step) && array_last)
						{
							exit = 1;
						}

						while (sz)
						{
							if (step < rec->fields[i].data.v.size)
							{
								uint32_t num_ne = htonl(*(uint32_t *)rec->fields[i].data.v.elements.i[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
							sz--;
						}

						if (exit)
						{
							/*write the epty update offset*/
							uint64_t empty_offset = bswap_64(0);
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}
					else
					{

						int exit = 0;
						for (k = 0; k < sz; k++)
						{
							if (step > 0 && k == 0)
							{
								if ((step + sz) > rec->fields[i].data.v.size)
								{
									int pad = sz - (rec->fields[i].data.v.size - step);
									padding_value += pad;

									sz = rec->fields[i].data.v.size - step;
									exit = 1;

									if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
									{
										__er_file_pointer(F, L - 1);
										return 0;
									}

									/* write the updated size of the array */
									uint32_t new_sz = htonl((uint32_t)sz);
									if (write(fd, &new_sz, sizeof(new_sz)) == -1)
									{
										perror("error in writing remaining size int array.\n");
										return 0;
									}

									/*write padding */
									uint32_t new_pd = htonl((uint32_t)padding_value);
									if (write(fd, &new_pd, sizeof(new_pd)) == -1)
									{
										perror("error in writing new padd int array.\n");
										return 0;
									}
								}
							}

							if (step < rec->fields[i].data.v.size)
							{
								if (!rec->fields[i].data.v.elements.i[step])
									continue;

								uint32_t num_ne = htonl(*(uint32_t *)rec->fields[i].data.v.elements.i[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
						}

						if (exit)
						{

							if (padding_value > 0)
							{
								if (move_in_file_bytes(fd, padding_value * sizeof(int)) == -1)
								{
									__er_file_pointer(F, L - 1);
									return 0;
								}
							}
							/*write the epty update offset*/
							uint64_t empty_offset = bswap_64(0);
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}

					if (padding_value > 0)
					{
						if (move_in_file_bytes(fd, padding_value * sizeof(int)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
					}

					uint64_t update_off_ne = 0;
					off_t go_back_to = get_file_offset(fd);

					if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
					{
						perror("failed read update off_t int array.\n");
						return 0;
					}

					if (go_back_to_first_rec == 0)
						go_back_to_first_rec = go_back_to + sizeof(update_off_ne);

					update_pos = (off_t)bswap_64(update_off_ne);
					if (update_pos == 0)
					{
						/*go to EOF*/
						if ((update_pos = go_to_EOF(fd)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
						/* write the size of the array */
						int size_left = rec->fields[i].data.v.size - step;
						uint32_t size_left_ne = htonl((uint32_t)size_left);
						if (write(fd, &size_left_ne, sizeof(size_left_ne)) == -1)
						{
							perror("error in writing remaining size int array.\n");
							return 0;
						}

						uint32_t padding_ne = htonl(0);
						if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						for (int j = 0; j < size_left; j++)
						{
							if (step < rec->fields[i].data.v.size)
							{
								if (!rec->fields[i].data.v.elements.i[step])
									continue;

								uint32_t num_ne = htonl(*(uint32_t *)rec->fields[i].data.v.elements.i[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
						}

						/*write the empty update offset*/
						uint64_t empty_offset = bswap_64(0);
						if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						if (find_record_position(fd, go_back_to) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}

						update_off_ne = (uint64_t)bswap_64((uint64_t)update_pos);
						if (write(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
						{
							fprintf(stderr, "can't write update position int array, %s:%d.\n",
									F, L - 1);
							return 0;
						}

						break;
					}

					if (find_record_position(fd, update_pos) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

				} while (update_pos > 0);

				if (rec->fields[i].data.v.size < sz)
				{

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/*write the size of the array */
					uint32_t size_ne = htonl((uint32_t)rec->fields[i].data.v.size);
					if (write(fd, &size_ne, sizeof(size_ne)) == -1)
					{
						perror("error in writing size array to file.\n");
						return 0;
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)ntohl(pad_ne);
					pd_he += (sz - rec->fields[i].data.v.size);

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/* write the padding to apply after the  array */
					pad_ne = htonl((uint32_t)pd_he);
					if (write(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					for (int k = step; k < rec->fields[i].data.v.size; k++)
					{
						if (!rec->fields[i].data.v.elements.i[k])
							continue;

						uint32_t num_ne = htonl(*(uint32_t *)rec->fields[i].data.v.elements.i[k]);
						if (write(fd, &num_ne, sizeof(num_ne)) == -1)
						{
							perror("failed write int array to file");
							return 0;
						}
					}

					/*
					 * move the file pointer after the array
					 * as much as the pad
					 * */
					if (move_in_file_bytes(fd, pd_he * sizeof(int)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					uint64_t update_arr_ne = bswap_64(0);
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}
				else if (rec->fields[i].data.v.size == sz)
				{
					/*
					 * the sizes are the same
					 * we simply write the array.
					 * */
					if (step > 0)
					{
						if (move_in_file_bytes(fd, -sizeof(sz)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						int size_left = rec->fields[i].data.v.size - step;
						uint32_t sz_ne = htonl((uint32_t)size_left);
						if (write(fd, &sz_ne, sizeof(sz_ne)) == -1)
						{
							fprintf(stderr, "write failed %s:%d.\n", F, L - 1);
							return 0;
						}
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)ntohl(pad_ne);
					for (int k = 0; k < rec->fields[i].data.v.size; k++)
					{
						if (step < rec->fields[i].data.v.size)
						{
							if (!rec->fields[i].data.v.elements.i[step])
								continue;

							uint32_t num_ne = htonl(*(uint32_t *)rec->fields[i].data.v.elements.i[step]);
							if (write(fd, &num_ne, sizeof(num_ne)) == -1)
							{
								perror("failed write int array to file");
								return 0;
							}
							step++;
						}
					}

					/*
					 * move the file pointer
					 * as much as the padding value
					 * if it si bigger than 0
					 * */
					if (pd_he > 0)
					{
						if (move_in_file_bytes(fd, pd_he * sizeof(int)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}
					}

					uint64_t update_arr_ne = bswap_64(0);
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				if (go_back_to_first_rec > 0)
				{
					if (find_record_position(fd, go_back_to_first_rec) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}

			break;
		}
		case TYPE_ARRAY_LONG:
		{
			if (!update)
			{
				/*write the size of the array */
				uint32_t size_ne = htonl((uint32_t)rec->fields[i].data.v.size);
				if (write(fd, &size_ne, sizeof(size_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				uint32_t padding_ne = htonl(0);
				if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				for (int k = 0; k < rec->fields[i].data.v.size; k++)
				{
					if (!rec->fields[i].data.v.elements.l[k])
						continue;

					uint64_t num_ne = bswap_64(*(uint64_t *)rec->fields[i].data.v.elements.l[k]);
					if (write(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				uint64_t upd_ne = bswap_64(0);
				if (write(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}
			}
			else
			{
				/* update branch*/
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int step = 0;
				int sz = 0;
				int k = 0;
				int padding_value = 0;
				do
				{
					/* check the size */
					uint32_t sz_ne = 0;
					if (read(fd, &sz_ne, sizeof(sz_ne)) == -1)
					{
						fprintf(stderr, "can't read int array size.\n");
						return 0;
					}

					sz = (int)ntohl(sz_ne);
					if (rec->fields[i].data.v.size < sz ||
						rec->fields[i].data.v.size == sz)
						break;

					/*read the padding data*/
					uint32_t pd_ne = 0;
					if (read(fd, &pd_ne, sizeof(pd_ne)) == -1)
					{
						fprintf(stderr, "can't read padding array.\n");
						return 0;
					}

					padding_value = (int)ntohl(pd_ne);

					if (step >= sz)
					{
						int array_last = 0;
						int exit = 0;
						if ((array_last = is_array_last_block(fd, sz, sizeof(int))) == -1)
						{
							fprintf(stderr, "can't verify array last block %s:%d.\n", F, L - 1);
							return 0;
						}

						if (rec->fields[i].data.v.size < (sz + step) && array_last)
						{
							int pad_value = sz - (rec->fields[i].data.v.size - step);
							padding_value += pad_value;

							sz = rec->fields[i].data.v.size - step;
							exit = 1;

							if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
							{
								__er_file_pointer(F, L - 1);
								return 0;
							}

							/* write the updated size of the array */
							uint32_t new_sz = htonl((uint32_t)sz);
							if (write(fd, &new_sz, sizeof(new_sz)) == -1)
							{
								perror("error in writing remaining size int array.\n");
								return 0;
							}

							/* write the updated padding value */
							uint32_t new_pd = htonl((uint32_t)padding_value);
							if (write(fd, &new_pd, sizeof(new_pd)) == -1)
							{
								perror("error in writing new pading value int array.\n");
								return 0;
							}
						}
						else if (rec->fields[i].data.v.size == (sz + step) && array_last)
						{
							exit = 1;
						}

						while (sz)
						{
							if (step < rec->fields[i].data.v.size)
							{
								uint64_t num_ne = bswap_64(*(uint64_t *)rec->fields[i].data.v.elements.l[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
							sz--;
						}

						if (exit)
						{
							/*write the epty update offset*/
							uint64_t empty_offset = bswap_64(0);
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}
					else
					{

						int exit = 0;
						for (k = 0; k < sz; k++)
						{
							if (step > 0 && k == 0)
							{
								if ((step + sz) > rec->fields[i].data.v.size)
								{
									int pad = sz - (rec->fields[i].data.v.size - step);
									padding_value += pad;

									sz = rec->fields[i].data.v.size - step;
									exit = 1;

									if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
									{
										__er_file_pointer(F, L - 1);
										return 0;
									}

									/* write the updated size of the array */
									uint32_t new_sz = htonl((uint32_t)sz);
									if (write(fd, &new_sz, sizeof(new_sz)) == -1)
									{
										perror("error in writing remaining size int array.\n");
										return 0;
									}

									/*write padding */
									uint32_t new_pd = htonl((uint32_t)padding_value);
									if (write(fd, &new_pd, sizeof(new_pd)) == -1)
									{
										perror("error in writing new padd int array.\n");
										return 0;
									}
								}
							}

							if (step < rec->fields[i].data.v.size)
							{
								if (!rec->fields[i].data.v.elements.l[step])
									continue;

								uint64_t num_ne = bswap_64(*(uint64_t *)rec->fields[i].data.v.elements.l[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
						}

						if (exit)
						{
							if (padding_value > 0)
							{
								if (move_in_file_bytes(fd, padding_value * sizeof(int)) == -1)
								{
									__er_file_pointer(F, L - 1);
									return 0;
								}
							}
							/*write the epty update offset*/
							uint64_t empty_offset = bswap_64(0);
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}

					if (padding_value > 0)
					{
						if (move_in_file_bytes(fd, padding_value * sizeof(int)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
					}

					uint64_t update_off_ne = 0;
					off_t go_back_to = get_file_offset(fd);

					if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
					{
						perror("failed read update off_t int array.\n");
						return 0;
					}

					if (go_back_to_first_rec == 0)
						go_back_to_first_rec = go_back_to + sizeof(update_off_ne);

					update_pos = (off_t)bswap_64(update_off_ne);
					if (update_pos == 0)
					{
						/*go to EOF*/
						if ((update_pos = go_to_EOF(fd)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
						/* write the size of the array */
						int size_left = rec->fields[i].data.v.size - step;
						uint32_t size_left_ne = htonl((uint32_t)size_left);
						if (write(fd, &size_left_ne, sizeof(size_left_ne)) == -1)
						{
							perror("error in writing remaining size int array.\n");
							return 0;
						}

						uint32_t padding_ne = htonl(0);
						if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						for (int j = 0; j < size_left; j++)
						{
							if (step < rec->fields[i].data.v.size)
							{
								if (!rec->fields[i].data.v.elements.l[step])
									continue;

								uint64_t num_ne = bswap_64(*(uint64_t *)rec->fields[i].data.v.elements.l[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
						}

						/*write the empty update offset*/
						uint64_t empty_offset = bswap_64(0);
						if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						if (find_record_position(fd, go_back_to) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}

						update_off_ne = (uint64_t)bswap_64((uint64_t)update_pos);
						if (write(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
						{
							fprintf(stderr, "can't write update position int array, %s:%d.\n",
									F, L - 1);
							return 0;
						}

						break;
					}

					if (find_record_position(fd, update_pos) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

				} while (update_pos > 0);

				if (rec->fields[i].data.v.size < sz)
				{

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/*write the size of the array */
					uint32_t size_ne = htonl((uint32_t)rec->fields[i].data.v.size);
					if (write(fd, &size_ne, sizeof(size_ne)) == -1)
					{
						perror("error in writing size array to file.\n");
						return 0;
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)ntohl(pad_ne);
					pd_he += (sz - rec->fields[i].data.v.size);

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/* write the padding to apply after the  array */
					pad_ne = htonl((uint32_t)pd_he);
					if (write(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					for (int k = step; k < rec->fields[i].data.v.size; k++)
					{
						if (!rec->fields[i].data.v.elements.l[k])
							continue;

						uint64_t num_ne = bswap_64(*(uint64_t *)rec->fields[i].data.v.elements.l[k]);
						if (write(fd, &num_ne, sizeof(num_ne)) == -1)
						{
							perror("failed write int array to file");
							return 0;
						}
					}

					/*
					 * move the file pointer after the array
					 * as much as the pad
					 * */
					if (move_in_file_bytes(fd, pd_he * sizeof(int)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					uint64_t update_arr_ne = bswap_64(0);
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}
				else if (rec->fields[i].data.v.size == sz)
				{
					/*
					 * the sizes are the same
					 * we simply write the array.
					 * */
					if (step > 0)
					{
						if (move_in_file_bytes(fd, -sizeof(sz)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						int size_left = rec->fields[i].data.v.size - step;
						uint32_t sz_ne = htonl((uint32_t)size_left);
						if (write(fd, &sz_ne, sizeof(sz_ne)) == -1)
						{
							fprintf(stderr, "write failed %s:%d.\n", F, L - 1);
							return 0;
						}
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)ntohl(pad_ne);
					for (int k = 0; k < rec->fields[i].data.v.size; k++)
					{
						if (step < rec->fields[i].data.v.size)
						{
							if (!rec->fields[i].data.v.elements.l[step])
								continue;

							uint64_t num_ne = bswap_64(*(uint64_t *)rec->fields[i].data.v.elements.l[step]);
							if (write(fd, &num_ne, sizeof(num_ne)) == -1)
							{
								perror("failed write int array to file");
								return 0;
							}
							step++;
						}
					}

					/*
					 * move the file pointer
					 * as much as the padding value
					 * if it si bigger than 0
					 * */
					if (pd_he > 0)
					{
						if (move_in_file_bytes(fd, pd_he * sizeof(int)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}
					}

					uint64_t update_arr_ne = bswap_64(0);
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				if (go_back_to_first_rec > 0)
				{
					if (find_record_position(fd, go_back_to_first_rec) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}
			break;
		}
		case TYPE_ARRAY_FLOAT:
		{
			if (!update)
			{
				/*write the size of the array */
				uint32_t size_ne = htonl((uint32_t)rec->fields[i].data.v.size);
				if (write(fd, &size_ne, sizeof(size_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				uint32_t padding_ne = htonl(0);
				if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				for (int k = 0; k < rec->fields[i].data.v.size; k++)
				{
					if (!rec->fields[i].data.v.elements.f[k])
						continue;

					uint32_t num_ne = htonf(*rec->fields[i].data.v.elements.f[k]);
					if (write(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				uint64_t upd_ne = bswap_64(0);
				if (write(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}
			}
			else
			{
				/* update branch*/
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int step = 0;
				int sz = 0;
				int k = 0;
				int padding_value = 0;
				do
				{
					/* check the size */
					uint32_t sz_ne = 0;
					if (read(fd, &sz_ne, sizeof(sz_ne)) == -1)
					{
						fprintf(stderr, "can't read int array size.\n");
						return 0;
					}

					sz = (int)ntohl(sz_ne);
					if (rec->fields[i].data.v.size < sz ||
						rec->fields[i].data.v.size == sz)
						break;

					/*read the padding data*/
					uint32_t pd_ne = 0;
					if (read(fd, &pd_ne, sizeof(pd_ne)) == -1)
					{
						fprintf(stderr, "can't read padding array.\n");
						return 0;
					}

					padding_value = (int)ntohl(pd_ne);

					if (step >= sz)
					{
						int array_last = 0;
						int exit = 0;
						if ((array_last = is_array_last_block(fd, sz, sizeof(int))) == -1)
						{
							fprintf(stderr, "can't verify array last block %s:%d.\n", F, L - 1);
							return 0;
						}

						if (rec->fields[i].data.v.size < (sz + step) && array_last)
						{
							int pad_value = sz - (rec->fields[i].data.v.size - step);
							padding_value += pad_value;

							sz = rec->fields[i].data.v.size - step;
							exit = 1;

							if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
							{
								__er_file_pointer(F, L - 1);
								return 0;
							}

							/* write the updated size of the array */
							uint32_t new_sz = htonl((uint32_t)sz);
							if (write(fd, &new_sz, sizeof(new_sz)) == -1)
							{
								perror("error in writing remaining size int array.\n");
								return 0;
							}

							/* write the updated padding value */
							uint32_t new_pd = htonl((uint32_t)padding_value);
							if (write(fd, &new_pd, sizeof(new_pd)) == -1)
							{
								perror("error in writing new pading value int array.\n");
								return 0;
							}
						}
						else if (rec->fields[i].data.v.size == (sz + step) && array_last)
						{
							exit = 1;
						}

						while (sz)
						{
							if (step < rec->fields[i].data.v.size)
							{
								uint32_t num_ne = htonf(*rec->fields[i].data.v.elements.f[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
							sz--;
						}

						if (exit)
						{
							/*write the epty update offset*/
							uint64_t empty_offset = bswap_64(0);
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}
					else
					{

						int exit = 0;
						for (k = 0; k < sz; k++)
						{
							if (step > 0 && k == 0)
							{
								if ((step + sz) > rec->fields[i].data.v.size)
								{
									int pad = sz - (rec->fields[i].data.v.size - step);
									padding_value += pad;

									sz = rec->fields[i].data.v.size - step;
									exit = 1;

									if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
									{
										__er_file_pointer(F, L - 1);
										return 0;
									}

									/* write the updated size of the array */
									uint32_t new_sz = htonl((uint32_t)sz);
									if (write(fd, &new_sz, sizeof(new_sz)) == -1)
									{
										perror("error in writing remaining size int array.\n");
										return 0;
									}

									/*write padding */
									uint32_t new_pd = htonl((uint32_t)padding_value);
									if (write(fd, &new_pd, sizeof(new_pd)) == -1)
									{
										perror("error in writing new padd int array.\n");
										return 0;
									}
								}
							}

							if (step < rec->fields[i].data.v.size)
							{
								if (!rec->fields[i].data.v.elements.f[step])
									continue;

								uint32_t num_ne = htonf(*rec->fields[i].data.v.elements.f[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
						}

						if (exit)
						{

							if (padding_value > 0)
							{
								if (move_in_file_bytes(fd, padding_value * sizeof(int)) == -1)
								{
									__er_file_pointer(F, L - 1);
									return 0;
								}
							}
							/*write the epty update offset*/
							uint64_t empty_offset = bswap_64(0);
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}

					if (padding_value > 0)
					{
						if (move_in_file_bytes(fd, padding_value * sizeof(int)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
					}

					uint64_t update_off_ne = 0;
					off_t go_back_to = get_file_offset(fd);

					if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
					{
						perror("failed read update off_t int array.\n");
						return 0;
					}

					if (go_back_to_first_rec == 0)
						go_back_to_first_rec = go_back_to + sizeof(update_off_ne);

					update_pos = (off_t)bswap_64(update_off_ne);
					if (update_pos == 0)
					{
						/*go to EOF*/
						if ((update_pos = go_to_EOF(fd)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
						/* write the size of the array */
						int size_left = rec->fields[i].data.v.size - step;
						uint32_t size_left_ne = htonl((uint32_t)size_left);
						if (write(fd, &size_left_ne, sizeof(size_left_ne)) == -1)
						{
							perror("error in writing remaining size int array.\n");
							return 0;
						}

						uint32_t padding_ne = htonl(0);
						if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						for (int j = 0; j < size_left; j++)
						{
							if (step < rec->fields[i].data.v.size)
							{
								if (!rec->fields[i].data.v.elements.f[step])
									continue;

								uint32_t num_ne = htonf(*rec->fields[i].data.v.elements.f[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
						}

						/*write the empty update offset*/
						uint64_t empty_offset = bswap_64(0);
						if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						if (find_record_position(fd, go_back_to) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}

						update_off_ne = (uint64_t)bswap_64((uint64_t)update_pos);
						if (write(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
						{
							fprintf(stderr, "can't write update position int array, %s:%d.\n",
									F, L - 1);
							return 0;
						}

						break;
					}

					if (find_record_position(fd, update_pos) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

				} while (update_pos > 0);

				if (rec->fields[i].data.v.size < sz)
				{

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/*write the size of the array */
					uint32_t size_ne = htonl((uint32_t)rec->fields[i].data.v.size);
					if (write(fd, &size_ne, sizeof(size_ne)) == -1)
					{
						perror("error in writing size array to file.\n");
						return 0;
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)ntohl(pad_ne);
					pd_he += (sz - rec->fields[i].data.v.size);

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/* write the padding to apply after the  array */
					pad_ne = htonl((uint32_t)pd_he);
					if (write(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					for (int k = step; k < rec->fields[i].data.v.size; k++)
					{
						if (!rec->fields[i].data.v.elements.f[k])
							continue;

						uint32_t num_ne = htonf(*rec->fields[i].data.v.elements.f[k]);
						if (write(fd, &num_ne, sizeof(num_ne)) == -1)
						{
							perror("failed write int array to file");
							return 0;
						}
					}

					/*
					 * move the file pointer after the array
					 * as much as the pad
					 * */
					if (move_in_file_bytes(fd, pd_he * sizeof(int)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					uint64_t update_arr_ne = bswap_64(0);
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}
				else if (rec->fields[i].data.v.size == sz)
				{
					/*
					 * the sizes are the same
					 * we simply write the array.
					 * */
					if (step > 0)
					{
						if (move_in_file_bytes(fd, -sizeof(sz)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						int size_left = rec->fields[i].data.v.size - step;
						uint32_t sz_ne = htonl((uint32_t)size_left);
						if (write(fd, &sz_ne, sizeof(sz_ne)) == -1)
						{
							fprintf(stderr, "write failed %s:%d.\n", F, L - 1);
							return 0;
						}
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)ntohl(pad_ne);
					for (int k = 0; k < rec->fields[i].data.v.size; k++)
					{
						if (step < rec->fields[i].data.v.size)
						{
							if (!rec->fields[i].data.v.elements.f[step])
								continue;

							uint32_t num_ne = htonl(*(uint32_t *)rec->fields[i].data.v.elements.i[step]);
							if (write(fd, &num_ne, sizeof(num_ne)) == -1)
							{
								perror("failed write int array to file");
								return 0;
							}
							step++;
						}
					}

					/*
					 * move the file pointer
					 * as much as the padding value
					 * if it si bigger than 0
					 * */
					if (pd_he > 0)
					{
						if (move_in_file_bytes(fd, pd_he * sizeof(int)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}
					}

					uint64_t update_arr_ne = bswap_64(0);
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				if (go_back_to_first_rec > 0)
				{
					if (find_record_position(fd, go_back_to_first_rec) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}

			break;
		}
		case TYPE_ARRAY_STRING:
		{
			if (!update)
			{
				/*write the size of the array */
				uint32_t size_ne = htonl((uint32_t)rec->fields[i].data.v.size);
				if (write(fd, &size_ne, sizeof(size_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				uint32_t padding_ne = htonl(0);
				if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				for (int k = 0; k < rec->fields[i].data.v.size; k++)
				{
					if (!rec->fields[i].data.v.elements.s[k])
						continue;

					lt = strlen(rec->fields[i].data.v.elements.s[k]);
					buff_update = lt * 2;

					/* adding 1 for '\0'*/
					lt++, buff_update++;
					buff_w = calloc(buff_update, sizeof(char));
					if (!buff_w)
					{
						__er_calloc(F, L - 2);
						return 0;
					}

					strncpy(buff_w, rec->fields[i].data.v.elements.s[k], lt - 1);

					uint64_t l_ne = bswap_64(lt);
					uint64_t bu_ne = bswap_64(buff_update);
					uint64_t str_loc_ne = bswap_64(str_loc);

					if (write(fd, &str_loc_ne, sizeof(str_loc_ne)) < 0 ||
						write(fd, &l_ne, sizeof(l_ne)) < 0 ||
						write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
						write(fd, buff_w, buff_update) < 0)
					{
						perror("write file failed: ");
						printf(" %s:%d", F, L - 2);
						free(buff_w);
						return 0;
					}

					free(buff_w);
				}

				uint64_t upd_ne = bswap_64(0);
				if (write(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}
			}
			else
			{
				/* update branch*/
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int step = 0;
				int sz = 0;
				int k = 0;
				int padding_value = 0;
				do
				{
					/* check the size */
					uint32_t sz_ne = 0;
					if (read(fd, &sz_ne, sizeof(sz_ne)) == -1)
					{
						fprintf(stderr, "can't read int array size.\n");
						return 0;
					}

					sz = (int)ntohl(sz_ne);
					if (rec->fields[i].data.v.size < sz ||
						rec->fields[i].data.v.size == sz)
						break;

					/*read the padding data*/
					uint32_t pd_ne = 0;
					if (read(fd, &pd_ne, sizeof(pd_ne)) == -1)
					{
						fprintf(stderr, "can't read padding array.\n");
						return 0;
					}

					padding_value = (int)ntohl(pd_ne);

					if (step >= sz)
					{
						int array_last = 0;
						int exit = 0;
						if ((array_last = is_array_last_block(fd, sz, sizeof(int))) == -1)
						{
							fprintf(stderr, "can't verify array last block %s:%d.\n", F, L - 1);
							return 0;
						}

						if (rec->fields[i].data.v.size < (sz + step) && array_last)
						{
							int pad_value = sz - (rec->fields[i].data.v.size - step);
							padding_value += pad_value;

							sz = rec->fields[i].data.v.size - step;
							exit = 1;

							if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
							{
								__er_file_pointer(F, L - 1);
								return 0;
							}

							/* write the updated size of the array */
							uint32_t new_sz = htonl((uint32_t)sz);
							if (write(fd, &new_sz, sizeof(new_sz)) == -1)
							{
								perror("error in writing remaining size int array.\n");
								return 0;
							}

							/* write the updated padding value */
							uint32_t new_pd = htonl((uint32_t)padding_value);
							if (write(fd, &new_pd, sizeof(new_pd)) == -1)
							{
								perror("error in writing new pading value int array.\n");
								return 0;
							}
						}
						else if (rec->fields[i].data.v.size == (sz + step) && array_last)
						{
							exit = 1;
						}

						while (sz)
						{
							if (step < rec->fields[i].data.v.size)
							{
								/*string update process*/
								/*save the starting offset for the string record*/
								if ((bg_pos = get_file_offset(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								/*read pos of new str if any*/
								uint64_t str_loc_ne = 0;
								if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == STATUS_ERROR)
								{
									perror("can't read string location: ");
									printf(" %s:%d", F, L - 3);
									return 0;
								}
								str_loc = (off_t)bswap_64(str_loc_ne);

								/*store record  beginning pos*/
								if ((af_str_loc_pos = get_file_offset(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								uint64_t bu_ne = 0;
								uint64_t l_ne = 0;
								if (read(fd, &l_ne, sizeof(l_ne)) < 0 ||
									read(fd, &bu_ne, sizeof(bu_ne)) < 0)
								{
									perror("can't read safety buffer before writing string.\n");
									printf("%s:%d", F, L - 3);
									return 0;
								}

								buff_update = (off_t)bswap_64(bu_ne);
								lt = (off_t)bswap_64(l_ne);

								/*save the end offset of the first string record */
								if ((go_back_to = get_file_offset(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								/*add the buffer size to this file off_t*/
								/*so we can reposition right after the 1st string after the writing */
								go_back_to += buff_update;
								if (str_loc > 0)
								{
									/*set the file pointer to str_loc*/
									if (find_record_position(fd, str_loc) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}
									/*
									 * in the case of a regular buffer update we have
									 *  to save the off_t to get back to it later
									 * */
									if ((move_to = get_file_offset(fd)) == -1)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}

									uint64_t bu_ne = 0;
									uint64_t l_ne = 0;
									if (read(fd, &l_ne, sizeof(l_ne)) < 0 ||
										read(fd, &bu_ne, sizeof(bu_ne)) < 0)
									{
										perror("read file.\n");
										printf("%s:%d", F, L - 3);
										return 0;
									}

									buff_update = (off_t)bswap_64(bu_ne);
									lt = (off_t)bswap_64(l_ne);
								}

								new_lt = strlen(rec->fields[i].data.v.elements.s[step]) + 1; /*get new str length*/

								if (new_lt > buff_update)
								{
									/*
									 * if the new length is bigger then the buffer,
									 * set the file pointer to the end of the file
									 * to write the new data
									 * */
									if ((eof = go_to_EOF(fd)) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}

									/*expand the buff_update only for the bytes needed*/
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
										__er_calloc(F, L - 2);
										return 0;
									}
								}

								strncpy(buff_w, rec->fields[i].data.v.elements.s[step], new_lt - 1);
								/*
								 * if we did not move to another position
								 * set the file pointer back to the begginning of the string record
								 * to overwrite the data accordingly
								 * */
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
									if (find_record_position(fd, move_to) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 3);
										free(buff_w);
										return 0;
									}
								}

								/*
								 * write the data to file --
								 * the file pointer is always pointing to the
								 * right position at this point */
								lt = new_lt;
								buff_update = __n_buff_update > 0 ? __n_buff_update : buff_update;
								l_ne = bswap_64(lt);
								bu_ne = bswap_64(buff_update);

								if (write(fd, &l_ne, sizeof(lt)) < 0 ||
									write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
									write(fd, buff_w, buff_update) < 0)
								{
									perror("error in writing type string (char *)file.\n");
									free(buff_w);
									return 0;
								}

								free(buff_w);

								/*
								 * if eof is bigger than 0 means we updated the string
								 * we need to save the off_t of the new written data
								 * at the start of the original.
								 * */
								if (eof > 0)
								{
									/*go at the beginning of the str record*/
									if (find_record_position(fd, bg_pos) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}

									/*update new string position*/
									uint64_t eof_ne = bswap_64((uint64_t)eof);
									if (write(fd, &eof_ne, sizeof(eof_ne)) == STATUS_ERROR)
									{
										perror("write file: ");
										printf(" %s:%d", F, L - 3);
										return 0;
									}

									/*set file pointer to the end of the 1st string rec*/
									/*this step is crucial to avoid losing data        */

									if (find_record_position(fd, go_back_to) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}
								}
								else if (str_loc > 0)
								{
									/*
									 * Make sure that in all cases
									 * we go back to the end of the 1st record
									 * */
									if (find_record_position(fd, go_back_to) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}
								}
								__n_buff_update = 0;
								eof = 0;
								step++;
							}
							sz--;
						}

						if (exit)
						{
							/*write the epty update offset*/
							uint64_t empty_offset = bswap_64(0);
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}
					else
					{

						int exit = 0;
						for (k = 0; k < sz; k++)
						{
							if (step > 0 && k == 0)
							{
								if ((step + sz) > rec->fields[i].data.v.size)
								{
									int pad = sz - (rec->fields[i].data.v.size - step);
									padding_value += pad;

									sz = rec->fields[i].data.v.size - step;
									exit = 1;

									if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
									{
										__er_file_pointer(F, L - 1);
										return 0;
									}

									/* write the updated size of the array */
									uint32_t new_sz = htonl((uint32_t)sz);
									if (write(fd, &new_sz, sizeof(new_sz)) == -1)
									{
										perror("error in writing remaining size int array.\n");
										return 0;
									}

									/*write padding */
									uint32_t new_pd = htonl((uint32_t)padding_value);
									if (write(fd, &new_pd, sizeof(new_pd)) == -1)
									{
										perror("error in writing new padd int array.\n");
										return 0;
									}
								}
							}

							if (step < rec->fields[i].data.v.size)
							{
								if (!rec->fields[i].data.v.elements.s[step])
									continue;

								/*string update process*/
								/*save the starting offset for the string record*/
								if ((bg_pos = get_file_offset(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								/*read pos of new str if any*/
								uint64_t str_loc_ne = 0;
								if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == STATUS_ERROR)
								{
									perror("can't read string location: ");
									printf(" %s:%d", F, L - 3);
									return 0;
								}
								str_loc = (off_t)bswap_64(str_loc_ne);

								/*store record  beginning pos*/
								if ((af_str_loc_pos = get_file_offset(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								uint64_t bu_ne = 0;
								uint64_t l_ne = 0;
								if (read(fd, &l_ne, sizeof(l_ne)) < 0 ||
									read(fd, &bu_ne, sizeof(bu_ne)) < 0)
								{
									perror("can't read safety buffer before writing string.\n");
									printf("%s:%d", F, L - 3);
									return 0;
								}

								buff_update = (off_t)bswap_64(bu_ne);
								lt = (off_t)bswap_64(l_ne);

								/*save the end offset of the first string record */
								if ((go_back_to = get_file_offset(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								/*add the buffer size to this file off_t*/
								/*so we can reposition right after the 1st string after the writing */
								go_back_to += buff_update;
								if (str_loc > 0)
								{
									/*set the file pointer to str_loc*/
									if (find_record_position(fd, str_loc) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}
									/*
									 * in the case of a regular buffer update we have
									 *  to save the off_t to get back to it later
									 * */
									if ((move_to = get_file_offset(fd)) == -1)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}

									uint64_t bu_ne = 0;
									uint64_t l_ne = 0;
									if (read(fd, &l_ne, sizeof(l_ne)) < 0 ||
										read(fd, &bu_ne, sizeof(bu_ne)) < 0)
									{
										perror("read file.\n");
										printf("%s:%d", F, L - 3);
										return 0;
									}

									buff_update = (off_t)bswap_64(bu_ne);
									lt = (off_t)bswap_64(l_ne);
								}

								new_lt = strlen(rec->fields[i].data.v.elements.s[step]) + 1; /*get new str length*/

								if (new_lt > buff_update)
								{
									/*
									 * if the new length is bigger then the buffer,
									 * set the file pointer to the end of the file
									 * to write the new data
									 * */
									if ((eof = go_to_EOF(fd)) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}

									/*expand the buff_update only for the bytes needed*/
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
										__er_calloc(F, L - 2);
										return 0;
									}
								}

								strncpy(buff_w, rec->fields[i].data.v.elements.s[step], new_lt - 1);
								/*
								 * if we did not move to another position
								 * set the file pointer back to the begginning of the string record
								 * to overwrite the data accordingly
								 * */
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
									if (find_record_position(fd, move_to) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 3);
										free(buff_w);
										return 0;
									}
								}

								/*
								 * write the data to file --
								 * the file pointer is always pointing to the
								 * right position at this point */
								lt = new_lt;
								buff_update = __n_buff_update > 0 ? __n_buff_update : buff_update;
								l_ne = bswap_64(lt);
								bu_ne = bswap_64(buff_update);

								if (write(fd, &l_ne, sizeof(lt)) < 0 ||
									write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
									write(fd, buff_w, buff_update) < 0)
								{
									perror("error in writing type string (char *)file.\n");
									free(buff_w);
									return 0;
								}

								free(buff_w);

								/*
								 * if eof is bigger than 0 means we updated the string
								 * we need to save the off_t of the new written data
								 * at the start of the original.
								 * */
								if (eof > 0)
								{
									/*go at the beginning of the str record*/
									if (find_record_position(fd, bg_pos) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}

									/*update new string position*/
									uint64_t eof_ne = bswap_64((uint64_t)eof);
									if (write(fd, &eof_ne, sizeof(eof_ne)) == STATUS_ERROR)
									{
										perror("write file: ");
										printf(" %s:%d", F, L - 3);
										return 0;
									}

									/*set file pointer to the end of the 1st string rec*/
									/*this step is crucial to avoid losing data        */

									if (find_record_position(fd, go_back_to) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}
								}
								else if (str_loc > 0)
								{
									/*
									 * Make sure that in all cases
									 * we go back to the end of the 1st record
									 * */
									if (find_record_position(fd, go_back_to) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}
								}
							}
							__n_buff_update = 0;
							eof = 0;
							step++;
						}

						if (exit)
						{

							if (padding_value > 0)
							{
								if (move_in_file_bytes(fd, padding_value * sizeof(int)) == -1)
								{
									__er_file_pointer(F, L - 1);
									return 0;
								}
							}
							/*write the epty update offset*/
							uint64_t empty_offset = bswap_64(0);
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}

					if (padding_value > 0)
					{
						if (move_in_file_bytes(fd, padding_value * sizeof(int)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
					}

					uint64_t update_off_ne = 0;
					off_t go_back_to_string_array = get_file_offset(fd);

					if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
					{
						perror("failed read update off_t int array.\n");
						return 0;
					}

					if (go_back_to_first_rec == 0)
						go_back_to_first_rec = go_back_to + sizeof(update_off_ne);

					update_pos = (off_t)bswap_64(update_off_ne);
					if (update_pos == 0)
					{
						/*go to EOF*/
						if ((update_pos = go_to_EOF(fd)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
						/* write the size of the array */
						int size_left = rec->fields[i].data.v.size - step;
						uint32_t size_left_ne = htonl((uint32_t)size_left);
						if (write(fd, &size_left_ne, sizeof(size_left_ne)) == -1)
						{
							perror("error in writing remaining size int array.\n");
							return 0;
						}

						uint32_t padding_ne = htonl(0);
						if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						for (int j = 0; j < size_left; j++)
						{
							if (step < rec->fields[i].data.v.size)
							{
								if (!rec->fields[i].data.v.elements.s[step])
									continue;

								/*string update process*/
								lt = strlen(rec->fields[i].data.v.elements.s[k]);
								buff_update = lt * 2;

								/* adding 1 for '\0'*/
								lt++, buff_update++;
								buff_w = calloc(buff_update, sizeof(char));
								if (!buff_w)
								{
									__er_calloc(F, L - 2);
									return 0;
								}

								strncpy(buff_w, rec->fields[i].data.v.elements.s[step], lt - 1);

								uint64_t l_ne = bswap_64(lt);
								uint64_t bu_ne = bswap_64(buff_update);
								uint64_t str_loc_ne = bswap_64(str_loc);

								if (write(fd, &str_loc_ne, sizeof(str_loc_ne)) < 0 ||
									write(fd, &l_ne, sizeof(l_ne)) < 0 ||
									write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
									write(fd, buff_w, buff_update) < 0)
								{
									perror("write file failed: ");
									printf(" %s:%d", F, L - 2);
									free(buff_w);
									return 0;
								}

								free(buff_w);

								step++;
							}
						}

						/*write the empty update offset*/
						uint64_t empty_offset = bswap_64(0);
						if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						if (find_record_position(fd, go_back_to_string_array) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}

						update_off_ne = (uint64_t)bswap_64((uint64_t)update_pos);
						if (write(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
						{
							fprintf(stderr, "can't write update position int array, %s:%d.\n",
									F, L - 1);
							return 0;
						}

						break;
					}

					if (find_record_position(fd, update_pos) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

				} while (update_pos > 0);

				if (rec->fields[i].data.v.size < sz)
				{

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/*write the size of the array */
					uint32_t size_ne = htonl((uint32_t)rec->fields[i].data.v.size);
					if (write(fd, &size_ne, sizeof(size_ne)) == -1)
					{
						perror("error in writing size array to file.\n");
						return 0;
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)ntohl(pad_ne);
					pd_he += (sz - rec->fields[i].data.v.size);

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/* write the padding to apply after the  array */
					pad_ne = htonl((uint32_t)pd_he);
					if (write(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					for (int k = step; k < rec->fields[i].data.v.size; k++)
					{
						if (!rec->fields[i].data.v.elements.s[k])
							continue;

						/*string update process*/
						if ((bg_pos = get_file_offset(fd)) == STATUS_ERROR)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						/*read pos of new str if any*/
						uint64_t str_loc_ne = 0;
						if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == STATUS_ERROR)
						{
							perror("can't read string location: ");
							printf(" %s:%d", F, L - 3);
							return 0;
						}
						str_loc = (off_t)bswap_64(str_loc_ne);

						/*store record  beginning pos*/
						if ((af_str_loc_pos = get_file_offset(fd)) == STATUS_ERROR)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						uint64_t bu_ne = 0;
						uint64_t l_ne = 0;
						if (read(fd, &l_ne, sizeof(l_ne)) < 0 ||
							read(fd, &bu_ne, sizeof(bu_ne)) < 0)
						{
							perror("can't read safety buffer before writing string.\n");
							printf("%s:%d", F, L - 3);
							return 0;
						}

						buff_update = (off_t)bswap_64(bu_ne);
						lt = (off_t)bswap_64(l_ne);

						/*save the end offset of the first string record */
						if ((go_back_to = get_file_offset(fd)) == STATUS_ERROR)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						/*add the buffer size to this file off_t*/
						/*so we can reposition right after the 1st string after the writing */
						go_back_to += buff_update;
						if (str_loc > 0)
						{
							/*set the file pointer to str_loc*/
							if (find_record_position(fd, str_loc) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}
							/*
							 * in the case of a regular buffer update we have
							 *  to save the off_t to get back to it later
							 * */
							if ((move_to = get_file_offset(fd)) == -1)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}

							uint64_t bu_ne = 0;
							uint64_t l_ne = 0;
							if (read(fd, &l_ne, sizeof(l_ne)) < 0 ||
								read(fd, &bu_ne, sizeof(bu_ne)) < 0)
							{
								perror("read file.\n");
								printf("%s:%d", F, L - 3);
								return 0;
							}

							buff_update = (off_t)bswap_64(bu_ne);
							lt = (off_t)bswap_64(l_ne);
						}

						new_lt = strlen(rec->fields[i].data.v.elements.s[k]) + 1; /*get new str length*/

						if (new_lt > buff_update)
						{
							/*
							 * if the new length is bigger then the buffer,
							 * set the file pointer to the end of the file
							 * to write the new data
							 * */
							if ((eof = go_to_EOF(fd)) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}

							/*expand the buff_update only for the bytes needed*/
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
								__er_calloc(F, L - 2);
								return 0;
							}
						}

						strncpy(buff_w, rec->fields[i].data.v.elements.s[k], new_lt - 1);
						/*
						 * if we did not move to another position
						 * set the file pointer back to the begginning of the string record
						 * to overwrite the data accordingly
						 * */
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
							if (find_record_position(fd, move_to) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 3);
								free(buff_w);
								return 0;
							}
						}

						/*
						 * write the data to file --
						 * the file pointer is always pointing to the
						 * right position at this point */
						lt = new_lt;
						buff_update = __n_buff_update > 0 ? __n_buff_update : buff_update;
						l_ne = bswap_64(lt);
						bu_ne = bswap_64(buff_update);

						if (write(fd, &l_ne, sizeof(lt)) < 0 ||
							write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
							write(fd, buff_w, buff_update) < 0)
						{
							perror("error in writing type string (char *)file.\n");
							free(buff_w);
							return 0;
						}

						free(buff_w);

						/*
						 * if eof is bigger than 0 means we updated the string
						 * we need to save the off_t of the new written data
						 * at the start of the original.
						 * */
						if (eof > 0)
						{
							/*go at the beginning of the str record*/
							if (find_record_position(fd, bg_pos) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}

							/*update new string position*/
							uint64_t eof_ne = bswap_64((uint64_t)eof);
							if (write(fd, &eof_ne, sizeof(eof_ne)) == STATUS_ERROR)
							{
								perror("write file: ");
								printf(" %s:%d", F, L - 3);
								return 0;
							}

							/*set file pointer to the end of the 1st string rec*/
							/*this step is crucial to avoid losing data        */

							if (find_record_position(fd, go_back_to) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}
						}
						else if (str_loc > 0)
						{
							/*
							 * Make sure that in all cases
							 * we go back to the end of the 1st record
							 * */
							if (find_record_position(fd, go_back_to) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}
						}
					}

					/*
					 * move the file pointer after the array
					 * as much as the pad
					 * */
					if (move_in_file_bytes(fd, pd_he * sizeof(int)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					uint64_t update_arr_ne = bswap_64(0);
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}
				else if (rec->fields[i].data.v.size == sz)
				{
					/*
					 * the sizes are the same
					 * we simply write the array.
					 * */
					if (step > 0)
					{
						if (move_in_file_bytes(fd, -sizeof(sz)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						int size_left = rec->fields[i].data.v.size - step;
						uint32_t sz_ne = htonl((uint32_t)size_left);
						if (write(fd, &sz_ne, sizeof(sz_ne)) == -1)
						{
							fprintf(stderr, "write failed %s:%d.\n", F, L - 1);
							return 0;
						}
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)ntohl(pad_ne);
					for (int k = 0; k < rec->fields[i].data.v.size; k++)
					{
						if (step < rec->fields[i].data.v.size)
						{
							if (!rec->fields[i].data.v.elements.s[step])
								continue;

							/*string update process*/
							if ((bg_pos = get_file_offset(fd)) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}

							/*read pos of new str if any*/
							uint64_t str_loc_ne = 0;
							if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == STATUS_ERROR)
							{
								perror("can't read string location: ");
								printf(" %s:%d", F, L - 3);
								return 0;
							}
							str_loc = (off_t)bswap_64(str_loc_ne);

							/*store record  beginning pos*/
							if ((af_str_loc_pos = get_file_offset(fd)) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}

							uint64_t bu_ne = 0;
							uint64_t l_ne = 0;
							if (read(fd, &l_ne, sizeof(l_ne)) < 0 ||
								read(fd, &bu_ne, sizeof(bu_ne)) < 0)
							{
								perror("can't read safety buffer before writing string.\n");
								printf("%s:%d", F, L - 3);
								return 0;
							}

							buff_update = (off_t)bswap_64(bu_ne);
							lt = (off_t)bswap_64(l_ne);

							/*save the end offset of the first string record */
							if ((go_back_to = get_file_offset(fd)) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}

							/*add the buffer size to this file off_t*/
							/*so we can reposition right after the 1st string after the writing */
							go_back_to += buff_update;
							if (str_loc > 0)
							{
								/*set the file pointer to str_loc*/
								if (find_record_position(fd, str_loc) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}
								/*
								 * in the case of a regular buffer update we have
								 *  to save the off_t to get back to it later
								 * */
								if ((move_to = get_file_offset(fd)) == -1)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								uint64_t bu_ne = 0;
								uint64_t l_ne = 0;
								if (read(fd, &l_ne, sizeof(l_ne)) < 0 ||
									read(fd, &bu_ne, sizeof(bu_ne)) < 0)
								{
									perror("read file.\n");
									printf("%s:%d", F, L - 3);
									return 0;
								}

								buff_update = (off_t)bswap_64(bu_ne);
								lt = (off_t)bswap_64(l_ne);
							}

							new_lt = strlen(rec->fields[i].data.v.elements.s[step]) + 1; /*get new str length*/

							if (new_lt > buff_update)
							{
								/*
								 * if the new length is bigger then the buffer,
								 * set the file pointer to the end of the file
								 * to write the new data
								 * */
								if ((eof = go_to_EOF(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								/*expand the buff_update only for the bytes needed*/
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
									__er_calloc(F, L - 2);
									return 0;
								}
							}

							strncpy(buff_w, rec->fields[i].data.v.elements.s[step], new_lt - 1);
							/*
							 * if we did not move to another position
							 * set the file pointer back to the begginning of the string record
							 * to overwrite the data accordingly
							 * */
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
								if (find_record_position(fd, move_to) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 3);
									free(buff_w);
									return 0;
								}
							}

							/*
							 * write the data to file --
							 * the file pointer is always pointing to the
							 * right position at this point */
							lt = new_lt;
							buff_update = __n_buff_update > 0 ? __n_buff_update : buff_update;
							l_ne = bswap_64(lt);
							bu_ne = bswap_64(buff_update);

							if (write(fd, &l_ne, sizeof(lt)) < 0 ||
								write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
								write(fd, buff_w, buff_update) < 0)
							{
								perror("error in writing type string (char *)file.\n");
								free(buff_w);
								return 0;
							}

							free(buff_w);

							/*
							 * if eof is bigger than 0 means we updated the string
							 * we need to save the off_t of the new written data
							 * at the start of the original.
							 * */
							if (eof > 0)
							{
								/*go at the beginning of the str record*/
								if (find_record_position(fd, bg_pos) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								/*update new string position*/
								uint64_t eof_ne = bswap_64((uint64_t)eof);
								if (write(fd, &eof_ne, sizeof(eof_ne)) == STATUS_ERROR)
								{
									perror("write file: ");
									printf(" %s:%d", F, L - 3);
									return 0;
								}

								/*set file pointer to the end of the 1st string rec*/
								/*this step is crucial to avoid losing data        */

								if (find_record_position(fd, go_back_to) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}
							}
							else if (str_loc > 0)
							{
								/*
								 * Make sure that in all cases
								 * we go back to the end of the 1st record
								 * */
								if (find_record_position(fd, go_back_to) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}
							}

							/*zeroing the variables for th new cycle*/
							__n_buff_update = 0;
							eof = 0;
							step++;
						}
					}

					/*
					 * move the file pointer
					 * as much as the padding value
					 * if it si bigger than 0
					 * */
					if (pd_he > 0)
					{
						if (move_in_file_bytes(fd, pd_he * sizeof(int)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}
					}

					uint64_t update_arr_ne = bswap_64(0);
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				if (go_back_to_first_rec > 0)
				{
					if (find_record_position(fd, go_back_to_first_rec) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}

			break;
		}
		case TYPE_ARRAY_BYTE:
		{
		}
		case TYPE_ARRAY_DOUBLE:
		{
		}
		default:
			break;
		}
	}

	//	printf("the off set before writing the update pos is %ld.\n",get_file_offset(fd));

	uint64_t uot_ne = bswap_64((uint64_t)update_off_t);
	if (write(fd, &uot_ne, sizeof(uot_ne)) == STATUS_ERROR)
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
	uint64_t urec_ne = 0;
	if (read(fd, &urec_ne, sizeof(urec_ne)) == STATUS_ERROR)
	{
		perror("could not read the update record position (file.c l 424).\n");
		return -1;
	}

	return (off_t)bswap_64(urec_ne);
}

struct Record_f *read_file(int fd, char *file_name)
{

	uint32_t rfn_ne = 0;
	if (read(fd, &rfn_ne, sizeof(rfn_ne)) < 0)
	{
		perror("could not read fields number:");
		printf(" %s:%d.\n", __FILE__, __LINE__ - 1);
		return NULL;
	}

	int fields_num_r = htonl(rfn_ne);
	struct Record_f *rec = create_record(file_name, fields_num_r);
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

		uint64_t str_l_ne = 0;
		if (read(fd, &str_l_ne, sizeof(str_l_ne)) < 0)
		{
			perror("could not read size of field name, file.c l 458.\n");
			free_record(rec, fields_num_r);
			return NULL;
		}
		size_t lt = (size_t)bswap_64(str_l_ne);

		//	printf("size is %ld",lt);
		rec->fields[i].field_name = calloc(lt + 1, sizeof(char));

		if (!rec->fields[i].field_name)
		{
			printf("no memory for field name, file.cl 466.\n");
			free_record(rec, rec->fields_num);
			return NULL;
		}

		if (read(fd, rec->fields[i].field_name, lt) < 0)
		{
			perror("could not read field name, file.c l 472");
			free_record(rec, rec->fields_num);
			return NULL;
		}

		rec->fields[i].field_name[lt] = '\0';
		uint32_t ty_ne = 0;
		if (read(fd, &ty_ne, sizeof(ty_ne)) < 0)
		{
			perror("could not read type file.c l 481.");
			free_record(rec, rec->fields_num);
			return NULL;
		}

		rec->fields[i].type = (enum ValueType)ntohl(ty_ne);
		switch (rec->fields[i].type)
		{
		case TYPE_INT:
		{
			uint32_t i_ne = 0;
			if (read(fd, &i_ne, sizeof(uint32_t)) < 0)
			{
				perror("could not read type int file.c 491.\n");
				free_record(rec, rec->fields_num);
				return NULL;
			}
			rec->fields[i].data.i = (int)ntohl(i_ne);
			break;
		}
		case TYPE_LONG:
		{
			uint64_t l_ne = 0;
			if (read(fd, &l_ne, sizeof(l_ne)) < 0)
			{
				perror("could not read type long, file.c 498.\n");
				free_record(rec, rec->fields_num);
				return NULL;
			}

			rec->fields[i].data.l = (long)bswap_64(l_ne);
			break;
		}
		case TYPE_FLOAT:
		{
			uint32_t f_ne = 0;
			if (read(fd, &f_ne, sizeof(uint32_t)) < 0)
			{
				perror("could not read type float, file.c l 505.\n");
				free_record(rec, rec->fields_num);
				return NULL;
			}

			rec->fields[i].data.f = ntohf(f_ne);
			break;
		}
		case TYPE_STRING:
		{
			/*read pos of new str if any*/
			uint64_t str_loc_ne = 0;
			if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == -1)
			{
				perror("can't read string location: ");
				printf(" %s:%d", F, L - 3);
				return 0;
			}
			str_loc = (size_t)bswap_64(str_loc_ne);

			uint64_t l_ne = 0;
			if (read(fd, &l_ne, sizeof(l_ne)) < 0)
			{
				perror("read from file failed: ");
				printf("%s:%d.\n", F, L - 2);
				free_record(rec, rec->fields_num);
				return NULL;
			}

			l = (size_t)bswap_64(l_ne);

			uint64_t bu_up_ne = 0;
			if (read(fd, &bu_up_ne, sizeof(bu_up_ne)) < 0)
			{
				perror("read from file failed: ");
				printf("%s:%d.\n", F, L - 2);
				free_record(rec, rec->fields_num);
				return NULL;
			}

			buff_update = (size_t)bswap_64(bu_up_ne);

			if (str_loc > 0)
			{
				/*save the offset past the buff_w*/
				move_to = get_file_offset(fd) + buff_update;
				/*move to (other)string location*/
				if (find_record_position(fd, str_loc) == -1)
				{
					__er_file_pointer(F, L - 2);
					free_record(rec, rec->fields_num);
					return NULL;
				}

				uint64_t l_ne = 0;
				uint64_t bu_up_ne = 0;
				if (read(fd, &l_ne, sizeof(l_ne)) < 0)
				{
					perror("read file: ");
					printf("%s:%d", F, L - 2);
					free_record(rec, rec->fields_num);
					return NULL;
				}

				if (read(fd, &bu_up_ne, sizeof(bu_up_ne)) < 0)
				{
					perror("read file: ");
					printf("%s:%d", F, L - 2);
					free_record(rec, rec->fields_num);
					return NULL;
				}

				l = (size_t)bswap_64(l_ne);
				buff_update = (size_t)bswap_64(bu_up_ne);
			}
			rec->fields[i].data.s = calloc(l, sizeof(char));
			if (!rec->fields[i].data.s)
			{
				printf("calloc failed: %s:%d.\n", F, L - 3);
				free_record(rec, rec->fields_num);
				return NULL;
			}

			char *all_buf = calloc(buff_update, sizeof(char));
			if (!all_buf)
			{
				printf("calloc failed file.c l 532.\n");
				free_record(rec, rec->fields_num);
				return NULL;
			}

			/*read the actual string*/
			if (read(fd, all_buf, buff_update) < 0)
			{
				perror("could not read buffer string, file.c l 539.\n");
				free_record(rec, rec->fields_num);
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
					free_record(rec, rec->fields_num);
					return NULL;
				}
			}

			break;
		}
		case TYPE_BYTE:
		{
			uint16_t b_ne = 0;
			if (read(fd, &b_ne, sizeof(b_ne)) < 0)
			{
				perror("could not read type byte: ");
				printf(" %s:%d.\n", F, L - 2);
				free_record(rec, rec->fields_num);
				return NULL;
			}

			rec->fields[i].data.b = ntohb(b_ne);
			break;
		}
		case TYPE_DOUBLE:
		{
			uint64_t d_ne = 0;
			if (read(fd, &d_ne, sizeof(d_ne)) < 0)
			{
				perror("could not read type double:");
				printf(" %s:%d.\n", F, L - 2);
				free_record(rec, rec->fields_num);
				return NULL;
			}

			rec->fields[i].data.d = ntohd(d_ne);
			break;
		}
		case TYPE_ARRAY_INT:
		{
			if (!rec->fields[i].data.v.elements.i)
			{
				rec->fields[i].data.v.insert = insert_element;
				rec->fields[i].data.v.destroy = free_dynamic_array;
			}

			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				if (read(fd, &size_array, sizeof(size_array)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				int sz = (int)ntohl(size_array);
				uint32_t padding = 0;
				if (read(fd, &padding, sizeof(padding)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				int padd = (int)ntohl(padding);

				for (int j = 0; j < sz; j++)
				{
					uint32_t num_ne = 0;
					if (read(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("can't read int array from file.\n");
						free_record(rec, rec->fields_num);
						return NULL;
					}
					int num = (int)ntohl(num_ne);
					rec->fields[i].data.v.insert((void *)&num,
												 &rec->fields[i].data.v,
												 rec->fields[i].type);
				}

				if (padd > 0)
				{
					if (move_in_file_bytes(fd, padd * sizeof(int)) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return NULL;
					}
				}

				uint64_t upd_ne = 0;
				if (read(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("can't read int array from file.\n");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				if (go_back_here == 0)
				{
					go_back_here = get_file_offset(fd);
				}

				array_upd = (off_t)bswap_64(upd_ne);
				if (array_upd > 0)
				{
					if (find_record_position(fd, array_upd) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return NULL;
					}
				}
			} while (array_upd > 0);

			if (find_record_position(fd, go_back_here) == -1)
			{
				__er_file_pointer(F, L - 1);
				free_record(rec, rec->fields_num);
				return NULL;
			}
			break;
		}
		case TYPE_ARRAY_LONG:
		{
			if (!rec->fields[i].data.v.elements.l)
			{
				rec->fields[i].data.v.insert = insert_element;
				rec->fields[i].data.v.destroy = free_dynamic_array;
			}

			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				if (read(fd, &size_array, sizeof(size_array)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				int sz = (int)ntohl(size_array);
				uint32_t padding = 0;
				if (read(fd, &padding, sizeof(padding)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				int padd = (int)ntohl(padding);

				for (int j = 0; j < sz; j++)
				{
					uint64_t num_ne = 0;
					if (read(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("can't read int array from file.\n");
						free_record(rec, rec->fields_num);
						return NULL;
					}
					long num = (long)bswap_64(num_ne);
					rec->fields[i].data.v.insert((void *)&num,
												 &rec->fields[i].data.v,
												 rec->fields[i].type);
				}

				if (padd > 0)
				{
					if (move_in_file_bytes(fd, padd * sizeof(int)) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return NULL;
					}
				}

				uint64_t upd_ne = 0;
				if (read(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("can't read int array from file.\n");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				if (go_back_here == 0)
				{
					go_back_here = get_file_offset(fd);
				}
				array_upd = (off_t)bswap_64(upd_ne);
				if (array_upd > 0)
				{
					if (find_record_position(fd, array_upd) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return NULL;
					}
				}
			} while (array_upd > 0);

			if (find_record_position(fd, go_back_here) == -1)
			{
				__er_file_pointer(F, L - 1);
				free_record(rec, rec->fields_num);
				return NULL;
			}
			break;
		}
		case TYPE_ARRAY_STRING:
		{
			if (!rec->fields[i].data.v.elements.l)
			{
				rec->fields[i].data.v.insert = insert_element;
				rec->fields[i].data.v.destroy = free_dynamic_array;
			}

			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				if (read(fd, &size_array, sizeof(size_array)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				int sz = (int)ntohl(size_array);
				uint32_t padding = 0;
				if (read(fd, &padding, sizeof(padding)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				int padd = (int)ntohl(padding);

				for (int j = 0; j < sz; j++)
				{

					/*read pos of new str if any*/
					uint64_t str_loc_ne = 0;
					if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == -1)
					{
						perror("can't read string location: ");
						printf(" %s:%d", F, L - 3);
						return 0;
					}
					str_loc = (size_t)bswap_64(str_loc_ne);

					uint64_t l_ne = 0;
					if (read(fd, &l_ne, sizeof(l_ne)) < 0)
					{
						perror("read from file failed: ");
						printf("%s:%d.\n", F, L - 2);
						free_record(rec, rec->fields_num);
						return NULL;
					}

					l = (size_t)bswap_64(l_ne);

					uint64_t bu_up_ne = 0;
					if (read(fd, &bu_up_ne, sizeof(bu_up_ne)) < 0)
					{
						perror("read from file failed: ");
						printf("%s:%d.\n", F, L - 2);
						free_record(rec, rec->fields_num);
						return NULL;
					}

					buff_update = (size_t)bswap_64(bu_up_ne);

					if (str_loc > 0)
					{
						/*save the offset past the buff_w*/
						move_to = get_file_offset(fd) + buff_update;
						/*move to (other)string location*/
						if (find_record_position(fd, str_loc) == -1)
						{
							__er_file_pointer(F, L - 2);
							free_record(rec, rec->fields_num);
							return NULL;
						}

						uint64_t l_ne = 0;
						uint64_t bu_up_ne = 0;
						if (read(fd, &l_ne, sizeof(l_ne)) < 0)
						{
							perror("read file: ");
							printf("%s:%d", F, L - 2);
							free_record(rec, rec->fields_num);
							return NULL;
						}

						if (read(fd, &bu_up_ne, sizeof(bu_up_ne)) < 0)
						{
							perror("read file: ");
							printf("%s:%d", F, L - 2);
							free_record(rec, rec->fields_num);
							return NULL;
						}

						l = (size_t)bswap_64(l_ne);
						buff_update = (size_t)bswap_64(bu_up_ne);
					}

					char *all_buf = calloc(buff_update, sizeof(char));
					if (!all_buf)
					{
						printf("calloc failed file.c l 532.\n");
						free_record(rec, rec->fields_num);
						return NULL;
					}

					/*read the actual string*/
					if (read(fd, all_buf, buff_update) < 0)
					{
						perror("could not read buffer string, file.c l 539.\n");
						free_record(rec, rec->fields_num);
						return NULL;
					}

					rec->fields[i].data.v.insert((void *)all_buf,
												 &rec->fields[i].data.v,
												 rec->fields[i].type);
					free(all_buf);

					/*set file pointer back at the end of the original str record*/
					if (str_loc > 0)
					{
						if (find_record_position(fd, move_to) == -1)
						{
							__er_file_pointer(F, L - 2);
							free_record(rec, rec->fields_num);
							return NULL;
						}
					}
				}

				if (padd > 0)
				{
					if (move_in_file_bytes(fd, padd * sizeof(int)) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return NULL;
					}
				}

				uint64_t upd_ne = 0;
				if (read(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("can't read int array from file.\n");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				if (go_back_here == 0)
				{
					go_back_here = get_file_offset(fd);
				}
				array_upd = (off_t)bswap_64(upd_ne);
				if (array_upd > 0)
				{
					if (find_record_position(fd, array_upd) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return NULL;
					}
				}
			} while (array_upd > 0);

			if (find_record_position(fd, go_back_here) == -1)
			{
				__er_file_pointer(F, L - 1);
				free_record(rec, rec->fields_num);
				return NULL;
			}
			break;
		}
		case TYPE_ARRAY_FLOAT:
		{
			if (!rec->fields[i].data.v.elements.f)
			{
				rec->fields[i].data.v.insert = insert_element;
				rec->fields[i].data.v.destroy = free_dynamic_array;
			}

			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				if (read(fd, &size_array, sizeof(size_array)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				int sz = (int)ntohl(size_array);
				uint32_t padding = 0;
				if (read(fd, &padding, sizeof(padding)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				int padd = (int)ntohl(padding);

				for (int j = 0; j < sz; j++)
				{
					uint32_t num_ne = 0;
					if (read(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("can't read int array from file.\n");
						free_record(rec, rec->fields_num);
						return NULL;
					}

					float num = ntohf(num_ne);
					rec->fields[i].data.v.insert((void *)&num,
												 &rec->fields[i].data.v,
												 rec->fields[i].type);
				}

				if (padd > 0)
				{
					if (move_in_file_bytes(fd, padd * sizeof(int)) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return NULL;
					}
				}

				uint64_t upd_ne = 0;
				if (read(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("can't read int array from file.\n");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				if (go_back_here == 0)
				{
					go_back_here = get_file_offset(fd);
				}

				array_upd = (off_t)bswap_64(upd_ne);
				if (array_upd > 0)
				{
					if (find_record_position(fd, array_upd) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return NULL;
					}
				}
			} while (array_upd > 0);

			if (find_record_position(fd, go_back_here) == -1)
			{
				__er_file_pointer(F, L - 1);
				free_record(rec, rec->fields_num);
				return NULL;
			}
			break;
		}
		case TYPE_ARRAY_DOUBLE:
		{
			off_t end_first_arr = 0;
			off_t update_array = 0;

			do
			{
				uint32_t size_array = 0;
				if (read(fd, &size_array, sizeof(size_array)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				int sz = (int)ntohl(size_array);
				if (!rec->fields[i].data.v.elements.b)
				{
					rec->fields[i].data.v.insert = insert_element;
					rec->fields[i].data.v.destroy = free_dynamic_array;
				}

				for (int j = 0; j < sz; j++)
				{
					uint32_t f_ne = 0;
					if (read(fd, &f_ne, sizeof(uint32_t)) < 0)
					{
						perror("could not read type float, file.c l 505.\n");
						free_record(rec, rec->fields_num);
						return NULL;
					}

					float num = ntohd(f_ne);
					rec->fields[i].data.v.insert((void *)&num,
												 &rec->fields[i].data.v,
												 rec->fields[i].type);
				}

				/*read update position*/
				uint64_t update_ne = 0;
				if (read(fd, &update_ne, sizeof(update_ne)) == -1)
				{
					perror("can't read int array update.\n");
					free_record(rec, rec->fields_num);
					return NULL;
				}

				if (end_first_arr == 0)
				{
					if ((end_first_arr = get_file_offset(fd)) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return NULL;
					}
				}

				update_array = (off_t)bswap_64(update_ne);
				if (update_array > 0)
				{
					/*
					 * go to the position and read
					 * the remaining array data
					 * */
					if (find_record_position(fd, update_array) == -1)
					{
						__er_file_pointer(F, L - 2);
						free_record(rec, rec->fields_num);
						return NULL;
					}
				}
			} while (update_array > 0);

			/* go back to end_first_arr to avoid losing data */
			if (find_record_position(fd, end_first_arr) == -1)
			{
				__er_file_pointer(F, L - 2);
				free_record(rec, rec->fields_num);
				return NULL;
			}

			break;
		}
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

	unsigned short actual_pd = (short)(bytes - (int)hd_st);
	double pd_intermediete = (double)actual_pd / (double)sizeof(uint16_t);

	pd_intermediete = (pd_intermediete);
	actual_pd = (unsigned short)pd_intermediete;

	uint16_t padding[actual_pd];

	short i = 0;
	unsigned char c = '0';
	uint16_t c_ne = htonb(c);
	for (i = 0; i < actual_pd; i++)
		padding[i] = c_ne;

	if (write(fd, &padding, sizeof(padding)) == -1)
	{
		perror("padding file.\n");
		return 0;
	}

	return 1; // succssed
}

off_t get_file_size(int fd, char *file_name)
{
	struct stat st;
	if (!file_name)
	{
		if (fstat(fd, &st) == -1)
		{
			perror("stat failed :");
			printf("%s:%d.\n", F, L - 3);
			return -1;
		}

		return (off_t)st.st_size;
	}
	else
	{
		if (stat(file_name, &st) == -1)
		{
			perror("stat failed :");
			printf("%s:%d.\n", F, L - 3);
			return -1;
		}

		return (off_t)st.st_size;
	}
}

int add_index(int index_nr, char *file_name, int bucket)
{
	if (index_nr == 0)
		return -1;

	size_t l = strlen(file_name) + strlen(".inx") + 1;
	char buff[l];
	memset(buff, 0, l);

	if (snprintf(buff, l, "%s%s", file_name, ".inx") < 0)
	{
		fprintf(stderr,
				"snprintf() failed. %s:%d.\n",
				F, L - 3);
		return -1;
	}

	int fd = open_file(buff, 0);
	if (file_error_handler(1, fd) > 0)
	{
		fprintf(stderr,
				"can't open %s, %s:%d.\n", buff, F, L - 3);
		return -1;
	}

	HashTable *ht = NULL;
	int ht_i = 0;
	if (!read_all_index_file(fd, &ht, &ht_i))
	{
		fprintf(stderr,
				"read_all_index_file(),failed %s:%d.\n",
				F, L - 4);
		return -1;
	}

	HashTable *ht_new = realloc(ht, (ht_i + index_nr) * sizeof(HashTable));
	if (!ht_new)
	{
		__er_realloc(F, L - 2);
		free_ht_array(ht, ht_i);
		return -1;
	}

	ht = ht_new;
	int total_indexes = ht_i + index_nr;
	close_file(1, fd);

	for (int j = ht_i; j < total_indexes; ++j)
	{
		HashTable dummy = {bucket, NULL, write_ht};
		ht[j] = dummy;
	}
	/*reopen the file with O_TRUNCATE flag to overwrite the old content*/
	fd = open_file(buff, 1);
	if (file_error_handler(1, fd) > 0)
	{
		fprintf(stderr,
				"can't open %s, %s:%d.\n", buff, F, L - 3);
		free_ht_array(ht, total_indexes);
		return -1;
	}

	if (!write_index_file_head(fd, total_indexes))
	{
		fprintf(stderr, "write_index_file_head() failed, %s:%d.",
				F, L - 3);
		free_ht_array(ht, total_indexes);
		return -1;
	}

	for (int i = 0; i < total_indexes; ++i)
	{
		/*
		 * create an Hashtable in the
		 * reallocated hastable array
		 * */
		if (ht[i].dataMap == NULL)
		{
			Node **data_map = calloc(bucket, sizeof(Node *));
			if (!data_map)
			{
				__er_calloc(F, L - 2);
				free_ht_array(ht, total_indexes);
				return -1;
			}

			ht[i].dataMap = data_map;

			if (write_index_body(fd, i, &ht[i]) == -1)
			{
				fprintf(stderr,
						"write_index_body() failed %s:%d.\n",
						F, L - 3);
				free_ht_array(ht, total_indexes);
				return -1;
			}

			destroy_hasht(&ht[i]);
			continue;
		}

		if (write_index_body(fd, i, &ht[i]) == -1)
		{
			fprintf(stderr,
					"write_index_body() failed %s:%d.\n",
					F, L - 3);
			free_ht_array(ht, total_indexes);
			return -1;
		}

		destroy_hasht(&ht[i]);
	}

	free(ht);
	close_file(1, fd);
	return 0;
}

static int is_array_last_block(int fd, int element_nr, size_t bytes_each_element)
{
	off_t go_back_to = 0;
	if ((go_back_to = get_file_offset(fd)) == -1)
	{
		__er_file_pointer(F, L - 1);
		return -1;
	}

	if (move_in_file_bytes(fd, element_nr * bytes_each_element) == -1)
	{
		__er_file_pointer(F, L - 1);
		return -1;
	}

	uint64_t update_off_ne = 0;

	if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
	{
		perror("failed read update off_t int array.\n");
		return 0;
	}

	if (find_record_position(fd, go_back_to) == -1)
	{
		__er_file_pointer(F, L - 1);
		return -1;
	}

	off_t update_pos = (off_t)bswap_64(update_off_ne);

	return update_pos == 0;
}
