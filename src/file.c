#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "file.h"
#include "debug.h" // take i t out once your done
#include "common.h"
#include "str_op.h"	  //not to happy about this
#include "hash_tbl.h" // not to happy about this

int open_file(char *fileName, int use_trunc)
{
	int fd = 0;

	if (!use_trunc)
	{
		fd = open(fileName, O_RDWR, S_IRWXU);
	}
	else
	{
		fd = open(fileName, O_WRONLY | O_TRUNC, S_IRWXU);
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
	int fd = open(fileName, O_RDONLY);
	if (fd != -1)
	{
		printf("File already exist.\n");
		close(fd);
		return -1;
	}

	fd = open(fileName, O_RDWR | O_CREAT, S_IRWXU);

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
		perror("seeking offset.\n");
		return (off_t)-1;
	}

	return pos;
}

int read_index_file(int fd, HashTable *ht)
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

ssize_t compute_record_size(Record_f *rec)
{
	register unsigned char i = 0;
	ssize_t sum = 0;
	sum += sizeof(rec->fields_num) + sizeof(rec->fields[i].type);

	for (i = 0; i < rec->fields_num; i++)
	{
		switch (rec->fields[i].type)
		{
		case TYPE_INT:
			sum += sizeof(rec->fields[i].data.i);
			break;
		case TYPE_LONG:
			sum += sizeof(rec->fields[i].data.l);
			break;
		case TYPE_FLOAT:
			sum += sizeof(rec->fields[i].data.f);
			break;
		case TYPE_STRING:
			sum += (strlen(rec->fields[i].data.s) * 2) + 1; // counting '\0'
			break;
		case TYPE_BYTE:
			sum += sizeof(rec->fields[i].data.b);
			break;
		case TYPE_DOUBLE:
			sum += sizeof(rec->fields[i].data.d);
			break;
		default:
			printf("type not supported!\n");
			return -1;
		}
	}
	return sum;
}

int write_file(int fd, Record_f *rec)
{
	ssize_t size = compute_record_size(rec);
	if (size == -1)
	{
		printf("cannot write to the file, unknown type(s).\n");
		return 0;
	}

	size += sizeof(ssize_t);

	if (write(fd, &size, sizeof(size)) == -1)
	{
		perror("write size of record to file");
		return 0;
	}

	if (write(fd, &rec->fields_num, sizeof(rec->fields_num)) < 0)
	{
		perror("could not write fields number");
		return 0;
	}

	register unsigned char i = 0;
	size_t lt = 0;
	size_t buff_update = 0;
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
			lt = strlen(rec->fields[i].data.s) + 1;
			buff_update = (strlen(rec->fields[i].data.s) * 2) + 1;
			buff_w = calloc(buff_update, sizeof(char));

			if (!buff_w)
			{
				printf("calloc failed, (file.c l 289).\n");
				return 0;
			}

			strncpy(buff_w, rec->fields[i].data.s, lt - 1);

			if (write(fd, &lt, sizeof(lt)) < 0 ||
				write(fd, &buff_update, sizeof(buff_update)) < 0 ||
				write(fd, buff_w, buff_update) < 0)
			{

				perror("error in writing type string (char *)file.\n");
				return 0;
			}

			free(buff_w);
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
	off_t update_off_t = 0;
	if (write(fd, &update_off_t, sizeof(update_off_t)) == -1)
	{
		perror("writing off_t for future update");
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

off_t get_update_offset(int fd, off_t record_pos)
{
	ssize_t size = get_record_size(fd);
	off_t move = size + record_pos;

	off_t up_pos = lseek(fd, record_pos, SEEK_SET);

	if (up_pos == -1)
	{
		perror("position update not found (file.c l 355).\n");
		return up_pos;
	}

	off_t urec_pos = 0;
	if (read(fd, &urec_pos, sizeof(urec_pos)) == -1)
	{
		perror("could not read the update record position (file.c l 365).\n");
		return -1;
	}

	return urec_pos;
}

Record_f *read_file(int fd, char *file_name)
{
	ssize_t size = get_record_size(fd);

	if (size == -1)
		return NULL;

	int fields_num_r = 0;
	if (read(fd, &fields_num_r, sizeof(fields_num_r)) < 0)
	{
		perror("could not read fields number.\n");
		return NULL;
	}
	Record_f *rec = create_record(file_name, fields_num_r);

	if (!rec)
	{
		printf("could not allocate for Record.\n");
		return NULL;
	}

	register unsigned char i = 0;

	size_t buff_update = 0;
	size_t l = 0;
	for (i = 0; i < rec->fields_num; i++)
	{
		size_t lt = 0;

		if (read(fd, &lt, sizeof(lt)) < 0)
		{
			perror("could not read size of field name");
			clean_up(rec, fields_num_r);
			return NULL;
		}
		//	printf("size is %ld",lt);
		rec->fields[i].field_name = malloc(lt + 1);

		if (!rec->fields[i].field_name)
		{
			printf("no memory for field name line 377 read\n");
			clean_up(rec, fields_num_r);
			return NULL;
		}

		if (read(fd, rec->fields[i].field_name, lt) < 0)
		{
			perror("could not read field name");
			clean_up(rec, fields_num_r);
			return NULL;
		}

		rec->fields[i].field_name[lt] = '\0';
		ValueType type_r;

		if (read(fd, &type_r, sizeof(type_r)) < 0)
		{
			perror("could not read type from file.");
			clean_up(rec, fields_num_r);
			return NULL;
		}

		rec->fields[i].type = type_r;

		switch (rec->fields[i].type)
		{
		case TYPE_INT:
			if (read(fd, &rec->fields[i].data.i, sizeof(int)) < 0)
			{
				perror("could not read type int.");
				clean_up(rec, fields_num_r);
				return NULL;
			}
			break;
		case TYPE_LONG:
			if (read(fd, &rec->fields[i].data.l, sizeof(long)) < 0)
			{
				perror("could not read type int.");
				clean_up(rec, fields_num_r);
				return NULL;
			}
			break;
		case TYPE_FLOAT:
			if (read(fd, &rec->fields[i].data.f, sizeof(float)) < 0)
			{
				perror("could not read type int.");
				clean_up(rec, fields_num_r);
				return NULL;
			}
			break;
		case TYPE_STRING:
			if (read(fd, &l, sizeof(l)) < 0)
			{
				perror("could not read size of type STRING(char *)");
				clean_up(rec, fields_num_r);
				return NULL;
			}

			if (read(fd, &buff_update, sizeof(buff_update)) < 0)
			{
				perror("could not read size of type STRING(char *)");
				clean_up(rec, fields_num_r);
				return NULL;
			}

			rec->fields[i].data.s = calloc(l, sizeof(char));

			if (!rec->fields[i].data.s)
			{
				perror("memory for string in reding file");
				clean_up(rec, fields_num_r);
				return NULL;
			}

			char *all_buf = calloc(buff_update, sizeof(char));
			if (!all_buf)
			{
				printf("no memory reading buff_up file.c l 445.\n");
				clean_up(rec, fields_num_r);
				return NULL;
			}

			if (read(fd, all_buf, buff_update) < 0)
			{
				perror("could not read type string.");
				clean_up(rec, fields_num_r);
				return NULL;
			}

			strncpy(rec->fields[i].data.s, all_buf, l);
			rec->fields[i].data.s[l - 1] = '\0';
			free(all_buf);
			break;
		case TYPE_BYTE:
			if (read(fd, &rec->fields[i].data.b, sizeof(unsigned char)) < 0)
			{
				perror("could not read type byte.");
				clean_up(rec, fields_num_r);
				return NULL;
			}
			break;
		case TYPE_DOUBLE:
			if (read(fd, &rec->fields[i].data.d, sizeof(double)) < 0)
			{
				perror("could not read type int.");
				clean_up(rec, fields_num_r);
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
