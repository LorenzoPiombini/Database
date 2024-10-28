#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>
#include "record.h"
#include "debug.h"
#include "str_op.h"

Record_f *create_record(char *file_name, int fields_num)
{
	Record_f *rec = malloc(sizeof(Record_f));

	if (!rec)
	{
		printf("memmory allocation, record.c l 11.\n");
		return NULL;
	}
	rec->file_name = strdup(file_name);
	if (!rec->file_name)
	{
		printf("memory allocation file_name, record.c l 17.\n");
		free(rec);
		return NULL;
	}

	rec->fields_num = fields_num;

	rec->fields = calloc(fields_num, sizeof(Field));

	if (!rec->fields)
	{
		printf("Memory allocation fields, record.c l 26.\n");
		free(rec->file_name);
		free(rec);
		return NULL;
	}

	return rec;
}

unsigned char set_field(Record_f *rec, int index, char *field_name, ValueType type, char *value)
{
	rec->fields[index].field_name = strdup(field_name);
	rec->fields[index].type = type;

	switch (type)
	{

	case TYPE_INT:
	{
		if (!is_integer(value))
		{
			printf("invalid value for integer type: %s.\n", value);
			return 0;
		}
		int range = 0;
		if ((range = is_number_in_limits(value)) == 0)
		{
			printf("integer value not allowed in this system.\n");
			return 0;
		}
		if (range == IN_INT)
		{
			/*convert the value to long and then cast it to int */
			/*i do not want to use atoi*/
			long n = strtol(value, NULL, 10);
			if (n == ERANGE || n == EINVAL)
			{
				printf("conversion ERROR type int %s:%d.\n", F, L - 2);
				return 0;
			}
			rec->fields[index].data.i = (int)n;
		}
		else
		{
			printf("the integer value is not valid for this system.\n");
			return 0;
		}
		break;
	}
	case TYPE_LONG:
	{
		if (!is_integer(value))
		{
			printf("invalid value for long integer type: %s.\n", value);
			return 0;
		}

		int range = 0;
		if ((range = is_number_in_limits(value)) == 0)
		{
			printf("long value not allowed in this system.\n");
			return 0;
		}
		if (range == IN_INT || range == IN_LONG)
		{
			long n = strtol(value, NULL, 10);
			if (n == ERANGE || n == EINVAL)
			{
				printf("conversion ERROR type long %s:%d.\n", F, L - 2);
				return 0;
			}

			rec->fields[index].data.l = n;
		}
		break;
	}
	case TYPE_FLOAT:
	{

		if (!is_floaintg_point(value))
		{
			printf("invalid value for float type: %s.\n", value);
			return 0;
		}

		int range = 0;
		if ((range = is_number_in_limits(value)) == 0)
		{
			printf("float value not allowed in this system.\n");
			return 0;
		}

		if (range == IN_FLOAT)
		{
			float f = strtof(value, NULL);
			if (f == ERANGE || f == EINVAL)
			{
				printf("conversion ERROR type float %s:%d.\n", F, L - 2);
				return 0;
			}
			rec->fields[index].data.f = f;
		}
		break;
	}
	case TYPE_STRING:
		rec->fields[index].data.s = strdup(value);
		break;
	case TYPE_BYTE:
	{
		if (!is_integer(value))
		{
			printf("invalid value for byte type: %s.\n", value);
			return 0;
		}

		unsigned long un = strtoul(value, NULL, 10);
		if (un == ERANGE || un == EINVAL)
		{
			printf("conversion ERROR type float %s:%d.\n", F, L - 2);
			return 0;
		}
		if (un > UCHAR_MAX)
		{
			printf("byte value not allowed in this system.\n");
			return 0;
		}

		rec->fields[index].data.b = (unsigned char)un;

		break;
	}
	case TYPE_DOUBLE:
	{

		if (!is_floaintg_point(value))
		{
			printf("invalid value for double type: %s.\n", value);
			return 0;
		}

		int range = 0;
		if ((range = is_number_in_limits(value)) == 0)
		{
			printf("float value not allowed in this system.\n");
			return 0;
		}

		if (range == IN_DOUBLE)
		{
			double d = strtod(value, NULL);
			if (d == ERANGE || d == EINVAL)
			{
				printf("conversion ERROR type double %s:%d.\n", F, L - 2);
				return 0;
			}
			rec->fields[index].data.d = d;
		}
		break;
	}
	default:
		printf("invalid type! type -> %d.", type);
		return 0;
	}

	return 1;
}

void clean_up(Record_f *rec, int fields_num)
{
	int i;
	if (!rec)
		return;

	for (i = 0; i < fields_num; i++)
	{
		if (rec->fields[i].field_name)
			free(rec->fields[i].field_name);

		if (rec->fields[i].type == TYPE_STRING)
		{
			if (rec->fields[i].data.s)
				free(rec->fields[i].data.s);
		}
	}

	free(rec->fields);
	free(rec->file_name);
	free(rec);
}

void print_record(int count, Record_f **recs)
{

	int i = 0, j = 0, max = 0;
	printf("#################################################################\n\n");
	printf("the Record data are: \n");

	for (j = 0; j < count; j++)
	{
		Record_f *rec = recs[j];
		for (i = 0; i < rec->fields_num; i++)
		{
			if (max < (int)strlen(rec->fields[i].field_name))
			{
				max = (int)strlen(rec->fields[i].field_name);
			}
		}

		for (i = 0; i < rec->fields_num; i++)
		{
			strip('"', rec->fields[i].field_name);
			printf("%-*s\t", max++, rec->fields[i].field_name);
			switch (rec->fields[i].type)
			{
			case TYPE_INT:
				printf("%d\n", rec->fields[i].data.i);
				break;
			case TYPE_LONG:
				printf("%ld\n", rec->fields[i].data.l);
				break;
			case TYPE_FLOAT:
				printf("%.2f\n", rec->fields[i].data.f);
				break;
			case TYPE_STRING:
				strip('"', rec->fields[i].data.s);
				printf("%s\n", rec->fields[i].data.s);
				break;
			case TYPE_BYTE:
				printf("%u\n", rec->fields[i].data.b);
				break;
			case TYPE_DOUBLE:
				printf("%.2f\n", rec->fields[i].data.d);
				break;
			default:
				break;
			}
		}
	}
	printf("\n#################################################################\n\n");
}

void free_record_array(int len, Record_f ***recs)
{
	int i = 0;
	for (i = 0; i < len; i++)
	{
		if (*recs)
		{
			if ((*recs)[i])
			{
				clean_up((*recs)[i], (*recs)[i]->fields_num);
			}
		}
	}
	free(*recs);
}

/* this parameters are:
	- int len => the length of the array() Record_f***
	- Record_f*** array => the arrays of record arrays
	- int* len_ia => length inner array,  an array that keeps track of all the found records array sizes
	- int size_ia => the actual size of len_ia,
*/
void free_array_of_arrays(int len, Record_f ****array, int *len_ia, int size_ia)
{
	if (!*array)
		return;

	if (!size_ia)
	{
		free(*array);
		return;
	}

	int i = 0;
	for (i = 0; i < len; i++)
	{
		if ((*array)[i])
		{
			if (i < size_ia)
			{
				if (len_ia[i] != 0)
				{
					free_record_array(len_ia[i], &(*array)[i]);
				}
			}
			else
			{
				free((*array)[i]);
				return;
			}
		}
	}

	free(*array);
}

unsigned char copy_rec(Record_f *src, Record_f **dest)
{
	*dest = create_record(src->file_name, src->fields_num);
	if (!*dest)
	{
		printf("create_record failed %s:%d.\n", F, L - 3);
		return 0;
	}

	int i = 0;
	char data[30];
	for (i = 0; i < src->fields_num; i++)
	{
		switch (src->fields[i].type)
		{
		case TYPE_INT:
			if (snprintf(data, 30, "%d", src->fields[i].data.i) < 0)
			{
				printf("snprintf failed %s:%d.\n", F, L - 2);
				clean_up((*dest), (*dest)->fields_num);
				return 0;
			}
			set_field(*dest, i, src->fields[i].field_name,
					  src->fields[i].type, data);
			break;
		case TYPE_STRING:
			set_field(*dest, i, src->fields[i].field_name,
					  src->fields[i].type, src->fields[i].data.s);
			break;
		case TYPE_LONG:
			if (snprintf(data, 30, "%ld", src->fields[i].data.l) < 0)
			{
				printf("snprintf failed %s:%d.\n", F, L - 2);
				clean_up((*dest), (*dest)->fields_num);
				return 0;
			}
			set_field(*dest, i, src->fields[i].field_name,
					  src->fields[i].type, data);
			break;
		case TYPE_FLOAT:
			if (snprintf(data, 30, "%.2f", src->fields[i].data.f) < 0)
			{
				printf("snprintf failed %s:%d.\n", F, L - 2);
				clean_up((*dest), (*dest)->fields_num);
				return 0;
			}
			set_field(*dest, i, src->fields[i].field_name,
					  src->fields[i].type, data);
			break;
		case TYPE_BYTE:
			if (snprintf(data, 30, "%d", src->fields[i].data.b) < 0)
			{
				printf("snprintf failed %s:%d.\n", F, L - 2);
				clean_up((*dest), (*dest)->fields_num);
				return 0;
			}
			set_field(*dest, i, src->fields[i].field_name,
					  src->fields[i].type, data);
			break;
		case TYPE_DOUBLE:
			if (snprintf(data, 30, "%.2f", src->fields[i].data.d) < 0)
			{
				printf("snprintf failed %s:%d.\n", F, L - 2);
				clean_up((*dest), (*dest)->fields_num);
				return 0;
			}
			set_field(*dest, i, src->fields[i].field_name,
					  src->fields[i].type, data);
			break;
		default:
			printf("type not supported, %s:%d.\n", F, L - 27);
			return 0;
		}
	}

	return 1;
}

unsigned char get_index_rec_field(char *field_name, Record_f **recs, int recs_len, int *field_i_r, int *rec_index)
{
	int i = 0, j = 0;
	size_t field_name_len = strlen(field_name);
	for (i = 0; i < recs_len; i++)
	{
		for (j = 0; j < recs[i]->fields_num; j++)
		{
			if (strncmp(field_name, recs[i]->fields[j].field_name, field_name_len) == 0)
			{
				*field_i_r = j;
				*rec_index = i;
				return 1;
			}
		}
	}

	return 0;
}
