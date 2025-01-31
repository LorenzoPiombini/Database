#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdint.h>
#include <limits.h>
#include <errno.h>
#include "str_op.h"
#include "debug.h"

int get_array_values(char *src, char ***values)
{
	int items = count_fields(src, ",");
	*values = calloc(items, sizeof(char *));
	if (!(*values))
	{
		__er_calloc(F, L - 2);
		return -1;
	}

	char *t = strtok(src, ",");
	if (!t)
	{
		fprintf(stderr, "strtok() failed.\n", F, L - 2);
		free(*values);
		return -1;
	}

	(*values)[0] = strdup(t);
	if (!(*values)[0])
	{
		fprintf(stderr, "strdup failed.\n", F, L - 2);
		free(*values);
		return -1;
	}

	for (int i = 1; ((t = strtok(NULL, ",")) != NULL) && (i < items); i++)
	{
		(*values)[i] = strdup(t);
		if (!(*values)[i])
		{
			fprintf(stderr, "strdup failed.\n", F, L - 2);
			free(*values);
			return -1;
		}
	}

	return 0;
}

int is_num(char *key)
{
	unsigned char uint = 2;
	unsigned char str = 1;

	for (; *key != '\0'; key++)
		if (isalpha(*key) || ispunct(*key))
			return str;

	return uint;
}

void *key_converter(char *key, int *key_type)
{
	void *converted = NULL;
	*key_type = is_num(key);
	switch (*key_type)
	{
	case 2:
	{
		char *end;
		errno = 0;
		long l = strtol(key, &end, 10);
		if (l == 0 || errno == EINVAL)
			return NULL;

		if (l == MAX_KEY)
		{
			fprintf(stderr, "key value out fo range.\n");
			return NULL;
		}

		converted = calloc(1, sizeof(uint32_t));
		if (!converted)
		{
			__er_calloc(F, L - 2);
			return NULL;
		}

		memcpy(converted, (uint32_t *)&l, sizeof(uint32_t));
		break;
	}
	case 1:
		return NULL;
	default:
		return NULL;
	}

	return converted;
}

char **two_file_path(char *file_path)
{
	static char *dat = ".dat";
	static char *ind = ".inx";

	size_t len = strlen(file_path) + strlen(ind) + 1;

	char *index = calloc(len, sizeof(char));
	char *data = calloc(len, sizeof(char));

	if (!index || !data)
	{
		printf("calloc failed. str_op.c l %d", __LINE__ - 3);

		if (!index)
		{
			free(data);
		}
		else
		{
			free(index);
		}
		return NULL;
	}

	if (snprintf(index, len, "%s%s", file_path, ind) < 0 ||
		snprintf(data, len, "%s%s", file_path, dat) < 0)
	{
		printf("snprintf() failed, %s:%d.\n", F, L - 3);
		free(index);
		free(data);
		return NULL;
	}

	char **arr = calloc(2, sizeof(char *));
	if (!arr)
	{
		free(index);
		free(data);
		__er_calloc(F, L - 4);
		return NULL;
	}

	arr[0] = index;
	arr[1] = data;

	return arr;
}

int count_fields(char *fields, const char *target)
{
	int c = 0;
	const char *p = fields;

	while ((p = strstr(p, target)) != NULL)
	{
		c++;
		p += strlen(target);
	}

	return c;
}

int get_type(char *s)
{

	if (strcmp(s, "TYPE_INT") == 0)
	{
		return 0;
	}
	else if (strcmp(s, "TYPE_LONG") == 0)
	{
		return 1;
	}
	else if (strcmp(s, "TYPE_FLOAT") == 0)
	{
		return 2;
	}
	else if (strcmp(s, "TYPE_STRING") == 0)
	{
		return 3;
	}
	else if (strcmp(s, "TYPE_BYTE") == 0)
	{
		return 4;
	}
	else if (strcmp(s, "TYPE_DOUBLE") == 0)
	{
		return 6;
	}
	else if (strcmp(s, "t_i") == 0)
	{
		return 0;
	}
	else if (strcmp(s, "t_l") == 0)
	{
		return 1;
	}
	else if (strcmp(s, "t_f") == 0)
	{
		return 2;
	}
	else if (strcmp(s, "t_s") == 0)
	{
		return 3;
	}
	else if (strcmp(s, "t_b") == 0)
	{
		return 4;
	}
	else if (strcmp(s, "t_d") == 0)
	{
		return 6;
	}

	return -1;
}

char **get_fileds_name(char *fields_name, int fields_count, int steps)
{

	if (fields_count == 0)
		return NULL;

	int j = 0;
	char **names_f = calloc(fields_count, sizeof(char *));
	char *s = NULL;

	if (!names_f)
	{
		perror("memory");
		return NULL;
	}
	char *cp_fv = fields_name;
	while ((s = strtok_r(cp_fv, ":", &cp_fv)) != NULL && j < fields_count)
	{
		strip('_', s);
		names_f[j] = strdup(s);
		if (!names_f[j])
		{
			perror("error duplicating token, str_op.c l 92\n");
			while (j-- > 0)
			{
				free(names_f[j]);
			}
			free(names_f);
			return NULL;
		}

		j++;

		strtok_r(NULL, ":", &cp_fv);

		if (steps == 3 && j < fields_count)
			strtok_r(NULL, ":", &cp_fv);
	}

	return names_f;
}

unsigned char check_fields_integrity(char **names, int fields_count)
{
	for (int i = 0; i < fields_count; i++)
	{
		if ((fields_count - i) == 1)
			break;

		for (int j = 0; j < fields_count; j++)
		{
			if (((fields_count - i) > 1) && ((fields_count - j) > 1))
			{
				if (j == i)
					continue;

				int l_i = strlen(names[i]);
				int l_j = strlen(names[j]);

				if (l_i == l_j)
				{
					if (strncmp(names[i], names[j], l_j) == 0)
						return 0;
				}
			}
		}
	}
	return 1;
}

enum ValueType *get_value_types(char *fields_input, int fields_count, int steps)
{

	if (fields_count == 0)
		return NULL;

	int i = steps == 3 ? 0 : 1;
	int j = 0;

	enum ValueType *types = calloc(fields_count, sizeof(enum ValueType));
	if (!types)
	{
		__er_calloc(F, L - 3);
		return NULL;
	}

	char *s = NULL;
	char *copy_fv = NULL;

	s = strtok_r(fields_input, ":", &copy_fv);

	s = strtok_r(NULL, ":", &copy_fv);

	if (s)
	{
		types[j] = get_type(s);
		if (types[j] == -1)
		{
			free(types);
			return NULL;
		}
		i++;
	}
	else
	{
		printf("type token not found in get_value_types().\n");
		free(types);
		return NULL;
	}

	while ((s = strtok_r(NULL, ":", &copy_fv)) != NULL)
	{
		if ((i % 3 == 0) && (j < fields_count - 1))
		{
			j++, types[j] = get_type(s);
			if (types[j] == -1)
			{
				printf("check input format: fieldName:TYPE:value.\n");
				free(types);
				return NULL;
			}
		}

		if (steps != 3 && i == 2)
		{
			i++;
		}
		else if (steps != 3 && i == 3)
		{
			i--;
		}
		else
		{
			i++;
		}
	}
	return types;
}

char **get_values(char *fields_input, int fields_count)
{
	int i = 0, j = 0;

	char **values = calloc(fields_count, sizeof(char *));
	if (!values)
	{
		perror("memory get values");
		return NULL;
	}

	char *s = NULL;
	char *cp_fv = NULL;
	strtok_r(fields_input, ":", &cp_fv);
	strtok_r(NULL, ":", &cp_fv);
	s = strtok_r(NULL, ":", &cp_fv);

	if (s)
	{
		values[j] = strdup(s);
		if (!values[j])
		{
			free(values);
			return NULL;
		}
		i++;
	}
	else
	{
		perror("value token not found in get_values();");
		free(values);
		return NULL;
	}

	while ((s = strtok_r(NULL, ":", &cp_fv)) != NULL)
	{
		if ((i % 3 == 0) && (fields_count - j >= 1))
			j++, values[j] = strdup(s);

		if (!values[j])
		{
			while (j-- > 0)
			{
				free(values[j]);
			}
			free(values);
			return NULL;
		}
		i++;
		s = NULL;
	}

	return values;
}

unsigned char create_blocks_data_to_add(char *dta_src, char ***dta_blocks, int *blocks)
{
	int dta_blocks_size = 1;

	*dta_blocks = calloc(dta_blocks_size, sizeof(char *));
	if (!(*dta_blocks))
	{
		__er_calloc(F, L - 3);
		return 0;
	}

	char *save;
	char *token = strtok_r(dta_src, "!", &save);
	if (token)
	{
		*dta_blocks[dta_blocks_size - 1] = strdup(token);
		if (!(*dta_blocks)[dta_blocks_size - 1])
		{
			printf("strdup() failed. %s:%d.\n", F, L - 3);
			free_strs(dta_blocks_size, 1, *dta_blocks);
			return 0;
		}
	}
	else
	{
		free_strs(dta_blocks_size, 1, dta_blocks);
		return 0;
	}

	while ((token = strtok_r(NULL, "!", &save)))
	{
		char **nw_dta_blk = realloc(*dta_blocks, (++dta_blocks_size) * sizeof(char *));
		if (!nw_dta_blk)
		{
			__er_realloc(F, L - 3);
			free_strs(dta_blocks_size, 1, *dta_blocks);
			return 0;
		}

		*dta_blocks = nw_dta_blk;

		(*dta_blocks)[dta_blocks_size - 1] = strdup(token);
		if (!(*dta_blocks)[dta_blocks_size - 1])
		{
			printf("strdup() failed. %s:%d.\n", F, L - 3);
			free_strs(dta_blocks_size, 1, *dta_blocks);
			return 0;
		}
	}

	*blocks = dta_blocks_size;
	return 1;
}

void free_strs(int fields_num, int count, ...)
{
	int i = 0, j = 0;
	va_list args;
	va_start(args, count);

	for (i = 0; i < count; i++)
	{
		char **str = va_arg(args, char **);
		for (j = 0; j < fields_num; j++)
		{
			if (str[j])
			{
				free(str[j]);
			}
		}
		free(str);
	}
}

int is_file_name_valid(char *str)
{
	int i = 0;
	size_t l = strlen(str);
	for (i = 0; i < l; i++)
	{
		if (ispunct(str[i]))
			if ((str[i] != '/' && str[i] != '_'))
				return 0;

		if (isspace(str[i]))
			return 0;
	}
	return 1;
}

void strip(const char c, char *str)
{
	size_t l = strlen(str);
	int i = 0;
	for (i = 0; i < l; i++)
		if (str[i] == c)
			str[i] = ' ';
}

void replace(const char c, const char with, char *str)
{
	size_t l = strlen(str);
	for (int i = 0; i < l; i++)
		if (str[i] == c)
			str[i] = with;
}
char return_first_char(char *str)
{
	return str[0];
}

char return_last_char(char *str)
{
	size_t l = strlen(str);
	return str[l - 1];
}

size_t digits_with_decimal(float n)
{
	char buf[20];
	if (snprintf(buf, 20, "%.2f", n) < 0)
	{
		printf("snprintf failed %s:%d.\n", F, L - 2);
		return -1;
	}

	size_t i = 0;
	for (i = 0; i < 20; i++)
	{
		if (buf[i] == '.')
			return ++i;
	}

	return (size_t)-1;
}

unsigned char is_floaintg_point(char *str)
{
	size_t l = strlen(str);
	int i = 0, point = 0;
	for (i = 0; i < l; i++)
	{
		if (isdigit(str[i]))
		{
			if (str[i] == '.')
				return 1;
		}
		else if (str[i] == '.')
		{
			point++;
		}
		else
		{
			return 0;
		}
	}

	return point == 1;
}
unsigned char is_integer(char *str)
{
	size_t l = strlen(str);
	int i = 0;
	for (i = 0; i < l; i++)
		if (!isdigit(str[i]))
			return 0;

	return 1;
}

size_t number_of_digit(int n)
{
	if (n < 10)
	{
		return 1;
	}
	else if (n >= 10 && n < 100)
	{
		return 2;
	}
	else if (n >= 100 && n < 1000)
	{
		return 3;
	}
	else if (n >= 1000 && n < 10000)
	{
		return 4;
	}
	else if (n >= 10000 && n < 100000)
	{
		return 5;
	}
	else if (n >= 100000 && n < 1000000)
	{
		return 6;
	}
	else if (n >= 1000000 && n < 1000000000)
	{
		return 7;
	}
	else if (n >= 1000000000)
	{
		return 10;
	}

	return -1;
}

float __round_alt(float n)
{
	char buff[20];
	if (snprintf(buff, 20, "%f", n) < 0)
		return -1;

	size_t digit = 0;

	if ((digit = digits_with_decimal(n) + 2) > 20)
		return -1;

	int num = buff[digit];
	if (num > 5)
	{
		n += 0.01;
	}

	return n;
}

unsigned char is_number_in_limits(char *value)
{
	if (!value)
		return 0;

	size_t l = strlen(value);
	unsigned char negative = value[0] == '-';
	int ascii_value = 0;

	if (is_integer(value))
	{

		for (int i = 0; i < l; i++)
			ascii_value += (int)value[i];

		if (negative)
		{
			if (-1 * ascii_value < -1 * ASCII_INT_MIN)
			{
				if (-1 * ascii_value < -1 * ASCII_LONG_MIN)
					return 0;
				else
					return IN_LONG;
			}

			return IN_INT;
		}

		if (ascii_value > ASCII_INT_MAX)
		{
			if (ascii_value > ASCII_LONG_MAX)
				return 0;
			else
				return IN_LONG;
		}

		return IN_INT;
	}

	if (is_floaintg_point(value))
	{
		for (int i = 0; i < l; i++)
			ascii_value += (int)value[i];

		if (negative)
		{
			if (-1 * ascii_value < -1 * ASCII_FLT_MAX_NEGATIVE)
			{
				if (-1 * ascii_value < -1 * ASCII_DBL_MAX_NEGATIVE)
					return 0;
				else
					return IN_DOUBLE;
			}

			return IN_FLOAT;
		}

		if (ascii_value > ASCII_FLT_MAX)
		{
			if (ascii_value > ASCII_DBL_MAX)
				return 0;
			else
				return IN_DOUBLE;
		}
		return IN_FLOAT;
	}

	return 0;
}

int find_last_char(const char c, char *src)
{
	size_t l = strlen(src);
	int last = -1;
	for (int i = 0; i < l; i++)
	{
		if (src[i] == c)
			last = i;
	}

	return last;
}
