#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdarg.h>
#include "str_op.h"
#include "record.h"
#include "parse.h"

char **two_file_path(char *file_path)
{
	static char *dat = ".dat";
	static char *ind = ".inx";
	//	static char *gl_path = "/u/db/";

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

	strncpy(index, file_path, strlen(file_path));
	strcat(index, ind);

	strncpy(data, file_path, strlen(file_path));
	strcat(data, dat);

	char **arr = calloc(2, sizeof(char *));
	if (!arr)
	{
		free(index);
		free(data);
		printf("array memory, str_op.c l %d.\n", __LINE__ - 4);
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
}

char **get_fileds_name(char *fields_name, int fields_count, int steps)
{
	int i = 0, j = 0;
	unsigned char oversized_f = 0;
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

		if ((strlen(names_f[j]) > MAX_FIELD_LT))
		{
			printf("%s, this fields is longer than the system limit! max %d per field name.\n",
				   names_f[j], MAX_FIELD_LT);
			oversized_f = 1;
		}

		j++;

		strtok_r(NULL, ":", &cp_fv);

		if (steps == 3 && j < fields_count)
			strtok_r(NULL, ":", &cp_fv);
	}

	if (oversized_f)
	{
		int i = 0;
		for (i = 0; i < fields_count; i++)
			if (names_f[i])
				free(names_f[i]);
		free(names_f);
		return NULL;
	}
	return names_f;
}

ValueType *get_value_types(char *fields_input, int fields_count, int steps)
{
	int i = steps == 3 ? 0 : 1;
	int j = 0;

	ValueType *types = calloc(fields_count, sizeof(ValueType));

	if (!types)
	{
		perror("memory");
		return NULL;
	}

	char *s = NULL;
	char *copy_fv = NULL;

	s = strtok_r(fields_input, ":", &copy_fv);

	s = strtok_r(NULL, ":", &copy_fv);

	if (s)
	{
		types[j] = get_type(s);
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
	for (i; i < l; i++)
	{
		if (ispunct(str[i]))
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
