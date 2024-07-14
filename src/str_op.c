#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "str_op.h"
#include "record.h"
#include "parse.h"

char **two_file_path(char *filePath)
{
	char *dat = ".dat";
	char *ind = ".inx";

	size_t len = strlen(filePath) + strlen(ind) + 1;
	char *index = (char *)malloc(len * sizeof(char));
	char *data = (char *)malloc(len * sizeof(char));

	if (!index || !data)
	{
		perror("memory");

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

	strcpy(index, filePath);
	strcat(index, ind);

	strcpy(data, filePath);
	strcat(data, dat);

	char **arr = malloc(2 * sizeof(char *));
	if (!arr)
	{
		free(index);
		free(data);
		perror("array memory");
		return NULL;
	}

	arr[0] = index;
	arr[1] = data;

	return arr;
}

int count_fields(char *fields)
{
	int c = 0;
	const char *target = "TYPE_";
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
		names_f[j] = s;
		if ((strlen(names_f[j]) > MAX_FIELD_LT))
		{
			printf("%s, this fields is longer than the system limit! max %d per field name.\n",
				   names_f[j], MAX_FIELD_LT);
			oversized_f = 1;
		}

		j++;

		strtok_r(NULL, ":", &cp_fv);

		if (steps == 3)
			strtok_r(NULL, ":", &cp_fv);
	}

	if (oversized_f)
	{
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
		i++;
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
		values[j] = s;
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
			j++, values[j] = s;

		i++;
		s = NULL;
	}

	return values;
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

int key_generator(Record_f rec)
{
}
