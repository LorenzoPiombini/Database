#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "record.h"
#include "str_op.h"
#include "parse.h"
#include "common.h"
#include "sort.h"
#include "debug.h"

int parse_d_flag_input(char *file_path, int fields_num, 
							char *buffer, 
							char *buf_t, 
							char *buf_v,
							struct Schema *sch, 
							int check_sch,
							struct Record_f *rec)
{

	char **names = get_fileds_name(buffer, fields_num, 3);
	if (!names) {
		printf("get_fields_name failed, parse.c l %d.\n", __LINE__ - 4);
		free_record(rec, rec->fields_num);
		return -1;
	}

	/*check if the fields name are correct- if not - input is incorrect */
	for (int i = 0; i < fields_num; i++)
	{
		if (names[i] == NULL)
		{
			printf("invalid input.\n");
			printf("input syntax: fieldName:TYPE:value\n");
			free_strs(fields_num, 1, names);
			return -1;
		}
		else if (strstr(names[i], "TYPE STRING") ||
				 strstr(names[i], "TYPE LONG") ||
				 strstr(names[i], "TYPE INT") ||
				 strstr(names[i], "TYPE BYTE") ||
				 strstr(names[i], "TYPE FLOAT") ||
				 strstr(names[i], "TYPE DOUBLE") ||
				 strstr(names[i], "TYPE ARRAY INT") ||
				 strstr(names[i], "TYPE ARRAY FLOAT") ||
				 strstr(names[i], "TYPE ARRAY LONG") ||
				 strstr(names[i], "TYPE ARRAY STRING") ||
				 strstr(names[i], "TYPE ARRAY BYTE") ||
				 strstr(names[i], "TYPE ARRAY DOUBLE")) {
			printf("invalid input.\n");
			printf("input syntax: fieldName:TYPE:value\n");
			free_strs(fields_num, 1, names);
			return -1;
		}
	}

	if (!check_fields_integrity(names, fields_num)) {
		printf("invalid input, one or more fields have the same name.\n");
		printf("input syntax: fieldName:TYPE:value\n");
		free_strs(fields_num, 1, names);
		return -1;
	}

	if (sch && check_sch == 0) {
		/* true when a new file is created */
		sch->fields_num = (unsigned short)fields_num;

		for (int j = 0; j < fields_num; j++)
			strncpy(sch->fields_name[j],names[j], strlen(names[j]));
	}

	enum ValueType *types_i = get_value_types(buf_t, fields_num, 3);

	if (!types_i)
	{
		printf("get_values_types failed, parse.c l %d.\n", __LINE__ - 4);
		free_strs(fields_num, 1, names);
		return -1;
	}

	if (sch && check_sch == 0)
	{ /* true when a new file is created or when the schema input is partial*/
		for (int i = 0; i < fields_num; i++)
			sch->types[i] = types_i[i];
	}

	char **values = get_values(buf_v, fields_num);

	if (!values) {
		printf("get_values failed, parse.c l %d.\n", __LINE__ - 3);
		free(types_i);
		free_strs(fields_num, 1, names);
		return -1;
	}

	int reorder_rtl = -1;
	if (check_sch == SCHEMA_EQ){
		reorder_rtl = sort_input_like_header_schema(check_sch, fields_num, sch, names, values, types_i);

		if (!reorder_rtl) {
			printf("sort_input_like_header_schema failed, parse.c l %d.\n", __LINE__ - 4);
			free(types_i);
			free_strs(fields_num, 2, names, values);
			return -1;
		}
	}

	if (check_sch == SCHEMA_NW) {
		reorder_rtl = sort_input_like_header_schema(check_sch, fields_num, sch, names, values, types_i);

		if (!reorder_rtl) {
			printf("sort_input_like_header_schema failed, %s:%d.\n", F, L - 4);
			free(types_i);
			free_strs(fields_num, 2, names, values);
			return -1;
		}

		int old_fn = sch->fields_num;
		sch->fields_num = fields_num;

		for (int i = old_fn; i < fields_num; i++) {
			strncpy(sch->fields_name[i],names[i],strlen(names[i]));
			sch->types[i] = types_i[i];
		}

		create_record(file_path, *sch,rec);
	}

	if (check_sch == SCHEMA_CT) { 
		/*the input contain one or more BUT NOT ALL, fields in the schema*/
		create_record(file_path, *sch,rec);

		int i = 0, j = 0, found = 0;
		for (i = 0; i < sch->fields_num; i++)
		{
			found = 0;
			for (j = 0; j < fields_num; j++) {
				if (strcmp(sch->fields_name[i], names[j]) == 0) {
					if (!set_field(rec, i, names[j], types_i[j], values[j],1)) {
						printf("set_field failed %s:%d.\n", F, L - 2);
						free_strs(fields_num, 2, names, values);
						free_record(rec, sch->fields_num);
						free(types_i);
						return -1;
					}
					found++;
				}
			}
			char *number = "0";
			char *fp = "0.0";
			char *str = "null";
			uint8_t bitfield = 0; 
			if (found == 0) {
				switch (sch->types[i]){
				case TYPE_INT:
				case TYPE_LONG:
				case TYPE_BYTE:
					if (!set_field(rec, i, sch->fields_name[i], sch->types[i], number,bitfield))
					{
						printf("set_field failed %s:%d.\n", F, L - 2);
						free(types_i);
						free_strs(fields_num, 2, names, values);
						free_record(rec, sch->fields_num);
						return -1;
					}
					break;
				case TYPE_STRING:
					if (!set_field(rec, i, sch->fields_name[i], sch->types[i], str,bitfield))
					{
						printf("set_field failed %s:%d.\n", F, L - 2);
						free(types_i);
						free_strs(fields_num, 2, names, values);
						free_record(rec, sch->fields_num);
						return -1;
					}
					break;
				case TYPE_FLOAT:
				case TYPE_DOUBLE:
					if (!set_field(rec, i, sch->fields_name[i], sch->types[i], fp,bitfield))
					{
						printf("set_field failed %s:%d.\n", F, L - 2);
						free(types_i);
						free_strs(fields_num, 2, names, values);
						free_record(rec, sch->fields_num);
						return -1;
					}
					break;
				case TYPE_ARRAY_INT:
				case TYPE_ARRAY_BYTE:
				case TYPE_ARRAY_LONG:
					if (!set_field(rec, i, sch->fields_name[i], sch->types[i], number,bitfield))
					{
						printf("set_field failed %s:%d.\n", F, L - 2);
						free(types_i);
						free_strs(fields_num, 2, names, values);
						free_record(rec, sch->fields_num);
						return -1;
					}
					break;
				case TYPE_ARRAY_FLOAT:
				case TYPE_ARRAY_DOUBLE:
					if (!set_field(rec, i, sch->fields_name[i], sch->types[i], fp,bitfield))
					{
						printf("set_field failed %s:%d.\n", F, L - 2);
						free(types_i);
						free_strs(fields_num, 2, names, values);
						free_record(rec, sch->fields_num);
						return -1;
					}
					break;
				case TYPE_ARRAY_STRING:
					if (!set_field(rec, i, sch->fields_name[i], sch->types[i], str,bitfield))
					{
						printf("set_field failed %s:%d.\n", F, L - 2);
						free(types_i);
						free_strs(fields_num, 2, names, values);
						free_record(rec, sch->fields_num);
						return -1;
					}
					break;
				default:
					printf("type no supported! %d.\n", sch->types[i]);
					free(types_i);
					free_strs(fields_num, 2, names, values);
					free_record(rec, sch->fields_num);
					return -1;
				}
			}
		}

		free(types_i);
		free_strs(fields_num, 2, names, values);
		return 0;
	}else {

		create_record(file_path, *sch,rec);
		for (int i = 0; i < fields_num; i++) {
			if (!set_field(rec, i, names[i], types_i[i], values[i],1)) {
				printf("set_field failed %s:%d.\n", F, L - 2);
				free(types_i);
				free_strs(fields_num, 2, names, values);
				free_record(rec, rec->fields_num);
				return -1;
			}
		}
	}

	free(types_i);
	free_strs(fields_num, 2, names, values);
	return 0;
}


int create_header(struct Header_d *hd)
{

	if (hd->sch_d.fields_name == NULL ||
		hd->sch_d.types == NULL)
	{
		printf("\nschema is NULL.\ncreate header failed, parse.c l %d.\n", __LINE__ - 3);
		return 0;
	}

	hd->id_n = HEADER_ID_SYS;
	hd->version = VS;

	return 1;
}

int write_empty_header(int fd, struct Header_d *hd)
{

	uint32_t id = htonl(hd->id_n); /*converting the endianess*/
	if (write(fd, &id, sizeof(id)) == -1)
	{
		perror("write header id.\n");
		printf("parse.c l %d.\n", __LINE__ - 3);
		return 0;
	}

	uint16_t vs = htons(hd->version);
	if (write(fd, &vs, sizeof(vs)) == -1)
	{
		perror("write header version.\n");
		printf("parse.c l %d.\n", __LINE__ - 3);
		return 0;
	}

	/* important we need to write 0 field_number when the user creates a file with no data*/
	uint16_t fn = htons((uint16_t)hd->sch_d.fields_num);
	if (write(fd, &fn, sizeof(fn)) == -1)
	{
		perror("writing fields number header.");
		printf("parse.c l %d.\n", __LINE__ - 3);
		return 0;
	}

	return 1;
}

int write_header(int fd, struct Header_d *hd)
{
	if (hd->sch_d.fields_name == NULL ||
		hd->sch_d.types == NULL)
	{
		printf("\nschema is NULL.\ncreate header failed, parse.c l %d.\n", __LINE__ - 3);
		return 0;
	}

	uint32_t id = htonl(hd->id_n); /*converting the endianness*/
	if (write(fd, &id, sizeof(id)) == -1)
	{
		perror("write header id.\n");
		printf("parse.c l %d.\n", __LINE__ - 3);
		return 0;
	}

	uint16_t vs = htons(hd->version);
	if (write(fd, &vs, sizeof(vs)) == -1)
	{
		perror("write header version.\n");
		printf("parse.c l %d.\n", __LINE__ - 3);
		return 0;
	}

	uint16_t fn = htons((uint16_t)hd->sch_d.fields_num);
	//	printf("Writing fields_num (network order): %u\n", fn);

	if (write(fd, &fn, sizeof(fn)) == -1)
	{
		perror("writing fields number header.");
		printf("parse.c l %d.\n", __LINE__ - 3);
		return 0;
	}

	register unsigned char i = 0;
	for (i = 0; i <= hd->sch_d.fields_num - 1; i++)
	{
		size_t l = strlen(hd->sch_d.fields_name[i]) + 1;
		uint32_t l_end = htonl((uint32_t)l);

		if (hd->sch_d.fields_name[i])
		{
			if (write(fd, &l_end, sizeof(l_end)) == -1)
			{
				perror("write size of name in header.\n");
				printf("parse.c l %d.\n", __LINE__ - 3);
				return 0;
			}

			if (write(fd, hd->sch_d.fields_name[i], l) == -1)
			{
				perror("write name of field in header.\n");
				printf("parse.c l %d.\n", __LINE__ - 3);
				return 0;
			}
		}
	}
	for (i = 0; i < hd->sch_d.fields_num; i++)
	{

		uint32_t type = htonl((uint32_t)hd->sch_d.types[i]);
		if (write(fd, &type, sizeof(type)) == -1)
		{
			perror("writing types from header.\n");
			printf("parse.c l %d.\n", __LINE__ - 3);
			return 0;
		}
	}

	return 1; // succseed
}

int read_header(int fd, struct Header_d *hd)
{
	unsigned int id = 0;
	if (read(fd, &id, sizeof(id)) == -1)
	{
		perror("reading id from header.\n");
		printf("parse.c l %d.\n", __LINE__ - 3);
		return 0;
	}

	id = ntohl(id); /*changing the bytes to host endianess*/
	if (id != HEADER_ID_SYS) {
		printf("this is not a db file.\n");
		return 0;
	}

	unsigned short vs = 0;
	if (read(fd, &vs, sizeof(vs)) == -1) {
		perror("reading version from header.\n");
		printf("parse.c l %d.\n", __LINE__ - 3);
		return 0;
	}

	vs = ntohs(vs);
	if (vs != VS) {
		printf("this file was edited from a different software.\n");
		return 0;
	}

	hd->id_n = id;
	hd->version = vs;

	uint16_t field_n = 0;
	if (read(fd, &field_n, sizeof(field_n)) == -1) {
		perror("reading field_number header.\n");
		printf("parse.c l %d.\n", __LINE__ - 3);
		return 0;
	}
	hd->sch_d.fields_num = (unsigned short)ntohs(field_n);

	if (hd->sch_d.fields_num == 0) {
		printf("no schema in this header.Please check data integrety.\n");
		return 1;
	}

	//	printf("fields number %u.", hd->sch_d.fields_num);
	for (int i = 0; i < hd->sch_d.fields_num; i++) {
		uint32_t l_end = 0;
		if (read(fd, &l_end, sizeof(l_end)) == -1) {
			perror("reading size of field name.\n");
			printf("parse.c l %d.\n", __LINE__ - 3);
			return 0;
		}
		size_t l = (size_t)ntohl(l_end);

		//	printf("size of name is %ld.\n", l);
		char field[l];
		memset(field,0,l);

		if (read(fd, field, l) == -1) {
			perror("reading name filed from header.\n");
			printf("parse.c l %d.\n", __LINE__ - 3);
			return 0;
		}

		//field[l - 1] = '\0';
		strncpy(hd->sch_d.fields_name[i],field,l);

	}

	for (int i = 0; i < hd->sch_d.fields_num; i++) {
		uint32_t type = 0;
		if (read(fd, &type, sizeof(type)) == -1) {
			perror("reading types from header.");
			printf("parse.c l %d.\n", __LINE__ - 3);
			return 0;
		}

		hd->sch_d.types[i] = ntohl(type);
	}

	return 1; // successed
}

unsigned char ck_input_schema_fields(char **names, enum ValueType *types_i, struct Header_d hd)
{
	int fields_eq = 0;
	int types_eq = 0;

	char **copy_sch = calloc(hd.sch_d.fields_num, sizeof(char *));

	if (!copy_sch)
	{
		printf("calloc failed, parse.c l %d.\n", __LINE__ - 3);
		return 0;
	}

	enum ValueType types_cp[hd.sch_d.fields_num];

	register unsigned char i = 0;
	for (i = 0; i < hd.sch_d.fields_num; i++)
	{
		copy_sch[i] = hd.sch_d.fields_name[i];
		types_cp[i] = hd.sch_d.types[i];
	}

	/*sorting the name and type arrays  */
	if (hd.sch_d.fields_num > 1)
	{
		quick_sort(types_i, 0, hd.sch_d.fields_num - 1);
		quick_sort(types_cp, 0, hd.sch_d.fields_num - 1);
		quick_sort_str(names, 0, hd.sch_d.fields_num - 1);
		quick_sort_str(copy_sch, 0, hd.sch_d.fields_num - 1);
	}

	register unsigned char j = 0;
	for (i = 0, j = 0; i < hd.sch_d.fields_num; i++, j++)
	{
		// printf("%s == %s\n",copy_sch[i],names[j]);
		if (strcmp(copy_sch[i], names[j]) == 0)
			fields_eq++;

		// printf("%d == %d\n",types_cp[i], types_i[j]);
		if (types_cp[i] == types_i[j])
			types_eq++;
	}

	if (fields_eq != hd.sch_d.fields_num || types_eq != hd.sch_d.fields_num)
	{
		printf("Schema different than file definition.\n");
		free(copy_sch);
		return SCHEMA_ERR;
	}

	free(copy_sch);
	return SCHEMA_EQ;
}

unsigned char check_schema(int fields_n, char *buffer, char *buf_t, struct Header_d hd)
{
	char *names_cs = strdup(buffer);
	char *types_cs = strdup(buf_t);

	enum ValueType *types_i = get_value_types(types_cs, fields_n, 3);

	if (!types_i)
	{
		printf("could not get types from input, parse.c:%d.\n", __LINE__ - 4);
		free(names_cs);
		free(types_cs);
		return 0;
	}

	char **names = get_fileds_name(names_cs, fields_n, 3);
	if (!names)
	{
		printf("could not get fields name from input, parse.c:%d.\n", __LINE__ - 3);
		;
		free(names_cs), free(types_cs), free(types_i);
		return 0;
	}

	if (hd.sch_d.fields_num == fields_n)
	{
		//	printf("the fields are == to the schema asper qty.\n");
		unsigned char ck_rst = ck_input_schema_fields(names, types_i, hd);
		switch (ck_rst)
		{
		case SCHEMA_ERR:
			free_strs(fields_n, 1, names);
			free(types_i), free(names_cs);
			free(types_cs);
			return SCHEMA_ERR;
		case SCHEMA_EQ:
			free_strs(fields_n, 1, names);
			free(types_i), free(names_cs);
			free(types_cs);
			return SCHEMA_EQ;
		default:
			printf("check on Schema failed.\n");
			free_strs(fields_n, 1, names);
			free(types_i), free(names_cs);
			free(types_cs);
			return 0;
		}
	}
	else if (hd.sch_d.fields_num < fields_n)
	{ /* case where the header needs to be updated */
		if (((fields_n - hd.sch_d.fields_num) + hd.sch_d.fields_num) > MAX_FIELD_NR)
		{
			printf("cannot add the new fileds, limit is %d fields.\n", MAX_FIELD_NR);
			return SCHEMA_ERR;
		}
		unsigned char ck_rst = ck_input_schema_fields(names, types_i, hd);

		switch (ck_rst)
		{
		case SCHEMA_ERR:
			free_strs(fields_n, 1, names);
			free(types_i), free(names_cs);
			free(types_cs);
			return SCHEMA_ERR;
		case SCHEMA_EQ:
			free_strs(fields_n, 1, names);
			free(types_i), free(names_cs);
			free(types_cs);
			return SCHEMA_NW;
		default:
			free_strs(fields_n, 1, names);
			free(types_i), free(names_cs);
			free(types_cs);
			return 0;
		}
	}
	else if (hd.sch_d.fields_num > fields_n)
	{ /*case where the fileds are less than the schema */
		// if they are in the schema and the types are correct, return SCHEMA_CT
		// create a record with only the values provided and set the other values to 0;
		// printf("last data should be here.\n");

		int ck_rst = ck_schema_contain_input(names, types_i, hd, fields_n);

		switch (ck_rst)
		{
		case SCHEMA_ERR:
			free_strs(fields_n, 1, names);
			free(types_i), free(names_cs);
			free(types_cs);
			return SCHEMA_ERR;
		case SCHEMA_CT:
			free_strs(fields_n, 1, names);
			free(types_i), free(names_cs);
			free(types_cs);
			return SCHEMA_CT;
		default:
			free_strs(fields_n, 1, names);
			free(types_i), free(names_cs);
			free(types_cs);
			return 0;
		}
	}

	// this is unreachable
	free_strs(fields_n, 1, names);
	free(types_i), free(names_cs);
	free(types_cs);
	return 1;
}

int sort_input_like_header_schema(int schema_tp, int fields_num, struct Schema *sch, char **names, char **values, enum ValueType *types_i)
{
	int f_n = schema_tp == SCHEMA_NW ? sch->fields_num : fields_num;
	int value_pos[f_n];
	register unsigned char i, j;

	for (i = 0; i < f_n; i++)
	{
		for (j = 0; j < sch->fields_num; j++)
		{
			if (strcmp(names[i], sch->fields_name[j]) == 0)
			{
				value_pos[i] = j;
				break;
			}
		}
	}

	char **temp_val = calloc(fields_num, sizeof(char *));
	if (!temp_val)
	{
		__er_calloc(F, L - 3);
		return 0;
	}

	char **temp_name = calloc(fields_num, sizeof(char *));
	if (!temp_name)
	{
		__er_calloc(F, L - 3);
		free(temp_val);
		return 0;
	}

	enum ValueType temp_types[fields_num];

	for (i = 0; i < f_n; i++)
	{
		temp_val[value_pos[i]] = values[i];
		temp_name[value_pos[i]] = names[i];
		temp_types[value_pos[i]] = types_i[i];
	}

	for (i = 0; i < f_n; i++)
	{
		values[i] = temp_val[i];
		names[i] = temp_name[i];
		types_i[i] = temp_types[i];
	}

	free(temp_val), free(temp_name);
	return 1;
}

unsigned char ck_schema_contain_input(char **names, enum ValueType *types_i, struct Header_d hd, int fields_num)
{
	// printf("fields are %d",fields_num);
	register unsigned char i = 0, j = 0;
	int found_f = 0;

	for (i = 0; i < fields_num; i++)
	{
		for (j = 0; j < hd.sch_d.fields_num; j++)
		{
			//		 printf("%s == %s\n",names[i],hd.sch_d.fields_name[j]);
			if (strcmp(names[i], hd.sch_d.fields_name[j]) == 0)
			{
				found_f++;
				//		 printf("%d == %d\n",types_i[i], hd.sch_d.types[j]);
				if ((int)types_i[i] != hd.sch_d.types[j])
				{
					printf("Schema different than file definition.\n");
					return SCHEMA_ERR;
				}
			}
		}
	}
	if (found_f == fields_num)
	{
		return SCHEMA_CT;
	}

	printf("Schema different than file definition.\n");
	return SCHEMA_ERR;
}

unsigned char add_fields_to_schema(int fields_num, char *buffer, char *buf_t, struct Schema *sch)
{
	char **names = get_fileds_name(buffer, fields_num, 2);

	if (!names) {
		printf("Error in getting the fields name");
		return 0;
	}

	enum ValueType *types_i = get_value_types(buf_t, fields_num, 2);

	if (!types_i) {
		printf("Error in getting the fields types");
		free_strs(fields_num, 1, names);
		return 0;
	}

	int x = 0;
	int found = 0;
	unsigned char new_fields = 0;

	int pos[fields_num]; /* to store the field position that are already in the schema*/
	memset(pos,-1,fields_num);
	
	for (int i = 0; i < sch->fields_num; i++) {
		for (int j = 0; j < fields_num; j++) {
			if(pos[j] == j) continue;
			if (strncmp(sch->fields_name[i], names[j],strlen(names[j])) == 0) {
				found++;
				pos[x] = j; /* save the position of the field that is already in the schema*/
				x++;
			} else {
				new_fields = 1;
			} 

			if (found == fields_num) {
				printf("fields already exist.\n");
				free_strs(fields_num, 1, names);
				free(types_i);
				return 0;
			}
		}
	}

	if (new_fields) {
		/* check which fields are already in the schema if any */

		for (int i = 0; i < fields_num; i++) {
			if(pos[i] == i) continue; 
			strncpy(sch->fields_name[sch->fields_num],names[i],strlen(names[i]));
			sch->types[sch->fields_num] = types_i[i];
			sch->fields_num++;
		}

		free_strs(fields_num, 1, names);
		free(types_i);
		return 1;
	}


	return 1;
}

int create_file_definition_with_no_value(int fields_num, char *buffer, char *buf_t, struct Schema *sch)
{

	char **names = get_fileds_name(buffer, fields_num, 2);

	if (!names)
	{
		printf("Error in getting the fields name");
		return 0;
	}

	/*check if the fields name are correct- if not - input is incorrect */
	for (int i = 0; i < fields_num; i++)
	{

		if (names[i] == NULL)
		{
			printf("invalid input.\n");
			printf("input syntax: fieldName:TYPE:value\n");
			free_strs(fields_num, 1, names);
			return 0;
		}
		else if (strstr(names[i], "TYPE STRING") ||
				 strstr(names[i], "TYPE LONG") ||
				 strstr(names[i], "TYPE INT") ||
				 strstr(names[i], "TYPE BYTE") ||
				 strstr(names[i], "TYPE FLOAT") ||
				 strstr(names[i], "TYPE DOUBLE") ||
				 strstr(names[i], "TYPE ARRAY DOUBLE") ||
				 strstr(names[i], "TYPE ARRAY INT") ||
				 strstr(names[i], "TYPE ARRAY LONG") ||
				 strstr(names[i], "TYPE ARRAY FLOAT") ||
				 strstr(names[i], "TYPE ARRAY BYTE") ||
				 strstr(names[i], "TYPE ARRAY STRING") ||
				 strstr(names[i], "t_s") ||
				 strstr(names[i], "t_l") ||
				 strstr(names[i], "t_i") ||
				 strstr(names[i], "t_b") ||
				 strstr(names[i], "t_f") ||
				 strstr(names[i], "t_d") ||
				 strstr(names[i], "t_ad") ||
				 strstr(names[i], "t_ai") ||
				 strstr(names[i], "t_al") ||
				 strstr(names[i], "t_af") ||
				 strstr(names[i], "t_ab") ||
				 strstr(names[i], "t_as"))
		{
			printf("invalid input.\ninput syntax: \
					fieldName:TYPE:value\n");
			free_strs(fields_num, 1, names);
			return 0;
		}

		if (strlen(names[i]) > MAX_FIELD_LT)
		{
			printf("invalid input.\n");
			printf("one or more filed names are too long.\n");
			free_strs(fields_num, 1, names);
			return 0;
		}
	}

	if (!check_fields_integrity(names, fields_num))
	{
		printf("invalid input, one or more fields have the same name.\n");
		printf("input syntax: fieldName:TYPE:value\n");
		free_strs(fields_num, 1, names);
		return 0;
	}

	for (int j = 0; j < fields_num; j++)
		if (names[j])
			strncpy(sch->fields_name[j],names[j],strlen(names[j]));

	enum ValueType *types_i = get_value_types(buf_t, fields_num, 2);

	if (!types_i) {
		printf("Error in getting the types.\n");
		free_strs(fields_num, 1, names);
		return 0;
	}

	for (int i = 0; i < fields_num; i++)
	{
		if (types_i[i] != TYPE_INT &&
			types_i[i] != TYPE_FLOAT &&
			types_i[i] != TYPE_LONG &&
			types_i[i] != TYPE_DOUBLE &&
			types_i[i] != TYPE_BYTE &&
			types_i[i] != TYPE_STRING &&
			types_i[i] != TYPE_ARRAY_INT &&
			types_i[i] != TYPE_ARRAY_LONG &&
			types_i[i] != TYPE_ARRAY_FLOAT &&
			types_i[i] != TYPE_ARRAY_STRING &&
			types_i[i] != TYPE_ARRAY_BYTE &&
			types_i[i] != TYPE_ARRAY_DOUBLE)
		{
			printf("invalid input.\n");
			printf("input syntax: fieldName:TYPE:value\n");
			free_strs(fields_num, 1, names);
			free(types_i);
			return 0;
		}
	}

	for (int i = 0; i < fields_num; i++)
		sch->types[i] = types_i[i];

	free_strs(fields_num, 1, names);
	free(types_i);
	return 1; // scheam creation succssed
}

unsigned char perform_checks_on_schema(char *buffer, 
					char *buf_t, 
					char *buf_v, 
					int fields_count,
					char *file_path, 
					struct Record_f *rec, 
					struct Header_d *hd)
{

	// check if the schema on the file is equal to the input Schema.

	if (hd->sch_d.fields_num != 0)
	{
		unsigned char check = check_schema(fields_count, buffer, buf_t, *hd);
		// printf("check schema is %d",check);
		switch (check){
		case SCHEMA_EQ:
			if(parse_d_flag_input(file_path, fields_count, buffer,
				buf_t, buf_v, &hd->sch_d, SCHEMA_EQ,rec) == -1) return SCHEMA_ERR;

			return SCHEMA_EQ;
		case SCHEMA_ERR:
			return SCHEMA_ERR;
		case SCHEMA_NW:
			if(parse_d_flag_input(file_path, fields_count, buffer,
				buf_t, buf_v, &hd->sch_d, SCHEMA_NW,rec) == -1) return SCHEMA_ERR;

			return SCHEMA_NW;
		case SCHEMA_CT:
			if(parse_d_flag_input(file_path, fields_count, buffer,
				buf_t, buf_v, &hd->sch_d, SCHEMA_CT,rec) == -1) return SCHEMA_ERR;

			return SCHEMA_CT;
		default:
			printf("check is %d -> no processable option for the SCHEMA. parse.c:%d.\n", check, __LINE__ - 17);
			return 0;
		}
	} else { /* in this case the SCHEMA IS ALWAYS NEW*/
		if(parse_d_flag_input(file_path, fields_count, buffer,
			buf_t, buf_v, &hd->sch_d, SCHEMA_EQ,rec) == -1) return SCHEMA_ERR;

		return 0;
	}

	return 1;
}

unsigned char compare_old_rec_update_rec(struct Record_f *rec_old, 
						struct Record_f *rec,
						struct Record_f *new_rec, 
						char *file_path,
						unsigned char check, 
						char *buffer, 
						int fields_num,
						struct Header_d hd)
{
	unsigned char i = 0, j = 0;
	int changed = 0;

	if (check == SCHEMA_CT ) {
		char **names = get_fileds_name(buffer, fields_num, 3);
		if (!names) {
			printf("no memory for names, parse.c l 864.\n");
			return 0;
		}

		for (j = 0; j < rec_old->fields_num; j++) {
			if (rec->field_set[j] == 1 && rec_old->field_set[j] == 1) {
				changed = 1;
				switch (rec->fields[j].type) {
				case TYPE_INT:
					if (rec_old->fields[j].data.i != rec->fields[j].data.i)
						rec_old->fields[j].data.i = rec->fields[j].data.i;
					break;
				case TYPE_LONG:
					if (rec_old->fields[j].data.l != rec->fields[j].data.l)
						rec_old->fields[j].data.l = rec->fields[j].data.l;

					break;
				case TYPE_FLOAT:
					if (rec_old->fields[j].data.f != rec->fields[j].data.f)
						rec_old->fields[j].data.f = rec->fields[j].data.f;
					break;
				case TYPE_STRING:
					if (strcmp(rec_old->fields[j].data.s, rec->fields[j].data.s) != 0)
					{
						// free memory before allocating other memory
						if (rec_old->fields[j].data.s != NULL)
						{
							free(rec_old->fields[j].data.s);
							rec_old->fields[j].data.s = NULL;
						}
						rec_old->fields[j].data.s = strdup(rec->fields[j].data.s);
						if (!rec_old->fields[j].data.s)
						{
							fprintf(stderr, "strdup failed, %s:%d.\n", F, L - 2);
							return 0;
						}
					}
					break;
				case TYPE_BYTE:
					if (rec_old->fields[j].data.b != rec->fields[j].data.b)
						rec_old->fields[j].data.b = rec->fields[j].data.b;
					break;
				case TYPE_DOUBLE:
					if (rec_old->fields[j].data.d != rec->fields[j].data.d)
						rec_old->fields[j].data.d = rec->fields[j].data.d;
					break;
				case TYPE_ARRAY_INT:
				{
					if (rec_old->fields[j].data.v.size == rec->fields[j].data.v.size){
						/*check values*/
						for (int a = 0; a < rec_old->fields[j].data.v.size; a++) {
							if (*rec_old->fields[j].data.v.elements.i[a] == *rec->fields[j].data.v.elements.i[a]) continue;

								*rec_old->fields[j].data.v.elements.i[a] = *rec->fields[j].data.v.elements.i[a];
						}
					}else {
								rec_old->fields[j].data.v.destroy(&rec_old->fields[j].data.v, rec->fields[j].type);
								for (int a = 0; a < rec->fields[j].data.v.size; a++){
									rec_old->fields[j].data.v.
										insert((void *)rec->fields[j].data.v.elements.i[a],
										&rec_old->fields[j].data.v, rec->fields[j].type);
								}
							}
							break;
						}
					case TYPE_ARRAY_LONG:
						{
							if (rec_old->fields[j].data.v.size == rec->fields[j].data.v.size)
							{
								/*check values*/
								for (int a = 0; a < rec_old->fields[j].data.v.size; a++)
								{
									if (*rec_old->fields[j].data.v.elements.l[a] == *rec->fields[j].data.v.elements.l[a])
										continue;
									*rec_old->fields[j].data.v.elements.l[a] = *rec->fields[j].data.v.elements.l[a];
								}
							}
							else
							{
								rec_old->fields[j].data.v.destroy(&rec_old->fields[j].data.v, rec->fields[j].type);
								for (int a = 0; a < rec->fields[j].data.v.size; a++)
								{
									rec_old->fields[j].data.v.insert((void *)rec->fields[j].data.v.elements.l[a], &rec_old->fields[j].data.v, rec->fields[j].type);
								}
							}
							break;
						}
					case TYPE_ARRAY_FLOAT:
						{
							if (rec_old->fields[j].data.v.size == rec->fields[j].data.v.size)
							{
								/*check values*/
								for (int a = 0; a < rec_old->fields[j].data.v.size; a++)
								{
									if (*rec_old->fields[j].data.v.elements.f[a] == *rec->fields[j].data.v.elements.f[a])
										continue;
									*rec_old->fields[j].data.v.elements.f[a] = *rec->fields[j].data.v.elements.f[a];
								}
							}
							else
							{
								rec_old->fields[j].data.v.destroy(&rec_old->fields[j].data.v, rec->fields[j].type);
								for (int a = 0; a < rec->fields[j].data.v.size; a++)
								{
									rec_old->fields[j].data.v.insert((void *)rec->fields[j].data.v.elements.f[a], &rec_old->fields[j].data.v, rec->fields[j].type);
								}
							}
							break;
						}
					case TYPE_ARRAY_BYTE:
						{
							if (rec_old->fields[j].data.v.size == rec->fields[j].data.v.size)
							{
								/*check values*/
								for (int a = 0; a < rec_old->fields[j].data.v.size; a++)
								{
									if (*rec_old->fields[j].data.v.elements.b[a] == *rec->fields[j].data.v.elements.b[a])
										continue;
									*rec_old->fields[j].data.v.elements.b[a] = *rec->fields[j].data.v.elements.b[a];
								}
							}
							else
							{
								rec_old->fields[j].data.v.destroy(&rec_old->fields[j].data.v, rec->fields[j].type);
								for (int a = 0; a < rec->fields[j].data.v.size; a++)
								{
									rec_old->fields[j].data.v.insert((void *)rec->fields[j].data.v.elements.b[a], &rec_old->fields[j].data.v, rec->fields[j].type);
								}
							}
							break;
						}
					case TYPE_ARRAY_STRING:
						{
							if (rec_old->fields[j].data.v.size == rec->fields[j].data.v.size)
							{
								/*check values*/
								for (int a = 0; a < rec_old->fields[j].data.v.size; a++)
								{

									if (strcmp(rec_old->fields[j].data.v.elements.s[a], rec->fields[j].data.v.elements.s[a]) != 0)
									{
										// free memory before allocating other memory
										if (rec_old->fields[j].data.v.elements.s[a] != NULL)
										{
											free(rec_old->fields[j].data.v.elements.s[a]);
											rec_old->fields[j].data.v.elements.s[a] = NULL;
										}

										rec_old->fields[j].data.v.elements.s[a] = strdup(rec->fields[j].data.v.elements.s[a]);
										if (!rec_old->fields[j].data.v.elements.s[a])
										{
											fprintf(stderr, "strdup failed, %s:%d.\n", F, L - 2);
											return 0;
										}
									}
								}
							}
							else
							{
								rec_old->fields[j].data.v.destroy(&rec_old->fields[j].data.v, rec->fields[j].type);
								for (int a = 0; a < rec->fields[j].data.v.size; a++)
								{
									rec_old->fields[j].data.v.insert((void *)rec->fields[j].data.v.elements.s[a], &rec_old->fields[j].data.v, rec->fields[j].type);
								}
							}
							break;
						}
					case TYPE_ARRAY_DOUBLE:
						{
							if (rec_old->fields[j].data.v.size == rec->fields[j].data.v.size)
							{
								/*check values*/
								for (int a = 0; a < rec_old->fields[j].data.v.size; a++)
								{
									if (*rec_old->fields[j].data.v.elements.d[a] == *rec->fields[j].data.v.elements.d[a])
										continue;
									*rec_old->fields[j].data.v.elements.d[a] = *rec->fields[j].data.v.elements.d[a];
								}
							}
							else
							{
								rec_old->fields[j].data.v.destroy(&rec_old->fields[j].data.v, rec->fields[j].type);
								for (int a = 0; a < rec->fields[j].data.v.size; a++)
								{
									rec_old->fields[j].data.v.insert((void *)rec->fields[j].data.v.elements.i[a], &rec_old->fields[j].data.v, rec->fields[j].type);
								}
							}
							break;
						}
					default:
						printf("invalid type! type -> %d.\n", rec->fields[j].type);
						return 0;
				}
			}
		}


		free_strs(fields_num, 1, names);
		if(changed){
			return UPDATE_OLD;
		}else {
			return UPDATE_OLDN;
		}
	}

	if (rec_old->fields_num == rec->fields_num)
	{
		for (i = 0; i < rec_old->fields_num; i++)
		{
			for (j = 0; j < rec_old->fields_num; j++)
			{
				if (strcmp(rec_old->fields[i].field_name, rec->fields[j].field_name) == 0)
				{
					switch (rec_old->fields[i].type)
					{
					case TYPE_INT:
						if (rec_old->fields[i].data.i != rec->fields[i].data.i)
							rec_old->fields[i].data.i = rec->fields[i].data.i;
						break;
					case TYPE_LONG:
						if (rec_old->fields[i].data.l != rec->fields[i].data.l)
							rec_old->fields[i].data.l = rec->fields[i].data.l;
						break;
					case TYPE_FLOAT:
						if (rec_old->fields[i].data.f != rec->fields[i].data.f)
							rec_old->fields[i].data.f = rec->fields[i].data.f;
						break;
					case TYPE_STRING:
						if (strcmp(rec_old->fields[i].data.s, rec->fields[i].data.s) != 0)
						{
							// free memory before allocating other memory
							if (rec_old->fields[i].data.s != NULL)
							{
								free(rec_old->fields[i].data.s);
								rec_old->fields[i].data.s = NULL;
							}
							rec_old->fields[i].data.s = strdup(rec->fields[i].data.s);
						}
						break;
					case TYPE_BYTE:
						if (rec_old->fields[i].data.b != rec->fields[i].data.b)
							rec_old->fields[i].data.b = rec->fields[i].data.b;
						break;
					case TYPE_DOUBLE:
						if (rec_old->fields[i].data.d != rec->fields[i].data.d)
							rec_old->fields[i].data.d = rec->fields[i].data.d;
						break;
					case TYPE_ARRAY_INT:
					{
						if (rec_old->fields[j].data.v.size == rec->fields[j].data.v.size)
						{
							/*check values*/
							for (int a = 0; a < rec_old->fields[j].data.v.size; a++)
							{
								if (*rec_old->fields[j].data.v.elements.i[a] == *rec->fields[j].data.v.elements.i[a])
									continue;
								*rec_old->fields[j].data.v.elements.i[a] = *rec->fields[j].data.v.elements.i[a];
							}
						}
						else
						{
							rec_old->fields[j].data.v.destroy(&rec_old->fields[j].data.v, rec->fields[j].type);
							for (int a = 0; a < rec->fields[j].data.v.size; a++)
							{
								rec_old->fields[j].data.v.insert((void *)rec->fields[j].data.v.elements.i[a], &rec_old->fields[j].data.v, rec->fields[j].type);
							}
						}
						break;
					}
					case TYPE_ARRAY_LONG:
					{
						if (rec_old->fields[j].data.v.size == rec->fields[j].data.v.size)
						{
							/*check values*/
							for (int a = 0; a < rec_old->fields[j].data.v.size; a++)
							{
								if (*rec_old->fields[j].data.v.elements.l[a] == *rec->fields[j].data.v.elements.l[a])
									continue;
								*rec_old->fields[j].data.v.elements.l[a] = *rec->fields[j].data.v.elements.l[a];
							}
						}
						else
						{
							rec_old->fields[j].data.v.destroy(&rec_old->fields[j].data.v, rec->fields[j].type);
							for (int a = 0; a < rec->fields[j].data.v.size; a++)
							{
								rec_old->fields[j].data.v.insert((void *)rec->fields[j].data.v.elements.l[a], &rec_old->fields[j].data.v, rec->fields[j].type);
							}
						}
						break;
					}
					case TYPE_ARRAY_FLOAT:
					{
						if (rec_old->fields[j].data.v.size == rec->fields[j].data.v.size)
						{
							/*check values*/
							for (int a = 0; a < rec_old->fields[j].data.v.size; a++)
							{
								if (*rec_old->fields[j].data.v.elements.f[a] == *rec->fields[j].data.v.elements.f[a])
									continue;
								*rec_old->fields[j].data.v.elements.f[a] = *rec->fields[j].data.v.elements.f[a];
							}
						}
						else
						{
							rec_old->fields[j].data.v.destroy(&rec_old->fields[j].data.v, rec->fields[j].type);
							for (int a = 0; a < rec->fields[j].data.v.size; a++)
							{
								rec_old->fields[j].data.v.insert((void *)rec->fields[j].data.v.elements.f[a], &rec_old->fields[j].data.v, rec->fields[j].type);
							}
						}
						break;
					}
					case TYPE_ARRAY_BYTE:
					{
						if (rec_old->fields[j].data.v.size == rec->fields[j].data.v.size)
						{
							/*check values*/
							for (int a = 0; a < rec_old->fields[j].data.v.size; a++)
							{
								if (*rec_old->fields[j].data.v.elements.b[a] == *rec->fields[j].data.v.elements.b[a])
									continue;
								*rec_old->fields[j].data.v.elements.b[a] = *rec->fields[j].data.v.elements.b[a];
							}
						}
						else
						{
							rec_old->fields[j].data.v.destroy(&rec_old->fields[j].data.v, rec->fields[j].type);
							for (int a = 0; a < rec->fields[j].data.v.size; a++)
							{
								rec_old->fields[j].data.v.insert((void *)rec->fields[j].data.v.elements.b[a], &rec_old->fields[j].data.v, rec->fields[j].type);
							}
						}
						break;
					}
					case TYPE_ARRAY_STRING:
					{
						if (rec_old->fields[j].data.v.size == rec->fields[j].data.v.size)
						{
							/*check values*/
							for (int a = 0; a < rec_old->fields[j].data.v.size; a++)
							{

								if (strcmp(rec_old->fields[j].data.v.elements.s[a], rec->fields[j].data.v.elements.s[a]) != 0)
								{
									// free memory before allocating other memory
									if (rec_old->fields[j].data.v.elements.s[a] != NULL)
									{
										free(rec_old->fields[j].data.v.elements.s[a]);
										rec_old->fields[j].data.v.elements.s[a] = NULL;
									}

									rec_old->fields[j].data.v.elements.s[a] = strdup(rec->fields[j].data.v.elements.s[a]);
									if (!rec_old->fields[j].data.v.elements.s[a])
									{
										fprintf(stderr, "strdup failed, %s:%d.\n", F, L - 2);
										return 0;
									}
								}
							}
						}
						else
						{
							rec_old->fields[j].data.v.destroy(&rec_old->fields[j].data.v, rec->fields[j].type);
							for (int a = 0; a < rec->fields[j].data.v.size; a++)
							{
								rec_old->fields[j].data.v.insert((void *)rec->fields[j].data.v.elements.s[a], &rec_old->fields[j].data.v, rec->fields[j].type);
							}
						}
						break;
					}
					case TYPE_ARRAY_DOUBLE:
					{
						if (rec_old->fields[j].data.v.size == rec->fields[j].data.v.size)
						{
							/*check values*/
							for (int a = 0; a < rec_old->fields[j].data.v.size; a++)
							{
								if (*rec_old->fields[j].data.v.elements.d[a] == *rec->fields[j].data.v.elements.d[a])
									continue;
								*rec_old->fields[j].data.v.elements.d[a] = *rec->fields[j].data.v.elements.d[a];
							}
						}
						else
						{
							rec_old->fields[j].data.v.destroy(&rec_old->fields[j].data.v, rec->fields[j].type);
							for (int a = 0; a < rec->fields[j].data.v.size; a++)
							{
								rec_old->fields[j].data.v.insert((void *)rec->fields[j].data.v.elements.i[a], &rec_old->fields[j].data.v, rec->fields[j].type);
							}
						}
						break;
					}
					default:
						printf("invalid type! type -> %d.\n", rec->fields[i].type);
						return 0;
					}
				}
			}
		}
		return UPDATE_OLD;
	}

	if (rec_old->fields_num < rec->fields_num)
	{
		int elements = rec->fields_num - rec_old->fields_num;

		create_record(file_path, hd.sch_d, new_rec);

		char **names = calloc(elements, sizeof(char *));
		if (!names)
		{
			printf("calloc failed at parse.c l 875.\n");
			return 0;
		}

		enum ValueType *types_i = calloc(elements, sizeof(enum ValueType));

		if (!types_i)
		{
			printf("calloc failed at parse.c l 881.\n");
			free_strs(elements, 1, names);
			return 0;
		}

		char **values = calloc(elements, sizeof(char *));
		if (!values)
		{
			printf("calloc failed at parse.c l 827.\n");
			free_strs(elements, 1, names);
			free(types_i);
			return 0;
		}

		for (i = 0; i < rec_old->fields_num; i++)
		{
			if (strcmp(rec_old->fields[i].field_name, rec->fields[i].field_name) == 0)
			{
				switch (rec_old->fields[i].type)
				{
				case TYPE_INT:
					if (rec->fields[i].data.i != 0)
						rec_old->fields[i].data.i = rec->fields[i].data.i;

					break;
				case TYPE_LONG:
					if (rec->fields[i].data.l != 0)
						rec_old->fields[i].data.l = rec->fields[i].data.l;

					break;
				case TYPE_FLOAT:
					if (rec->fields[i].data.f != 0.0)
					{
						rec_old->fields[i].data.f = rec->fields[i].data.f;
					}
					break;
				case TYPE_STRING:
					if (strcmp(rec->fields[i].data.s, "null") != 0)
					{
						// free memory before allocating other memory
						if (rec_old->fields[i].data.s != NULL)
						{
							free(rec_old->fields[i].data.s);
							rec_old->fields[i].data.s = NULL;

							rec_old->fields[i].data.s = strdup(rec->fields[i].data.s);
						}
					}
					break;
				case TYPE_BYTE:
					if (rec->fields[i].data.b != 0)
						rec_old->fields[i].data.b = rec->fields[i].data.b;
					break;
				case TYPE_DOUBLE:
					if (rec->fields[i].data.d != 0.0)
						rec_old->fields[i].data.d = rec->fields[i].data.d;
					break;
				case TYPE_ARRAY_INT:
					if (rec->fields[i].data.v.elements.i)
					{
						if (rec->fields[i].data.v.size == 1 && *rec->fields[i].data.v.elements.i[0] == 0)
							break;

						if (rec_old->fields[i].data.v.elements.i)
						{
							if (rec_old->fields[i].data.v.size == rec->fields[i].data.v.size)
							{
								for (int a = 0; a < rec->fields[i].data.v.size; a++)
								{
									if (*rec_old->fields[i].data.v.elements.i[a] == *rec->fields[i].data.v.elements.i[a])
										continue;

									*rec_old->fields[i].data.v.elements.i[a] = *rec->fields[i].data.v.elements.i[a];
								}
							}
							else
							{
								rec_old->fields[i].data.v.destroy(&rec_old->fields[i].data.v, rec_old->fields[i].type);
								for (int a = 0; a < rec->fields[i].data.v.size; a++)
								{
									rec_old->fields[i].data.v.insert((void *)rec->fields[i].data.v.elements.i[a],
																		&rec_old->fields[i].data.v, rec_old->fields[i].type);
								}
							}
						}
						else
						{
							rec_old->fields[i].data.v.insert = insert_element;
							rec_old->fields[i].data.v.destroy = free_dynamic_array;

							for (int a = 0; a < rec->fields[i].data.v.size; a++)
							{
								rec_old->fields[i].data.v.insert((void *)rec->fields[i].data.v.elements.i[a],
																	&rec_old->fields[i].data.v, rec_old->fields[i].type);
							}
						}
					}
					break;
				case TYPE_ARRAY_LONG:
					if (rec->fields[i].data.v.elements.l)
					{
						if (rec->fields[i].data.v.size == 1 && *rec->fields[i].data.v.elements.l[0] == 0)
							break;

						if (rec_old->fields[i].data.v.elements.l)
						{
							if (rec_old->fields[i].data.v.size == rec->fields[i].data.v.size)
							{
								for (int a = 0; a < rec->fields[i].data.v.size; a++)
								{
									if (*rec_old->fields[i].data.v.elements.l[a] == *rec->fields[i].data.v.elements.l[a])
										continue;

									*rec_old->fields[i].data.v.elements.l[a] = *rec->fields[i].data.v.elements.l[a];
								}
							}
							else
							{
								rec_old->fields[i].data.v.destroy(&rec_old->fields[i].data.v, rec_old->fields[i].type);
								for (int a = 0; a < rec->fields[i].data.v.size; a++)
								{
									rec_old->fields[i].data.v.insert((void *)rec->fields[i].data.v.elements.l[a],
																		&rec_old->fields[i].data.v, rec_old->fields[i].type);
								}
							}
						}
						else
						{
							rec_old->fields[i].data.v.insert = insert_element;
							rec_old->fields[i].data.v.destroy = free_dynamic_array;
							for (int a = 0; a < rec->fields[i].data.v.size; a++)
							{
								rec_old->fields[i].data.v.insert((void *)rec->fields[i].data.v.elements.l[a],
																	&rec_old->fields[i].data.v, rec_old->fields[i].type);
							}
						}
					}
					break;
				case TYPE_ARRAY_FLOAT:
					if (rec->fields[i].data.v.elements.f)
					{
						if (rec->fields[i].data.v.size == 1 && *rec->fields[i].data.v.elements.f[0] == 0.0)
							break;

						if (rec_old->fields[i].data.v.elements.f)
						{
							if (rec_old->fields[i].data.v.size == rec->fields[i].data.v.size)
							{
								for (int a = 0; a < rec->fields[i].data.v.size; a++)
								{
									if (*rec_old->fields[i].data.v.elements.f[a] == *rec->fields[i].data.v.elements.f[a])
										continue;

									*rec_old->fields[i].data.v.elements.f[a] = *rec->fields[i].data.v.elements.f[a];
								}
							}
							else
							{
								rec_old->fields[i].data.v.destroy(&rec_old->fields[i].data.v, rec_old->fields[i].type);
								for (int a = 0; a < rec->fields[i].data.v.size; a++)
								{
									rec_old->fields[i].data.v.insert((void *)rec->fields[i].data.v.elements.f[a],
																		&rec_old->fields[i].data.v, rec_old->fields[i].type);
								}
							}
						}
						else
						{
							rec_old->fields[i].data.v.insert = insert_element;
							rec_old->fields[i].data.v.destroy = free_dynamic_array;
							for (int a = 0; a < rec->fields[i].data.v.size; a++)
							{
								rec_old->fields[i].data.v.insert((void *)rec->fields[i].data.v.elements.f[a],
																	&rec_old->fields[i].data.v, rec_old->fields[i].type);
							}
						}
					}
					break;
				case TYPE_ARRAY_DOUBLE:
					if (rec->fields[i].data.v.elements.d)
					{
						if (rec->fields[i].data.v.size == 1 && *rec->fields[i].data.v.elements.d[0] == 0)
							break;

						if (rec_old->fields[i].data.v.elements.d)
						{
							if (rec_old->fields[i].data.v.size == rec->fields[i].data.v.size)
							{
								for (int a = 0; a < rec->fields[i].data.v.size; a++)
								{
									if (*rec_old->fields[i].data.v.elements.d[a] == *rec->fields[i].data.v.elements.d[a])
										continue;

									*rec_old->fields[i].data.v.elements.d[a] = *rec->fields[i].data.v.elements.d[a];
								}
							}
							else
							{
								rec_old->fields[i].data.v.destroy(&rec_old->fields[i].data.v, rec_old->fields[i].type);
								for (int a = 0; a < rec->fields[i].data.v.size; a++)
								{
									rec_old->fields[i].data.v.insert((void *)rec->fields[i].data.v.elements.d[a],
																		&rec_old->fields[i].data.v, rec_old->fields[i].type);
								}
							}
						}
						else
						{
							rec_old->fields[i].data.v.insert = insert_element;
							rec_old->fields[i].data.v.destroy = free_dynamic_array;
							for (int a = 0; a < rec->fields[i].data.v.size; a++)
							{
								rec_old->fields[i].data.v.insert((void *)rec->fields[i].data.v.elements.d[a],
																	&rec_old->fields[i].data.v, rec_old->fields[i].type);
							}
						}
					}
					break;
				case TYPE_ARRAY_BYTE:
					if (rec->fields[i].data.v.elements.b)
					{
						if (rec->fields[i].data.v.size == 1 && *rec->fields[i].data.v.elements.b[0] == 0)
							break;

						if (rec_old->fields[i].data.v.elements.b)
						{
							if (rec_old->fields[i].data.v.size == rec->fields[i].data.v.size)
							{
								for (int a = 0; a < rec->fields[i].data.v.size; a++)
								{
									if (*rec_old->fields[i].data.v.elements.b[a] == *rec->fields[i].data.v.elements.b[a])
										continue;

									*rec_old->fields[i].data.v.elements.b[a] = *rec->fields[i].data.v.elements.b[a];
								}
							}
							else
							{
								rec_old->fields[i].data.v.destroy(&rec_old->fields[i].data.v, rec_old->fields[i].type);
								for (int a = 0; a < rec->fields[i].data.v.size; a++)
								{
									rec_old->fields[i].data.v.insert((void *)rec->fields[i].data.v.elements.b[a],
																		&rec_old->fields[i].data.v, rec_old->fields[i].type);
								}
							}
						}
						else
						{
							rec_old->fields[i].data.v.insert = insert_element;
							rec_old->fields[i].data.v.destroy = free_dynamic_array;
							for (int a = 0; a < rec->fields[i].data.v.size; a++)
							{
								rec_old->fields[i].data.v.insert((void *)rec->fields[i].data.v.elements.b[a],
																	&rec_old->fields[i].data.v, rec_old->fields[i].type);
							}
						}
					}
					break;
				case TYPE_ARRAY_STRING:
					if (rec->fields[i].data.v.elements.s)
					{
						if (rec->fields[i].data.v.size == 1 && strcmp(rec->fields[i].data.v.elements.s[0], "null") == 0)
							break;

						if (rec_old->fields[i].data.v.elements.s)
						{
							if (rec_old->fields[i].data.v.size == rec->fields[i].data.v.size)
							{
								for (int a = 0; a < rec->fields[i].data.v.size; a++)
								{
									if (strcmp(rec_old->fields[i].data.v.elements.s[a], rec->fields[i].data.v.elements.s[a]) == 0)
										continue;

									free(rec_old->fields[i].data.v.elements.s[a]);
									rec_old->fields[i].data.v.elements.s[a] = NULL;
									rec_old->fields[i].data.v.elements.s[a] = strdup(rec->fields[i].data.v.elements.s[a]);
									if (!rec_old->fields[i].data.v.elements.s[a])
									{
										fprintf(stderr, "strdup() failed,%s:%d.\n", F, L - 2);
										free_strs(elements, 1, names);
										free(types_i);
										return 0;
									}
								}
							}
							else
							{
								rec_old->fields[i].data.v.destroy(&rec_old->fields[i].data.v, rec_old->fields[i].type);
								for (int a = 0; a < rec->fields[i].data.v.size; a++)
								{
									rec_old->fields[i].data.v.insert((void *)rec->fields[i].data.v.elements.s[a],
																		&rec_old->fields[i].data.v, rec_old->fields[i].type);
								}
							}
						}
						else
						{
							rec_old->fields[i].data.v.insert = insert_element;
							rec_old->fields[i].data.v.destroy = free_dynamic_array;
							for (int a = 0; a < rec->fields[i].data.v.size; a++)
							{
								rec_old->fields[i].data.v.insert((void *)rec->fields[i].data.v.elements.s[a],
																	&rec_old->fields[i].data.v, rec_old->fields[i].type);
							}
						}
					}
					break;
				default:
					printf("invalid type! type -> %d.\n", rec->fields[i].type);
					free_strs(elements, 1, names);
					free(types_i);
					return 0;
				}
			}
		}

		for (i = 0, j = 0; i < rec->fields_num; i++)
		{
			if (i < rec_old->fields_num)
				continue;

			names[j] = strdup(rec->fields[i].field_name);
			if (!names[j])
			{
				fprintf(stderr, "strdup() failed, %s:%d", F, L - 2);
				free_strs(elements, 1, names);
				free(types_i);
				return 0;
			}
			types_i[j] = rec->fields[i].type;
			char buffer[64];

			switch (rec->fields[i].type)
			{
			case TYPE_INT:
				if (snprintf(buffer, sizeof(buffer), "%d", rec->fields[i].data.i) < 0)
				{
					fprintf(stderr, "snprintf() failed, %s:%d", F, L - 1);
					free_strs(elements, 1, names);
					free(types_i);
					return 0;
				}
				values[j] = strdup(buffer);
				if (!values[j])
				{
					fprintf(stderr, "strdup() failed, %s:%d", F, L - 2);
					free_strs(elements, 1, names);
					free(types_i);
					return 0;
				}
				break;
			case TYPE_LONG:
				if (snprintf(buffer, sizeof(buffer), "%ld", rec->fields[i].data.l) < 0)
				{
					fprintf(stderr, "snprintf() failed, %s:%d", F, L - 1);
					free_strs(elements, 1, names);
					free(types_i);
					return 0;
				}
				values[j] = strdup(buffer);
				if (!values[j])
				{
					fprintf(stderr, "strdup() failed, %s:%d", F, L - 2);
					free_strs(elements, 1, names);
					free(types_i);
					return 0;
				}
				break;
			case TYPE_FLOAT:
				if (snprintf(buffer, sizeof(buffer), "%f", rec->fields[i].data.f))
				{
					fprintf(stderr, "snprintf() failed, %s:%d", F, L - 1);
					free_strs(elements, 1, names);
					free(types_i);
					return 0;
				}

				values[j] = strdup(buffer);
				if (!values[j])
				{
					fprintf(stderr, "strdup() failed, %s:%d", F, L - 2);
					free_strs(elements, 1, names);
					free(types_i);
					return 0;
				}
				break;
			case TYPE_STRING:
				values[j] = strdup(rec->fields[i].data.s);
				if (!values[j])
				{
					fprintf(stderr, "strdup() failed, %s:%d", F, L - 2);
					free_strs(elements, 1, names);
					free(types_i);
					return 0;
				}
				break;
			case TYPE_BYTE:
				if (snprintf(buffer, sizeof(buffer), "%u", rec->fields[i].data.b) < 0)
				{
					fprintf(stderr, "snprintf() failed, %s:%d", F, L - 1);
					free_strs(elements, 1, names);
					free(types_i);
					return 0;
				}
				values[j] = strdup(buffer);
				if (!values[j])
				{
					fprintf(stderr, "strdup() failed, %s:%d", F, L - 2);
					free_strs(elements, 1, names);
					free(types_i);
					return 0;
				}
				break;
			case TYPE_DOUBLE:
				if (snprintf(buffer, sizeof(buffer), "%lf", rec->fields[i].data.d) < 0)
				{
					fprintf(stderr, "snprintf() failed, %s:%d", F, L - 1);
					free_strs(elements, 1, names);
					free(types_i);
					return 0;
				}
				values[j] = strdup(buffer);
				if (!values[j])
				{
					fprintf(stderr, "strdup() failed, %s:%d", F, L - 2);
					free_strs(elements, 1, names);
					free(types_i);
					return 0;
				}
				break;
			case TYPE_ARRAY_INT:
			{
				strncpy(new_rec->fields[j].field_name,names[j],strlen(names[j]));

				new_rec->fields[j].type = rec->fields[i].type;
				new_rec->fields[j].data.v.insert = insert_element;
				new_rec->fields[j].data.v.destroy = free_dynamic_array;

				for (int a = 0; a < rec->fields[i].data.v.size; a++) {
					new_rec->fields[j].data.v.insert((void *)rec->fields[i].data.v.elements.i[a],
									&new_rec->fields[j].data.v, rec->fields[i].type);

				}
				break;
			}
			case TYPE_ARRAY_LONG:
			{
				strncpy(new_rec->fields[j].field_name,names[j],strlen(names[j]));
				new_rec->fields[j].type = rec->fields[i].type;
				new_rec->fields[j].data.v.insert = insert_element;
				new_rec->fields[j].data.v.destroy = free_dynamic_array;

				for (int a = 0; a < rec->fields[i].data.v.size; a++) {
					new_rec->fields[j].data.v.insert((void *)rec->fields[i].data.v.elements.l[a],
								&new_rec->fields[j].data.v, rec->fields[i].type);
				}
				break;
			}
			case TYPE_ARRAY_FLOAT:
			{
				strncpy(new_rec->fields[j].field_name,names[j],strlen(names[j]));

				new_rec->fields[j].type = rec->fields[i].type;
				new_rec->fields[j].data.v.insert = insert_element;
				new_rec->fields[j].data.v.destroy = free_dynamic_array;

				for (int a = 0; a < rec->fields[i].data.v.size; a++) {
					new_rec->fields[j].data.v.insert((void *)rec->fields[i].data.v.elements.f[a],
								&new_rec->fields[j].data.v, rec->fields[i].type);
				}
				break;
			}
			case TYPE_ARRAY_BYTE:
			{
				strncpy(new_rec->fields[j].field_name,names[j],strlen(names[j]));

				new_rec->fields[j].type = rec->fields[i].type;
				new_rec->fields[j].data.v.insert = insert_element;
				new_rec->fields[j].data.v.destroy = free_dynamic_array;

				for (int a = 0; a < rec->fields[i].data.v.size; a++) {
					new_rec->fields[j].data.v.insert((void *)rec->fields[i].data.v.elements.b[a],
									&new_rec->fields[j].data.v, rec->fields[i].type);
				}
				break;
			}
			case TYPE_ARRAY_STRING:
			{
				strncpy(new_rec->fields[j].field_name,names[j],strlen(names[j]));

				new_rec->fields[j].type = rec->fields[i].type;
				new_rec->fields[j].data.v.insert = insert_element;
				new_rec->fields[j].data.v.destroy = free_dynamic_array;

				for (int a = 0; a < rec->fields[i].data.v.size; a++) {
					new_rec->fields[j].data.v.insert((void *)rec->fields[i].data.v.elements.s[a],
							&new_rec->fields[j].data.v, rec->fields[i].type);
				}
				break;
			}
			case TYPE_ARRAY_DOUBLE:
			{
				strncpy(new_rec->fields[j].field_name,names[j],strlen(names[j]));

				new_rec->fields[j].type = rec->fields[i].type;
				new_rec->fields[j].data.v.insert = insert_element;
				new_rec->fields[j].data.v.destroy = free_dynamic_array;

				for (int a = 0; a < rec->fields[i].data.v.size; a++) {
					new_rec->fields[j].data.v.insert((void *)rec->fields[i].data.v.elements.d[a],
								&new_rec->fields[j].data.v, rec->fields[i].type);
				}
				break;
			}
			default:
				printf("invalid type! type -> %d.\n", rec->fields[i].type);
				free_strs(elements, 1, names);
				free(types_i);
				return 0;
			}
			j++;
		}

		for (i = 0; i < elements; i++) {
			if (types_i[i] == TYPE_ARRAY_INT ||
				types_i[i] == TYPE_ARRAY_LONG ||
				types_i[i] == TYPE_ARRAY_FLOAT ||
				types_i[i] == TYPE_ARRAY_BYTE ||
				types_i[i] == TYPE_ARRAY_STRING ||
				types_i[i] == TYPE_ARRAY_DOUBLE)
				continue;

			if (!set_field(new_rec, i, names[i], types_i[i], values[i],1)) {
				printf("set_field failed, %s:%d.\n", F, L - 2);
				free_strs(elements, 2, names, values);
				free(types_i);
				return 0;
			}
		}

		free_strs(elements, 2, names, values);
		free(types_i);
		return UPDATE_OLDN;
	}
	return 0;
}

void find_fields_to_update(struct Record_f *recs_old, char *positions, struct Record_f *rec, int index)
{
	int i = 0, j = 0, x = 0;
	for (i = 0; i < index; i++) {
		if (positions[i] != 'y')
			positions[i] = 'n';

		for (j = 0; j < recs_old[i].fields_num; j++) {
			for (x = 0; x < rec->fields_num; x++) {
				if(rec->field_set[x] == 0) continue;

				if (rec->field_set[x] == 1 && recs_old[i].field_set[j] == 1) {
					switch (rec->fields[x].type) {
					case TYPE_INT:
						if (rec->fields[x].data.i != 0){
							if (recs_old[i].fields[j].data.i != rec->fields[x].data.i) {
								recs_old[i].fields[j].data.i = rec->fields[x].data.i;
								positions[i] = 'y';
							}
						}
						break;
					case TYPE_LONG:
						if (rec->fields[x].data.l != 0){
							if (recs_old[i].fields[j].data.l != rec->fields[x].data.l) {
								recs_old[i].fields[j].data.l = rec->fields[x].data.l;
								positions[i] = 'y';
							}
						}
						break;
					case TYPE_FLOAT:
						if (rec->fields[x].data.f != 0.0)
						{
							if (recs_old[i].fields[j].data.f != rec->fields[x].data.f)
							{
								recs_old[i].fields[j].data.f = rec->fields[x].data.f;
								positions[i] = 'y';
							}
						}
						break;
					case TYPE_STRING:
						if (strcmp(rec->fields[x].data.s, "null") != 0)
						{
							if (strcmp(recs_old[i].fields[j].data.s,
									   rec->fields[x].data.s) != 0)
							{
								if (recs_old[i].fields[j].data.s != NULL) {
									free(recs_old[i].fields[j].data.s);
									recs_old[i].fields[j].data.s = NULL;
								}

								recs_old[i].fields[j].data.s = strdup(rec->fields[x].data.s);
								positions[i] = 'y';
							}
						}
						break;
					case TYPE_BYTE:
						if (rec->fields[x].data.b != 0.0)
						{
							if (recs_old[i].fields[j].data.b != rec->fields[x].data.b)
							{
								recs_old[i].fields[j].data.b = rec->fields[x].data.b;
								positions[i] = 'y';
							}
						}
						break;
					case TYPE_DOUBLE:
						if (rec->fields[x].data.d != 0.0)
						{
							if (recs_old[i].fields[j].data.d != rec->fields[x].data.d)
							{
								recs_old[i].fields[j].data.d = rec->fields[x].data.d;
								positions[i] = 'y';
							}
						}
						break;
					case TYPE_ARRAY_INT:
						if (rec->fields[x].data.v.elements.i)
						{
							if (rec->fields[x].data.v.size == 1 && *rec->fields[x].data.v.elements.i[0] == 0)
								break;

							/*check the values*/
							if (rec->fields[x].data.v.size == recs_old[i].fields[j].data.v.size)
							{
								for (int a = 0; a < rec->fields[x].data.v.size; a++)
								{
									if (*recs_old[i].fields[j].data.v.elements.i[a] == *rec->fields[x].data.v.elements.i[a])
										continue;
									*recs_old[i].fields[j].data.v.elements.i[a] = *rec->fields[x].data.v.elements.i[a];
								}
								positions[i] = 'y';
								break;
							}
							else
							{
								/*
								 * if the sizes of the two arrays are different,
								 * simply we destroy the old one,
								 * and in the old record we create a new one we the data
								 * of the new record
								 * */
								recs_old[i].fields[j].data.v.
									destroy(&recs_old[i].fields[j].data.v, 
											rec->fields[x].type);

								for (int a = 0; a < rec->fields[x].data.v.size; a++) {
									recs_old[i].fields[j].data.v.
									insert((void *)rec->fields[x].data.v.elements.i[a],
									&recs_old[i].fields[j].data.v, rec->fields[x].type);
								}
								positions[i] = 'y';
								break;
							}
						}

						break;
					case TYPE_ARRAY_LONG:
						if (rec->fields[x].data.v.elements.l)
						{
							if (rec->fields[x].data.v.size == 1 && *rec->fields[x].data.v.elements.l[0] == 0)
								break;
							/*check the values*/
							if (rec->fields[x].data.v.size == recs_old[i].fields[j].data.v.size)
							{
								for (int a = 0; a < rec->fields[x].data.v.size; a++)
								{
									if (*recs_old[i].fields[j].data.v.elements.l[a] == *rec->fields[x].data.v.elements.l[a])
										continue;
									*recs_old[i].fields[j].data.v.elements.l[a] = *rec->fields[x].data.v.elements.l[a];
								}
								positions[i] = 'y';
								break;
							}
							else
							{
								/*
								 * if the sizes of the two arrays are different,
								 * simply we destroy the old one,
								 * and in the old record we create a new one we the data
								 * of the new record
								 * */
								recs_old[i].fields[j].data.v.
									destroy(&recs_old[i].fields[j].data.v, 
											rec->fields[x].type);

								for (int a = 0; a < rec->fields[x].data.v.size; a++) {
									recs_old[i].fields[j].data.v.
									insert((void *)rec->fields[x].data.v.elements.l[a],
									 &recs_old[i].fields[x].data.v, rec->fields[x].type);
								}
								positions[i] = 'y';
								break;
							}
						}

						break;
					case TYPE_ARRAY_FLOAT:
						if (rec->fields[x].data.v.elements.f)
						{
							if (rec->fields[x].data.v.size == 1 && *rec->fields[x].data.v.elements.f[0] == 0.0)
								break;
							/*check the values*/
							if (rec->fields[x].data.v.size == recs_old[i].fields[j].data.v.size)
							{
								for (int a = 0; a < rec->fields[x].data.v.size; a++)
								{
									if (*recs_old[i].fields[j].data.v.elements.f[a] == *rec->fields[x].data.v.elements.f[a])
										continue;
									*recs_old[i].fields[j].data.v.elements.f[a] = *rec->fields[x].data.v.elements.f[a];
								}
								positions[i] = 'y';
								break;
							}
							else
							{
								/*
								 * if the sizes of the two arrays are different,
								 * simply we destroy the old one,
								 * and in the old record we create a new one we the data
								 * of the new record
								 * */
								recs_old[i].fields[j].data.v.destroy(&recs_old[i].fields[j].data.v, rec->fields[x].type);
								for (int a = 0; a < rec->fields[x].data.v.size; a++)
								{
									recs_old[i].fields[j].data.v.insert((void *)rec->fields[x].data.v.elements.f[a],
																		 &recs_old[i].fields[j].data.v, rec->fields[x].type);
								}
								positions[i] = 'y';
								break;
							}
						}

						break;
					case TYPE_ARRAY_DOUBLE:
						if (rec->fields[x].data.v.elements.d)
						{
							if (rec->fields[x].data.v.size == 1 && *rec->fields[x].data.v.elements.d[0] == 0.0)
								break;
							/*check the values*/
							if (rec->fields[x].data.v.size == recs_old[i].fields[j].data.v.size)
							{
								for (int a = 0; a < rec->fields[x].data.v.size; a++)
								{
									if (*recs_old[i].fields[j].data.v.elements.d[a] == *rec->fields[x].data.v.elements.d[a])
										continue;
									*recs_old[i].fields[j].data.v.elements.d[a] = *rec->fields[x].data.v.elements.d[a];
								}
								positions[i] = 'y';
								break;
							}
							else
							{
								/*
								 * if the sizes of the two arrays are different,
								 * simply we destroy the old one,
								 * and in the old record we create a new one we the data
								 * of the new record
								 * */
								recs_old[i].fields[j].data.v.destroy(&recs_old[i].fields[j].data.v, rec->fields[x].type);
								for (int a = 0; a < rec->fields[x].data.v.size; a++)
								{
									recs_old[i].fields[j].data.v.insert((void *)rec->fields[x].data.v.elements.d[a],
																		 &recs_old[i].fields[j].data.v, rec->fields[x].type);
								}
								positions[i] = 'y';
								break;
							}
						}

						break;
					case TYPE_ARRAY_BYTE:
						if (rec->fields[x].data.v.elements.b)
						{
							if (rec->fields[x].data.v.size == 1 && *rec->fields[x].data.v.elements.b[0] == 0)
								break;
							/*check the values*/
							if (rec->fields[x].data.v.size == recs_old[i].fields[j].data.v.size)
							{
								for (int a = 0; a < rec->fields[x].data.v.size; a++)
								{
									if (*recs_old[i].fields[j].data.v.elements.b[a] == *rec->fields[x].data.v.elements.b[a])
										continue;
									*recs_old[i].fields[j].data.v.elements.b[a] = *rec->fields[x].data.v.elements.b[a];
								}
								positions[i] = 'y';
								break;
							}
							else
							{
								/*
								 * if the sizes of the two arrays are different,
								 * simply we destroy the old one,
								 * and in the old record we create a new one we the data
								 * of the new record
								 * */
								recs_old[i].fields[j].data.v.destroy(&recs_old[i].fields[j].data.v, rec->fields[x].type);
								for (int a = 0; a < rec->fields[x].data.v.size; a++)
								{
									recs_old[i].fields[j].data.v.insert((void *)rec->fields[x].data.v.elements.b[a],
																		 &recs_old[i].fields[j].data.v, rec->fields[x].type);
								}
								positions[i] = 'y';
								break;
							}
						}

						break;
					case TYPE_ARRAY_STRING:
						if (rec->fields[x].data.v.elements.s)
						{
							if (rec->fields[x].data.v.size == 1 && strcmp(rec->fields[x].data.v.elements.s[0], "null") == 0)
								break;
							/*check the values*/
							if (rec->fields[x].data.v.size == recs_old[i].fields[j].data.v.size)
							{
								for (int a = 0; a < rec->fields[x].data.v.size; a++)
								{
									if (strcmp(recs_old[i].fields[j].data.v.elements.s[a], rec->fields[x].data.v.elements.s[a]) == 0)
									{
										free(recs_old[i].fields[j].data.v.elements.s[a]);
										recs_old[i].fields[j].data.v.elements.s[a] = NULL;
										recs_old[i].fields[j].data.v.elements.s[a] = strdup(rec->fields[x].data.v.elements.s[a]);
										if (!recs_old[i].fields[j].data.v.elements.s[a])
										{
											fprintf(stderr, "strdup() failed %s:%d.\n", F, L - 2);
											positions[0] = '0';
											return;
										}
									}
								}
								positions[i] = 'y';
								break;
							}
							else
							{
								/*
								 * if the sizes of the two arrays are different,
								 * simply we destroy the old one,
								 * and in the old record we create a new one we the data
								 * of the new record
								 * */
								recs_old[i].fields[j].data.v.destroy(&recs_old[i].fields[j].data.v, rec->fields[x].type);
								for (int a = 0; a < rec->fields[x].data.v.size; a++)
								{
									recs_old[i].fields[j].data.v.insert((void *)rec->fields[x].data.v.elements.s[a],
																		 &recs_old[i].fields[j].data.v, rec->fields[x].type);
								}
								positions[i] = 'y';
								break;
							}
						}

						break;
					default:
						printf("no matching type\n");
						positions[0] = '0';
						return;
					}
				}
			}
		}
	}
}

unsigned char create_new_fields_from_schema(struct Record_f *recs_old, 
							struct Record_f *rec,
							struct Schema *sch, 
							int index,
							struct Record_f *new_rec, 
							char *file_path)
{
	int i = 0;
	unsigned char sum = 0;
	for (i = 0; i < index; i++)
		sum += recs_old[i].fields_num;

	create_record(file_path, *sch, new_rec);

	i = 0;
	register unsigned char j = 0, x = 0, n_i = 0, found = 0;
	for (j = 0; j < sch->fields_num; j++) {
		if (i == index) {
			switch (sch->types[j]) {
			case TYPE_INT:
			case TYPE_BYTE:
			case TYPE_LONG:
				if (!set_field(new_rec, n_i, sch->fields_name[j], sch->types[j], "0",0)){
					printf("set_field failed, %s:%d.\n", F, L - 2);
					return -1;
				}
				break;
			case TYPE_FLOAT:
			case TYPE_DOUBLE:
				if (!set_field(new_rec, n_i, sch->fields_name[j], sch->types[j], "0.0",0)) {
					printf("set_field failed, %s:%d.\n", F, L - 2);
					return -1;
				}
				break;
			case TYPE_STRING:
				if (!set_field(new_rec, n_i, sch->fields_name[j], sch->types[j], "null",0)) {
					printf("set_field failed, %s:%d.\n", F, L - 2);
					return -1;
				}
				break;
			case TYPE_ARRAY_INT:
			case TYPE_ARRAY_LONG:
			case TYPE_ARRAY_BYTE:
				if (!set_field(new_rec, n_i, sch->fields_name[j], sch->types[j], "0",0)) {
					printf("set_field failed, %s:%d.\n", F, L - 2);
					return -1;
				}
				break;
			case TYPE_ARRAY_FLOAT:
			case TYPE_ARRAY_DOUBLE:
				if (!set_field(new_rec, n_i, sch->fields_name[j], sch->types[j], "0.0",0)) {
					printf("set_field failed, %s:%d.\n", F, L - 2);
					return -1;
				}
				break;
			case TYPE_ARRAY_STRING:
				if (!set_field(new_rec, n_i, sch->fields_name[j], sch->types[j], "null,null",0)) {
					printf("set_field failed, %s:%d.\n", F, L - 2);
					return -1;
				}
				break;
			default:
				printf("invalid type %d.", sch->types[j]);
				return -1;
			}
			continue;
		}

		for (x = 0; x < recs_old[i].fields_num; x++) {
			if (strcmp(recs_old[i].fields[x].field_name, sch->fields_name[j]) == 0){
				++found;
				break;
			}
		}

		if (found == recs_old[i].fields_num) {
			found = 0;
			i++;
		}
	}

	/* setting the proper value for the new part of the schema */

	for (j = 0; j < rec->fields_num; j++) {
		for (x = 0; x < (*new_rec).fields_num; x++) {
			if (strcmp(rec->fields[j].field_name, (*new_rec).fields[x].field_name) == 0) {
				switch (rec->fields[j].type){
				case TYPE_INT:
					new_rec->fields[x].data.i = rec->fields[j].data.i;
					new_rec->field_set[x] = 1;
					break;
				case TYPE_BYTE:
					new_rec->fields[x].data.b = rec->fields[j].data.b;
					new_rec->field_set[x] = 1;
					break;
				case TYPE_LONG:
					new_rec->fields[x].data.l = rec->fields[j].data.l;
					new_rec->field_set[x] = 1;
					break;
				case TYPE_FLOAT:
					new_rec->fields[x].data.f = rec->fields[j].data.f;
					new_rec->field_set[x] = 1;
					break;
				case TYPE_DOUBLE:
					new_rec->fields[x].data.d = rec->fields[j].data.d;
					new_rec->field_set[x] = 1;
					break;
				case TYPE_STRING:
					free(new_rec->fields[x].data.s);
					new_rec->fields[x].data.s = NULL;
					new_rec->fields[x].data.s = strdup(rec->fields[j].data.s);
					if(!new_rec->fields[x].data.s){
						fprintf(stderr,"strdup() failed, %s:%d.\n",F,L-2);
						return -1;
					}
					new_rec->field_set[x] = 1;
					break;
				case TYPE_ARRAY_INT:
					new_rec->fields[x].data.v.destroy(&rec->fields[j].data.v, rec->fields[j].type);
					for (int a = 0; a < rec->fields[j].data.v.size; a++){
						new_rec->fields[x].data.v.insert((void *)rec->fields[j].data.v.elements.i[a],
								&rec->fields[j].data.v, rec->fields[j].type);
					}
					new_rec->field_set[x] = 1;
					break;
				case TYPE_ARRAY_LONG:
					new_rec->fields[x].data.v.destroy(&rec->fields[j].data.v, rec->fields[j].type);
					for (int a = 0; a < rec->fields[j].data.v.size; a++) {
						new_rec->fields[x].data.v.insert((void *)rec->fields[j].data.v.elements.l[a],
								&rec->fields[j].data.v, rec->fields[j].type);
					}
					new_rec->field_set[x] = 1;
					break;
				case TYPE_ARRAY_BYTE:
					new_rec->fields[x].data.v.destroy(&rec->fields[j].data.v, rec->fields[j].type);
					for (int a = 0; a < rec->fields[j].data.v.size; a++){
						new_rec->fields[x].data.v.insert((void *)rec->fields[j].data.v.elements.b[a],
								&rec->fields[j].data.v, rec->fields[j].type);
					}
					new_rec->field_set[x] = 1;
					break;
				case TYPE_ARRAY_FLOAT:
					new_rec->fields[x].data.v.destroy(&rec->fields[j].data.v, rec->fields[j].type);
					for (int a = 0; a < rec->fields[j].data.v.size; a++){
						new_rec->fields[x].data.v.insert((void *)rec->fields[j].data.v.elements.f[a],
								&rec->fields[j].data.v, rec->fields[j].type);
					}
					new_rec->field_set[x] = 1;
					break;
				case TYPE_ARRAY_DOUBLE:
					new_rec->fields[x].data.v.destroy(&rec->fields[j].data.v, rec->fields[j].type);
					for (int a = 0; a < rec->fields[j].data.v.size; a++) {
						new_rec->fields[x].data.v.insert((void *)rec->fields[j].data.v.elements.d[a],
								&rec->fields[j].data.v, rec->fields[j].type);
					}
					new_rec->field_set[x] = 1;
					break;
				case TYPE_ARRAY_STRING:
					new_rec->fields[x].data.v.destroy(&rec->fields[j].data.v, rec->fields[j].type);
					for (int a = 0; a < rec->fields[j].data.v.size; a++) {
						new_rec->fields[x].data.v.insert((void *)rec->fields[j].data.v.elements.s[a],
								&rec->fields[j].data.v, rec->fields[j].type);
					}
					new_rec->field_set[x] = 1;
					break;
				default:
					printf("data type not suprted: %d.", rec->fields[j].type);
					return -1;
				}
			}
		}
	}

	return 0;
}

void print_schema(struct Schema sch)
{
	if (sch.fields_name && sch.types)
	{
		printf("definition:\n");
		int i = 0;

		char c = ' ';
		printf("Field Name%-*cType\n", 11, c);
		printf("__________________________\n");
		for (i = 0; i < sch.fields_num; i++)
		{
			printf("%s%-*c", sch.fields_name[i], (int)(15 - strlen(sch.fields_name[i])), c);
			switch (sch.types[i])
			{
			case TYPE_INT:
				printf("int.\n");
				break;
			case TYPE_FLOAT:
				printf("float.\n");
				break;
			case TYPE_LONG:
				printf("long.\n");
				break;
			case TYPE_BYTE:
				printf("byte.\n");
				break;
			case TYPE_DOUBLE:
				printf("double.\n");
				break;
			case TYPE_STRING:
				printf("string.\n");
				break;
			case TYPE_ARRAY_INT:
				printf("int[].\n");
				break;
			case TYPE_ARRAY_LONG:
				printf("long[].\n");
				break;
			case TYPE_ARRAY_FLOAT:
				printf("float[].\n");
				break;
			case TYPE_ARRAY_STRING:
				printf("string[].\n");
				break;
			case TYPE_ARRAY_BYTE:
				printf("byte[].\n");
				break;
			case TYPE_ARRAY_DOUBLE:
				printf("double[].\n");
				break;
			default:
				printf("\n");
				break;
			}
		}
	}

	printf("\n");
}

void print_header(struct Header_d hd)
{
	printf("Header: \n");
	printf("id: %x,\nversion: %d", hd.id_n, hd.version);
	printf("\n");
	print_schema(hd.sch_d);
}

size_t compute_size_header(void *header)
{
	size_t sum = 0;

	struct Header_d *hd = (struct Header_d *)header;

	sum += sizeof(hd->id_n) + sizeof(hd->version) + sizeof(hd->sch_d.fields_num) + sizeof(hd->sch_d);
	int i = 0;

	for (i = 0; i < hd->sch_d.fields_num; i++)
	{
		if (hd->sch_d.fields_name[i])
		{
			sum += strlen(hd->sch_d.fields_name[i]);
		}

		sum += sizeof(hd->sch_d.types[i]);
	}

	sum += hd->sch_d.fields_num; // acounting for n '\0'
	return sum;
}

unsigned char create_data_to_add(struct Schema *sch, char data_to_add[][500])
{
	for (int i = 0; i < sch->fields_num; i++)
	{
		size_t l = strlen(sch->fields_name[i]);

		strncpy(data_to_add[i], sch->fields_name[i], l);

		switch (sch->types[i])
		{
		case TYPE_INT:
		{
			char *t_i = (sch->fields_num - i) > 1 ? ":t_i:!:" : ":t_i:";
			size_t len = strlen(t_i);
			size_t lc = strlen(data_to_add[i]);
			strncpy(&data_to_add[i][lc], t_i, len + 1);
			replace(' ', '_', data_to_add[i]);
			break;
		}
		case TYPE_LONG:
		{
			char *t_l = (sch->fields_num - i) > 1 ? ":t_l:!:" : ":t_l:";
			size_t len = strlen(t_l);
			size_t lc = strlen(data_to_add[i]);
			strncpy(&data_to_add[i][lc], t_l, len + 1);
			replace(' ', '_', data_to_add[i]);
			break;
		}
		case TYPE_BYTE:
		{
			char *t_b = (sch->fields_num - i) > 1 ? ":t_b:!:" : ":t_b:";
			size_t len = strlen(t_b);
			size_t lc = strlen(data_to_add[i]);
			strncpy(&data_to_add[i][lc], t_b, len + 1);
			replace(' ', '_', data_to_add[i]);
			break;
		}
		case TYPE_STRING:
		{
			char *t_s = (sch->fields_num - i) > 1 ? ":t_s:!:" : ":t_s:";
			size_t len = strlen(t_s);
			size_t lc = strlen(data_to_add[i]);
			strncpy(&data_to_add[i][lc], t_s, len + 1);
			replace(' ', '_', data_to_add[i]);

			break;
		}
		case TYPE_DOUBLE:
		{
			char *t_d = (sch->fields_num - i) > 1 ? ":t_d:!:" : ":t_d:";
			size_t len = strlen(t_d);
			size_t lc = strlen(data_to_add[i]);
			strncpy(&data_to_add[i][lc], t_d, len + 1);
			replace(' ', '_', data_to_add[i]);
			break;
		}
		case TYPE_FLOAT:
		{
			char *t_f = (sch->fields_num - i) > 1 ? ":t_f:!:" : ":t_f:";
			size_t len = strlen(t_f);
			size_t lc = strlen(data_to_add[i]);
			strncpy(&data_to_add[i][lc], t_f, len + 1);
			replace(' ', '_', data_to_add[i]);
			break;
		}
		default:
			printf("unkown type!\n");
			return 0;
		}
	}

	return 1;
}
