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

Record_f *parse_d_flag_input(char *file_path, int fields_num, char *buffer, char *buf_t, char *buf_v, Schema *sch, int check_sch)
{

	Record_f *rec = create_record(file_path, fields_num);

	if (!rec)
	{
		printf("unable to create the record");
		return NULL;
	}

	char **names = get_fileds_name(buffer, fields_num, 3);

	if (!names)
	{
		printf("Error in getting the fields name.\n");
		clean_up(rec, fields_num);
		return NULL;
	}

	if (sch && check_sch == 0)
	{ /* true when a new file is created */
		char **sch_names = malloc(fields_num * sizeof(char *));
		if (!sch_names)
		{
			printf("no memory for Schema fileds name.");
			free(rec), free(names);
			return NULL;
		}
		sch->fields_num = (unsigned short)fields_num;
		sch->fields_name = sch_names;

		register unsigned char j = 0;
		for (j = 0; j < fields_num; j++)
		{
			sch->fields_name[j] = strdup(names[j]);

			if (!sch->fields_name[j])
			{
				printf("strdup failed, schema creation field.\n");
				free(names), free(rec);
				return NULL;
			}
		}
	}

	ValueType *types_i = get_value_types(buf_t, fields_num, 3);

	if (!types_i)
	{
		printf("Error in getting the fields types");
		free(names);
		free(rec);
		return NULL;
	}

	if (sch && check_sch == 0)
	{ /* true when a new file is created or when the schema input is partial*/
		ValueType *sch_types = calloc(fields_num, sizeof(ValueType));

		if (!sch_types)
		{
			printf("No memory for schema types.\n");
			free(names), free(rec), free(types_i);
			return NULL;
		}

		sch->types = sch_types;
		register unsigned char i = 0;
		for (i = 0; i < fields_num; i++)
		{
			sch->types[i] = types_i[i];
		}
	}

	char **values = get_values(buf_v, fields_num);

	if (!values)
	{
		printf("Error in getting the fields value");
		free(names), free(types_i);
		free(rec);
		return NULL;
	}

	int reorder_rtl = -1;
	if (check_sch == SCHEMA_EQ)
		reorder_rtl = sort_input_like_header_schema(check_sch, fields_num, sch, names, values, types_i);

	if (!reorder_rtl)
	{
		printf("failed to reorder like header schema.(parse.c l 91)\n");
		free(names), free(types_i);
		free(rec), free(values);
		return NULL;
	}

	if (check_sch == SCHEMA_NW)
	{
		reorder_rtl = sort_input_like_header_schema(check_sch, fields_num, sch, names, values, types_i);

		if (!reorder_rtl)
		{
			printf("failed to reorder like header schema.(parse.c l 101)\n");
			free(names), free(types_i);
			free(rec), free(values);
			return NULL;
		}

		char **temp_name = realloc(sch->fields_name, fields_num * sizeof(char *));

		if (!temp_name)
		{ /*do not free sch->fields_name */
			printf("can`t perform realloc. (parse.c l 138).\n");
			free(rec), free(values);
			free(names), free(types_i);
			return NULL;
		}

		ValueType *types_n = realloc(sch->types, fields_num * sizeof(ValueType));

		if (!types_n)
		{ /*do not free sch->fields_name */
			printf("can't perform realloc. (parse.c l 146)");
			free(names), free(types_i);
			free(rec), free(values);
			return NULL;
		}

		int old_fn = sch->fields_num;
		sch->fields_num = fields_num;
		sch->fields_name = temp_name;
		sch->types = types_n;
		register unsigned char i = 0;

		for (i = old_fn; i < fields_num; i++)
		{
			sch->fields_name[i] = NULL;
			sch->types[i] = 0;
		}

		for (i = old_fn; i < fields_num; i++)
		{

			sch->fields_name[i] = strdup(names[i]);
			sch->types[i] = types_i[i];
		}
	}

	if (check_sch == SCHEMA_CT)
	{
		reorder_rtl = sort_input_like_header_schema(check_sch, fields_num, sch, names, values, types_i);

		if (!reorder_rtl)
		{
			printf("failed to reorder like header schema.(parse.c l 148)\n");
			free(names), free(types_i);
			free(rec), free(values);
			return NULL;
		}
	}
	register unsigned char i = 0;
	for (i = 0; i < fields_num; i++)
	{
		set_field(rec, i, names[i], types_i[i], values[i]);
	}

	free(types_i), free(names), free(values);

	return rec;
}

void clean_schema(Schema *sch)
{
	if (!sch)
		return;

	register unsigned char i = 0;
	for (i = 0; i < sch->fields_num; i++)
	{
		if (sch->fields_name != NULL)
		{
			if (sch->fields_name[i])
			{
				free(sch->fields_name[i]);
			}
		}
	}

	if (sch->fields_name != NULL)
		free(sch->fields_name);

	if (sch->types != NULL)
		free(sch->types);
}

int create_header(Header_d *hd)
{

	if (hd->sch_d.fields_name == NULL ||
		hd->sch_d.types == NULL)
	{

		printf("\nschema is NULL.\ncould not create header.\n");
		return 0;
	}

	hd->id_n = HEADER_ID_SYS;
	hd->version = VS;

	return 1;
}

int write_empty_header(int fd, Header_d *hd)
{

	uint32_t id = htonl(hd->id_n); /*converting the endianess*/
	if (write(fd, &id, sizeof(id)) == -1)
	{
		perror("write header id.\n");
		return 0;
	}

	uint16_t vs = htons(hd->version);
	if (write(fd, &vs, sizeof(vs)) == -1)
	{
		perror("write header version.\n");
		return 0;
	}

	/* important we need to write 0 field_number when the user creates a file with no data*/
	uint16_t fn = htons((uint16_t)hd->sch_d.fields_num);
	if (write(fd, &fn, sizeof(fn)) == -1)
	{
		perror("writing fields number header.");
		return 0;
	}

	return 1;
}

int write_header(int fd, Header_d *hd)
{
	if (hd->sch_d.fields_name == NULL ||
		hd->sch_d.types == NULL)
	{
		printf("schema is NULL.\ncould not create header.\n");
		return 0;
	}

	uint32_t id = htonl(hd->id_n); /*converting the endianess*/
	if (write(fd, &id, sizeof(id)) == -1)
	{
		perror("write header id.\n");
		return 0;
	}

	uint16_t vs = htons(hd->version);
	if (write(fd, &vs, sizeof(vs)) == -1)
	{
		perror("write header version.\n");
		return 0;
	}

	uint16_t fn = htons((uint16_t)hd->sch_d.fields_num);
	//	printf("Writing fields_num (network order): %u\n", fn);

	if (write(fd, &fn, sizeof(fn)) == -1)
	{
		perror("writing fields number header.");
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
				return 0;
			}

			if (write(fd, hd->sch_d.fields_name[i], l) == -1)
			{
				perror("write name of field in header.\n");
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
			return 0;
		}
	}

	return 1; // succseed
}

int read_header(int fd, Header_d *hd)
{
	unsigned int id = 0;
	if (read(fd, &id, sizeof(id)) == -1)
	{
		perror("reading id from header.\n");
		return 0;
	}

	id = ntohl(id); /*changing the bytes to host endianess*/
	// printf("id is %x\n",id);
	if (id != HEADER_ID_SYS)
	{
		printf("this is not a db file.\n");
		return 0;
	}

	unsigned short vs = 0;
	if (read(fd, &vs, sizeof(vs)) == -1)
	{
		perror("reading version from header.\n");
		return 0;
	}

	vs = ntohs(vs);
	if (vs != VS)
	{
		printf("this file was edited from a different software.\n");
		return 0;
	}

	hd->id_n = id;
	hd->version = vs;

	uint16_t field_n = 0;
	if (read(fd, &field_n, sizeof(field_n)) == -1)
	{
		perror("reading field_number header.\n");
		return 0;
	}
	hd->sch_d.fields_num = (unsigned short)ntohs(field_n);

	if (hd->sch_d.fields_num == 0)
	{
		printf("no schema in this header.Please check data integrety.\n");
		return 1;
	}

	//	printf("fields number %u.", hd->sch_d.fields_num);
	char **names = calloc(hd->sch_d.fields_num, sizeof(char *));

	if (!names)
	{
		printf("no memory for fields name, reading header.\n");
		return 0;
	}

	hd->sch_d.fields_name = names;

	register unsigned char i = 0;
	for (i = 0; i < hd->sch_d.fields_num; i++)
	{
		uint32_t l_end = 0;
		if (read(fd, &l_end, sizeof(l_end)) == -1)
		{
			perror("reading size of field name.\n");
			clean_schema(&hd->sch_d);
			return 0;
		}
		size_t l = (size_t)ntohl(l_end);

		//	printf("size of name is %ld.\n", l);
		char *field = malloc(l);
		if (!field)
		{
			printf("no memory for field name, reading from header.\n");
			clean_schema(&hd->sch_d);
			return 0;
		}

		if (read(fd, field, l) == -1)
		{
			perror("reading name filed from header.\n");
			clean_schema(&hd->sch_d);
			free(field);
			return 0;
		}

		field[l - 1] = '\0';
		hd->sch_d.fields_name[i] = strdup(field);
		free(field);

		if (!hd->sch_d.fields_name[i])
		{
			printf("strdup failed in dupolicating name field form header.");
			clean_schema(&hd->sch_d);
			return 0;
		}
	}

	ValueType *types_h = calloc(hd->sch_d.fields_num, sizeof(ValueType));
	if (!types_h)
	{
		printf("no memory for types. reading header.\n");
		clean_schema(&hd->sch_d);
		return 0;
	}

	hd->sch_d.types = types_h;
	for (i = 0; i < hd->sch_d.fields_num; i++)
	{
		uint32_t type = 0;
		if (read(fd, &type, sizeof(type)) == -1)
		{
			perror("reading types from header.");
			clean_schema(&hd->sch_d);
			return 0;
		}

		hd->sch_d.types[i] = ntohl(type);
	}

	return 1; // successed
}

unsigned char ck_input_schema_fields(char **names, ValueType *types_i, Header_d hd)
{
	int fields_eq = 0;
	int types_eq = 0;

	char **copy_sch = calloc(hd.sch_d.fields_num, sizeof(char *));

	if (!copy_sch)
	{
		printf("could not perform calloc (parse.c l 366).\n");
		free(names), free(types_i);
		return 0;
	}

	ValueType types_cp[hd.sch_d.fields_num];

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

unsigned char check_schema(int fields_n, char *buffer, char *buf_t, Header_d hd)
{
	char *names_cs = strdup(buffer);
	char *types_cs = strdup(buf_t);

	ValueType *types_i = get_value_types(types_cs, fields_n, 3);

	if (!types_i)
	{
		printf("could not get types from input(parse.c l 287).\n");
		free(names_cs), free(types_cs);
		return 0;
	}

	char **names = get_fileds_name(names_cs, fields_n, 3);

	if (!names)
	{
		printf("could not get fields name from input(parse.c l 287).\n");
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
			free(names), free(types_i), free(names_cs);
			free(types_cs);
			return SCHEMA_ERR;
		case SCHEMA_EQ:
			free(names), free(types_i), free(names_cs);
			free(types_cs);
			return SCHEMA_EQ;
		default:
			printf("check on Schema failed.\n");
			return 0;
		}
	}
	else if (hd.sch_d.fields_num < fields_n)
	{ /* case where the header needs to be updated */

		unsigned char ck_rst = ck_input_schema_fields(names, types_i, hd);

		switch (ck_rst)
		{
		case SCHEMA_ERR:
			free(names), free(types_i), free(names_cs);
			free(types_cs);
			return SCHEMA_ERR;
		case SCHEMA_EQ:
			free(names), free(types_i), free(names_cs);
			free(types_cs);
			return SCHEMA_NW;
		default:
			free(names), free(types_i), free(names_cs);
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
			free(names), free(types_i), free(names_cs);
			free(types_cs);
			return SCHEMA_ERR;
		case SCHEMA_CT:
			free(names), free(types_i), free(names_cs);
			free(types_cs);
			return SCHEMA_CT;
		default:
			free(names), free(types_i), free(names_cs);
			free(types_cs);
			return 0;
		}
	}

	// this is unreachable
	free(names), free(types_i), free(names_cs);
	free(types_cs);
	return 1;
}

int sort_input_like_header_schema(int schema_tp, int fields_num, Schema *sch, char **names, char **values, ValueType *types_i)
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
		printf("could not perform calloc,(parse.c l 469).\n");
		return 0;
	}

	char **temp_name = calloc(fields_num, sizeof(char *));

	if (!temp_name)
	{
		printf("could not perform calloc, (parse.c l 477).\n");
		free(temp_val);
		return 0;
	}

	ValueType temp_types[fields_num];

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

unsigned char ck_schema_contain_input(char **names, ValueType *types_i, Header_d hd, int fields_num)
{

	register unsigned char i = 0, j = 0;
	int found_f = 0, found_t = 0;

	for (i = 0; i < fields_num; i++)
	{
		found_t = 0;
		for (j = 0; j < hd.sch_d.fields_num; j++)
		{
			// printf("%s == %s\n",names[i],hd.sch_d.fields_name[j]);
			if (strcmp(names[i], hd.sch_d.fields_name[j]) == 0)
				found_f++;

			// printf("%d == %d\n",types_i[i], hd.sch_d.types[j]);
			if (found_t < fields_num)
			{
				if (types_i[i] == hd.sch_d.types[j])
					found_t++;
			}

			if (found_t == 0)
			{
				printf("Schema different than file definition.\n");
				return SCHEMA_ERR;
			}
		}
	}

	if (found_f == fields_num && found_t == fields_num)
	{
		return SCHEMA_CT;
	}

	printf("Schema different than file definition.\n");
	return SCHEMA_ERR;
}

int create_file_definition_with_no_value(int fields_num, char *buffer, char *buf_t, Schema *sch)
{

	char **names = get_fileds_name(buffer, fields_num, 2);

	if (!names)
	{
		printf("Error in getting the fields name");
		return 0;
	}

	if (sch)
	{

		char **sch_names = calloc(fields_num, sizeof(char *));
		if (!sch_names)
		{
			printf("no memory for Schema fileds name.");
			free(names);
			return 0;
		}
		sch->fields_name = sch_names;

		register unsigned char j = 0;
		for (j = 0; j < fields_num; j++)
		{
			// printf("%d%s\n",j,names[j]);

			if (names[j])
			{

				sch->fields_name[j] = strdup(names[j]);
				if (!sch->fields_name[j])
				{
					printf("strdup failed, schema creation field.\n");
					clean_schema(sch);
					free(names);
					return 0;
				}
			}
		}
	}

	ValueType *types_i = get_value_types(buf_t, fields_num, 2);

	if (!types_i)
	{
		printf("Error in getting the fields types");
		free(names);
		clean_schema(sch);
		return 0;
	}

	if (sch)
	{
		ValueType *sch_types = calloc(fields_num, sizeof(ValueType));

		if (!sch_types)
		{
			printf("No memory for schema types.\n");
			clean_schema(sch);
			free(names), free(types_i);
		}

		sch->types = sch_types;
		register unsigned char i = 0;
		for (i = 0; i < fields_num; i++)
		{
			sch->types[i] = types_i[i];
		}
	}

	free(names), free(types_i);
	return 1; // scheam creation succssed
}

unsigned char perform_checks_on_schema(char *buffer, char *buf_t, char *buf_v, int fields_count, int fd_data,
									   char *file_path, Record_f **rec, Header_d *hd)
{

	// check if the schema on the file is equal to the input Schema.

	if (!read_header(fd_data, hd))
	{
		printf("can`t read the header, parse.c l 770.\n");
		return 0;
	}

	// print_schema(hd.sch_d);

	if (hd->sch_d.fields_num != 0)
	{
		unsigned char check = check_schema(fields_count, buffer, buf_t, *hd);
		// printf("check schema is %d",check);
		switch (check)
		{
		case SCHEMA_EQ:
			*rec = parse_d_flag_input(file_path, fields_count, buffer,
									  buf_t, buf_v, &hd->sch_d, SCHEMA_EQ);
			return SCHEMA_EQ;
		case SCHEMA_ERR:
			return SCHEMA_ERR;
		case SCHEMA_NW:
			*rec = parse_d_flag_input(file_path, fields_count, buffer,
									  buf_t, buf_v, &hd->sch_d, SCHEMA_NW);
			return SCHEMA_NW;
		case SCHEMA_CT:
			*rec = parse_d_flag_input(file_path, fields_count, buffer,
									  buf_t, buf_v, &hd->sch_d, SCHEMA_CT);
			return SCHEMA_CT;
		default:
			printf("no processable option for the SCHEMA. parse.c l 703");
			return 0;
		}
	}
	else
	{
		*rec = parse_d_flag_input(file_path, fields_count, buffer, buf_t, buf_v, &hd->sch_d, 0);
	}

	return 1;
}

unsigned char compare_old_rec_update_rec(Record_f *rec_old, Record_f *rec, Record_f **new_rec, char *file_path)
{
	unsigned char i = 0, j = 0;

	if (rec_old->fields_num == rec->fields_num)
		return UPDATE_OLD;

	if (rec_old->fields_num < rec->fields_num)
	{
		int elements = rec->fields_num - rec_old->fields_num;
		char **names = calloc(elements, sizeof(char *));

		*new_rec = create_record(file_path, elements);
		if (!*new_rec)
		{
			printf("no memory for new record update parse.c l 822");
			return 0;
		}

		if (!names)
		{
			printf("calloc failed at parse.c l 822.\n");
			return 0;
		}

		ValueType *types_i = calloc(elements, sizeof(ValueType));

		if (!types_i)
		{
			printf("calloc failed at parse.c l 827.\n");
			free(names);
			return 0;
		}

		char **values = calloc(elements, sizeof(char *));
		if (!values)
		{
			printf("calloc failed at parse.c l 827.\n");
			free(names);
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
					rec_old->fields[i].data.i = rec->fields[i].data.i;
					break;
				case TYPE_LONG:
					rec_old->fields[i].data.l = rec->fields[i].data.l;
					break;
				case TYPE_FLOAT:
					rec_old->fields[i].data.f = rec->fields[i].data.f;
					break;
				case TYPE_STRING:
					// free memory before allocating other memory
					if (rec_old->fields[i].data.s != NULL)
					{
						free(rec_old->fields[i].data.s);
						rec_old->fields[i].data.s = NULL;
					}
					rec_old->fields[i].data.s = strdup(rec->fields[i].data.s);
					break;
				case TYPE_BYTE:
					rec_old->fields[i].data.b = rec->fields[i].data.b;
					break;
				case TYPE_DOUBLE:
					rec_old->fields[i].data.d = rec->fields[i].data.d;
					break;
				default:
					printf("invalid type! type -> %d.", rec->fields[i].type);
					return 0;
				}
			}
		}

		for (i = 0, j = 0; i < rec->fields_num; i++)
		{
			if (i < rec_old->fields_num)
				continue;

			names[j] = strdup(rec->fields[i].field_name);
			//	printf("%s\n",names[i]);
			types_i[j] = rec->fields[i].type;
			char buffer[64];

			switch (rec->fields[i].type)
			{
			case TYPE_INT:
				snprintf(buffer, sizeof(buffer), "%d", rec->fields[i].data.i);
				values[j] = strdup(buffer);
				break;
			case TYPE_LONG:
				snprintf(buffer, sizeof(buffer), "%ld", rec->fields[i].data.l);
				values[j] = strdup(buffer);
				break;
			case TYPE_FLOAT:
				snprintf(buffer, sizeof(buffer), "%f", rec->fields[i].data.f);
				values[j] = strdup(buffer);
				break;
			case TYPE_STRING:
				values[j] = strdup(rec->fields[i].data.s);
				break;
			case TYPE_BYTE:
				snprintf(buffer, sizeof(buffer), "%u", rec->fields[i].data.b);
				values[j] = strdup(buffer);
				break;
			case TYPE_DOUBLE:
				snprintf(buffer, sizeof(buffer), "%lf", rec->fields[i].data.d);
				values[j] = strdup(buffer);
				break;
			default:
				printf("invalid type! type -> %d.", rec->fields[i].type);
				return 0;
			}
			j++;
			// printf("%s\n",values[j-1]);
		}
		// printf("before setting fields in the new rec.\n");
		for (i = 0; i < elements; i++)
		{
			set_field(*new_rec, i, names[i], types_i[i], values[i]);
		}

		for (i = 0; i < elements; i++)
		{
			free(names[i]);
			free(values[i]);
		}
		free(names), free(types_i), free(values);
		return UPDATE_OLDN;
	}
}
