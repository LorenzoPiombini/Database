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

void create_record(char *file_name, struct Schema sch, struct Record_f *rec)
{
	strncpy(rec->file_name,file_name,strlen(file_name));
	rec->fields_num = sch.fields_num;
	for(int i = 0; i < sch.fields_num;i++){
		strncpy(rec->fields[i].field_name,sch.fields_name[i],strlen(sch.fields_name[i]));
		rec->fields[i].type = sch.types[i];
	}
}

unsigned char set_field(struct Record_f *rec, 
				int index, 
				char *field_name, 
				enum ValueType type, 
				char *value,
				uint8_t field_bit)
{
	strncpy(rec->fields[index].field_name,field_name,strlen(field_name));
	rec->fields[index].type = type;
	rec->field_set[index] = field_bit;

	int t = (int)type;
	switch (t) {
	case TYPE_INT:
	case TYPE_ARRAY_INT:
	{
		if (type == TYPE_ARRAY_INT) {
			if (!rec->fields[index].data.v.elements.i)
			{
				rec->fields[index].data.v.insert = insert_element;
				rec->fields[index].data.v.destroy = free_dynamic_array;
			}

			char *t = strtok(value, ",");
			while (t)
			{

				if (!is_integer(t)) {
					printf("invalid value for integer type: %s.\n", value);
					return 0;
				}

				int range = 0;
				if ((range = is_number_in_limits(t)) == 0) {
					printf("integer value not allowed in this system.\n");
					return 0;
				}

				if (range == IN_INT) {
					/*convert the value to long and then cast it to int */
					/*i do not want to use atoi*/
					long n = strtol(t, NULL, 10);
					if (n == ERANGE || n == EINVAL) {
						printf("conversion ERROR type int %s:%d.\n", F, L - 2);
						return 0;
					}

					int num = (int)n;
					rec->fields[index].data.v.insert((void *)&num,
							&rec->fields[index].data.v,
							 type);
				}else{
					printf("the integer value is not valid for this system.\n");
					return 0;
				}

				t = strtok(NULL, ",");
			}
		} else {

			if (!is_integer(value)){
				printf("invalid value for integer type: %s.\n", value);
				return 0;
			}

			int range = 0;
			if ((range = is_number_in_limits(value)) == 0) {
				printf("integer value not allowed in this system.\n");
				return 0;
			}

			if (range == IN_INT){
				/*convert the value to long and then cast it to int */
				/*i do not want to use atoi*/
				long n = strtol(value, NULL, 10);
				if (n == ERANGE || n == EINVAL) {
					printf("conversion ERROR type int %s:%d.\n", F, L - 2);
					return 0;
				}

				rec->fields[index].data.i = (int)n;
			}else {
				printf("the integer value is not valid for this system.\n");
				return 0;
			}
		}
		break;
	}
	case TYPE_LONG:
	case TYPE_ARRAY_LONG:
	{

		if (type == TYPE_ARRAY_LONG){
			if (!rec->fields[index].data.v.elements.l) {
				rec->fields[index].data.v.insert = insert_element;
				rec->fields[index].data.v.destroy = free_dynamic_array;
			}

			char *t = strtok(value, ",");
			while (t){
				if (!is_integer(t)){
					printf("invalid value for long integer type: %s.\n", value);
					return 0;
				}

				int range = 0;
				if ((range = is_number_in_limits(t)) == 0) {
					printf("long value not allowed in this system.\n");
					return 0;
				}

				if (range == IN_INT || range == IN_LONG) {
					long n = strtol(t, NULL, 10);
					if (n == ERANGE || n == EINVAL){
						printf("conversion ERROR type long %s:%d.\n", F, L - 2);
						return 0;
					}
					rec->fields[index].data.v.insert((void *)&n,
								 &rec->fields[index].data.v,type);
				}
				t = strtok(NULL, ",");
			}
		} else {

			if (!is_integer(value)) {
				printf("invalid value for long integer type: %s.\n", value);
				return 0;
			}

			int range = 0;
			if ((range = is_number_in_limits(value)) == 0) {
				printf("long value not allowed in this system.\n");
				return 0;
			}

			if (range == IN_INT || range == IN_LONG) {
				long n = strtol(value, NULL, 10);
				if (n == ERANGE || n == EINVAL) {
					printf("conversion ERROR type long %s:%d.\n", F, L - 2);
					return 0;
				}
				rec->fields[index].data.l = n;
			}
		}
		break;
	}
	case TYPE_FLOAT:
	case TYPE_ARRAY_FLOAT:
	{
		if (type == TYPE_ARRAY_FLOAT)
		{
			if (!rec->fields[index].data.v.elements.f)
			{
				rec->fields[index].data.v.insert = insert_element;
				rec->fields[index].data.v.destroy = free_dynamic_array;
			}

			char *t = strtok(value, ",");
			while (t){
				if (!is_floaintg_point(t)) {
					printf("invalid value for float type: %s.\n", value);
					return 0;
				}

				int range = 0;
				if ((range = is_number_in_limits(t)) == 0)
				{
					printf("float value not allowed in this system.\n");
					return 0;
				}

				if (range == IN_FLOAT)
				{
					float f = strtof(t, NULL);
					if (f == ERANGE || f == EINVAL)
					{
						printf("conversion ERROR type float %s:%d.\n", F, L - 2);
						return 0;
					}

					rec->fields[index].data.v.insert((void *)&f,
							 &rec->fields[index].data.v,type);
				}
				t = strtok(NULL, ",");
			}
		} else {

			if (!is_floaintg_point(value)) {
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
		}
		break;
	}
	case TYPE_STRING:
	case TYPE_ARRAY_STRING:
	{
		if (type == TYPE_ARRAY_STRING)
		{
			if (!rec->fields[index].data.v.elements.s)
			{
				rec->fields[index].data.v.insert = insert_element;
				rec->fields[index].data.v.destroy = free_dynamic_array;
			}

			char *t = strtok(value, ",");
			while (t) {
				rec->fields[index].data.v.insert((void *)t,&rec->fields[index].data.v,type);
				t = strtok(NULL, ",");
			}
		} else {
			rec->fields[index].data.s = strdup(value);
			if (!rec->fields[index].data.s) {
				fprintf(stderr,"strdup() failed %s:%d.\n",F, L - 4);
				return 0;
			}
		}
		break;
	}
	case TYPE_BYTE:
	case TYPE_ARRAY_BYTE:
	{
		if (type == TYPE_ARRAY_BYTE)
		{
			if (!rec->fields[index].data.v.elements.b)
			{
				rec->fields[index].data.v.insert = insert_element;
				rec->fields[index].data.v.destroy = free_dynamic_array;
			}

			char *t = strtok(value, ",");
			while (t)
			{
				if (!is_integer(t))
				{
					printf("invalid value for byte type: %s.\n", value);
					return 0;
				}

				unsigned long un = strtoul(t, NULL, 10);
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

				unsigned char num = (unsigned char)un;
				rec->fields[index].data.v.insert((void *)&num,&rec->fields[index].data.v,type);
				t = strtok(NULL, ",");
			}
		}
		else
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
		}
		break;
	}
	case TYPE_DOUBLE:
	case TYPE_ARRAY_DOUBLE:
	{
		if (type == TYPE_ARRAY_DOUBLE)
		{
			if (!rec->fields[index].data.v.elements.d)
			{
				rec->fields[index].data.v.insert = insert_element;
				rec->fields[index].data.v.destroy = free_dynamic_array;
			}

			char *t = strtok(value, ",");
			while (t)
			{
				if (!is_floaintg_point(t))
				{
					printf("invalid value for double type: %s.\n", value);
					return 0;
				}

				int range = 0;
				if ((range = is_number_in_limits(t)) == 0)
				{
					printf("float value not allowed in this system.\n");
					return 0;
				}

				if (range == IN_DOUBLE)
				{
					double d = strtod(t, NULL);
					if (d == ERANGE || d == EINVAL)
					{
						printf("conversion ERROR type double %s:%d.\n", F, L - 2);
						return 0;
					}

					rec->fields[index].data.v.insert((void *)&d,
													 &rec->fields[index].data.v,
													 type);
				}

				t = strtok(NULL, ",");
			}
		}
		else
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
		}
		break;
	}
	case -1:
		break;
	default:
		printf("invalid type! type -> %d.", type);
		return 0;
	}

	return 1;
}

void free_record(struct Record_f *rec, int fields_num)
{
	for (int i = 0; i < fields_num; i++){
		int t = (int)rec->fields[i].type;
		switch (t) {
		case -1:
		case TYPE_INT:
		case TYPE_LONG:
		case TYPE_FLOAT:
		case TYPE_BYTE:
		case TYPE_OFF_T:
		case TYPE_DOUBLE:
			break;
		case TYPE_STRING:
			if(rec->fields[i].data.s)
				free(rec->fields[i].data.s);
			break;
		case TYPE_ARRAY_INT:
		case TYPE_ARRAY_LONG:
		case TYPE_ARRAY_FLOAT:
		case TYPE_ARRAY_STRING:
		case TYPE_ARRAY_BYTE:
		case TYPE_ARRAY_DOUBLE:
			if(rec->fields[i].data.v.destroy)
				rec->fields[i].data.v.destroy(&rec->fields[i].data.v, 
							rec->fields[i].type);
			break;
		default:
			fprintf(stderr, "type not supported.\n");
			return;
		}
	}
}

void print_record(int count, struct Record_f *recs)
{

	int i = 0, j = 0, max = 0;
	printf("#################################################################\n\n");
	printf("the Record data are: \n");

	for (j = 0; j < count; j++)
	{
		struct Record_f rec = recs[j];
		for (i = 0; i < rec.fields_num; i++)
		{
			if (max < (int)strlen(rec.fields[i].field_name))
			{
				max = (int)strlen(rec.fields[i].field_name);
			}
		}

		for (i = 0; i < rec.fields_num; i++)
		{
			if(rec.field_set[i] == 0) continue;

			strip('"', rec.fields[i].field_name);
			printf("%-*s\t", max++, rec.fields[i].field_name);
			int t = (int)rec.fields[i].type;
			switch (t){
			case -1:
				break;
			case TYPE_INT:
				printf("%d\n", rec.fields[i].data.i);
				break;
			case TYPE_LONG:
				printf("%ld\n", rec.fields[i].data.l);
				break;
			case TYPE_FLOAT:
				printf("%.2f\n", rec.fields[i].data.f);
				break;
			case TYPE_STRING:
				strip('"', rec.fields[i].data.s);
				printf("%s\n", rec.fields[i].data.s);
				break;
			case TYPE_BYTE:
				printf("%u\n", rec.fields[i].data.b);
				break;
			case TYPE_DOUBLE:
				printf("%.2f\n", rec.fields[i].data.d);
				break;
			case TYPE_ARRAY_INT:
			{
				int k;
				for (k = 0; k < rec.fields[i].data.v.size; k++)
				{
					if (rec.fields[i].data.v.size - k > 1)
					{
						if (!rec.fields[i].data.v.elements.i[k])
							continue;

						printf("%d, ", *rec.fields[i].data.v.elements.i[k]);
					}
					else
					{
						printf("%d.\n", *rec.fields[i].data.v.elements.i[k]);
					}
				}

				break;
			}
			case TYPE_ARRAY_LONG:
			{
				int k;
				for (k = 0; k < rec.fields[i].data.v.size; k++)
				{
					if (rec.fields[i].data.v.size - k > 1)
					{
						printf("%ld, ", *rec.fields[i].data.v.elements.l[k]);
					}
					else
					{
						printf("%ld.\n", *rec.fields[i].data.v.elements.l[k]);
					}
				}
				break;
			}
			case TYPE_ARRAY_FLOAT:
			{
				int k;
				for (k = 0; k < rec.fields[i].data.v.size; k++)
				{
					if (rec.fields[i].data.v.size - k > 1)
					{
						printf("%.2f, ", *rec.fields[i].data.v.elements.f[k]);
					}
					else
					{
						printf("%.2f.\n", *rec.fields[i].data.v.elements.f[k]);
					}
				}
				break;
			}
			case TYPE_ARRAY_STRING:
			{
				int k;
				for (k = 0; k < rec.fields[i].data.v.size; k++)
				{
					if (rec.fields[i].data.v.size - k > 1)
					{
						strip('"', rec.fields[i].data.v.elements.s[k]);
						printf("%s, ", rec.fields[i].data.v.elements.s[k]);
					}
					else
					{
						printf("%s.\n", rec.fields[i].data.v.elements.s[k]);
					}
				}
				break;
			}
			case TYPE_ARRAY_BYTE:
			{
				int k;
				for (k = 0; k < rec.fields[i].data.v.size; k++)
				{
					if (rec.fields[i].data.v.size - k > 1)
					{
						printf("%u, ", *rec.fields[i].data.v.elements.b[k]);
					}
					else
					{
						printf("%u.\n", *rec.fields[i].data.v.elements.b[k]);
					}
				}
				break;
			}
			case TYPE_ARRAY_DOUBLE:
			{
				int k;
				for (k = 0; k < rec.fields[i].data.v.size; k++)
				{
					if (rec.fields[i].data.v.size - k > 1)
					{
						printf("%.2f, ", *rec.fields[i].data.v.elements.d[k]);
					}
					else
					{
						printf("%ld.\n", *rec.fields[i].data.v.elements.l[k]);
					}
				}
				break;
			}
			default:
				break;
			}
		}
	}
	printf("\n#################################################################\n\n");
}

void free_record_array(int len, struct Record_f **recs)
{
	for (int i = 0; i < len; i++) {
			free_record(&(*recs)[i],  (*recs)[i].fields_num);
	}
	free(*recs);
}

void free_records(int len,struct Record_f *recs)
{
	for (int i = 0; i < len; i++) {
			free_record(&recs[i],  recs[i].fields_num);
	}
	free(recs);
}
/* this parameters are:
	- int len => the length of the array() Record_f***
	- Record_f*** array => the arrays of record arrays
	- int* len_ia => length inner array,  an array that keeps track of all the found records array sizes
	- int size_ia => the actual size of len_ia,
*/
void free_array_of_arrays(int len, struct Record_f ****array, int *len_ia, int size_ia)
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
					free_record_array(len_ia[i], (*array)[i]);
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

unsigned char copy_rec(struct Record_f *src, struct Record_f *dest, struct Schema sch)
{
	create_record(src->file_name, sch, dest);

	char data[30];
	for (int i = 0; i < src->fields_num; i++)
	{
		switch (src->fields[i].type)
		{
		case TYPE_INT:
			memset(data, 0, 30);
			if (snprintf(data, 30, "%d", src->fields[i].data.i) < 0) {
				printf("snprintf failed %s:%d.\n", F, L - 2);
				free_record(dest, (*dest).fields_num);
				return 0;
			}

			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, data,src->field_set[i])) {
				printf("set_fields() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}
			break;
		case TYPE_STRING:
			memset(data, 0, 30);
			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, data,src->field_set[i])){
				printf("set_field() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}
			break;
		case TYPE_LONG:
			memset(data, 0, 30);
			if (snprintf(data, 30, "%ld", src->fields[i].data.l) < 0) {
				printf("snprintf failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}

			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, data,src->field_set[i])){
				printf("set_field() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}
			break;
		case TYPE_FLOAT:
			memset(data, 0, 30);
			if (snprintf(data, 30, "%.2f", src->fields[i].data.f) < 0)
			{
				printf("snprintf failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}
			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, data,src->field_set[i])) {
				printf("set_field() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}
			break;
		case TYPE_BYTE:
			memset(data, 0, 30);
			if (snprintf(data, 30, "%d", src->fields[i].data.b) < 0) {
				printf("snprintf failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}
			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, data,src->field_set[i])) {
				printf("set_field() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}
			break;
		case TYPE_DOUBLE:
			memset(data, 0, 30);
			if (snprintf(data, 30, "%.2f", src->fields[i].data.d) < 0)
			{
				printf("snprintf failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}
			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, data,src->field_set[i])) {
				printf("set_field() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}
			break;
		case TYPE_ARRAY_INT:
			memset(data, 0, 30);
			if (snprintf(data, 30, "%d", *src->fields[i].data.v.elements.i[0]) < 0)
			{
				printf("snprintf failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}

			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, data,src->field_set[i])) {
				printf("set_field() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}

			for (int j = 1; j < src->fields[i].data.v.size; j++) {
				dest->fields[i].data.v.insert((void *)src->fields[i].data.v.elements.i[j],
												 &dest->fields[i].data.v,
												 src->fields[i].type);
			}

			break;
		case TYPE_ARRAY_BYTE:
			memset(data, 0, 30);
			if (snprintf(data, 30, "%d", *src->fields[i].data.v.elements.b[0]) < 0) {
				printf("snprintf failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}

			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, data,src->field_set[i])) {
				printf("set_field() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}

			for (int j = 1; j < src->fields[i].data.v.size; j++) {
				dest->fields[i].data.v.insert((void *)src->fields[i].data.v.elements.b[j],
												 &dest->fields[i].data.v,
												 src->fields[i].type);
			}
			break;
		case TYPE_ARRAY_LONG:
			memset(data, 0, 30);
			if (snprintf(data, 30, "%ld", *src->fields[i].data.v.elements.l[0]) < 0) {
				printf("snprintf failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}

			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, data,src->field_set[i])){
				printf("set_field() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}

			for (int j = 1; j < src->fields[i].data.v.size; j++) {
				dest->fields[i].data.v.insert((void *)src->fields[i].data.v.elements.l[j],
												 &dest->fields[i].data.v,
												 src->fields[i].type);
			}
			break;
		case TYPE_ARRAY_DOUBLE:
			memset(data, 0, 30);
			if (snprintf(data, 30, "%2.f", *src->fields[i].data.v.elements.d[0]) < 0) {
				printf("snprintf failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}

			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, data,src->field_set[i])){
				printf("set_field() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}

			for (int j = 1; j < src->fields[i].data.v.size; j++)
			{
				dest->fields[i].data.v.insert((void *)src->fields[i].data.v.elements.d[j],
												 &dest->fields[i].data.v,
												 src->fields[i].type);
			}
			break;
		case TYPE_ARRAY_FLOAT:
			memset(data, 0, 30);
			if (snprintf(data, 30, "%2.f", *src->fields[i].data.v.elements.f[0]) < 0)
			{
				printf("snprintf failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}

			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, data,src->field_set[i])) {
				printf("set_field() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}

			for (int j = 1; j < src->fields[i].data.v.size; j++) {
				dest->fields[i].data.v.insert((void *)src->fields[i].data.v.elements.f[j],
												 &dest->fields[i].data.v,
												 src->fields[i].type);
			}
			break;
		case TYPE_ARRAY_STRING:
			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, 
						   src->fields[i].data.v.elements.s[0],
						   src->field_set[i])) {
				printf("set_field() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}

			for (int j = 1; j < src->fields[i].data.v.size; j++) {
				dest->fields[i].data.v.insert((void *)src->fields[i].data.v.elements.s[j],
												 &dest->fields[i].data.v,
												 src->fields[i].type);
			}
			break;
		default:
			printf("type not supported, %s:%d.\n", F, L - 27);
			return 0;
		}
	}

	return 1;
}

unsigned char get_index_rec_field(char *field_name, struct Record_f **recs,
								  int recs_len, int *field_i_r, int *rec_index)
{
	size_t field_name_len = strlen(field_name);
	for (int i = 0; i < recs_len; i++)
	{
		for (int j = 0; j < recs[i]->fields_num; j++)
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

int init_array(struct array **v, enum ValueType type)
{
	(*(*v)).size = DEF_SIZE;
	switch (type)
	{
	case TYPE_ARRAY_INT:
	{
		(*(*v)).elements.i = calloc(DEF_SIZE, sizeof(int *));
		if (!(*(*v)).elements.i)
		{
			__er_calloc(F, L - 2);
			return -1;
		}

		break;
	}
	case TYPE_ARRAY_LONG:
	{
		(*(*v)).elements.l = calloc(DEF_SIZE, sizeof(long *));
		if (!(*(*v)).elements.l)
		{
			__er_calloc(F, L - 2);
			return -1;
		}
		break;
	}
	case TYPE_ARRAY_FLOAT:
	{
		(*(*v)).elements.f = calloc(DEF_SIZE, sizeof(float *));
		if (!(*(*v)).elements.f)
		{
			__er_calloc(F, L - 2);
			return -1;
		}
		break;
	}
	case TYPE_ARRAY_STRING:
	{
		(*(*v)).elements.s = calloc(DEF_SIZE, sizeof(char *));
		if (!(*(*v)).elements.s)
		{
			__er_calloc(F, L - 2);
			return -1;
		}
		break;
	}
	case TYPE_ARRAY_BYTE:
	{
		(*(*v)).elements.b = calloc(DEF_SIZE, sizeof(unsigned char *));
		if (!(*(*v)).elements.b)
		{
			__er_calloc(F, L - 2);
			return -1;
		}
		break;
	}
	case TYPE_ARRAY_DOUBLE:
	{
		(*(*v)).elements.d = calloc(DEF_SIZE, sizeof(double *));
		if (!(*(*v)).elements.d)
		{
			__er_calloc(F, L - 2);
			return -1;
		}
		break;
	}
	default:
		fprintf(stderr, "type not supported.\n");
		return -1;
	}
	return 0;
}

int insert_element(void *element, struct array *v, enum ValueType type)
{

	switch (type)
	{
	case TYPE_ARRAY_INT:
	{
		/*check if the array has been initialized */
		if (!(*v).elements.i)
		{
			if (init_array(&v, type) == -1)
			{
				fprintf(stderr, "init array failed.\n");
				return -1;
			}
		}

		/*check if there is enough space for new item */
		if (!(*v).elements.i[(*v).size - 1])
		{
			for (int i = 0; i < (*v).size; i++)
			{
				if ((*v).elements.i[i])
					continue;

				(*v).elements.i[i] = malloc(sizeof(int));
				if (!(*v).elements.i[i])
				{
					__er_malloc(F, L - 2);
					return -1;
				}
				*((*v).elements.i[i]) = *(int *)element;
				return 0;
			}
		}

		/*not enough space, increase the size */
		int **elements_new = realloc((*v).elements.i, ++(*v).size * sizeof(int *));
		if (!elements_new)
		{
			__er_realloc(F, L - 2);
			return -1;
		}

		(*v).elements.i = elements_new;
		(*v).elements.i[(*v).size - 1] = malloc(sizeof(int));
		if (!(*v).elements.i[(*v).size - 1])
		{
			__er_malloc(F, L - 2);
			return -1;
		}

		*((*v).elements.i[(*v).size - 1]) = *(int *)element;
		return 0;
	}
	case TYPE_ARRAY_LONG:
	{
		/*check if the array has been initialized */
		if (!(*v).elements.l)
		{
			if (init_array(&v, type) == -1)
			{
				fprintf(stderr, "init array failed.\n");
				return -1;
			}
		}

		/*check if there is enough space for new item */
		if (!(*v).elements.l[(*v).size - 1])
		{
			for (int i = 0; i < (*v).size; i++)
			{
				if ((*v).elements.l[i])
					continue;

				(*v).elements.l[i] = malloc(sizeof(long));
				if (!(*v).elements.l[i])
				{
					__er_malloc(F, L - 2);
					return -1;
				}
				*((*v).elements.l[i]) = *(long *)element;
				return 0;
			}
		}
		/*not enough space, increase the size */
		long **elements_new = realloc((*v).elements.l, ++(*v).size * sizeof(long *));
		if (!elements_new)
		{
			__er_realloc(F, L - 2);
			return -1;
		}

		(*v).elements.l = elements_new;
		(*v).elements.l[(*v).size - 1] = malloc(sizeof(long));
		if (!(*v).elements.l[(*v).size - 1])
		{
			__er_malloc(F, L - 2);
			return -1;
		}

		*((*v).elements.l[(*v).size - 1]) = *(long *)element;
		return 0;
	}
	case TYPE_ARRAY_FLOAT:
	{
		/*check if the array has been initialized */
		if (!(*v).elements.f)
		{
			if (init_array(&v, type) == -1)
			{
				fprintf(stderr, "init array failed.\n");
				return -1;
			}
		}

		/*check if there is enough space for new item */
		if (!(*v).elements.f[(*v).size - 1])
		{
			for (int i = 0; i < (*v).size; i++)
			{
				if ((*v).elements.f[i])
					continue;

				(*v).elements.f[i] = malloc(sizeof(float));
				if (!(*v).elements.f[i])
				{
					__er_malloc(F, L - 2);
					return -1;
				}
				*((*v).elements.f[i]) = *(float *)element;
				return 0;
			}
		}
		/*not enough space, increase the size */
		float **elements_new = realloc((*v).elements.f, ++(*v).size * sizeof(float *));
		if (!elements_new)
		{
			__er_realloc(F, L - 2);
			return -1;
		}

		(*v).elements.f = elements_new;
		(*v).elements.f[(*v).size - 1] = malloc(sizeof(float));
		if (!(*v).elements.f[(*v).size - 1])
		{
			__er_malloc(F, L - 2);
			return -1;
		}

		*((*v).elements.f[(*v).size - 1]) = *(float *)element;
		return 0;
	}
	case TYPE_ARRAY_STRING:
	{
		/*check if the array has been initialized */
		if (!(*v).elements.s)
		{
			if (init_array(&v, type) == -1)
			{
				fprintf(stderr, "init array failed.\n");
				return -1;
			}
		}

		/*check if there is enough space for new item */
		if (!(*v).elements.s[(*v).size - 1])
		{
			for (int i = 0; i < (*v).size; i++)
			{
				if ((*v).elements.s[i])
					continue;

				size_t l = strlen((char *)element) + 1;

				(*v).elements.s[i] = malloc(l * sizeof(char));
				if (!(*v).elements.s[(*v).size - 1])
				{
					__er_malloc(F, L - 2);
					return -1;
				}

				strncpy((*v).elements.s[i], (char *)element, l);

				return 0;
			}
		}

		/*not enough space, increase the size */
		char **elements_new = realloc((*v).elements.s, ++(*v).size * sizeof(char *));
		if (!elements_new)
		{
			__er_realloc(F, L - 2);
			return -1;
		}

		(*v).elements.s = elements_new;

		size_t l = strlen((char *)element) + 1;
		(*v).elements.s[(*v).size - 1] = malloc(l * sizeof(char));
		if (!(*v).elements.s[(*v).size - 1])
		{
			__er_malloc(F, L - 2);
			return -1;
		}

		strncpy((*v).elements.s[(*v).size - 1], (char *)element, l);
		return 0;
	}
	case TYPE_ARRAY_BYTE:
	{
		/*check if the array has been initialized */
		if (!(*v).elements.b)
		{
			if (init_array(&v, type) == -1)
			{
				fprintf(stderr, "init array failed.\n");
				return -1;
			}
		}

		/*check if there is enough space for new item */
		if (!(*v).elements.b[(*v).size - 1])
		{
			for (int i = 0; i < (*v).size; i++)
			{
				if ((*v).elements.b[i])
					continue;

				(*v).elements.b[i] = malloc(sizeof(unsigned char));
				if (!(*v).elements.b[i])
				{
					__er_malloc(F, L - 2);
					return -1;
				}
				*((*v).elements.b[i]) = *(unsigned char *)element;
				return 0;
			}
		}

		/*not enough space, increase the size */
		unsigned char **elements_new = realloc((*v).elements.b, ++(*v).size * sizeof(unsigned char *));
		if (!elements_new)
		{
			__er_realloc(F, L - 2);
			return -1;
		}

		(*v).elements.b = elements_new;
		(*v).elements.b[(*v).size - 1] = malloc(sizeof(unsigned char));
		if (!(*v).elements.i[(*v).size - 1])
		{
			__er_malloc(F, L - 2);
			return -1;
		}

		*((*v).elements.b[(*v).size - 1]) = *(unsigned char *)element;
		return 0;
	}
	case TYPE_ARRAY_DOUBLE:
	{
		/*check if the array has been initialized */
		if (!(*v).elements.d)
		{
			if (init_array(&v, type) == -1)
			{
				fprintf(stderr, "init array failed.\n");
				return -1;
			}
		}

		/*check if there is enough space for new item */
		if (!(*v).elements.d[(*v).size - 1])
		{
			for (int i = 0; i < (*v).size; i++)
			{
				if ((*v).elements.d[i])
					continue;

				(*v).elements.d[i] = malloc(sizeof(double));
				if (!(*v).elements.d[i])
				{
					__er_malloc(F, L - 2);
					return -1;
				}
				*((*v).elements.d[i]) = *(double *)element;
				return 0;
			}
		}

		/*not enough space, increase the size */
		double **elements_new = realloc((*v).elements.d, ++(*v).size * sizeof(double *));
		if (!elements_new)
		{
			__er_realloc(F, L - 2);
			return -1;
		}

		(*v).elements.d = elements_new;
		(*v).elements.d[(*v).size - 1] = malloc(sizeof(double));
		if (!(*v).elements.d[(*v).size - 1])
		{
			__er_malloc(F, L - 2);
			return -1;
		}

		*((*v).elements.d[(*v).size - 1]) = *(double *)element;
		return 0;
	}
	default:
		fprintf(stderr, "type not supperted");
		return -1;
	}

	return 0; /*success*/
}

void free_dynamic_array(struct array *v, enum ValueType type)
{
	switch (type)
	{
	case TYPE_ARRAY_INT:
	{
		for (int i = 0; i < v->size; i++)
		{
			if (v->elements.i[i])
				free(v->elements.i[i]);
		}
		free(v->elements.i);
		v->elements.i = NULL;
		break;
	}
	case TYPE_ARRAY_LONG:
	{
		for (int i = 0; i < v->size; i++)
		{
			if (v->elements.l[i])
				free(v->elements.l[i]);
		}
		free(v->elements.l);
		v->elements.l = NULL;
		break;
	}
	case TYPE_ARRAY_FLOAT:
	{
		for (int i = 0; i < v->size; i++)
		{
			if (v->elements.f[i])
				free(v->elements.f[i]);
		}
		free(v->elements.f);
		v->elements.f = NULL;
		break;
	}
	case TYPE_ARRAY_STRING:
	{
		for (int i = 0; i < v->size; i++)
		{
			if (v->elements.s[i])
				free(v->elements.s[i]);
		}
		free(v->elements.s);
		v->elements.s = NULL;
		break;
	}
	case TYPE_ARRAY_BYTE:
	{
		for (int i = 0; i < v->size; i++)
		{
			if (v->elements.b[i])
				free(v->elements.b[i]);
		}
		free(v->elements.b);
		v->elements.s = NULL;
		break;
	}
	case TYPE_ARRAY_DOUBLE:
	{
		for (int i = 0; i < v->size; i++)
		{
			if (v->elements.d[i])
				free(v->elements.d[i]);
		}
		free(v->elements.d);
		v->elements.d = NULL;
		break;
	}
	default:
		fprintf(stderr, "array type not suported.\n");
		return;
	}
}


int insert_rec(struct Recs_old *buffer, struct Record_f *rec, off_t pos)
{
	if(buffer->capacity < MAX_RECS_OLD_CAP) {
		for(int i = 0; i < rec->fields_num; i++){
			if(rec->field_set[i] == 0){
				buffer->recs[buffer->capacity].field_set[i] = 0;
				strncpy(buffer->recs[buffer->capacity].fields[i].field_name,
						rec->fields[i].field_name,
						strlen(rec->fields[i].field_name));
				continue;
			}

			buffer->recs[buffer->capacity].field_set[i] = 1;
			strncpy(buffer->recs[buffer->capacity].fields[i].field_name,
					rec->fields[i].field_name,
					strlen(rec->fields[i].field_name));
			buffer->recs[buffer->capacity].fields[i].type = rec->fields[i].type;
			switch(rec->fields[i].type){
			case TYPE_INT:
				buffer->recs[buffer->capacity].fields[i].data.i = rec->fields[i].data.i;
				break;
			case TYPE_DOUBLE:
				buffer->recs[buffer->capacity].fields[i].data.d = rec->fields[i].data.d;
				break;
			case TYPE_FLOAT:
				buffer->recs[buffer->capacity].fields[i].data.f = rec->fields[i].data.f;
				break;
			case TYPE_LONG:
				buffer->recs[buffer->capacity].fields[i].data.l = rec->fields[i].data.l;
				break;
			case TYPE_BYTE:
				buffer->recs[buffer->capacity].fields[i].data.b = rec->fields[i].data.b;
				break;
			case TYPE_STRING:
				buffer->recs[buffer->capacity].fields[i].data.s = strdup(rec->fields[i].data.s);
				if(!buffer->recs[buffer->capacity].fields[i].data.s){
					fprintf(stderr,"strdup() failed %s:%d",__FILE__,__LINE__-1);
					return -1;
				} 
				break;
			case TYPE_ARRAY_INT:
			case TYPE_ARRAY_LONG:
			case TYPE_ARRAY_BYTE:
			case TYPE_ARRAY_FLOAT:
			case TYPE_ARRAY_DOUBLE:
			case TYPE_ARRAY_STRING:
				buffer->recs[buffer->capacity].fields[i].data.v.insert = insert_element;
				buffer->recs[buffer->capacity].fields[i].data.v.destroy = free_dynamic_array;
				switch(rec->fields[i].type){
				case TYPE_ARRAY_INT:
					for(int a = 0 ; a < rec->fields[i].data.v.size; a++){
						buffer->recs[buffer->capacity].fields[i].data.v
							.insert((void*)rec->fields[i].data.v.elements.i[a],
								&buffer->recs[buffer->capacity].fields[i].data.v,
								rec->fields[i].type);
					}
					break;
				case TYPE_ARRAY_LONG:
					for(int a = 0 ; a < rec->fields[i].data.v.size; a++){
						buffer->recs[buffer->capacity].fields[i].data.v
							.insert((void*)rec->fields[i].data.v.elements.l[a],
								&buffer->recs[buffer->capacity].fields[i].data.v,
								rec->fields[i].type);
					}
					break;
				case TYPE_ARRAY_BYTE:
					for(int a = 0 ; a < rec->fields[i].data.v.size; a++){
						buffer->recs[buffer->capacity].fields[i].data.v
							.insert((void*)rec->fields[i].data.v.elements.b[a],
								&buffer->recs[buffer->capacity].fields[i].data.v,
								rec->fields[i].type);
					}
					break;
				case TYPE_ARRAY_FLOAT:
					for(int a = 0 ; a < rec->fields[i].data.v.size; a++){
						buffer->recs[buffer->capacity].fields[i].data.v
							.insert((void*)rec->fields[i].data.v.elements.f[a],
								&buffer->recs[buffer->capacity].fields[i].data.v,
								rec->fields[i].type);
					}
					break;
				case TYPE_ARRAY_DOUBLE:
					for(int a = 0 ; a < rec->fields[i].data.v.size; a++){	
						buffer->recs[buffer->capacity].fields[i].data.v
							.insert((void*)rec->fields[i].data.v.elements.d[a],
								&buffer->recs[buffer->capacity].fields[i].data.v,
								rec->fields[i].type);
					}
					break;
				case TYPE_ARRAY_STRING:
					for(int a = 0 ; a < rec->fields[i].data.v.size; a++){	
						buffer->recs[buffer->capacity].fields[i].data.v
							.insert((void*)rec->fields[i].data.v.elements.s[a],
								&buffer->recs[buffer->capacity].fields[i].data.v,
								rec->fields[i].type);
					}
					break;
				default:
					return -1;
				}
				break;
			default:
				break;

			}
		}
		buffer->recs[buffer->capacity].fields_num = rec->fields_num;
		buffer->pos_u[buffer->capacity] = pos;
		buffer->capacity++;
		return 0;
	}
	
	errno = 0;
	if(buffer->dynamic_capacity == 0){
		buffer->recs_r = calloc(MAX_RECS_OLD_CAP,sizeof(struct Record_f));
		if(!buffer->recs_r){
			perror("recs_r calloc");
			if(errno == ENOMEM) return errno;
			return -1;
		}

		buffer->pos_u_r = calloc(MAX_RECS_OLD_CAP,sizeof(off_t));
		if(!buffer->pos_u_r){
			perror("pos_u_r calloc");
			if(errno == ENOMEM) return errno;
			return -1;
		}

		buffer->recs_r[buffer->dynamic_capacity] = *rec;
		buffer->pos_u_r[buffer->dynamic_capacity] = pos;
		buffer->dynamic_capacity++;
		return 0;

	}else if(buffer->dynamic_capacity < MAX_RECS_OLD_CAP){
		buffer->recs_r[buffer->dynamic_capacity] = *rec;
		buffer->dynamic_capacity++;
		return 0;
	}else if(buffer->dynamic_capacity == 500 || buffer->dynamic_capacity > 500){

		struct Record_f *new_recs_r = realloc(buffer->recs_r,++(buffer->dynamic_capacity) * sizeof(struct Record_f));
		if(!new_recs_r){
			perror("new_recs_r realloc");
			if(errno == ENOMEM) return errno;
			return -1;
		}
		
		off_t *new_pos_u_r = realloc(buffer->pos_u_r,buffer->dynamic_capacity * sizeof(off_t));
		if(!new_pos_u_r){
			perror("new_pos_u_r realloc");
			if(errno == ENOMEM) return errno;
			return -1;
		}

		buffer->recs_r = new_recs_r;
		buffer->recs_r[buffer->dynamic_capacity - 1] = *rec;
		buffer->pos_u_r = new_pos_u_r;
		buffer->pos_u_r[buffer->dynamic_capacity] = pos;
		return 0;
	}
	
	return -1;
}

void free_recs_old(struct Recs_old *buffer)
{
	if(buffer->dynamic_capacity == 0){
		for(int i = 0; i < buffer->capacity; i++)
			free_record(&buffer->recs[i],buffer->recs[i].fields_num);
	}else {
		struct Record_f *p = buffer->recs;
		free_record_array(buffer->capacity,&p);
		free_record_array(buffer->dynamic_capacity,&buffer->recs_r);
		free(buffer->pos_u_r);
	}
}

int schema_has_type(struct Header_d *hd)
{
	for(short int i = 0;i < hd->sch_d.fields_num;i++)
		if(hd->sch_d.types[i] == -1) return 0;

	return 1;

}
