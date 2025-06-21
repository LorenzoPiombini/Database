#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>
#include <assert.h>
#include "record.h"
#include "debug.h"
#include "str_op.h"
#include "file.h"
#include "parse.h"
#include "lock.h"
#include "common.h"

  
 
static char prog[] = "db";
#define ERR_MSG_PAR prog,__FILE__,__LINE__

static void display_data(struct Record_f rec,int max);
static void clean_input(char *value);

void create_record(char *file_name, struct Schema sch, struct Record_f *rec)
{
	strncpy(rec->file_name,file_name,strlen(file_name));
	rec->fields_num = sch.fields_num;
	rec->count = 1;
	for(int i = 0; i < sch.fields_num;i++){
		strncpy(rec->fields[i].field_name,sch.fields_name[i],strlen(sch.fields_name[i]));
		rec->fields[i].type = sch.types[i];
	}
}

void set_schema(char names[][MAX_FIELD_LT], int *types_i, struct Schema *sch, int fields_c){
	for(int i = 0; i < fields_c; i++){
		strncpy(sch->fields_name[i],names[i],strlen(names[i]));
		sch->types[i] = types_i[i];
	}
	sch->fields_num = fields_c;
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

	if(!value && (field_bit == 0)) return 1;
	int t = (int)type;
	switch (t) {
	case -1:
		break;
	case TYPE_FILE:
	{	
		/*
		 * understand how many records you have to create
		 * */
		char *close_c = NULL;
		size_t cp_l = strlen(value) + 1;
		char cpy[cp_l];
		memset(cpy,0,cp_l);
		strncpy(cpy,value,cp_l);

		int count = 0;
		while((close_c = strstr(cpy,"]"))) {
			*close_c = '@';
			count++;	
		}
		

		/*count should never be 0*/
		assert(count > 0);
		char values[count][500];
		memset(values,0,count*500);
		if(count > 1){
			int i = 0;
			int stop = 0;
			while((close_c = strstr(value,"]"))){
				close_c++;
				if(*close_c == ',' || *close_c == '\0') {
					if(stop != 0){
						int start = stop;
						stop = close_c - value;

						if(stop > 500){
							/* handle this hedge case*/

						}
						if(i < count)
							strncpy(values[i],&value[start+1],stop-start-1);

						i++;
						*(--close_c) = '@';
						continue;

					}
					stop = close_c - value;
					if(stop > 500){
						/* handle this hedge case*/
						
					}
					if(i < count)
						strncpy(values[i],value,stop-1);

					i++;
					*(--close_c) = '@';
					continue;
				} 
				/* should be unreachable BUT is a safety feature
				 * if we reach here we sub ']' with '@' 
				 * to avoid an infinite loop
				 * */
				*(--close_c) = '@';
				
			}
		}
		/*
		 * if schema file exist => read schema file-> check input match with schema
		 * 	else :
		 * 	create the schema file -> create_file_with_no_value (src/parse)
		 * */
		for(int i = 0; i < count; i++){
			if(count > 1 )
				clean_input(values[i]);
			else
				clean_input(value);		
			
		}


		struct Schema sch = {0};
		memset(sch.types,-1,sizeof(int)*MAX_FIELD_NR);
		
		char *sfx = ".sch"; 
		size_t sfxl = strlen(sfx);
		size_t fl = strlen(rec->fields[index].field_name); 
		size_t l = fl + sfxl + 1;
		char file_name[l];
		memset(file_name,0,l);
		strncpy(file_name,rec->fields[index].field_name,l);
		strncat(file_name,sfx,sfxl);
	
		int fd_schema = -1;
		int cr = 0;
		if((fd_schema = open_file(file_name,0)) == -1 || fd_schema == ENOENT){
			if((fd_schema = create_file(file_name)) == -1){
				fprintf(stderr,"(%s): cannot create file %s:%d.\n",ERR_MSG_PAR-1);
				return 0; 
			}
			cr = 1;
		}

		if(cr){
			/*here the file is new*/	
			int mode = 0;
			if(count == 1)
				mode = check_handle_input_mode(value, FCRT) | DF;
			else
				mode = check_handle_input_mode(values[0], FCRT) | DF;

			switch(mode){
			case NO_TYPE_DF:
			case HYB_DF:
				if(!create_file_definition_with_no_value(mode,-1,
							count == 1 ? value : values[0],NULL,&sch)){
					fprintf(stderr,"create_file_definition_with_no_value failed, %s:%d.\n",
							__FILE__,__LINE__-2);
					return 0;
				}	
				break;
			case TYPE_DF:
			{
				char *buff = NULL;
				char *buff_t = NULL;
				int f_count = 0; 
				if(count == 1){		
					buff = strdup(value);
					buff_t = strdup(value);
					f_count = count_fields(value,NULL);
				}else{
					buff = strdup(values[0]);
					buff_t = strdup(values[0]);
					f_count = count_fields(values[0],NULL);
				}

				if(!buff || !buff_t){
					fprintf(stderr,"(%s): strdup() failed %s:%d.\n",prog,__FILE__,__LINE__-2);
					close_file(1,fd_schema);	
					return 0;
				}
					
				if (f_count == 0) {
					fprintf(stderr,"(%s): type syntax might be wrong.\n",prog);
					close_file(1,fd_schema);	
					return 0;
				}

				if (f_count > MAX_FIELD_NR) {
					fprintf(stderr,"(%s): too many fields, max %d fields each file definition.\n"
							,prog, MAX_FIELD_NR);
					close_file(1,fd_schema);	
					return 0;
				}

				if(!create_file_definition_with_no_value(mode,f_count,
							count == 1 ? value : values[0],buff_t,&sch)){
					fprintf(stderr,"create_file_definition_with_no_value failed, %s:%d.\n",
							__FILE__,__LINE__-2);
					close_file(1,fd_schema);	
					return 0;
				}	

				free(buff);
				free(buff_t);
				break;
			}
			default:
				fprintf(stderr,"(%s): mode not supported, %s:%d.\n",ERR_MSG_PAR);
				close_file(1,fd_schema);	
				return 0;
			}

			//write the schema to the file
			struct Header_d hd = {0, 0, sch};

			hd.id_n = HEADER_ID_SYS;
			hd.version = VS;
			if (!write_header(fd_schema, &hd)) {
					__er_write_to_file(F, L - 1);
					close_file(1,fd_schema);
					return 0;
			}

			close_file(1,fd_schema);	

			rec->fields[index].data.file.recs = calloc(1,sizeof(struct Record_f));
			rec->fields[index].data.file.count = 1;
			if(!rec->fields[index].data.file.recs){
				__er_calloc(F,L-2);
				return 0;
			}
			
			
			create_record(rec->fields[index].field_name, hd.sch_d, rec->fields[index].data.file.recs);

		}else {
			/*file exist*/
			/* init the Schema structure*/
			struct Schema sch = {0};
			memset(sch.types,-1,sizeof(int)*MAX_FIELD_NR);
			struct Header_d hd = {0, 0, sch};

			/* ensure the file is a db file */
			if (!read_header(fd_schema, &hd)) {
				close_file(1,fd_schema);
				return 0;
			}

			int fields_count = 0;
			unsigned char check = 0;
			rec->fields[index].data.file.recs = calloc(count ,sizeof(struct Record_f));
			rec->fields[index].data.file.count = count;
			if(!rec->fields[index].data.file.recs){
				__er_calloc(F,L-2);
				return 0;
			}

			int mode = 0; 
			if(count == 1){
				mode = check_handle_input_mode(value, FWRT) | WR;
			}else{
				for(int i = 0; i < count; i++){
					mode = check_handle_input_mode(values[i], FWRT) | WR;

					if(mode == TYPE_WR){
						fields_count = count_fields(values[i],NULL);

						if(fields_count == 0){
							fprintf(stderr,"(%s):check input syntax.\n",prog);
							close_file(1,fd_schema);
							return 0;
						}

						if (fields_count > MAX_FIELD_NR) {
							printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
							close_file(1,fd_schema);
							return 0;
						}

						char *buffer = strdup(value);
						if(!buffer){
							fprintf(stderr,"(%s): strdup faild, %s:%d.\n",prog, __FILE__,__LINE__-1);
							close_file(1,fd_schema);
							return 0;
						}

						check = perform_checks_on_schema(mode,buffer, fields_count,
								rec->fields[index].field_name,
								&rec->fields[index].data.file.recs[i],
								&hd);
						free(buffer);
					}else{
						check = perform_checks_on_schema(mode,values[i],-1,
								rec->fields[index].field_name,
								&rec->fields[index].data.file.recs[i],
								&hd);

					}

					if (check == SCHEMA_ERR || check == 0) {
						close_file(1,fd_schema);
						return 0;
					}

					int r = 0;
					if (check == SCHEMA_NW ||
							check == SCHEMA_NW_NT ||
							check == SCHEMA_CT_NT ||
							check == SCHEMA_EQ_NT){
						/*
						 * if the schema is one between 
						 * SCHEMA_NW 
						 * SCHEMA_EQ_NT 
						 * SCHEMA_NW_NT 
						 * SCHEMA_CT_NT
						 * we update the header
						 * */

						/* aquire lock */
						while(is_locked(1,fd_schema) == LOCKED);
						while((r = lock(fd_schema,WLOCK)) == WTLK);
						if(r == -1){
							fprintf(stderr,"can't acquire or release proper lock.\n");
							close_file(1,fd_schema);
							return 0;
						}

						close_file(1,fd_schema);
						fd_schema = open_file(file_name,1); /*open with O_TRUNCATE*/

						if(file_error_handler(1,fd_schema) != 0){
							return 0;
						}

						if (!write_header(fd_schema, &hd)) {
							__er_write_to_file(F, L - 1);
							close_file(1,fd_schema);
							return 0;
						}

						while(lock(fd_schema,UNLOCK) == WTLK);
					}
				}
			}

			/*check schema*/
			if(count == 1){
				if(mode == TYPE_WR){
					fields_count = count_fields(value,NULL);

					if(fields_count == 0){
						fprintf(stderr,"(%s):check input syntax.\n",prog);
						close_file(1,fd_schema);
						return 0;
					}

					if (fields_count > MAX_FIELD_NR) {
						printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
						close_file(1,fd_schema);
						return 0;
					}

					char *buffer = strdup(value);
					if(!buffer){
						fprintf(stderr,"(%s): strdup faild, %s:%d.\n",prog, __FILE__,__LINE__-1);
						close_file(1,fd_schema);
						return 0;
					}

					check = perform_checks_on_schema(mode,buffer, fields_count,rec->fields[index].field_name,
							rec->fields[index].data.file.recs,
							&hd);
					free(buffer);
				} else {
					check = perform_checks_on_schema(mode,value, -1,rec->fields[index].field_name,
								rec->fields[index].data.file.recs,
								&hd);
				}			

				if (check == SCHEMA_ERR || check == 0) {
					close_file(1,fd_schema);
					return 0;
				}

				int r = 0;
				if (check == SCHEMA_NW ||
						check == SCHEMA_NW_NT ||
						check == SCHEMA_CT_NT ||
						check == SCHEMA_EQ_NT){
					/*
					 * if the schema is one between 
					 * SCHEMA_NW 
					 * SCHEMA_EQ_NT 
					 * SCHEMA_NW_NT 
					 * SCHEMA_CT_NT
					 * we update the header
					 * */

					/* aquire lock */
					while(is_locked(1,fd_schema) == LOCKED);
					while((r = lock(fd_schema,WLOCK)) == WTLK);
					if(r == -1){
						fprintf(stderr,"can't acquire or release proper lock.\n");
						close_file(1,fd_schema);
						return 0;
					}

					close_file(1,fd_schema);
					fd_schema = open_file(file_name,1); /*open with O_TRUNCATE*/

					if(file_error_handler(1,fd_schema) != 0){
						return 0;
					}

					if (!write_header(fd_schema, &hd)) {
						__er_write_to_file(F, L - 1);
						close_file(1,fd_schema);
						return 0;
					}

					while(lock(fd_schema,UNLOCK) == WTLK);
				} /* end of update schema branch*/
			}
			close_file(1,fd_schema);
		}
		break;
	}
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
					errno = 0;
					long n = strtol(t, NULL, 10);
					if (errno == ERANGE || errno == EINVAL) {
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
				errno = 0;
				long n = strtol(value, NULL, 10);
				if (errno == ERANGE || errno == EINVAL) {
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
					errno = 0;
					long n = strtol(t, NULL, 10);
					if (errno == ERANGE || errno == EINVAL){
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
				errno = 0;
				long n = strtol(value, NULL, 10);
				if (errno == ERANGE || errno == EINVAL) {
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
			if (!rec->fields[index].data.v.elements.f){
				rec->fields[index].data.v.insert = insert_element;
				rec->fields[index].data.v.destroy = free_dynamic_array;
			}

			char *t = strtok(value, ",");
			while (t){
				if (!is_floaintg_point(t)) {
					if(is_integer(t)){
						char *decimal = ".00";
						size_t vs = strlen(t);
						size_t ds = strlen(decimal);
						size_t s = vs + ds + 1; 
						char cpy[s];
						memset(cpy,0,s);
						strncpy(cpy,t,vs);
						strncat(cpy,decimal,ds);

						int range = 0;
						if ((range = is_number_in_limits(cpy)) == 0){
							printf("float value not allowed in this system.\n");
							return 0;
						}

						if (range == IN_FLOAT){
							errno = 0;
							float f = strtof(cpy, NULL);
							if (errno == ERANGE || errno == EINVAL){
								printf("conversion ERROR type float %s:%d.\n", F, L - 2);
								return 0;
							}
							rec->fields[index].data.v.insert((void *)&f,
									&rec->fields[index].data.v,type);

							t = strtok(NULL, ",");
							continue;
						}else{
							fprintf(stderr,"float value '%s' is out of range.\n",cpy);
							return 0;
						}
					}

					printf("invalid value for float type: %s.\n", value);
					return 0;
				}

				int range = 0;
				if ((range = is_number_in_limits(t)) == 0)
				{
					printf("float value not allowed in this system.\n");
					return 0;
				}

				if (range == IN_FLOAT){
					float f = strtof(t, NULL);
					if (f == ERANGE || f == EINVAL){
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
				if(is_integer(value)){
					char *decimal = ".00";
					size_t vs = strlen(value);
					size_t ds = strlen(decimal);
					size_t s = vs + ds + 1; 
					char cpy[s];
					memset(cpy,0,s);
					strncpy(cpy,value,vs);
					strncat(cpy,decimal,ds);

					int range = 0;
					if ((range = is_number_in_limits(cpy)) == 0){
						printf("float value not allowed in this system.\n");
						return 0;
					}

					if (range == IN_FLOAT){
						float f = strtof(cpy, NULL);
						if (f == ERANGE || f == EINVAL){
							printf("conversion ERROR type float %s:%d.\n", F, L - 2);
							return 0;
						}
						rec->fields[index].data.f = f;
						break;
					}else{
						fprintf(stderr,"float value '%s' is out of range.\n",cpy);
						return 0;
					}
				}
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

				errno = 0;
				unsigned long un = strtoul(t, NULL, 10);
				if (errno == ERANGE || un == EINVAL)
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

			errno = 0;
			unsigned long un = strtoul(value, NULL, 10);
			if (errno == ERANGE || errno == EINVAL)
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
	case TYPE_PACK:
	{
			if (!is_integer(value)){
				printf("invalid value for pack type: %s.\n", value);
				return 0;
			}

			errno = 0;
			char *endp;
			long p = strtol(value, &endp, 10);
			if (errno == ERANGE || errno == EINVAL || *endp != '\0'){
				printf("conversion ERROR type float %s:%d.\n", F, L - 2);
				return 0;
			}

			if (p > MAX_KEY){
				printf("pack value not allowed on this system.\n");
				return 0;
			}
			rec->fields[index].data.p = (uint32_t)p;

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
				if (!is_floaintg_point(t)) {
					if(is_integer(t)){
						char *decimal = ".00";
						size_t vs = strlen(t);
						size_t ds = strlen(decimal);
						size_t s = vs + ds + 1; 
						char cpy[s];
						memset(cpy,0,s);
						strncpy(cpy,value,vs);
						strncat(cpy,decimal,ds);

						int range = 0;
						if ((range = is_number_in_limits(cpy)) == 0){
							printf("double value not allowed in this system.\n");
							return 0;
						}

						if (range == IN_DOUBLE || range == IN_FLOAT) {
							errno = 0;
							double d = strtod(cpy, NULL);
							if (errno == ERANGE || errno == EINVAL) {
								printf("conversion ERROR type double %s:%d.\n", F, L - 2);
								return 0;
							}

							rec->fields[index].data.v.insert((void *)&d,
									&rec->fields[index].data.v,
									type);
							t = strtok(NULL, ",");
							continue;
						} else {
							fprintf(stderr,"value '%s', ids out of range for double.\n",t);
							return 0;
						}
					}

					printf("invalid value for double type: %s.\n", value);
					return 0;
				}

				int range = 0;
				if ((range = is_number_in_limits(t)) == 0) {
					printf("float value not allowed in this system.\n");
					return 0;
				}

				if (range == IN_DOUBLE || range == IN_FLOAT) {
					double d = strtod(t, NULL);
					if (d == ERANGE || d == EINVAL) {
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

			if (!is_floaintg_point(value)){
				if(is_integer(value)){
					char *decimal = ".00";
					size_t vs = strlen(value);
					size_t ds = strlen(decimal);
					size_t s = vs + ds + 1; 
					char cpy[s];
					memset(cpy,0,s);
					strncpy(cpy,value,vs);
					strncat(cpy,decimal,ds);

					int range = 0;
					if ((range = is_number_in_limits(cpy)) == 0){
						printf("double value not allowed in this system.\n");
						return 0;
					}

					if (range == IN_DOUBLE || range == IN_FLOAT) {
						errno = 0;
						double d = strtod(cpy, NULL);
						if (errno == ERANGE || errno == EINVAL) {
							printf("conversion ERROR type double %s:%d.\n", F, L - 2);
							return 0;
						}
						rec->fields[index].data.d = d;
						break;

					} else {
						fprintf(stderr,"value '%s', is out of range for double.\n",value);
						return 0;
					}
					printf("invalid value for double type: %s.\n", value);
					return 0;
				}
			}
			int range = 0;
			if ((range = is_number_in_limits(value)) == 0)
			{
				printf("float value not allowed in this system.\n");
				return 0;
			}

			if (range == IN_DOUBLE || range == IN_FLOAT) {
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
	default:
		printf("invalid type! type -> %d.", type);
		return 0;
	}

	return 1;
}

void free_type_file(struct Record_f *rec)
{
	int index = -1;
	for(int i = 0; i < rec->fields_num; i++){
		if(rec->field_set[i] == 1 && rec->fields[i].type == TYPE_FILE) index = i;	
	}

	if(index == -1) return;

	for(uint32_t j = 0; j < rec->fields[index].data.file.count; j++){
		struct Record_f *temp = rec->fields[index].data.file.recs[j].next;
		if(temp){
			while(rec->fields[index].data.file.recs[j].count > 1){
				rec->fields[index].data.file.recs[j].next = temp->next;
				temp->next = NULL;
				free_record(temp,temp->fields_num);
				free(temp);
				rec->fields[index].data.file.recs[j].count--;
			}
		}
		free_record(&rec->fields[index].data.file.recs[j],
				rec->fields[index].data.file.recs[j].fields_num);
	}
	free(rec->fields[index].data.file.recs);
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
		case TYPE_PACK:
		case TYPE_DOUBLE:
			break;
		case TYPE_STRING:
			if(rec->fields[i].data.s)
				free(rec->fields[i].data.s);
			break;
		case TYPE_FILE:
			free_type_file(rec);
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

	if(rec->count > 1){
		struct Record_f *temp = rec->next;
		if(temp){
			while(rec->count > 1){
				rec->next = temp->next;
				temp->next = NULL;
				free_record(temp,temp->fields_num);
				free(temp);
				temp = rec->next;  
				rec->count--;
				if(!temp)break; 
			}
		}
	}
}

void print_record(int count, struct Record_f recs)
{

	printf("#################################################################\n\n");
	printf("the Record data are: \n");


	display_data(recs,0);
	if(count > 1){
		while(recs.next){
			struct Record_f rec = *recs.next;
			display_data(rec,0);
			recs.next =recs.next->next;
		}
	}
	printf("\n#################################################################\n\n");
}

static void display_data(struct Record_f rec, int max)
{
	for (int i = 0; i < rec.fields_num; i++){
		if (max < (int)strlen(rec.fields[i].field_name))
		{
			max = (int)strlen(rec.fields[i].field_name);
		}
	}

	for (int i = 0; i < rec.fields_num; i++)
	{
		if(rec.field_set[i] == 0) continue;

		strip('"', rec.fields[i].field_name);
		strip('_', rec.fields[i].field_name);
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
		case TYPE_PACK:
		{
			uint8_t packed[5];
			pack(rec.fields[i].data.p,packed);
			print_pack_str(packed);
			printf("\n");
			break;
		}
		case TYPE_DOUBLE:
			printf("%.2f\n", rec.fields[i].data.d);
			break;
		case TYPE_ARRAY_INT:
		{
			int k;
			for (k = 0; k < rec.fields[i].data.v.size; k++){
				if (rec.fields[i].data.v.size - k > 1) {
					if (!rec.fields[i].data.v.elements.i[k])
						continue;

					printf("%d, ", *rec.fields[i].data.v.elements.i[k]);
				}else{
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
					printf("%.2f.\n", *rec.fields[i].data.v.elements.d[k]);
				}
			}
			break;
		}
		case TYPE_FILE:

		printf("\n");
				for(uint32_t x =0; x < rec.fields[i].data.file.count; x++)
					display_data(rec.fields[i].data.file.recs[x],max);
			break;
		default:
			break;
		}
	}
}

void free_record_array(int len, struct Record_f **recs)
{
	for (int i = 0; i < len; i++) {
			free_record(&(*recs)[i],  (*recs)[i].fields_num);
	}
	free(*recs);
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

unsigned char copy_rec(struct Record_f *src, struct Record_f *dest, struct Schema *sch)
{
	if(sch)
		create_record(src->file_name, *sch, dest);

	/*if(!sch){
		if(dest->fields_num == 0) return 0;
		if(dest->fields[0].field_name[0] == '\0') return 0;
	}*/

	
	strncpy(dest->file_name,src->file_name,strlen(src->file_name));
	dest->fields_num = src->fields_num;

	char data[30];
	for (int i = 0; i < src->fields_num; i++)
	{
		switch (src->fields[i].type)
		{
		case -1:
			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, NULL,src->field_set[i])) {
				printf("set_fields() failed %s:%d.\n", F, L - 2);
				free_record(dest, dest->fields_num);
				return 0;
			}
			break;
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
			if (!set_field(dest, i, src->fields[i].field_name,
						   src->fields[i].type, src->fields[i].data.s,src->field_set[i])){
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
		case TYPE_FILE:
			dest->field_set[i] = 1;
			dest->fields[i].data.file.count = src->fields[i].data.file.count;
			dest->fields[i].data.file.recs = calloc(dest->fields[i].data.file.count,sizeof(struct Record_f));
			if(!dest->fields[i].data.file.recs){
				__er_calloc(F,L-2);
				free_record(dest, dest->fields_num);
				return 0;
			}	
			for (uint32_t j = 0; j < src->fields[i].data.file.count; j++) {
				if(!copy_rec(&src->fields[i].data.file.recs[j],&dest->fields[i].data.file.recs[j],sch)){
					fprintf(stderr,"copy_rec() failed, %s:%d.\n",F,L-1);
					free_record(dest, dest->fields_num);
					return 0;
				}	
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
	switch (type){
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
		strncpy(buffer->recs[buffer->capacity].file_name,rec->file_name,strlen(rec->file_name));

		for(int i = 0; i < rec->fields_num; i++){
			if(rec->field_set[i] == 0){
				buffer->recs[buffer->capacity].field_set[i] = 0;
				strncpy(buffer->recs[buffer->capacity].fields[i].field_name,
						rec->fields[i].field_name,
						strlen(rec->fields[i].field_name));
				buffer->recs[buffer->capacity].fields[i].type = rec->fields[i].type;
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
			case TYPE_FILE:
			{
				/* open the schema file*/
				int rlen =(int)strlen(rec->fields[i].field_name);
				int sfxl =strlen(".sch");
				int len = sfxl + rlen + 1;
				char fn[len];
				memset(fn,0,len);
				strncpy(fn,rec->fields[i].field_name,rlen);
				strncat(fn,".sch",sfxl);
				int fd_sch = open_file(fn,0);
				if(file_error_handler(1,fd_sch) != 0) return -1;
				struct Schema sch = {0};
				memset(sch.types,-1,MAX_FIELD_NR * sizeof(int));

				struct Header_d hd = {0,0,sch};

				if(!read_header(fd_sch,&hd)){
					fprintf(stderr,"(%s): read_header() failed, %s:%d.\n",ERR_MSG_PAR-1);
					close_file(1,fd_sch);
					return -1;
				}
				close_file(1,fd_sch);

				buffer->recs[buffer->capacity].fields[i].data.file.count = rec->fields[i].data.file.count;
				buffer->recs[buffer->capacity].fields[i].data.file.recs = calloc(rec->fields[i].data.file.count,sizeof(struct Record_f));
				if(!buffer->recs[buffer->capacity].fields[i].data.file.recs){
					__er_calloc(F,L-2);
					return -1;
				} 
				for(uint32_t j = 0; j < rec->fields[i].data.file.count; j++){
					if(!copy_rec(&rec->fields[i].data.file.recs[j],
								&buffer->recs[buffer->capacity].fields[i].data.file.recs[j],
								&hd.sch_d)){
						fprintf(stderr,"(%s): copy_rec() failed, %s:%d.\n",ERR_MSG_PAR-3);
						return -1;
				        }
				}
				break;
			}	
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


int compare_rec(struct Record_f *src, struct Record_f *dest)
{
	if (src->fields_num > dest->fields_num) return -1;
	if (src->fields_num < dest->fields_num) return 1;
	if (src->fields_num == dest->fields_num){
		int c = 0;
		int active = 0;	
		int ai = 0;
		int al = 0;
		int ab = 0;
		int af = 0;
		int ad = 0;
		int as = 0;
		int dif = 0;
		
		for(int i = 0; i < src->fields_num; i++){
			if(src->field_set[i] == 1 && dest->field_set[i] == 1){
				active++;
				switch(src->fields[i].type){
				case TYPE_INT:
					if(src->fields[i].data.i == dest->fields[i].data.i) c++;
					 
					if(c == active) break;
					return i;
				case TYPE_LONG:
					if(src->fields[i].data.l == dest->fields[i].data.l) c++;
					if(c == active) break;
					return i;
				case TYPE_BYTE:
					if(src->fields[i].data.b == dest->fields[i].data.b) c++;
					if(c == active) break;
					return i;
				case TYPE_FLOAT:
					if(src->fields[i].data.f == dest->fields[i].data.f) c++;
					if(c == active) break;
					return i;
				case TYPE_DOUBLE:
					if(src->fields[i].data.d == dest->fields[i].data.d) c++;
					if(c == active) break;
					return i;
				case TYPE_STRING:
				{
					size_t l = strlen(dest->fields[i].data.s);
					if(strncmp(src->fields[i].data.s,dest->fields[i].data.s,l) == 0) c++;
					if(c == active) break;
					return i;
				}
				case TYPE_ARRAY_INT:
				if (dest->fields[i].data.v.elements.i){
					if (dest->fields[i].data.v.size == 1 && *dest->fields[i].data.v.elements.i[0] == 0){
						if(src->fields[i].data.v.size == 1 && 
								*src->fields[i].data.v.elements.i[0] == 0) c++;
						
						if(c == active) break;
						return i;
					}

					/*check the values*/
					if (dest->fields[i].data.v.size == src->fields[i].data.v.size) {
						for (int a = 0; a < dest->fields[i].data.v.size; a++){
							if (*src->fields[i].data.v.elements.i[a] == 
									*dest->fields[i].data.v.elements.i[a]) ai++;

						}
					}
					if(dest->fields[i].data.v.size == ai) c++;

					if(c == active) break;
					return i;
				}
				break;
				case TYPE_ARRAY_LONG:
				if (dest->fields[i].data.v.elements.l){
					if (dest->fields[i].data.v.size == 1 && *dest->fields[i].data.v.elements.l[0] == 0){
						if(src->fields[i].data.v.size == 1 && 
								*src->fields[i].data.v.elements.l[0] == 0) c++;
						
						if(c == active) break;
						return i;
					}

					/*check the values*/
					if (dest->fields[i].data.v.size == src->fields[i].data.v.size) {
						for (int a = 0; a < dest->fields[i].data.v.size; a++){
							if (*src->fields[i].data.v.elements.l[a] == 
									*dest->fields[i].data.v.elements.l[a]) al++;
						}
					}
					if(dest->fields[i].data.v.size == al) c++;

					if(c == active) break;
					return i;
				}
				break;
				case TYPE_ARRAY_BYTE:
				if (dest->fields[i].data.v.elements.b){
					if (dest->fields[i].data.v.size == 1 && *dest->fields[i].data.v.elements.b[0] == 0){
						if(src->fields[i].data.v.size == 1 && 
								*src->fields[i].data.v.elements.b[0] == 0) c++;
						
						if(c == active) break;
						return i;
					}

					/*check the values*/
					if (dest->fields[i].data.v.size == src->fields[i].data.v.size) {
						for (int a = 0; a < dest->fields[i].data.v.size; a++){
							if (*src->fields[i].data.v.elements.b[a] == 
									*dest->fields[i].data.v.elements.b[a]) ab++;

						}
					}
					if(dest->fields[i].data.v.size == ab) c++;

					if(c == active) break;
					return i;
				}
				break;
				case TYPE_ARRAY_FLOAT:
				if (dest->fields[i].data.v.elements.f){
					if (dest->fields[i].data.v.size == 1 && *dest->fields[i].data.v.elements.f[0] == 0){
						if(src->fields[i].data.v.size == 1 && 
								*src->fields[i].data.v.elements.f[0] == 0) c++;
						
						if(c == active) break;
						return i;
					}

					/*check the values*/
					if (dest->fields[i].data.v.size == src->fields[i].data.v.size) {
						for (int a = 0; a < dest->fields[i].data.v.size; a++){
							if (*src->fields[i].data.v.elements.f[a] == 
									*dest->fields[i].data.v.elements.f[a]) af++;
						}
					}
					if(dest->fields[i].data.v.size == af) c++;

					if(c == active) break;
					return i;
				}
				break;
				case TYPE_ARRAY_DOUBLE:
				if (dest->fields[i].data.v.elements.d){
					if (dest->fields[i].data.v.size == 1 && *dest->fields[i].data.v.elements.d[0] == 0){
						if(src->fields[i].data.v.size == 1 && 
								*src->fields[i].data.v.elements.d[0] == 0) c++;
						
						if(c == active) break;
						return i;
					}

					/*check the values*/
					if (dest->fields[i].data.v.size == src->fields[i].data.v.size) {
						for (int a = 0; a < dest->fields[i].data.v.size; a++){
							if (*src->fields[i].data.v.elements.d[a] == 
									*dest->fields[i].data.v.elements.d[a]) ad++;

						}
					}
					if(dest->fields[i].data.v.size == ad) c++;

					if(c == active) break;
					return i;
				}
				break;
				case TYPE_ARRAY_STRING:
				if (dest->fields[i].data.v.elements.s){
					if (dest->fields[i].data.v.size == 1 && 
							strncmp(dest->fields[i].data.v.elements.s[0],
								"null",strlen("null"))== 0){
						if(src->fields[i].data.v.size == 1 && 
							strncmp(dest->fields[i].data.v.elements.s[0],
								"null",strlen("null"))== 0) c++;
						
						if(c == active) break;
						return i;
					}

					/*check the values*/
					if (dest->fields[i].data.v.size == src->fields[i].data.v.size) {
						for (int a = 0; a < dest->fields[i].data.v.size; a++){
							if (strncmp(src->fields[i].data.v.elements.s[a], 
									dest->fields[i].data.v.elements.s[a],
									strlen(src->fields[i].data.v.elements.s[a])) == 0) as++;
						}
					}
					if(dest->fields[i].data.v.size == as) c++;

					if(c == active) break;
					return i;
				}
				break;
				case TYPE_FILE:
				{
					if(src->fields[i].data.file.recs){
						int comp = 0;
						/*if the files have the same count*/
						if(src->fields[i].data.file.count == dest->fields[i].data.file.count){
							for(uint32_t a = 0;a < src->fields[i].data.file.count; a++){
								if((comp = compare_rec(&src->fields[i].data.file.recs[a],
									&dest->fields[i].data.file.recs[a])) == -1) continue; 

								return i;
							}
						}
						
						if(src->fields[i].data.file.count > dest->fields[i].data.file.count) return i;
						if(src->fields[i].data.file.count < dest->fields[i].data.file.count) return i;
						/*files have a different count*/
						break;
					}
					break;
				}
				default:
					fprintf(stderr,"type not supported %s:%d.\n",__FILE__,__LINE__);
					return E_RCMP;
				}

			}else{
				dif++;
			} 
		}

		if(dif > 0)
			if(dif == src->fields_num) return DIF;

		if(c == active) return -1;
	}
	return E_RCMP;	
}

static void clean_input(char *value)
{
	strip('[',value);
	strip(']',value);
	/*
	 * this is to clean the string value from trailig 
	 * white spaces
	 * */
	int i = 0;
	while(value[i] != '\0'){
		if(value[i+1] == '\0'){
			value[i] = '\0';
			break;
		}
		value[i] = value[i+1];	
		if(value[i] == ' ') value[i] = '\0';
		i++;
	}
}
