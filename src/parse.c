#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "record.h"
#include "str_op.h"
#include "parse.h"
#include "common.h"
#include "sort.h"

Record_f* parse_d_flag_input(char* file_path,int fields_num, char* buffer, char* buf_t, char* buf_v,Schema *sch,                               int check_sch){
	
	Record_f *rec = create_record(file_path,fields_num);

        if(!rec){
		printf("unable to create the record");
		return NULL;
	}

	char** names = get_fileds_name(buffer, fields_num);
	
	if(!names){
		printf("Error in getting the fields name");
		free(rec);
                return NULL;
        }
	
	 if(sch && check_sch != SCHEMA_EQ){
                char** sch_names = malloc(fields_num * sizeof(char*));
		if(!sch_names){
			printf("no memory for Schema fileds name.");
			free(rec), free(names);
			return NULL;
		}
                sch->fields_num =(unsigned short) fields_num;
		sch->fields_name = sch_names;

		int j = 0;
	        for(j ; j < fields_num; j++){
                	sch->fields_name[j] = strdup(names[j]);
			
			if(!sch->fields_name[j]){
				printf("strdup failed, schema creation field.\n");
				clean_schema(sch);
				free(names), free(rec);
				return NULL;
			}
		}

        }


        ValueType* types_i = get_value_types(buf_t,fields_num );

	if(!types_i){
		printf("Error in getting the fields types");
		free(names);
		free(rec);
		return NULL;
        } 

	
	if(sch && check_sch != SCHEMA_EQ){
                ValueType* sch_types = calloc(fields_num, sizeof(ValueType));
                
		if(!sch_types){
                        printf("No memory for schema types.\n");
                        	clean_schema(sch);
				free(names), free(rec), free(types_i);
                }

		sch->types = sch_types;
		//we can declare the i variable again here due to the scope rules
		int i = 0;
		for(i ; i < fields_num; i++){
			sch->types[i] = types_i[i];
		}
        }

	char** values = get_values(buf_v,fields_num);
        
	if(!values){
        	printf("Error in getting the fields value");
		free(names), free(types_i);
		free(rec);
		return NULL;
	}
       

	
	if(check_sch == SCHEMA_EQ){
		int value_pos[sch->fields_num];
                int i, j;
	
		for(i = 0; i < sch->fields_num; i++){
			for(j = 0; j < sch->fields_num; j++){
				if(strcmp(names[i],sch->fields_name[j]) == 0){
					value_pos[i] = j;
					printf("%d, ",j);
					break;
				}
			}
		}

		char** temp_val = calloc(sch->fields_num,sizeof(char*));
		
		if(!temp_val){
			printf("could not perform calloc,(parse.c l 103).\n");
			return NULL;
		}
	
	
		char** temp_name = calloc(sch->fields_num,sizeof(char*));

		if(!temp_name){
			printf("could not perform calloc,(parse.c l 111).\n");
			free(temp_val);
			return NULL;
		}
	
		ValueType temp_types[sch->fields_num];	
		for(i = 0; i < sch->fields_num; i++){
			temp_val[value_pos[i]] = values[i]; 
			temp_name[value_pos[i]] = names[i];
			temp_types[value_pos[i]] = types_i[i];
		}

		for(i = 0; i < sch->fields_num; i++){
			values[i] = temp_val[i];
			names[i] = temp_name[i];
			types_i[i] = temp_types[i];
		}
		
		free(temp_val), free(temp_name);
        }


 
        int i = 0;	
        for(i; i < fields_num; i++){
		set_field(rec,i,names[i], types_i[i], values[i]);
	}


	
	free(types_i),free(names),free(values);

	return rec;

}

void clean_schema(Schema *sch){
	if(!sch)
	 	return;
	
	int i = 0;
	for(i ; i < sch->fields_num; i++){
		if(sch->fields_name != NULL){
			if(sch->fields_name[i])
				free(sch->fields_name[i]);
                }

	}
	
	if(sch->fields_name != NULL)
		free(sch->fields_name);

	if(sch->types != NULL)
		free(sch->types);

}

int create_header(Header_d *hd){
	if(hd->sch_d.fields_name == NULL ||
		hd->sch_d.types == NULL){
		printf("schema is NULL.\ncould not create header.\n");
		return 0;
	}
	
	hd->id_n = HEADER_ID_SYS;
	hd->version = VS;
		
	return 1;
}

int write_header(int fd, Header_d *hd){
	if(hd->sch_d.fields_name == NULL ||
                hd->sch_d.types == NULL){
                printf("schema is NULL.\ncould not create header.\n");
                return 0;
        }

	if(write(fd, &hd->id_n, sizeof(hd->id_n)) == -1){
		perror("write header id.\n");
		return 0;
	}

	if(write(fd, &hd->version, sizeof(hd->version)) == -1){
		perror("write header version.\n");
		return 0;
	}


	if(write(fd, &hd->sch_d.fields_num, sizeof(hd->sch_d.fields_num)) == -1){
		perror("writing fields number header.");
		return 0;
	}

	int i = 0;
	for(i; i < hd->sch_d.fields_num; i++){
		size_t l = strlen(hd->sch_d.fields_name[i]) + 1;
		if(write(fd, &l, sizeof(l)) == -1){
			perror("write size of name in header.\n");
			return 0;
		}
	
		if(write(fd, hd->sch_d.fields_name[i], l) == -1){
			perror("write name of field in header.\n");
			return 0;
		}
	}

	for(i = 0; i < hd->sch_d.fields_num; i++){
		if(write(fd, &hd->sch_d.types[i], sizeof(hd->sch_d.types[i])) == -1){
			perror("writing types from header.\n");
			return 0;
		}
	}
	
	return 1; // succseed
}

int read_header(int fd, Header_d *hd){
	unsigned int id = 0;
	if(read(fd, &id, sizeof(id)) == -1){
		perror("reading id from header.\n");
		return 0;
	}
	
	if(id != HEADER_ID_SYS){
		printf("this is not a db file.\n");
		return 0;
	}
	
	unsigned short vs = 0;
	if(read(fd, &vs, sizeof(vs)) == -1){
		perror("reading version from header.\n");
		return 0;
	}	
	
	if(vs != VS){
		printf("this file was edited from a different software.\n");
		return 0;
	}
	
	hd->id_n = id;
	hd->version = vs;
		
	if(read(fd, &hd->sch_d.fields_num, sizeof(hd->sch_d.fields_num)) == -1){
		perror("reading field_number header.\n");
		return 0;
	}
	
	//printf("fields number %u.", hd->sch_d.fields_num);
	char** names = calloc(hd->sch_d.fields_num, sizeof(char*));
	
	if(!names){
		printf("no memory for fields name, reading header.\n");
		return 0;
	}

	hd->sch_d.fields_name = names;

	int i = 0;
	for(i = 0 ; i < hd->sch_d.fields_num; i++ ){
		size_t l = 0;
		if(read(fd, &l, sizeof(l)) == - 1){
			perror("reading size of field name.\n");
			clean_schema(&hd->sch_d);
			return 0;
		}
	//	printf("size of name is %ld.\n", l);		
		char *field = malloc(l);
		if(!field){
			printf("no memory for field name, reading from header.\n");
			clean_schema(&hd->sch_d);
                        return 0;
		} 

		if(read(fd, field, l) == -1){
			perror("reading name filed from header.\n");
 			clean_schema(&hd->sch_d);
			free(field);
			return 0;
		}

		field[l - 1 ] = '\0';
		hd->sch_d.fields_name[i] = strdup(field);
		free(field);
		
		if(!hd->sch_d.fields_name[i]){
			printf("strdup failed in dupolicating name field form header.");
			clean_schema(&hd->sch_d);
			return 0;
		} 
	}
	
	ValueType* types_h = calloc(hd->sch_d.fields_num, sizeof(ValueType));
	if(!types_h){
		printf("no memory for types. reading header.\n");
		clean_schema(&hd->sch_d);
		return 0;
	}
	
	hd->sch_d.types = types_h;
	for(i = 0; i < hd->sch_d.fields_num ; i++){
		if(read(fd, &hd->sch_d.types[i], sizeof(hd->sch_d.types[i]) ) == - 1){
			perror("reading types from header.");
			clean_schema(&hd->sch_d);
			return 0;
		}
	}

	return 1;//successed
}

int check_schema(int fields_n, char* buffer, char* buf_t, Header_d hd){
	int i , j;
	int fields_eq = 0;
	int types_eq = 0;
	
	char* names_cs = strdup(buffer);
	char* types_cs = strdup(buf_t);	
	

	ValueType* types_i = get_value_types(types_cs, fields_n);
	
	if(!types_i){
		printf("could not get types from input(parse.c l 287).\n");
		free(names_cs), free(types_cs);
		return 0;
        }

	char** names = get_fileds_name(names_cs, fields_n);
	
	if(!names){
                printf("could not get fields name from input(parse.c l 287).\n");
                free(names_cs), free(types_cs), free(types_i);
                return 0;
        }




	if(hd.sch_d.fields_num == fields_n){
		
		char** copy_sch = calloc(hd.sch_d.fields_num, sizeof(char*));
			
		if(!copy_sch){
			printf("could not perform calloc (parse.c l 366).\n");
			free(names), free(types_i), free(names_cs), free(types_cs);
			return 0;
			
		}

		ValueType types_cp[hd.sch_d.fields_num];

		for(i=0; i < hd.sch_d.fields_num; i++){
			copy_sch[i] = hd.sch_d.fields_name[i];
			types_cp[i] = hd.sch_d.types[i];		
		}
		
		/*sorting the name and type arrays  */
        	if(fields_n > 1 ){
                	quick_sort(types_i,0,fields_n - 1);
                	quick_sort(types_cp, 0, fields_n - 1);
                	quick_sort_str(names,0,fields_n - 1);
                	quick_sort_str(copy_sch,0, fields_n - 1);
       		 }




		for(i = 0, j = 0; i < fields_n; i++,j++){
					
			if(strcmp(copy_sch[i],names[j]) == 0)
				fields_eq++;
				
			if(types_cp[i] == types_i[j])
				types_eq++;
				
		}
		
		if(fields_eq != fields_n || types_eq != fields_n){
			printf("Schema is not equal to file definition.\n");
			free(names), free(types_i), free(names_cs);
			free(types_cs), free(copy_sch);
			return SCHEMA_ERR;
		}

		free(copy_sch);
	}
	
	free(names), free(types_i), free(names_cs); 
	free(types_cs);
	return 1; // schema's good
}
