#include <stdio.h>
#include <getopt.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "journal.h"
#include "common.h"
#include "file.h"
#include "record.h"
#include "input.h"
#include "hash_tbl.h"
#include "str_op.h"
#include "lock.h"
#include "parse.h"
#include "debug.h"
#include "build.h"

char prog[] = "db";
int main(int argc, char *argv[])
{
	if (argc < 2) {
		print_usage(argv);
		return 1;
	}

	/* file descriptors */
	int fd_index = -1; 
	int fd_data = -1;
	int fd_schema = -1;

	/*----------- bool values-------------------*/

	unsigned char new_file = 0;
	unsigned char del = 0;
	unsigned char update = 0;
	unsigned char list_def = 0;
	unsigned char del_file = 0;
	unsigned char build = 0;
	unsigned char list_keys = 0;
	unsigned char create = 0;
	unsigned char options = 0;
	unsigned char index_add = 0;
	/*------------------------------------------*/

	/* parameters populated with the flag from getopt()*/
	int c = 0;
	char *file_path = NULL;
	char *data_to_add = NULL;
	char *key = NULL;
	char *schema_def = NULL;
	char *txt_f = NULL;
	char *option = NULL;
	int bucket_ht = 0;
	int indexes = 0;
	int index_nr = 0;
	int only_dat = 0;

	while ((c = getopt(argc, argv, "nItAf:a:k:D:R:uleb:s:x:c:i:o:")) != -1)
	{
		switch (c){
		case 'a':
			data_to_add = optarg;
			break;
		case 'n':
			new_file = 1; // true
			break;
		case 'f':
			file_path = optarg;
			break;
		case 'k':
			key = optarg;
			break;
		case 'D':
		{
			del = 1;
			char *endptr;
			long l = strtol(optarg,&endptr,10);
			if(*endptr == '\0'){
				index_nr = (int) l;
			}else{
				fprintf(stderr,"option -D value is not a valid number.\n");
				return -1;
			}
			break;
		}
		case 't':
			print_types();
			return 0;
		case 'R':
			schema_def = optarg;
			break;
		case 'u':
			update = 1;
			break;
		case 'l':
			list_def = 1;
			break;
		case 'e':
			del_file = 1;
			break;
		case 'b':
			build = 1, txt_f = optarg;
			break;
		case 's':
		{
			char *endptr;
			long l = strtol(optarg,&endptr,10);
			if(*endptr == '\0'){
				bucket_ht = (int)l;
			}else{
				fprintf(stderr,"option -s value is not a valid number.\n");
				return -1;
			}
			break;

		}
		case 'x':
			list_keys = 1;
			char *endptr;
			long l = strtol(optarg,&endptr,10);
			if(*endptr == '\0'){
				index_nr = (int)l;
			}else{
				fprintf(stderr,"option -x value is not a valid number.\n");
				return -1;
			}
			break;
		case 'c':
			create = 1, txt_f = optarg;
			break;
		case 'i':
		{
			char *endptr;
			long l = strtol(optarg,&endptr,10);
			if(*endptr == '\0'){
				indexes = (int)l;
			}else{
				fprintf(stderr,"option -i value is not a valid number.\n");
				return -1;
			}
			break;
		}
		case 'o':
			options = 1, option = optarg;
			break;
		case 'A':
			index_add = 1;
			break;
		case 'I':
			only_dat = 1;
			break;
		default:
			printf("Unknow option -%c\n", c);
			return 1;
		}
	}

	if (!check_input_and_values(file_path, data_to_add, key,
			argv, del, list_def, new_file, update, del_file,	
			build, create, options, index_add)) {
		return 1;
	}

	if (create){
		if (!create_system_from_txt_file(txt_f)) {
			return STATUS_ERROR;
		}
		printf("system created!\n");
		return 0;
	}

	if (build) {
		if (build_from_txt_file(file_path, txt_f)) {
			return 0;
		}

		return STATUS_ERROR;
	}

	if (new_file) {
		/*creates three name from the file_path  "str_op.h" */

		char files[3][MAX_FILE_PATH_LENGTH] = {0};  
		if(three_file_path(file_path, files) == EFLENGTH){
			fprintf(stderr,"(%s): file name or path '%s' too long",prog,file_path);
			return STATUS_ERROR;
		}

		if (only_dat) {
			fd_data = create_file(files[1]);
			fd_schema = create_file(files[2]);
			/*
			 * file_error_handler will close the file descriptors if there are issues
			 *  and print error messages to the console
			 *  */
			if (file_error_handler(2, fd_data, fd_schema) != 0) return STATUS_ERROR;
		
		}else {
			fd_index = create_file(files[0]);
			fd_data = create_file(files[1]);
			fd_schema = create_file(files[2]);
			/*
			 * file_error_handler will close the file descriptors if there are issues
			 *  and print error messages to the console
			 *  */
			if (file_error_handler(3, fd_index, fd_data, fd_schema) != 0) return STATUS_ERROR;
		}

		if (schema_def) { 
			/* case when user creates a file with only file definition*/

			int mode = check_handle_input_mode(schema_def, FCRT) | DF;
			int fields_count = 0; 
			char *buf_sdf = NULL; 
			char *buf_t = NULL;
			/* init he Schema structure*/
			struct Schema sch = {0};
			sch.fields_num = fields_count;
			memset(sch.types,-1,sizeof(int)*MAX_FIELD_NR);

			switch(mode){
			case TYPE_DF:
			{
				fields_count = count_fields(schema_def,NULL);

				if (fields_count == 0) {
					fprintf(stderr,"(%s): type syntax might be wrong.\n",prog);
					goto clean_on_error_1;
				}

				if (fields_count > MAX_FIELD_NR) {
					fprintf(stderr,"(%s): too many fields, max %d fields each file definition.\n"
							,prog, MAX_FIELD_NR);
					goto clean_on_error_1;
				}

				buf_sdf = strdup(schema_def);
				buf_t = strdup(schema_def);
				if(!buf_t || !buf_sdf){
					fprintf(stderr,"(%s): strdup failed, %s:%d.\n",prog,__FILE__,__LINE__);
					if(buf_sdf) free(buf_sdf);	
					if(buf_t) free(buf_t);	
					goto clean_on_error_1;
				}
				if (!create_file_definition_with_no_value(mode,fields_count, buf_sdf, buf_t, &sch)) {
					fprintf(stderr,"(%s): can't create file definition %s:%d.\n",prog, F, L - 1);
					free(buf_sdf);
					free(buf_t);
					goto clean_on_error_1;
				}
				free(buf_sdf);
				free(buf_t);
				break;
			}
			case HYB_DF:
			case NO_TYPE_DF	:	
			{
				if (!create_file_definition_with_no_value(mode,fields_count, schema_def,NULL, &sch)) {
					fprintf(stderr,"(%s): can't create file definition %s:%d.\n",prog, F, L - 1);
					goto clean_on_error_1;
				}
				break;
			}
			default:
				fprintf(stderr,"(%s):invalid input.\n",prog);
				goto clean_on_error_1;
			}

			struct Header_d hd = {0, 0, sch};

			if (!create_header(&hd)) goto clean_on_error_1;

			if (!write_header(fd_schema, &hd)) {
				fprintf(stderr,"(%s): write schema failed, %s:%d.\n",prog, F, L - 1);
				goto clean_on_error_1;
			}


			if (only_dat) {
				fprintf(stdout,"(%s): File created successfully!\n",prog);
				close_file(2, fd_data, fd_schema);
				return 0;
			}

			/*  write the index file */
			int bucket = bucket_ht > 0 ? bucket_ht : 7;
			int index_num = indexes > 0 ? indexes : 5;

			if (!write_index_file_head(fd_index, index_num)) {
				fprintf(stderr,"(%s) write index file head failed, %s:%d",prog, F, L - 2);
				goto clean_on_error_1;
			}

			int i = 0;
			for (i = 0; i < index_num; i++) {
				HashTable ht = {0};
				ht.size = bucket;
				ht.write = write_ht;

				if (!write_index_body(fd_index, i, &ht)) {
					printf("write to file failed. %s:%d.\n", F, L - 2);
					destroy_hasht(&ht);
					goto clean_on_error_1;
				}

				destroy_hasht(&ht);
			}

			fprintf(stdout,"(%s): File created successfully!\n",prog);

			close_file(3, fd_index, fd_data);
			return 0;

			clean_on_error_1:
			close_file(3, fd_index, fd_data,fd_schema);
			delete_file(3, files[0], files[1], files[2]);
			return STATUS_ERROR;
		}

		if (data_to_add) { 
			/* creates a file with full definitons (fields and value)*/
			int mode = check_handle_input_mode(data_to_add, FCRT) | WR;
			int fields_count = 0; 
			char *buffer = NULL; 
			char *buf_t = NULL;
			char *buf_v = NULL;
			/* init he Schema structure*/
			struct Schema sch = {0};
			struct Record_f rec = {0};
			sch.fields_num = fields_count;
			memset(sch.types,-1,sizeof(int)*MAX_FIELD_NR);
			
			switch(mode){
			case NO_TYPE_WR:			
			case HYB_WR:			
			{
				char names[MAX_FIELD_NR][MAX_FILED_LT] ={0};
				int types_i[MAX_FIELD_NR];
				memset(types_i,-1,sizeof(int)*MAX_FIELD_NR);
				char **values = NULL;

				switch (mode){
				case NO_TYPE_WR:			
				{	
					values = extract_fields_value_types_from_input(data_to_add,names,
							types_i,
							&fields_count);
					if(!values){
						fprintf(stderr,"(%s): cannot extract value from input,%s:%d.\n",prog,
								__FILE__,__LINE__-1);
						goto clean_on_error_2;
					}
					break;
				}
				case HYB_WR:			
				{
					if((fields_count = get_name_types_hybrid(mode,data_to_add
								,names,
								types_i)) == -1) goto clean_on_error_2;
					if(get_values_hyb(data_to_add,
							&values,
							fields_count) == -1) goto clean_on_error_2;

					for(int i = 0; i < fields_count; i++){
						if(types_i[i] == -1) types_i[i] = assign_type(values[i]);		
					}
					break;
				}	
				default:
					goto clean_on_error_2;
				}
				set_schema(names,types_i,&sch,fields_count);	

				if(parse_input_with_no_type(file_path,fields_count, names, 
							types_i, values,&sch,0,&rec) == -1){
					fprintf(stderr,"(%s): error creating the record, %s:%d.\n",prog, __FILE__, __LINE__ - 1);
					free_strs(fields_count,1,values);
					goto clean_on_error_2;
				}
				free_strs(fields_count,1,values);
				break;
			}
			case TYPE_WR:			
			{ 
				fields_count = count_fields(data_to_add,NULL);
				if (fields_count > MAX_FIELD_NR) {
					fprintf(stderr,"(%s): too many fields, max %d each file definition.",prog, MAX_FIELD_NR);
					goto clean_on_error_2;
				}

				buffer = strdup(data_to_add);
				buf_t = strdup(data_to_add);
				buf_v = strdup(data_to_add);
					
				if(!buf_t || !buf_v || !buffer){
					fprintf(stderr,"(%s): strdup failed, %s:%d.\n",prog,__FILE__,__LINE__ -5);
					if(buf_v) free(buf_v);	
					if(buf_t) free(buf_t);	
					if(buffer) free(buffer);
					goto clean_on_error_2;
				}

				if(parse_d_flag_input(file_path, fields_count,
							buffer, buf_t, buf_v, &sch, 0,&rec) == -1) {
					fprintf(stderr,"(%s): error creating the record, %s:%d.\n",prog, __FILE__, __LINE__ - 1);
					goto clean_on_error_2;
				}

				free(buffer); 
				free(buf_t);
				free(buf_v);
				break;
			}

			default:
				goto clean_on_error_2;
			}

			struct Header_d hd = {0, 0, sch};
			if (!create_header(&hd)) {
				fprintf(stderr,"%s:%d.\n", F, L - 1);
				goto clean_on_error_2;
			}

			if (!write_header(fd_schema, &hd)) {
				printf("write to file failed, %s:%d.\n", __FILE__, __LINE__ - 1);
				goto clean_on_error_2;
			}

			if (only_dat) {
				printf("File created successfully!\n");
				free_record(&rec, fields_count);
				close_file(2, fd_data,fd_schema);
				return 0;
			}

			/*  write the index file */
			int bucket = bucket_ht > 0 ? bucket_ht : 7;
			int index_num = indexes > 0 ? indexes : 5;

			if (!write_index_file_head(fd_index, index_num)) {
				printf("write to file failed, %s:%d", F, L - 2);
				goto clean_on_error_2;
			}

			int i = 0;
			for (i = 0; i < index_num; i++) {
				HashTable ht = {0};
				ht.size = bucket;
				ht.write = write_ht;

				if (i == 0) {
					off_t offset = get_file_offset(fd_data);
					if (offset == -1) {
						__er_file_pointer(F, L - 3);
						goto clean_on_error_2;
					}

					int key_type = 0;
					void *key_conv = key_converter(key, &key_type);
					if (key_type == UINT && !key_conv) {
						fprintf(stderr, "(%s): error to convert key.\n",prog);
						goto clean_on_error_2;
					} else if (key_type == UINT) {
						if (key_conv) {
							if (!set(key_conv, key_type, offset, &ht)) {
								free(key_conv);
								goto clean_on_error_2;
							}
							free(key_conv);
						}
					} else if (key_type == STR) {
						/*create a new key value pair in the hash table*/
						if (!set((void *)key, key_type, offset, &ht)) {
							destroy_hasht(&ht);
							goto clean_on_error_2;
						}
					}

					if (!write_file(fd_data, &rec, 0, update)) {
						printf("write to file failed, %s:%d.\n", F, L - 1);
						destroy_hasht(&ht);
						goto clean_on_error_2;
					}
				}

				if (!write_index_body(fd_index, i, &ht)) {
					printf("write to file failed. %s:%d.\n", F, L - 2);
					destroy_hasht(&ht);
					goto clean_on_error_2;
				}

				destroy_hasht(&ht);
			}

			fprintf(stdout,"(%s): File created successfully.\n",prog);
			free_record(&rec, fields_count); // this free the memory allocated for the record
			close_file(3, fd_index, fd_data,fd_schema);
			return 0;
			
			clean_on_error_2:
			close_file(3, fd_index, fd_data,fd_schema);
			delete_file(3, files[0], files[1], files[2]);
			free_record(&rec, fields_count);
			return STATUS_ERROR;

		}else {
			fprintf(stderr,"(%s): no data to write to file %s.\n",prog, file_path);
			fprintf(stderr,"(%s): %s has been created, you can add to the file using option -a.\n",prog, file_path);
			print_usage(argv);

			/* init the Schema structure*/
			struct Schema sch = {0};
			memset(sch.types,-1,sizeof(int)*MAX_FIELD_NR);

			struct Header_d hd = {HEADER_ID_SYS, VS, sch};

			if (!write_empty_header(fd_schema, &hd)) {
				printf("%s:%d.\n", F, L - 1);
				goto clean_on_error_3;
			}

			/*  write the index file */
			int bucket = bucket_ht > 0 ? bucket_ht : 7;
			int index_num = indexes > 0 ? indexes : 5;

			if (!write_index_file_head(fd_index, index_num)) {
				printf("write to file failed, %s:%d", F, L - 2);
				goto clean_on_error_3;
			}

			int i = 0;
			for (i = 0; i < index_num; i++) {
				HashTable ht = {0};
				ht.size = bucket;
				ht.write = write_ht;

				if (!write_index_body(fd_index, i, &ht)) {
					printf("write to file failed. %s:%d.\n", F, L - 2);
					goto clean_on_error_3;
				}

				destroy_hasht(&ht);
			}

			printf("File created successfully.\n");

			close_file(3, fd_index, fd_data,fd_schema);
			return 0;

			clean_on_error_3:
			close_file(3, fd_index, fd_data,fd_schema);
			delete_file(3, files[0], files[1], files[2]);
			return STATUS_ERROR;

		}

	} else { /*file already exist. we can perform CRUD operation*/

		/*creates three name from the file_path => from "str_op.h" */

		char files[3][MAX_FILE_PATH_LENGTH] = {0};  
		if(three_file_path(file_path, files) == EFLENGTH){
			fprintf(stderr,"(%s): file name or path '%s' too long",prog,file_path);
			return STATUS_ERROR;
		}

		if (list_def) {
			fd_schema = open_file(files[2], 0);
			/* file_error_handler will close the file descriptors if there are issues */
			if (file_error_handler(1, fd_schema) != 0){
				printf("Error in creating or opening files,%s:%d.\n", F, L - 2);
				return STATUS_ERROR;
			}
		} else {
			fd_index = open_file(files[0], 0);
			fd_data = open_file(files[1], 0);
			fd_schema = open_file(files[2], 0);
			/* file_error_handler will close the file descriptors if there are issues */
			if (file_error_handler(2, fd_index, fd_data) != 0) {
				printf("Error in creating or opening files,%s:%d.\n", F, L - 2);
				return STATUS_ERROR;
			}
		}

		/* there is no real lock when flag RLOCK is passed to lock funtion
		 * so if an error occured we do not have to release the lock */
		while((is_locked(3,fd_schema,fd_data,fd_index)) == LOCKED);

		/* ensure the file is a db file */
		/* init the Schema structure*/
		struct Schema sch = {0};
		memset(sch.types,-1,sizeof(int)*MAX_FIELD_NR);

		struct Header_d hd = {0, 0, sch};

		if (!read_header(fd_schema, &hd)) {
			close_file(3,fd_schema,fd_data,fd_index);
			return STATUS_ERROR;
		}

		if (index_add) {
			close_file(2, fd_schema, fd_data);

			/*  write the index file
			 *  if the user does not specify the indexes number
			 *  that they want to add, then only one will
			 *  be added.
			 *  */
			int bucket = bucket_ht > 0 ? bucket_ht : 7;
			int index_num = indexes > 0 ? indexes : 1;
			/* acquire write lock */
			int lock_f = 0;
			int r = 0;
			while(is_locked(3, fd_schema,fd_index,fd_schema) == LOCKED);
			while((r = lock(fd_index,WLOCK)) == WTLK);
			if(r == -1){
				fprintf(stderr,"can't acquire or release proper lock.\n");
				goto clean_on_error_4;
			}

			lock_f = 1;
			
			if (add_index(index_num, file_path, bucket) == -1) {
				fprintf(stderr, "can't add index %s:%d",F, L - 2);
				goto clean_on_error_4;
			}

			char *mes = (index_num > 1) ? "indexes" : "index";
			printf("%d %s added.\n", index_num, mes);

			/*release the lock*/
			while((r = lock(fd_index,UNLOCK)) == WTLK);
			if(r == -1){
				fprintf(stderr,"can't acquire or release proper lock.\n");
				goto clean_on_error_4;
			}	

			return 0;
			clean_on_error_4:
			if(lock_f) while((r = lock(fd_index,UNLOCK)) == WTLK);
			close_file(1,fd_index);
			return STATUS_ERROR;
		}

		if (del_file) { 
			/*delete file */

			close_file(2,fd_data,fd_schema);
			/* acquire lock */
			int r = 0;
			while(is_locked(3, fd_schema,fd_index,fd_schema) == LOCKED);
			while((r = lock(fd_index,WLOCK)) == WTLK);
			if(r == -1){
				fprintf(stderr,"can't acquire or release proper lock.\n");
				return STATUS_ERROR;
			}

			/* we can safely delete the files, here, this process is the only one owning locks
				for both the index and the data file */
			struct stat st;
			if(fstat(fd_index,&st) != 0){
				fprintf(stderr,"(%s): delete file '%s' failed.\n",prog,file_path);
				while((r = lock(fd_index,UNLOCK)) == WTLK);
				close_file(1, fd_index);
				return STATUS_ERROR;
			}
			
			close_file(1, fd_index);
			delete_file(3, files[0], files[1],files[2]);
			printf("file %s, deleted.\n", file_path);

			/*release the lock*/
			size_t l = number_of_digit(st.st_ino) + strlen(".lock") + 1;
			char buf[l];
			memset(buf,0,l);
			
			if(snprintf(buf,l,"%ld.lock",st.st_ino) < 0){
				fprintf(stderr,"cannot release the lock");
				return STATUS_ERROR;
			}

			unlink(buf);
			return 0;
		} /* end of delete file path*/

		if (schema_def){ 
			/* add field to schema */
			/* locks here are released that is why we do not have to release them if an error occurs*/
			/*check if the fields are in limit*/
			int mode = check_handle_input_mode(schema_def,FCRT) | WR;
			int fields_count = 0; 
			switch(mode){
			case NO_TYPE_WR:
				if (!add_fields_to_schema(mode, fields_count, schema_def, NULL, &hd.sch_d)) {
					fprintf(stderr,"(%s): add_fields_to_schema() failed, %s:%d\n",prog,F,L-1);
					goto clean_on_error_5;
				}
				break;
			case TYPE_WR:
				fields_count = count_fields(schema_def,NULL);
				if (fields_count > MAX_FIELD_NR || hd.sch_d.fields_num + fields_count > MAX_FIELD_NR) {
					printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
					goto clean_on_error_5;
				}

				/*add field provided to the schema*/
				char *buffer = strdup(schema_def);
				char *buff_t = strdup(schema_def);
			
				if (!add_fields_to_schema(mode, fields_count, buffer, buff_t, &hd.sch_d)) {
					fprintf(stderr,"(%s): add_fields_to_schema() failed, %s:%d\n",prog,F,L-1);
					free(buffer), free(buff_t);
					goto clean_on_error_5;
				}

				free(buffer);
				free(buff_t);
				break;
			case HYB_WR:
				if (!add_fields_to_schema(mode, fields_count, schema_def, NULL, &hd.sch_d)) {
					fprintf(stderr,"(%s): add_fields_to_schema() failed, %s:%d\n",prog,F,L-1);
					goto clean_on_error_5;
				}
				break;
			default:
				goto clean_on_error_5;

			}


			/* acquire lock */
			int lock_f = 0;
			int r = 0;
			while(is_locked(3,fd_index,fd_schema,fd_data) == LOCKED);
			while((r = lock(fd_schema,WLOCK)) == WTLK);
			if(r == -1){
				fprintf(stderr,"can't acquire or release proper lock.\n");
				goto clean_on_error_5;
			}	

			lock_f = 1;	

			close_file(1,fd_schema);
			fd_schema = open_file(files[2],1);
			
			if(file_error_handler(1,fd_schema) != 0){
				goto clean_on_error_5;
			}

			if (!write_header(fd_schema, &hd)) {
				printf("write to file failed, %s:%d.\n", F, L - 2);
				goto clean_on_error_5;
			}


			/*release lock */
			while((r = lock(fd_schema,UNLOCK)) == WTLK);
			if(r == -1){
				fprintf(stderr,"can't acquire or release proper lock.\n");
				goto clean_on_error_5;
			}	
			lock_f = 0;
		
			printf("data added to schema!\n");
			close_file(3, fd_index, fd_data, fd_schema);
			
			return 0;

			clean_on_error_5:
			if(lock_f) while((r = lock(fd_schema,UNLOCK)) == WTLK);
			close_file(3, fd_index, fd_data,fd_schema);
			return STATUS_ERROR;
			
		} /* end of add new fields to schema path*/

		if (del) { 
			/* del a record in a file or the all content in the file */

			/* acquire write*/
			int lock_f = 0;
			int r = 0;
			while(is_locked(3,fd_index,fd_schema,fd_data) == LOCKED);
			while((r = lock(fd_index,WLOCK)) == WTLK);
			if(r == -1){
				fprintf(stderr,"can't acquire or release proper lock.\n");
				goto clean_on_error_6;
			}
			lock_f = 1;
			if (options) {
				if (option) {
					switch (convert_options(option)) {
					case ALL:
					{
						int ind = 0, *p_i_nr = &ind;
						if (!indexes_on_file(fd_index, p_i_nr)) {
							printf("er index nr,%s:%d.\n", F, L - 4);
							goto option_clean_on_error;
						}

						int buc_t = 0, *pbuck = &buc_t;
						if (!nr_bucket(fd_index, pbuck)) {
							printf("er index nr,%s:%d.\n", F, L - 4);
							goto option_clean_on_error;
						}
						/* create *p_i_nr of ht and write them to file*/
						HashTable *ht = calloc(*p_i_nr, sizeof(HashTable));
						if (!ht) {
							__er_calloc(F, L - 4);
							goto option_clean_on_error;
						}
						for (int i = 0; i < *p_i_nr; i++) {
							HashTable ht_n = {0};
							ht_n.size = *pbuck;
							ht_n.write = write_ht;

							ht[i] = ht_n;
						}

						close_file(1, fd_index);
						// opening with O_TRUNC
						fd_index = open_file(files[0], 1);

						/*  write the index file */

						if (!write_index_file_head(fd_index, *p_i_nr)) {
							printf("write to file failed,%s:%d",F, L - 2);
							free(ht);
							goto option_clean_on_error;
						}

						for (int i = 0; i < *p_i_nr; i++) {
							if (!write_index_body(fd_index, i, &ht[i])) {
								printf("write to file failed. %s:%d.\n", F, L - 2);
								free(ht);
								goto option_clean_on_error;
							}
						}
						/* release the lock */
						while((r = lock(fd_index,UNLOCK)) == WTLK);

						close_file(3,fd_index,fd_data,fd_schema);
						free(ht);
						printf("all record deleted from %s file.\n", file_path);
						return 0;

						option_clean_on_error:
						if(lock_f) while((r = lock(fd_index,UNLOCK)) == WTLK);
						close_file(3,fd_index,fd_data,fd_schema);
						return STATUS_ERROR;
					}
					default:
						printf("options not valid.\n");
						if(lock_f) while((r = lock(fd_index,UNLOCK)) == WTLK);
						close_file(3,fd_index,fd_data,fd_schema);
						return STATUS_ERROR;
					}
				}
			}/*end of option path*/

			/* delete the record specified by the -D option, in the index file*/
			HashTable *ht = NULL;
			int index = 0;
			int *p_index = &index;
			/* load all indexes in memory */
			if (!read_all_index_file(fd_index, &ht, p_index)) {
				goto clean_on_error_6;
			}

			int indexes = 0;
			if (!indexes_on_file(fd_index, &indexes)) {
				printf("indexes_on_file() failed, %s:%d.\n", F, L - 2);
				free_ht_array(ht,index);
				goto clean_on_error_6;
			}

			if (index_nr >= indexes) {
				printf("index out of bound.\n");
				free_ht_array(ht,index);
				goto clean_on_error_6;
			}

			int key_type = 0;
			void *key_conv = key_converter(key, &key_type);
			Node *record_del = NULL;
			if (key_conv) {
				record_del = delete (key_conv, &ht[index_nr], key_type);
				free(key_conv);
			}else if (key_type == STR) {
				record_del = delete ((void *)key, &ht[index_nr], key_type);
			} else {
				fprintf(stderr, "error key_converter().\n");
				free_ht_array(ht, index);
				goto clean_on_error_6;
			}

			if (!record_del) {
				printf("record %s not found.\n", key);
				free_ht_array(ht,index);
				goto clean_on_error_6;
			}
			
			/*this will save the record that we deleted,
			 * so we can undo this operations */
			if(record_del->key_type == STR){
 				if(journal(fd_index, 
						record_del->value, 
						(void*)record_del->key.s, 
						record_del->key_type, 
						J_DEL) == -1){
					fprintf(stderr,"(%s): failed to save del data.\n",prog);
				}
			}else{
				uint32_t kn = record_del->key.n;
				if(journal(fd_index, 
						record_del->value, 
						(void*)&kn, 
						record_del->key_type, 
						J_DEL) == -1){
					fprintf(stderr,"(%s): failed to save del data.\n",prog);
				}

			}

			printf("record %s deleted!.\n", key);
			free_ht_node(record_del);
			close_file(1, fd_index);
			fd_index = open_file(files[0], 1); // opening with o_trunc

			/*  write the index file */

			if (!write_index_file_head(fd_index, index)) {
				printf("write to file failed, %s:%d", F, L- 2);
				free_ht_array(ht,index);
				goto clean_on_error_6;
			}

			int i = 0;
			for (i = 0; i < index; i++){

				if (!write_index_body(fd_index, i, &ht[i])) {
					printf("write to file failed. %s:%d.\n", F, L - 2);
					free_ht_array(ht,index);
					goto clean_on_error_6;
				}

				destroy_hasht(&ht[i]);
			}

			while(lock(fd_index,UNLOCK) == WTLK);
			lock_f = 0;
			close_file(3, fd_index, fd_data,fd_schema);
			free(ht);
			return 0;
			
			clean_on_error_6:
			if(lock_f) while(lock(fd_index,UNLOCK) == WTLK);
			close_file(3, fd_index, fd_data, fd_schema);
			return STATUS_ERROR;
		} /*end of del path (either del the all content or a record )*/

		if (!update && data_to_add) { 
			/* append data to the specified file*/
		
			int fields_count = 0;
			unsigned char check = 0;
			struct Record_f rec = {0};
			int mode = check_handle_input_mode(data_to_add, FWRT) | WR;
			
			/*check schema*/
			if(mode == TYPE_WR){
				fields_count = count_fields(data_to_add,NULL);
				if(fields_count == 0){
					fprintf(stderr,"(%s):check input syntax.\n",prog);
					return STATUS_ERROR;
				}
				if (fields_count > MAX_FIELD_NR) {
					printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
					goto clean_on_error_7;
				}

				char *buffer = strdup(data_to_add);
				char *buf_t = strdup(data_to_add);
				char *buf_v = strdup(data_to_add);

				if(!buffer || !buf_t || !buf_v){
					fprintf(stderr,"(%s): strdup() failed, %s:%d.\n",prog,F,L-5);
					if(buffer) free(buffer);
					if(buf_t) free(buf_t);
					if(buf_v) free(buf_v);
					goto clean_on_error_7;
				}

				check = perform_checks_on_schema(mode,buffer, buf_t, buf_v, fields_count,
										file_path, &rec, &hd);
				free(buffer);
				free(buf_t);
				free(buf_v);
			
			} else {
				check = perform_checks_on_schema(mode,data_to_add, NULL, NULL, -1,
										file_path, &rec, &hd);
			}
			
			if (check == SCHEMA_ERR || check == 0) {
				goto clean_on_error_7;
			}
			/*
			 * here you have to save the schema file for 
			 * SCHEMA_NW 
			 * SCHEMA_EQ_NT 
			 * SCHEMA_NW_NT 
			 * SCHEMA_CT_NT
			 * */	
			int lock_f = 0;
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
				while(is_locked(3,fd_index,fd_schema,fd_data) == LOCKED);
				while((r = lock(fd_index,WLOCK)) == WTLK);
				if(r == -1){
					fprintf(stderr,"can't acquire or release proper lock.\n");
					goto clean_on_error_7;
				}
							lock_f = 1;close_file(1,fd_schema);
				fd_schema = open_file(files[2],1); /*open with O_TRUNCATE*/

				if(file_error_handler(1,fd_schema) != 0){
					goto clean_on_error_7;
				}

				if (!write_header(fd_schema, &hd)) {
					__er_write_to_file(F, L - 1);
					goto clean_on_error_7;
				}

			} /* end of update schema branch*/

			if(!lock_f){
				while(is_locked(3,fd_index,fd_schema,fd_data) == LOCKED);
				while((r = lock(fd_index,WLOCK)) == WTLK);
				if(r == -1){
					fprintf(stderr,"can't acquire or release proper lock.\n");
					goto clean_on_error_7;
				}
						lock_f = 1;}

			off_t eof = go_to_EOF(fd_data);
			if (eof == -1) {
				__er_file_pointer(F, L - 1);
				goto clean_on_error_7;
			}

			HashTable *ht = NULL;
			int index = 0;
			int *p_index = &index;
			/* load al indexes in memory */
			if (!read_all_index_file(fd_index, &ht, p_index)) {
				printf("read file failed. %s:%d.\n", F, L - 2);
				goto clean_on_error_7;
			}

			int key_type = 0;
			void *key_conv = key_converter(key, &key_type);
			if (key_type == UINT && !key_conv) {
				fprintf(stderr, "error to convert key");
				goto clean_on_error;
			} else if (key_type == UINT) {
				if (key_conv) {
					if (!set(key_conv, key_type, eof, &ht[0])){
						free(key_conv);
						free_ht_array(ht, index);
						goto clean_on_error_7;
					}
					free(key_conv);
				}
			} else if (key_type == STR) {
				/*create a new key value pair in the hash table*/
				if (!set((void *)key, key_type, eof, &ht[0])) {
					free_ht_array(ht, index);
					goto clean_on_error_7;
				}
			}

			if(!write_file(fd_data, &rec, 0, update)) {
				printf("write to file failed, main.c l %d.\n", __LINE__ - 1);
				free_ht_array(ht, index);
				goto clean_on_error_7;
			}

			free_record(&rec, rec.fields_num);
			close_file(1, fd_index);

			fd_index = open_file(files[0], 1); // opening with O_TRUNC

			/* write the new indexes to file */

			if (!write_index_file_head(fd_index, index)) {
				printf("write to file failed, %s:%d", F, L - 2);
				free_ht_array(ht, index);
				goto clean_on_error_7;
			}

			int i = 0;
			for (i = 0; i < index; i++) {

				if (!write_index_body(fd_index, i, &ht[i])) {
					printf("write to file failed. %s:%d.\n", F, L - 2);
					free_ht_array(ht, index);
					goto clean_on_error;
				}

				destroy_hasht(&ht[i]);
			}


			free(ht);
			while((r = lock(fd_index,UNLOCK)) == WTLK);
			if(r == -1){
				fprintf(stderr,"can't acquire or release proper lock.\n");
				lock_f = 0;
				goto clean_on_error_7;
			}


			close_file(3, fd_index, fd_data, fd_schema);
			printf("record %s wrote succesfully.\n", key);
			return 0;

			clean_on_error_7:
			if(lock_f) while(lock(fd_index,UNLOCK) == WTLK);
			close_file(3,fd_schema, fd_index, fd_data);
			free_record(&rec, rec.fields_num);
			return STATUS_ERROR;
		}

		if (update && data_to_add && key) { 
			/* updating an existing record */

			// 1 - check the schema with the one on file
			struct Record_f rec = {0};
			struct Record_f rec_old = {0};
			struct Record_f new_rec = {0};
			int mode = check_handle_input_mode(data_to_add,FWRT) | WR;
			int fields_count = 0;
			unsigned char check = 0;
			if(mode == TYPE_WR){
				fields_count = count_fields(data_to_add,NULL);

				if (fields_count > MAX_FIELD_NR) {
					printf("Too many fields, max %d each file definition.", MAX_FIELD_NR);
					goto clean_on_error;
				}

				char *buffer = strdup(data_to_add);
				char *buf_t = strdup(data_to_add);
				char *buf_v = strdup(data_to_add);

				check = perform_checks_on_schema(mode, buffer, buf_t, buf_v, fields_count,
						file_path, &rec, &hd);
				free(buffer);
				free(buf_t);
				free(buf_v);
			} else{
				check = perform_checks_on_schema(mode, data_to_add, NULL, NULL, -1,
						file_path, &rec, &hd);
			}

			if (check == SCHEMA_ERR || check == 0) {
				goto clean_on_error;
			}

			/* updating the schema if it is new */
			int r = 0;
			int lock_f = 0;
			if (check == SCHEMA_NW 		||
				check == SCHEMA_NW_NT 	||
				check == SCHEMA_EQ_NT 	||
				check == SCHEMA_CT_NT ){
				/*acquire lock */

				while(is_locked(3,fd_index,fd_schema,fd_data) == LOCKED);
				while((r = lock(fd_index,WLOCK)) == WTLK);
				if(r == -1){
					fprintf(stderr,"can't acquire or release proper lock.\n");
					goto clean_on_error;
				}
				lock_f = 1;
				close_file(1,fd_schema);
				fd_schema = open_file(files[2],1); /*open with O_TRUNCATE*/

				if(file_error_handler(1,fd_schema) != 0){
					goto clean_on_error;
				}

				if (!write_header(fd_schema, &hd)) {
					printf("can`t write header, main.c l %d.\n", __LINE__ - 1);
					goto clean_on_error;
				}

			} /*end of the update schema path in case the schema is new*/

			/* 2 - schema is good, thus load the old record into memory and update the fields if any
			 -- at this point you already checked the header, and you have an updated header,
				and the updated record in memory*/

			if(!lock_f){
				while(is_locked(3,fd_index,fd_schema,fd_data) == LOCKED);
				while((r = lock(fd_index,WLOCK)) == WTLK);
				if(r == -1){
					fprintf(stderr,"can't acquire or release proper lock.\n");
					goto clean_on_error;
				}
				lock_f = 1;
			}


			HashTable ht = {0};
			HashTable *p_ht = &ht;
			if (!read_index_nr(0, fd_index, &p_ht)) {
				printf("index file reading failed. %s:%d.\n", F, L - 1);
				goto clean_on_error;
			}

			off_t offset = 0;
			int key_type = 0;
			void *key_conv = key_converter(key, &key_type);
			if (key_type == UINT && !key_conv) {
				fprintf(stderr, "error to convert key");
				destroy_hasht(p_ht);
				goto clean_on_error;
			}else if (key_type == UINT){
				if (key_conv) {
					offset = get(key_conv, p_ht, key_type); /*look for the key in the ht */
					free(key_conv);
				}
			}else if (key_type == STR) {
				/*look for the key in the ht */
				offset = get((void *)key, p_ht, key_type);
			}

			if (offset == -1) {
				printf("record not found.\n");
				destroy_hasht(p_ht);
				goto clean_on_error;
			}

			destroy_hasht(p_ht);

			/*find record in the file*/
			if (find_record_position(fd_data, offset) == -1) {
				__er_file_pointer(F, L - 1);
				goto clean_on_error;
			}

			/*read the old record, aka the record that we want to update*/
			if(read_file(fd_data, file_path,&rec_old,hd.sch_d) == -1) {
				printf("reading record failed, %s:%d.\n",__FILE__, __LINE__ - 2);
				goto clean_on_error;
			}

			/*after each record in the file there is the offset of the next part of the record
				(if any) in the file*/
			off_t updated_rec_pos = get_update_offset(fd_data);
			if (updated_rec_pos == -1) {
				__er_file_pointer(F, L - 1);
				goto clean_on_error;
			}

			/*if update_rec_pos is bigger than 0, it means that this record is
				in different locations in the file,here we check for this case,
				and if the record is fragmented we read all the data
				and we store in the recs_old */

			struct Recs_old recs_old = {0};
			if (updated_rec_pos > 0) {
				/*first record position*/
				insert_rec(&recs_old,&rec_old,offset);
				free_record(&rec_old,rec_old.fields_num);

				if (find_record_position(fd_data, updated_rec_pos) == -1) {
					__er_file_pointer(F, L - 1);
					free_recs_old(&recs_old);
					goto clean_on_error;
				}

				struct Record_f rec_old_s = {0};
				if(read_file(fd_data, file_path,&rec_old_s,hd.sch_d) == -1) {
					printf("error reading file, main.c l %d.\n", __LINE__ - 2);
					free_recs_old(&recs_old);
					goto clean_on_error;
				}

				/*first updated record position*/
				insert_rec(&recs_old,&rec_old_s,updated_rec_pos);
				free_record(&rec_old_s,rec_old_s.fields_num);
				
				/*at this point we have the first two fragment of the record,
					 but potentially there could be many fragments,
					so we check with a loop that stops when the read of the
					update_rec_pos gives 0 or -1 (error)*/

				while ((updated_rec_pos = get_update_offset(fd_data)) > 0) {
					struct Record_f rec_old_new  = {0};
					if(read_file(fd_data, file_path, &rec_old_new, hd.sch_d) == -1){
						printf("error reading file, %s:%d.\n", F, L - 1);
						free_recs_old(&recs_old);
						goto clean_on_error;
					}

					insert_rec(&recs_old,&rec_old_new,updated_rec_pos);
					free_record(&rec_old_new,rec_old_new.fields_num);
				}

				/* here we have the all record in memory and we have
					to check which fields in the record we have to update*/

				int size_pos = recs_old.capacity + recs_old.dynamic_capacity;
				char positions[size_pos];
				memset(positions,0,size_pos);

				/* this function check all records from the file
				   against the new record setting the values that we have to update
				   and populates in  the char array positions. If an element contain 'y'
				   you have to update the field  at that index position of 'y'. */

				find_fields_to_update(&recs_old, positions, &rec);

				if (positions[0] != 'n' && positions[0] != 'y' && positions[0] != 'e') {
					printf("check on fields failed, %s:%d.\n", F, L - 1);
					free_recs_old(&recs_old);
					goto clean_on_error;
				}

				/* write the update records to file */
				int i = 0;
				int changed = 0;
				int no_updates = 0;
				unsigned short updates = 0; /* bool value if 0 no updates*/
				for (i = 0; i < size_pos; i++) {
					if (positions[i] == 'n')
						continue;

					if(positions[i] == 'e') {
						no_updates = 1;
						continue;
					}

					++updates;
					changed = 1;
					if (find_record_position(fd_data, recs_old.pos_u[i]) == -1) {
						__er_file_pointer(F, L - 1);
						free_recs_old(&recs_old);
						goto clean_on_error;
					}

					off_t right_update_pos = 0;
					if ((size_pos - i) > 1)
						right_update_pos = recs_old.pos_u[i + 1];

					if (size_pos - i == 1 && check == SCHEMA_NW) {
						right_update_pos = go_to_EOF(fd_data);
						if (find_record_position(fd_data, recs_old.pos_u[i]) == -1 ||
							right_update_pos == -1){
							printf("error file pointer, %s:%d.\n", F, L - 2);
							free_recs_old(&recs_old);
							goto clean_on_error;
						}
					}

					if (!write_file(fd_data, &recs_old.recs[i], right_update_pos, update)) {
						printf("error write file, %s:%d.\n", F, L - 1);
						free_recs_old(&recs_old);
						goto clean_on_error;
					}
				}
				
				if((check == SCHEMA_CT  ||  check == SCHEMA_CT_NT) && !changed) {
					if(no_updates){
						free_recs_old(&recs_old);
						goto clean_on_error;
					}
					off_t eof = go_to_EOF(fd_data); /* file pointer to the end*/
					if (eof == -1) {
						__er_file_pointer(F, L - 1);
						free_recs_old(&recs_old);
						goto clean_on_error;
					}

					/*writing the new part of the schema to the file*/
					int write_op = 0;
					if ((write_op = write_file(fd_data, &rec, 0, 0)) == 0) {
						printf("write to file failed, %s:%d.\n", F, L - 1);
						free_recs_old(&recs_old);
						goto clean_on_error;
					}

					if(write_op == NTG_WR){
						printf("record %s updated!\n", key);
						while(lock(fd_index,UNLOCK) == WTLK);
						close_file(3,fd_schema, fd_index, fd_data);
						free_record(&rec, rec.fields_num);
						free_recs_old(&recs_old);
						return 0;
					}	

					if(size_pos <= MAX_RECS_OLD_CAP){
						if(find_record_position(fd_data,recs_old.pos_u[size_pos-1]) == -1){
							__er_file_pointer(F, L - 1);
							free_recs_old(&recs_old);
							goto clean_on_error;
						}

						if(!write_file(fd_data,&recs_old.recs[size_pos-1],eof,update)){
							__er_file_pointer(F, L - 1);
							free_recs_old(&recs_old);
							goto clean_on_error;
						}
					 } else if(size_pos > MAX_RECS_OLD_CAP){
						 int index = size_pos - MAX_RECS_OLD_CAP -1;
						 if(find_record_position(fd_data,recs_old.pos_u_r[index]) == -1){
							__er_file_pointer(F, L - 1);
							free_recs_old(&recs_old);
							goto clean_on_error;
						}

						if(!write_file(fd_data,&recs_old.recs_r[index],eof,update)){
							__er_file_pointer(F, L - 1);
							free_recs_old(&recs_old);
							goto clean_on_error;
						}
					 }
				}

				if (check == SCHEMA_NW && updates > 0) {
					if (create_new_fields_from_schema(&recs_old, &rec, &hd.sch_d,
							 &new_rec, file_path) == -1) {
						printf("create new fileds failed,  %s:%d.\n", F, L - 1);
						free_recs_old(&recs_old);
						goto clean_on_error;
					}

					off_t eof = go_to_EOF(fd_data); /* file pointer to the end*/
					if (eof == -1) {
						__er_file_pointer(F, L - 1);
						free_recs_old(&recs_old);
						goto clean_on_error;
					}

					/*writing the new part of the schema to the file*/
					if (!write_file(fd_data, &new_rec, 0, 0)) {
						printf("write to file failed, %s:%d.\n", F, L - 1);
						free_recs_old(&recs_old);
						goto clean_on_error;
					}

					free_record(&new_rec, new_rec.fields_num);
					/*the position of new_rec in the old part of the record
					 was already set at line ????*/
				}
	
				if (check == SCHEMA_NW && updates == 0) {
					/* store the EOF value*/
					off_t eof = 0;
					if ((eof = go_to_EOF(fd_data)) == -1) {
						__er_file_pointer(F, L - 1);
						free_recs_old(&recs_old);
						goto clean_on_error;
					}

					/*re-write the record*/
					if(size_pos <= MAX_RECS_OLD_CAP){
						/*find the position of the last piece of the record*/
						if ((find_record_position(fd_data, recs_old.pos_u[size_pos - 1])) == -1) {
							__er_file_pointer(F, L - 1);
							free_recs_old(&recs_old);
							goto clean_on_error;
						}


						if (write_file(fd_data, &recs_old.recs[size_pos - 1], eof, update) == -1) {
							printf("write to file failed, %s:%d.\n", F, L - 1);
							free_recs_old(&recs_old);
							goto clean_on_error;
						}
					} else if (size_pos > MAX_RECS_OLD_CAP){
						int index = size_pos - MAX_RECS_OLD_CAP -1;

						/*find the position of the last piece of the record*/
						if ((find_record_position(fd_data, recs_old.pos_u_r[index])) == -1) {
							__er_file_pointer(F, L - 1);
							free_recs_old(&recs_old);
							goto clean_on_error;
						}


						if (write_file(fd_data, &recs_old.recs_r[index], eof, update) == -1) {
							printf("write to file failed, %s:%d.\n", F, L - 1);
							free_recs_old(&recs_old);
							goto clean_on_error;
						}

					}

					/*move to EOF*/
					if ((go_to_EOF(fd_data)) == -1) {
						__er_file_pointer(F, L - 1);
						free_recs_old(&recs_old);
						goto clean_on_error;
					}

					/*create the new record*/
					struct Record_f new_rec = {0};
					if (!create_new_fields_from_schema(&recs_old, &rec, &hd.sch_d,
								&new_rec, file_path)) {
						printf("create new fields failed, %s:%d.\n",__FILE__, __LINE__ - 2);
						free_recs_old(&recs_old);
						goto clean_on_error;
					}

					/*write the actual new data*/
					if (!write_file(fd_data, &new_rec, 0, 0)) {
						printf("write to file failed, %s:%d.\n", F, L - 1);
						free_recs_old(&recs_old);
						goto clean_on_error;
					}
					free_record(&new_rec, new_rec.fields_num);
				}

				printf("record %s updated!\n", key);
				while(lock(fd_index,UNLOCK) == WTLK);
				close_file(3,fd_schema, fd_index, fd_data);
				free_record(&rec, rec.fields_num);
				free_recs_old(&recs_old);
				return 0;
			} /*end of if(update_pos > 0)*/


			/*updated_rec_pos is 0, THE RECORD IS ALL IN ONE PLACE */
			memset(&new_rec,0,sizeof(struct Record_f));
			
			if(insert_rec(&recs_old,&rec_old,offset) == -1){
				printf("insert_rec, %s:%d.\n", F, L - 4);
				goto clean_on_error;
			}
			
			unsigned char comp_rr = compare_old_rec_update_rec(&recs_old, &rec, &new_rec,
									file_path, check,hd);
			if (comp_rr == 0) {
				printf("compare records failed, %s:%d.\n", F, L - 4);
				goto clean_on_error;
			}

			if (updated_rec_pos == 0 && comp_rr == UPDATE_OLD) {
				// set the position back to the record
				if (find_record_position(fd_data, recs_old.pos_u[0]) == -1) {
					__er_file_pointer(F, L - 1);
					goto clean_on_error;
				}

				// write the updated record to the file
				if (!write_file(fd_data, &recs_old.recs[0], 0, update)) {
					printf("write to file failed, %s:%d.\n", F, L - 1);
					goto clean_on_error;
				}

				printf("record %s updated!\n", key);
				while(lock(fd_index,UNLOCK) == WTLK);
				close_file(2, fd_index, fd_data);
				free_record(&rec, rec.fields_num);
				free_record(&rec_old, rec_old.fields_num);
				free_recs_old(&recs_old);
				return 0;
			}

			/*updating the record but we need to write some data in another place in the file*/
			if (updated_rec_pos == 0 && comp_rr == UPDATE_OLDN && 
					(check == SCHEMA_CT || check == SCHEMA_CT_NT)) {

				off_t eof = 0;
				if ((eof = go_to_EOF(fd_data)) == -1) {
					__er_file_pointer(F, L - 1);
					goto clean_on_error;
				}

				// put the position back to the record
				if (find_record_position(fd_data, recs_old.pos_u[0]) == -1) {
					__er_file_pointer(F, L - 1);
					goto clean_on_error;
				}

				// update the old record :
				if (!write_file(fd_data, &recs_old.recs[0], eof, update)) {
					printf("can't write record, %s:%d.\n", __FILE__, __LINE__ - 1);
					goto clean_on_error;
				}

				eof = go_to_EOF(fd_data);
				if (eof == -1) {
					__er_file_pointer(F, L - 1);
					goto clean_on_error;
				}

				/*passing update as 0 becuase is a "new_rec", (right most paramaters) */
				if (!write_file(fd_data, &rec, 0, 0)) {
					printf("can't write record, main.c l %d.\n", __LINE__ - 1);
					goto clean_on_error;
				}

				printf("record %s updated!\n", key);
				/*free the lock */
				while(lock(fd_index,UNLOCK) == WTLK);
				close_file(3, fd_schema, fd_index, fd_data);
				free_record(&rec, rec.fields_num);
				free_record(&rec_old, rec_old.fields_num);
				free_record(&new_rec, new_rec.fields_num);
				free_recs_old(&recs_old);
				return 0;
			}

			return 0; /*this should be unreachable*/
			clean_on_error:
			if(lock_f) while(lock(fd_index,UNLOCK) == WTLK);
			close_file(3, fd_schema, fd_index, fd_data);
			free_record(&rec, rec.fields_num);
			//free_record(&rec_old, rec_old.fields_num);
			free_record(&new_rec, new_rec.fields_num);
			return STATUS_ERROR;
		} /*end of update path*/

		/* reading the file to show data */
		if (list_def) { /* show file definitions */
			print_schema(hd.sch_d);
			close_file(1, fd_schema);
			return 0;
		}

		/*display the keys in the file*/
		if (list_keys)
		{
			while(is_locked(3,fd_index,fd_schema,fd_data) == LOCKED);
			
			HashTable ht = {0};
			HashTable *p_ht = &ht;
			if (!read_index_nr(index_nr, fd_index, &p_ht)) {
				printf("reading index file failed, %s:%d.\n", F, L - 1);
				close_file(3,fd_schema, fd_index, fd_data);
				return STATUS_ERROR;
			}

			struct Keys_ht *keys_data = keys(p_ht);
			char keyboard = '0';
			int end = len(ht), i = 0, j = 0;
			for (i = 0, j = i; i < end; i++) {
				switch (keys_data->types[i])
				{
				case STR:
					printf("%d. %s\n", ++j, (char *)keys_data->k[i]);
					break;
				case UINT:
					printf("%d. %u\n", ++j, *(uint32_t *)keys_data->k[i]);
					break;
				default:
					break;
				}
				if (i > 0 && (i % 20 == 0))
					printf("press return key. . .\n"
						   "enter q to quit . . .\n"),
						keyboard = (char)getc(stdin);

				if (keyboard == 'q')
					break;
			}

			destroy_hasht(p_ht);
			close_file(3,fd_schema, fd_index, fd_data);
			free_keys_data(keys_data);
			return 0;
		}

		if (key) { 
			/*display record*/
			while(is_locked(3,fd_index,fd_data) == LOCKED);

			HashTable ht = {0};
			HashTable *p_ht = &ht;
			if (!read_index_nr(0, fd_index, &p_ht)) {
				printf("reading index file failed, %s:%d.\n", F, L - 1);
				close_file(3, fd_index, fd_data);
				return STATUS_ERROR;
			}

			off_t offset = 0;
			int key_type = 0;
			void *key_conv = key_converter(key, &key_type);
			if (key_type == UINT && !key_conv) {
				fprintf(stderr, "error to convert key");
				destroy_hasht(p_ht);
				close_file(3, fd_schema,fd_index, fd_data);
				return STATUS_ERROR;
			} else if (key_type == UINT) {
				if (key_conv) {
					offset = get(key_conv, p_ht, key_type); /*look for the key in the ht */
					free(key_conv);
				}
			} else if (key_type == STR) {
				offset = get((void *)key, p_ht, key_type); /*look for the key in the ht */
			}

			if (offset == -1) {
				printf("record not found.\n");
				destroy_hasht(p_ht);
				close_file(3, fd_schema,fd_index, fd_data);
				return STATUS_ERROR;
			}

			destroy_hasht(p_ht);
			if (find_record_position(fd_data, offset) == -1) {
				__er_file_pointer(F, L - 1);
				close_file(3, fd_index, fd_data);
				return STATUS_ERROR;
			}

			struct Record_f rec = {0};
			if(read_file(fd_data, file_path, &rec, hd.sch_d) == -1) {
				printf("read record failed, %s:%d.\n",__FILE__, __LINE__ - 1);
				close_file(3, fd_schema,fd_index, fd_data);
				return STATUS_ERROR;
			}

			off_t offt_rec_up_pos = get_file_offset(fd_data);
			off_t update_rec_pos = get_update_offset(fd_data);
			if (update_rec_pos == -1) {
				close_file(3, fd_schema,fd_index, fd_data);
				free_record(&rec, rec.fields_num);
				return STATUS_ERROR;
			}

			int counter = 1;
			struct Record_f *recs = NULL;
			struct Record_f rec_n = {0};

			if (update_rec_pos > 0) {
				recs = calloc(counter, sizeof(struct Record_f));
				if (!recs) {
					printf("calloc failed, main.c l %d.\n", __LINE__ - 2);
					free_record(&rec, rec.fields_num);
					close_file(3, fd_schema,fd_index, fd_data);
					return STATUS_ERROR;
				}

				recs[0] = rec;

				// set the file pointer back to update_rec_pos (we need to read it)
				//  again for the reading process to be successful
				if (find_record_position(fd_data, offt_rec_up_pos) == -1) {
					__er_file_pointer(F, L - 1);
					free_records(counter, recs);
					close_file(3, fd_schema,fd_index, fd_data);
					return STATUS_ERROR;
				}

				while ((update_rec_pos = get_update_offset(fd_data)) > 0) {
					counter++;
					struct Record_f *recs_new = realloc(recs, counter * sizeof(struct Record_f));
					if (!recs_new) {
						printf("realloc failed, main.c l %d.\n", __LINE__ - 2);
						free_records(counter, recs);
						close_file(3, fd_schema,fd_index, fd_data);
						return STATUS_ERROR;
					}
					recs = recs_new;
					if (find_record_position(fd_data, update_rec_pos) == -1) {
						__er_file_pointer(F, L - 1);
						close_file(3, fd_schema,fd_index, fd_data);
						free_records(counter, recs);
						return STATUS_ERROR;
					}

					memset(&rec_n,0,sizeof(struct Record_f));
					if(read_file(fd_data, file_path,&rec_n,hd.sch_d) == -1) {
						printf("read record failed, %s:%d.\n",__FILE__, __LINE__ - 2);
						free_records(counter, recs);
						close_file(3, fd_schema,fd_index, fd_data);
						return STATUS_ERROR;
					}
					recs[counter - 1] = rec_n;
				}
			}

			if (counter == 1) {
				print_record(1, &rec);
				free_record(&rec, rec.fields_num);
			}else {
				print_record(counter, recs);
				free_records(counter, recs);
			}

			close_file(3, fd_schema,fd_index, fd_data);
			return 0;
		}
	}

	return 0;
} /*-- program end --*/
