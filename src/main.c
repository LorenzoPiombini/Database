#include <stdio.h>
#include <getopt.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
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
#include "crud.h"


char prog[] = "db";
int main(int argc, char *argv[])
{
	if (argc < 2) {
		print_usage(argv);
		return 1;
	}

	__UTILITY = 1;
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
	unsigned char import_from_data = 0;
	unsigned char options = 0;
	unsigned char index_add = 0;
	unsigned char file_field = 0;
	/*------------------------------------------*/

	/* parameters populated with the flag from getopt()*/
	int c = 0;
	char file_path[1024] = {0};
	char data_to_add[1024] = {0};
	char key[1024] = {0};
	char schema_def[1024] = {0};
	char txt_f[1024] = {0};

	char *option = NULL;
	int bucket_ht = 0;
	int indexes = 0;
	int index_nr = 0;
	int only_dat = 0;

	while ((c = getopt(argc, argv, "nItAf:F:a:k:D:R:uleB:b:s:x:c:i:o:")) != -1)
	{
		switch (c){
		case 'a':
		{
			size_t l = strlen(optarg);
			strncpy(data_to_add,optarg,l);
			break;
		}
		case 'n':
			new_file = 1; // true
			break;
		case 'f':
		{
			size_t l = strlen(optarg);
			strncpy(file_path,optarg,l);
			break;
		}
		case 'F':
		{
			size_t l = strlen(optarg);
			strncpy(file_path,optarg,l);
			file_field = 1;
			break;
		}
		case 'k':
		{
			size_t l = strlen(optarg);
			strncpy(key,optarg,l);
			break;
		}
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
		{
			size_t l = strlen(optarg);
			strncpy(schema_def,optarg,l);
			break;
		}
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
		{
			build = 1;
			size_t l = strlen(optarg);
			strncpy(txt_f,optarg,l);
			break;
		}
		case 'B':
		{
			import_from_data = 1;
			size_t l = strlen(optarg);
			strncpy(txt_f,optarg,l);
			break;
		}
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
		{
			create = 1;
			size_t l = strlen(optarg);
			strncpy(txt_f,optarg,l);
			break;
		}
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
			build, create, options, index_add, file_field,import_from_data)) {
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

	if(import_from_data){
		if(import_data_to_system(txt_f) == -1) {
			fprintf(stderr,"(%s): could not import data from '%s'.\n",prog,txt_f);
			return -1;
		}
		return 0;
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
		
		}else if(file_field){
			fd_schema = create_file(files[2]);
			/*
			 * file_error_handler will close the file descriptors if there are issues
			 *  and print error messages to the console
			 *  */
			if (file_error_handler(1, fd_schema) != 0) return STATUS_ERROR;
		}else{
			fd_index = create_file(files[0]);
			fd_data = create_file(files[1]);
			fd_schema = create_file(files[2]);
			/*
			 * file_error_handler will close the file descriptors if there are issues
			 *  and print error messages to the console
			 *  */
			if (file_error_handler(3, fd_index, fd_data, fd_schema) != 0) return STATUS_ERROR;
		}

		if (schema_def[0] != '\0' ) { 
			/* case when user creates a file with only file definition*/

			int mode = check_handle_input_mode(schema_def, FCRT) | DF;
			if(mode == -1){
				fprintf(stderr,"(%s): check the input, value might be missng.\n",prog);
				goto clean_on_error_1;
			}
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

			if(file_field){
				fprintf(stdout,"(%s): File created successfully!\n",prog);
				close_file(1, fd_schema);
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

			close_file(3, fd_index, fd_data,fd_schema);
			return 0;

			clean_on_error_1:
			close_file(3, fd_index, fd_data,fd_schema);
			delete_file(3, files[0], files[1], files[2]);
			return STATUS_ERROR;
		}

		if (data_to_add[0] != '\0') { 
			/* creates a file with full definitons (fields and value)*/
			int mode = check_handle_input_mode(data_to_add, FWRT) | WR;
			if(mode == -1){
				fprintf(stderr,"(%s): check the input, value might be missng.\n",prog);
				goto clean_on_error_2;
			}
			int fields_count = 0; 

			/* init he Schema structure*/
			struct Schema sch = {0};
			struct Record_f rec = {0};
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

				size_t l = strlen(data_to_add);
				char cpy[l + 1];
				memset(cpy,0,l + 1);
				strncpy(cpy,data_to_add,l);
					
				if(parse_d_flag_input(file_path, fields_count,cpy, &sch, 0,&rec,NULL) == -1) {
					fprintf(stderr,"(%s): error creating the record, %s:%d.\n",prog, __FILE__, __LINE__ - 1);
					goto clean_on_error_2;
				}

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


		/*open the file*/
		int fds[3];
		memset(fds,-1,sizeof(int)*3);
		char files[3][MAX_FILE_PATH_LENGTH] = {0};  
		/* init the Schema structure*/
		struct Schema sch = {0};
		memset(sch.types,-1,sizeof(int)*MAX_FIELD_NR);
		struct Header_d hd = {0, 0, sch};

		if (list_def) {
			if(open_files(file_path,fds,files,ONLY_SCHEMA) == -1) return STATUS_ERROR;

			fd_schema = fds[2];
			while((is_locked(1,fd_schema)) == LOCKED);
			/* ensure the file is a db file */
			if (is_db_file(&hd, fds) == -1) {
				close_file(1,fd_schema);
				return STATUS_ERROR;
			}
		} else {
			if(open_files(file_path,fds,files,0) == -1) return STATUS_ERROR;
			fd_index = fds[0];
			fd_data = fds[1];
			fd_schema = fds[2]; 
			

			while((is_locked(3,fd_schema,fd_data,fd_index)) == LOCKED);
			/* ensure the file is a db file */
			if (is_db_file(&hd, fds) == -1) {
				close_file(3,fd_schema,fd_data,fd_index);
				return STATUS_ERROR;
			}
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

		if (schema_def[0] != '\0' ){ 
			/* add field to schema */
			/* locks here are released that is why we do not have to release them if an error occurs*/
			/*check if the fields are in limit*/
			int mode = check_handle_input_mode(schema_def,FCRT) | WR;
			if(mode == -1){
				fprintf(stderr,"(%s): check the input, value might be missng.\n",prog);
				goto clean_on_error_5;
			}
			int fields_count = 0; 
			switch(mode){
			case NO_TYPE_WR:
				if (!add_fields_to_schema(mode, fields_count, schema_def, NULL, &hd.sch_d)) {
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

			/* acquire lock*/
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
			if(record_del->key.type == STR){
 				if(journal(fd_index, 
						record_del->value, 
						(void*)record_del->key.k.s, 
						record_del->key.type, 
						J_DEL) == -1){
					fprintf(stderr,"(%s): failed to save del data.\n",prog);
				}
			}else{
				uint32_t kn = record_del->key.k.n;
				if(journal(fd_index, 
						record_del->value, 
						(void*)&kn, 
						record_del->key.type, 
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

		if (!update && (data_to_add[0] != '\0')) { 
			/* append data to the specified file*/
		
			struct Record_f rec = {0};

			int lock_f = 0;
			int check = 0;
			int r =0;
			if(( check = check_data(file_path,data_to_add,fds,files,&rec,&hd,&lock_f)) == -1) goto clean_on_error_7;

			if(!lock_f){
				while(is_locked(3,fd_index,fd_schema,fd_data) == LOCKED);
				while((r = lock(fd_index,WLOCK)) == WTLK);
				if(r == -1){
					fprintf(stderr,"can't acquire or release proper lock.\n");
					goto clean_on_error_7;
				}
				lock_f = 1;
			}

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
				goto clean_on_error_7;
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
					goto clean_on_error_7;
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

		if (update && (data_to_add[0] != '\0') && key[0] != '\0' ) { 
			/* updating an existing record */
			struct Record_f rec = {0};

			int lock_f = 0;
			int check = 0;
			if((check = check_data(file_path,data_to_add,fds,files,&rec,&hd,&lock_f)) == -1) goto clean_on_error;

			if(update_rec(file_path,fds,key,&rec,hd,check,&lock_f) == -1) goto clean_on_error;

			printf("record %s updated!\n", key);
			while(lock(fd_index,UNLOCK) == WTLK);
			close_file(3,fd_schema, fd_index, fd_data);
			free_record(&rec, rec.fields_num);
			return 0;

			clean_on_error:
			if(lock_f) while(lock(fd_index,UNLOCK) == WTLK);
			close_file(3, fd_schema, fd_index, fd_data);
			free_record(&rec, rec.fields_num);
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
				close_file(3,fd_schema, fd_index, fd_data);
				return STATUS_ERROR;
			}

			struct Keys_ht keys_data = {0};
			if(keys(p_ht,&keys_data) == -1){
				fprintf(stderr,"(%s): cannot get all keys from index file.\n",prog);
				close_file(3,fd_schema, fd_index, fd_data);
				return STATUS_ERROR;
			}
			char keyboard = '0';
			int end = len(ht), i = 0, j = 0;
			for (i = 0, j = i; i < end; i++) {
				switch (keys_data.keys[i].type){
				case STR:
					printf("%d. %s\n", ++j, keys_data.keys[i].k.s);
					break;
				case UINT:
				{
					printf("%d. %u   ", ++j, keys_data.keys[i].k.n);
					print_pack_str(keys_data.keys[i].paked_k);
					printf("\n");

				//	printf("%d. %u\n", ++j, *(uint32_t *)keys_data->k[i]);
					break;
				}
				default:
					break;
				}
				if (i > 0 && (i % 20 == 0)){
					printf("press return key. . .\nenter q to quit . . .\n");
					keyboard = (char)getc(stdin);
				}
				if (keyboard == 'q')
					break;
			}

			destroy_hasht(p_ht);
			close_file(3,fd_schema, fd_index, fd_data);
			free_keys_data(&keys_data);
			return 0;
		}

		if (key[0] != '\0' ) { 
			/*display record*/
			while(is_locked(3,fd_index,fd_data,fd_schema) == LOCKED);

			struct Record_f rec = {0};
			if(get_record(-1,file_path,&rec,(void *)key, hd,fds) == -1){
				free_record(&rec,rec.fields_num);
				close_file(3, fd_schema,fd_index, fd_data);
				return STATUS_ERROR;
			}

			if (rec.count == 1) {
				print_record(1, rec);
				free_record(&rec, rec.fields_num);
			}else {
				print_record(rec.count, rec);
				free_record(&rec, rec.fields_num);
			}

			if(ram.mem) close_ram_file(&ram);
			close_file(3, fd_schema,fd_index, fd_data);
			return 0;
		}
	}

	return 0;
} /*-- program end --*/
