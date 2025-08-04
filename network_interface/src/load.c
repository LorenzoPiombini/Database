#include <stdio.h>
#include <stdlib.h>
#define _GNU_SOURCE             /* See feature_test_macros(7) */
#include <fcntl.h>              /* Definition of O_* constants */
#include <unistd.h>
#include <sys/wait.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <crud.h>
#include <lock.h>
#include <file.h>
#include <str_op.h>
#include "key.h"
#include "load.h"
#include "request.h"
#include "end_points.h"

static char prog[] = "db_listener";
static char *convert_json(char* body);

int load_resource(struct Request *req, struct Content *cont)
{
	int resource = map_end_point(req->resource); 
	if(resource == -1) return -1;

	switch(req->method){
	case POST:
	{
		switch(resource){
		case NEW_SORD:
		{
			int pipefd[2];
			int wstatus = 0;
			pid_t w = -1;
			if(pipe(pipefd) == -1){
				/*log errors*/
				return -1;
			}

			pid_t child = fork();
			if(child == -1){
				/*log errors*/
				return -1;
			}
			if(child == 0){
				/*closing pipe that child does not need
				 * in this case is the READ pipe*/
				close(pipefd[0]); 
				/*save the sales order in the db */
				char *db = NULL;
				if(req->req_body.d_cont)
					db = convert_json(req->req_body.d_cont);
				else
					db = convert_json(req->req_body.content);

				assert(db != NULL);

				printf("%s\n",db);

				/*process the string and separate the two file sintax*/

				char *lines_start = strstr(db,"sales_orders_lines");
				if(!lines_start) return -1;

			
				size_t lines_len = strlen((lines_start + strlen("sales_orders_lines:")));
				char orders_line[lines_len+1];
				memset(orders_line,0,lines_len+1);
				strncpy(orders_line,lines_start+strlen("sales_orders_lines:"),lines_len);

				char orders_head[((lines_start - db) - strlen("sales_orders_head:")) + 1];
				memset(orders_head,0,((lines_start - db) -strlen("sales_orders_head:")) +1);
				strncpy(orders_head,&db[strlen("sales_orders_head:")],((lines_start - db)-strlen("sales_orders_head:")));

				/* writing to database*/
				int fds[3];
				memset(fds,-1,sizeof(int)*3);
				int lock_f = 0;
				char files[3][1024] = {0};
				struct Record_f rec = {0};
				struct Schema sch = {0};
				memset(sch.types,-1,sizeof(int)*MAX_FIELD_NR);
				struct Header_d hd = {0, 0, sch};


				if(open_files(SALES_ORDERS_H,fds, files, -1) == -1) exit(-1);
				if(is_db_file(&hd,fds) == -1) goto post_exit_error; 
				if(check_data(SALES_ORDERS_H,orders_head,fds,files,&rec,&hd,&lock_f) == -1) goto post_exit_error;
				uint32_t key = generate_numeric_key(fds,NEW_SORD);
				if(!key) return -1; 
				if(write_record(fds,&key,UINT,&rec, 0,files,&lock_f,-1) == -1) goto post_exit_error;

				free_record(&rec,rec.fields_num);
				if(lock_f){
					while(lock(fds[0],UNLOCK) == WTLK);
					lock_f = 0;
				}
				close_file(3,fds[0],fds[1],fds[2]);

				memset(&rec,0,sizeof(struct Record_f));
				memset(&sch,0,sizeof(struct Schema));
				memset(&hd,0,sizeof(struct Header_d));
				memset(files,0,3*1024);
				memset(fds,-1,sizeof(int)*3);
				memset(sch.types,-1,sizeof(int)*MAX_FIELD_NR);
				hd.sch_d = sch;
			
				if(open_files(SALES_ORDERS_L,fds, files, -1) == -1) goto post_exit_error;
				if(is_db_file(&hd,fds) == -1) goto post_exit_error; 
				char *sub_str = NULL;
				uint16_t count = 1;
				while((sub_str = get_sub_str("[","]",orders_line))){
					struct String cpy_str = {0};
					init(&cpy_str,sub_str);

					/*create the key for each line*/
					size_t line_key_sz = number_of_digit(count) + number_of_digit(key) + 1;
					struct String key_each_line = {0};
					init(&key_each_line,NULL);
					if(snprintf(key_each_line.base,++line_key_sz,"%u/%u",key,count) == -1)goto post_exit_error;

					/* write each line */
					if(check_data(SALES_ORDERS_L,&cpy_str.base[2],fds,files,&rec,&hd,&lock_f) == -1) goto post_exit_error;
					cpy_str.close(&cpy_str);

					if(write_record(fds,&key_each_line.base,STR,&rec,0,files,&lock_f,-1) == -1) goto post_exit_error;
					free_record(&rec,rec.fields_num);
					memset(&rec,0,sizeof(struct Record_f));
					key_each_line.close(&key_each_line);

					/*clear the order_line_array to make sure we get the right result*/
					char *find = strstr(orders_line,"[");
					if(find) *find = ' ';
					find = strstr(orders_line,"]");
					if(find) * find = ' ';
					count++;
				}
			
				if(lock_f) {
					while(lock(fds[0],UNLOCK) == WTLK);
					lock_f = 0;
				}
			

				close_file(3,fds[0],fds[1],fds[2]);
				if(snprintf(cont->cnt_st,1024,"{ \"message\" : \"order nr %d, created!\"}",key) == -1) exit(-1);
				if(write(pipefd[1],cont->cnt_st,strlen(cont->cnt_st)) == -1) goto post_exit_error;
				close(pipefd[1]);
				exit(0);
post_exit_error:
				free_record(&rec,rec.fields_num);
				if(lock_f) while(lock(fds[0],UNLOCK) == WTLK);
				close_file(3,fds[0],fds[1],fds[2]);
				exit(-1);
			} else {                    /* Code executed by parent */
				close(pipefd[1]);
				do{
					w = waitpid(child, &wstatus,0);
					if(w == -1){
						/*log errors*/
						return -1;
					}

				}while(!WIFEXITED(wstatus));

				if(WEXITSTATUS(wstatus) == 0){
					
					if(read(pipefd[0],cont->cnt_st,4096) == -1){
						/*log errors*/
						close(pipefd[0]);
						return -1;
					}
					close(pipefd[0]);
					return 0;
				}else{ 
					/*log errors*/
					close(pipefd[0]);
					return -1;
				}
			}
		}
		case CUSTOMER:
		{

		}
		case S_ORD:
		{
			break;
		}
		default:
			break;
		}
		break;
	}
	case GET:
	{
		switch(resource){
		case S_ORD:
		{
			int pipefd[2];
			int wstatus = 0;
			pid_t w = -1;
			if(pipe(pipefd) == -1){
				/*log errors*/
				return -1;
			}

			pid_t child = fork();
			if(child == -1){
				/*log errors*/
				return -1;
			}

			if(child == 0){
				close(pipefd[0]);
				/*get the all keys for the sales order file*/
				int fds[3];
				memset(fds,-1,sizeof(int)*3);
				int lock_f = 0;
				char files[3][1024] = {0};
				if(open_files(SALES_ORDERS_H,fds, files, ONLY_INDEX) == -1) exit(-1);
				/*lock file*/
				int r = -1;
				while((is_locked(1,fds[0])) == LOCKED);
				while((r = lock(fds[0],WLOCK)) == WTLK);
				if(r == -1){
					/*change file*/
					fprintf(stderr,"can't acquire or release proper lock.\n");
					close(fds[0]);
					exit(-1);
				}
				lock_f = 1;
				char *keys = get_all_keys_for_file(fds,0);
				if(!keys){
					/*log errors*/	
					exit(-1);
				}

				size_t l = strlen(keys);
				if(l > MAX_CONT_SZ) {
					cont->cnt_dy = keys;
					cont->size = l;
					if(lock_f) while(lock(fds[0],UNLOCK) == WTLK);
					lock_f = 0;
					close(fds[0]);
					if(write(pipefd[1],cont->cnt_dy,cont->size) == -1) exit(-1);
					close(pipefd[1]);
					exit(0);
				}else{
					size_t mes_l = strlen("{\"message\" : ") + l + strlen(" }");
					if(snprintf(cont->cnt_st,mes_l,"{ \"message\" : %s",keys) == -1){
						free(keys);
						if(lock_f) while(lock(fds[0],UNLOCK) == WTLK);
						lock_f = 0;
						close(fds[0]);
						close(pipefd[1]);
						exit(-1);
					}
					strncpy(&cont->cnt_st[strlen(cont->cnt_st)]," }",strlen(" }")+1);
					cont->size = mes_l;
					free(keys);
					if(lock_f) while(lock(fds[0],UNLOCK) == WTLK);
					lock_f = 0;
					close(fds[0]);

					if(write(pipefd[1],cont->cnt_st,strlen(cont->cnt_st)) == -1) exit(-1);
					close(pipefd[1]);
					exit(0);
				}
			}else{
				close(pipefd[1]);
				do{
					w = waitpid(child,&wstatus,0);
					if(w == -1){
						/*log errors*/
						return -1;
					}

				}while(!WIFEXITED(wstatus));

				if(WEXITSTATUS(wstatus) == 0){
					if(read(pipefd[0],cont->cnt_st,4096) == -1){
						/*log error*/
						close(pipefd[0]);
						return -1;
					}
					close(pipefd[0]);
					return 0;
				} else {
						/*log error*/
					close(pipefd[0]);
					return -1;
				}
			}
			break;
		}
		default:
			break;
		}
		break;
	}
	default:
		break;	
	}
	return -1;
}

void clear_content(struct Content *cont){
	if(cont->cnt_dy) free(cont->cnt_dy);

	memset(cont->cnt_st,0,MAX_CONT_SZ);
	cont->size = 0;
}	

static char *convert_json(char* body)
{
	static char db_entry[1024] = {0};
	memset(db_entry,0,1024);
	int array = 0;
	int n_array = 0;
	int n_obj_arr = 0;
	int n_obj = 0;
	int string = 0;
	int i = 0;
	for(char *p = &body[1]; *p != '\0'; p++){
		if(*p == ']'){
			if(n_array) 
				n_array = 0;
			else
				array = 0;

			continue;
		}

		if(*p == ',' && !string) {
			db_entry[i] = ':';
			i++;
			continue;
		}

		if(*p == '}'){
			if(n_obj_arr){
				n_obj_arr = 0;
				db_entry[i] = ']';
				i++;
				/* 
				 * the following if statment check if we have more
				 * than one object in the array
				 * and format the db_entry accordingly
				 * */
				if(*(p + 1) == ','){
					db_entry[i] = ',';
					i++;
					p++;
				}
			}else if (n_obj){
				n_obj = 0;
			}
			continue;
		}

		if(*p == '{'){
			if(array){
				n_obj_arr = 1;
				/*file as a field syntax*/
				db_entry[i] = '[';
				i++;
				db_entry[i] = 'w';
				i++;
				db_entry[i] = '|';
				i++;
			}else{	
				n_obj = 1;
			}
		}

		if(*p == '['){
			if(array)
				n_array = 1;
			else
				array = 1;
			continue;
		}

		if(*p == ':' && string == 0){
			db_entry[i] = *p;
			i++;
			continue;
		}

		if(*p == ' ' && !string) continue;
		if(*p == '"') {
			if(string)
				string = 0;
			else
				string = 1;
			continue;
		}

		if(string){
			db_entry[i] = *p;
			i++;
			continue;
		}	
	}

	return &db_entry[0];
}
