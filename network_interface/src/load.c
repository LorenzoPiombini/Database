#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include "key.h"
#include "crud.h"
#include "load.h"
#include "lock.h"
#include "file.h"
#include "request.h"
#include "end_points.h"

static char prog[] = "db_listener";
static char *convert_json(char* body);

int load_resource(struct Request *req, char *json_obj_response)
{
	int resource = map_end_point(req->resource); 
	if(resource == -1) return -1;

	switch(req->method){
	case POST:
	{
		switch(resource){
		case NEW_SORD:
		{
			/*save the sales order in the db */
			char *db = NULL;
			if(req->req_body.d_cont)
				db = convert_json(req->req_body.d_cont);
			else
				db = convert_json(req->req_body.content);

			assert(db != NULL);

			printf("%s\n",db);

			/*process the string and separate the two file  sintax*/

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

			if(open_files(SALES_ORDERS_H,fds, files, -1) == -1) return -1;
			uint32_t key = generate_numeric_key(fds,NEW_SORD);
			if(!key) return -1; 
			size_t l = strlen("head_id:") + number_of_digit((int)key);
			char head_id_field[l+1];
			memset(head_id_field,0,l+1);

			if(is_db_file(&hd,fds) == -1) goto post_exit_error; 
			if(check_data(SALES_ORDERS_H,orders_head,fds,files,&rec,&hd,&lock_f) == -1) goto post_exit_error;
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
			if(check_data(SALES_ORDERS_L,orders_line,fds,files,&rec,&hd,&lock_f) == -1) goto post_exit_error;
			if(write_record(fds,&key,UINT,&rec, 0,files,&lock_f,-1) == -1) goto post_exit_error;
			
			free_record(&rec,rec.fields_num);
			memset(&rec,0,sizeof(struct Record_f));
			if(snprintf(head_id_field,l,"%s%d","head_id:",key) == -1) goto post_exit_error;
			int check = 0;
			if((check = check_data(SALES_ORDERS_L,head_id_field,fds,files,&rec,&hd,&lock_f)) == -1) goto post_exit_error;
			if(update_rec(SALES_ORDERS_L,fds,(void*)&key,UINT,&rec,hd,check,&lock_f) == -1) goto post_exit_error;
			
			if(lock_f) {
				while(lock(fds[0],UNLOCK) == WTLK);
				lock_f = 0;
			}

			free_record(&rec,rec.fields_num);
			close_file(3,fds[0],fds[1],fds[2]);
			if(snprintf(json_obj_response,1024,"{ \"message\" : \"order nr %d, created!\"}",key) == -1) return -1;
			return 0;
post_exit_error:
			free_record(&rec,rec.fields_num);
			if(lock_f) while(lock(fds[0],UNLOCK) == WTLK);
			close_file(3,fds[0],fds[1],fds[2]);
			return -1;
		}
		case CUSTOMER:
		{

		}
		default:
			break;
		}
	}
	case GET:
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
