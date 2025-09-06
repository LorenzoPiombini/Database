#include <stdio.h>
#include <stdlib.h>
#define _GNU_SOURCE             /* See feature_test_macros(7) */
#include <fcntl.h>              /* Definition of O_* constants */
#include <unistd.h>
#include <sys/wait.h>
#include <ctype.h>
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

static char prog[] = "net_interface";
static char *convert_json(char* body);
static int data_to_json(char **buffer, struct Record_f *rec,int end_point);

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
			struct Header_d hd = {0, 0, &sch};

			if(open_files(SALES_ORDERS_H,fds, files, -1) == -1) exit(-1);
			if(is_db_file(&hd,fds) == -1) goto post_exit_error; 
			if(check_data(SALES_ORDERS_H,orders_head,fds,files,&rec,&hd,&lock_f,-1) == -1) goto post_exit_error;
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
			hd.sch_d = &sch;

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
				if(snprintf(key_each_line.base,line_key_sz+1,"%u/%u",key,count) == -1)goto post_exit_error;

				/* write each line */
				if(check_data(SALES_ORDERS_L,&cpy_str.base[2],fds,files,&rec,&hd,&lock_f,-1) == -1) goto post_exit_error;
				cpy_str.close(&cpy_str);

				if(write_record(fds,&key_each_line.base,STR,&rec,0,files,&lock_f,-1) == -1) goto post_exit_error;
				free_record(&rec,rec.fields_num);
				memset(&rec,0,sizeof(struct Record_f));
				key_each_line.close(&key_each_line);

				/*clear the order_line_array to make sure we get the right result*/
				char *find = strstr(orders_line,"[");
				if(find) *find = ' ';
				find = strstr(orders_line,"]");
				if(find) *find = ' ';
				count++;
			}

			if(lock_f) {
				while(lock(fds[0],UNLOCK) == WTLK);
				lock_f = 0;
			}

			close_file(3,fds[0],fds[1],fds[2]);
			if(snprintf(cont->cnt_st,1024,"{ \"message\" : \"order nr %d, created!\"}",key) == -1){
				/*log error*/
				return -1;
			}
			cont->size = strlen(cont->cnt_st);
			return 0;
post_exit_error:
			free_record(&rec,rec.fields_num);
			if(lock_f) while(lock(fds[0],UNLOCK) == WTLK);
			close_file(3,fds[0],fds[1],fds[2]);
			return -1;
		}
		case CUSTOMER:
		{
			break;
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
				/*log errors*/
				fprintf(stderr,"(%s): can't acquire or release proper lock.\n",prog);
				return -1;
			}
			lock_f = 1;
			char *keys = get_all_keys_for_file(fds,0);
			if(!keys){
				/*log errors*/	
				return -1;
			}

			size_t l = strlen(keys);
			size_t mes_l = strlen("{\"message\" : ") + l + strlen(" }");
			if((l+mes_l) >= MAX_CONT_SZ) {
				
				cont->cnt_dy = malloc(mes_l+l+1);
				if(!cont->cnt_dy){
					/*log error*/
					return -1;
				}
				memset(cont->cnt_dy, 0,mes_l+l+1);
				if(snprintf(cont->cnt_st,mes_l+l,"{ \"message\" : %s}",keys) == -1){
					free(keys);
					if(lock_f) while(lock(fds[0],UNLOCK) == WTLK);
					lock_f = 0;
					close(fds[0]);
					return -1;
				}

				free(keys);
				cont->size = l+mes_l;
				if(lock_f) while(lock(fds[0],UNLOCK) == WTLK);
				lock_f = 0;
				return 0;
			}else{
				if(snprintf(cont->cnt_st,mes_l+l,"{ \"message\" : %s}",keys) == -1){
					free(keys);
					if(lock_f) while(lock(fds[0],UNLOCK) == WTLK);
					lock_f = 0;
					close(fds[0]);
					return -1;
				}
				strncpy(&cont->cnt_st[strlen(cont->cnt_st)]," }",strlen(" }")+1);
				cont->size = mes_l;
				free(keys);
				if(lock_f) while(lock(fds[0],UNLOCK) == WTLK);
				lock_f = 0;
				return 0;
			}
		}
		case S_ORD_GET:
		{
			char *p = req->resource;
			p += strlen(SALES_ORDERS) + 1;

			uint32_t k = 0;
			uint8_t type = is_num(p);

			char key[1024];
			memset(key,0,1024);
			switch(type){
			case UINT:
			{
				/*convert to number */	
				char *endp;
				long l = strtol(p,&endp,10);
				if(*endp == '\0'){
					k = (uint32_t) l;
				}else{
					/*log error*/
					return -1;
				}

				int fds[3];
				memset(fds,-1,sizeof(int)*3);
				char files[3][1024] = {0};
				struct Record_f rec = {0};
				struct Schema sch = {0};
				struct Header_d hd = {0, 0, &sch};

				if(open_files(SALES_ORDERS_H,fds, files, -1) == -1) exit(-1);
				if(is_db_file(&hd,fds) == -1) goto s_ord_get_exit_error; 

				if(get_record(-1,SALES_ORDERS_H,&rec,(void *)&k,type, hd,fds) == -1) goto s_ord_get_exit_error; 

				int field_ix = 0;
				int rec_index = 0;
				struct Record_f *r = &rec;
				if(!get_index_rec_field("lines_nr", &r,1, &field_ix, &rec_index)) goto s_ord_get_exit_error;

				long lines = rec.fields[field_ix].data.l;

				/*stringfy the orders head here*/
				cont->cnt_dy = calloc(1024*4,sizeof(char));
				if(!cont->cnt_dy){
					/*log error*/	
					goto s_ord_get_exit_error;
				}

				/*message formatting*/
				size_t position_in_the_message = strlen("{ ");
				strncpy(cont->cnt_dy,"{ ",strlen("{ ")+1);
				strncpy(&cont->cnt_dy[position_in_the_message],"\"message\" : { ",strlen("\"message\" : { ") + 1);
				position_in_the_message += strlen("\"message\" : { ");

				strncpy(&cont->cnt_dy[position_in_the_message],"\"sales_orders_head\"",strlen("\"sales_orders_head\"") + 1);
				position_in_the_message += strlen("\"sales_orders_head\"");
				strncpy(&cont->cnt_dy[position_in_the_message]," : { ",strlen(" : { ")+1);
				position_in_the_message += strlen(" : { ");

				if(data_to_json(&cont->cnt_dy,&rec,S_ORD_GET) == -1) goto s_ord_get_exit_error;

				position_in_the_message = strlen(cont->cnt_dy);
				strncpy(&cont->cnt_dy[position_in_the_message],", ",3);
				position_in_the_message += 2;

				strncpy(&cont->cnt_dy[position_in_the_message],"\"sales_orders_lines\"",strlen("\"sales_orders_lines\"")+1);
				position_in_the_message += strlen("\"sales_orders_lines\"");
				strncpy(&cont->cnt_dy[position_in_the_message]," : { ",strlen(" : { ")+1);
				position_in_the_message += strlen(" : { ");

				free_record(&rec,rec.fields_num);
				close_file(3,fds[0],fds[1],fds[2]);
				memset(&rec,0,sizeof(struct Record_f));
				memset(&sch,0,sizeof(struct Schema));
				memset(&hd,0,sizeof(struct Header_d));
				memset(files,0,3*1024);
				memset(fds,-1,sizeof(int)*3);
				hd.sch_d = &sch;

				if(open_files(SALES_ORDERS_L,fds, files, -1) == -1) goto s_ord_get_exit_error;
				if(is_db_file(&hd,fds) == -1) goto s_ord_get_exit_error;

				long i;	
				for(i = 0;i < lines;i++){

					size_t l = number_of_digit(k) + number_of_digit(i+1)+1;
					if(l >= 1024){
						/*allocate memory*/
					}

					if(snprintf(key,l+1,"%d/%ld",k,i+1) == -1) goto s_ord_get_exit_error;

					if(get_record(-1,SALES_ORDERS_L,&rec,
								(void *)key,STR, hd,fds) == -1) goto s_ord_get_exit_error; 

					/*put the line in the bigger string */
					if(data_to_json(&cont->cnt_dy,&rec,S_ORD_GET) == -1) goto s_ord_get_exit_error;


					free_record(&rec,rec.fields_num);
					memset(&rec,0,sizeof(struct Record_f));
					memset(key,0,sizeof(char));
				}

				close_file(3,fds[0],fds[1],fds[2]);
				/*close the message*/
				position_in_the_message = strlen(cont->cnt_dy);
				strncpy(&cont->cnt_dy[position_in_the_message],"}}",strlen("}}")+1);
				position_in_the_message+=2;
				memset(&cont->cnt_dy[strlen(cont->cnt_dy)],0,(1024*4) - strlen(cont->cnt_dy));	

				cont->size = strlen(cont->cnt_dy);
				/*printf("%s\nsize is %ld\nlast char is '%c'\n",message,strlen(message),message[strlen(message)-1]);*/

				return 0;
s_ord_get_exit_error:
				free_record(&rec,rec.fields_num);
				if(cont->cnt_dy) free(cont->cnt_dy);
				close_file(4,fds[0],fds[1],fds[2]);
				return -1;
			}
			default:
				return -1;
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
	return 0;
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

		if(isdigit(*p)){
			db_entry[i] = *p;
			i++;
			continue;
		}
	}

	return &db_entry[0];
}

static int data_to_json(char **buffer, struct Record_f *rec,int end_point)
{
	switch(end_point){	
		case S_ORD_GET: 
			{
				if(parse_record_to_json(rec,buffer) == -1) return -1;
				break;
			}
		default:
			return -1;
	}
	return 0;
}
