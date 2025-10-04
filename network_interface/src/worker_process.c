#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <errno.h>

#include <crud.h>
#include <str_op.h>
#include <memory.h>
#include <file.h>
#include "end_points.h"
#include "key.h"
#include "handlesig.h"
#include "load.h"
#include "common.h"


int work_process(int sock)
{
	char err[1024];
	char succ[1024];
	int data_sock = -1;
	char buffer[EIGTH_Kib];
	char *d_buff = NULL;

	memset(buffer,0,EIGTH_Kib);
	for(;;){
	 	/*accept connection*/
		if((data_sock = accept(sock,NULL,NULL)) == -1){
			break;
		}
	
		if(read(data_sock,buffer,sizeof(buffer)) == -1){
			break;
		}

		buffer[sizeof(buffer) - 1] = '\0';

		int operation_to_perform = (int)buffer[0] - '0';	
		if(create_arena(FIVE_H_Kib) == -1){
			memset(err,0,1024);
			write(data_sock,err,sizeof(err));
			close(data_sock);
			data_sock = -1;
			continue;
		}

	
		switch(operation_to_perform){
		case NEW_SORD:
		case UPDATE_SORD:
		{
			size_t len = 0;
			char *t = NULL;

			char key_up[len+1];
			memset(key_up,0,len+1);
			if(operation_to_perform == UPDATE_SORD){
				t = tok(&buffer[1],"^");
				if(t){
					len = strlen(t);	
				}else{
					memset(err,0,1024);
					write(data_sock,err,sizeof(err));
					close(data_sock);
					data_sock = -1;
					continue;
				}
				strncpy(key_up,t,len);
			}
			t = tok(&buffer[1],"^");
			if(t){
				len = strlen(t);	
			}else{
				memset(err,0,1024);
				write(data_sock,err,sizeof(err));
				close(data_sock);
				data_sock = -1;
				continue;
			}

			char orders_head[len+1];
			memset(orders_head,0,len+1);
			strncpy(orders_head,t,len);

			t = tok(NULL,"^");
			if(t){
				len = strlen(t);	
			}else{
				memset(err,0,1024);
				write(data_sock,err,sizeof(err));
				close(data_sock);
				data_sock = -1;
				continue;
			}

			char orders_line[len+1];
			memset(orders_line,0,len+1);
			strncpy(orders_line,t,len);

			/* writing to database*/
			int fds_order_header[3];
			memset(fds_order_header,-1,sizeof(int)*3);
			int fds_order_line[3];
			memset(fds_order_line,-1,sizeof(int)*3);
			int lock_f = 1;
			char files_order_head[3][1024];
			char files_order_line[3][1024];
			memset(files_order_head,0,1024*3);
			memset(files_order_line,0,1024*3);

			struct Record_f rec;
			memset(&rec,0,sizeof(struct Record_f));
			struct Schema sch_header;
			memset(&sch_header,0,sizeof(struct Schema));

			struct Schema sch_line;
			memset(&sch_line,0,sizeof(struct Schema));

			struct Header_d hd_head = {0, 0, &sch_header};
			struct Header_d hd_line = {0, 0, &sch_line};

			uint32_t key = 0;
			if(operation_to_perform == UPDATE_SORD){
				char *p = key_up;
				p += strlen(UPDATE_ORDERS) + 1;

				uint8_t type = is_num(p);

				switch(type){
					case UINT:
						{
							errno = 0;
							long l = string_to_long(p);
							if(errno == EINVAL){
								fprintf(stderr,"cannot convert string to long %s:%d.\n",__FILE__,__LINE__);
								goto error;
							}
							if(l > MAX_KEY){
								fprintf(stderr,"key value is too big for the system %s:%d.\n",__FILE__,__LINE__);
								goto error;
							}

							key = (uint32_t)l;
						}
					case STR:
						/*TODO*/
					default:
						break;
				}
			}


			if(open_files(SALES_ORDERS_H,fds_order_header, files_order_head, -1) == -1) return -1;
			if(is_db_file(&hd_head,fds_order_header) == -1) goto error; 

			if(open_files(SALES_ORDERS_L,fds_order_line, files_order_line, -1) == -1) goto error;
			if(is_db_file(&hd_line,fds_order_line) == -1) goto error; 

			int check = 0;
			if((check = check_data(SALES_ORDERS_H,
							orders_head,
							fds_order_header,
							files_order_head,
							&rec,
							&hd_head,
							&lock_f,-1)) == -1) goto error;

			if(operation_to_perform == NEW_SORD){
				key = generate_numeric_key(fds_order_header,NEW_SORD);
				if(!key) goto error; 
			}

			if(operation_to_perform == NEW_SORD){
				if(write_record(fds_order_header,&key,UINT,&rec, 0,files_order_head,&lock_f,-1) == -1) {
					fprintf(stderr,"write_record failed! %s:%d.\n",__FILE__,__LINE__-1);
					goto error;
				}
			}else if(operation_to_perform == UPDATE_SORD){
				if(update_rec(SALES_ORDERS_H,fds_order_header,&key,UINT,&rec,hd_head,check,&lock_f,NULL) == -1 )goto error;
			}

			free_record(&rec,rec.fields_num);
			memset(&rec,0,sizeof(struct Record_f));

			char *sub_str = NULL;
			uint16_t count = 1;
			while((sub_str = get_sub_str("[","]",orders_line))){
				struct String cpy_str;
				memset(&cpy_str,0,sizeof(struct String));
				init(&cpy_str,sub_str);

				/*create the key for each line*/
				size_t line_key_sz = number_of_digit(count) + number_of_digit(key) + 1;
				struct String key_each_line;
				memset(&key_each_line,0,sizeof(struct String));

				init(&key_each_line,NULL);
				if(copy_to_string(key_each_line.base,line_key_sz+1,"%u/%u",key,count) == -1)goto error;

				/* write each line */
				int check = 0;
				if((check = check_data(SALES_ORDERS_L,
								&cpy_str.base[2],
								fds_order_line,
								files_order_line,
								&rec,
								&hd_line,
								&lock_f,-1)) == -1) goto error;

				cpy_str.close(&cpy_str);

				if(operation_to_perform == NEW_SORD){
					if(write_record(fds_order_line,
								&key_each_line.base,
								STR,
								&rec,
								0,
								files_order_line,
								&lock_f,
								-1) == -1) {
						fprintf(stderr,"write_record failed! %s:%d.\n",__FILE__,__LINE__-8);
						goto error;
					}
				}else if(operation_to_perform == UPDATE_SORD){
					/* 
					 * the err variable and the if 
					 * statement after the update_rec
					 * are ment to perform an update  of an order
					 * with new lines 
					 * */
					int err = 0;
					if(( err = update_rec(SALES_ORDERS_L,
									fds_order_line,
									&key_each_line.base,
									STR,
									&rec,
									hd_line,
									check,
									&lock_f,
									NULL)) == -1) goto error;

					if(err == KEY_NOT_FOUND){
						if(write_record(fds_order_line,
									&key_each_line.base,
									STR,
									&rec,
									0,
									files_order_line,
									&lock_f,-1) == -1) goto error;
					}
				}

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

			close_file(6,fds_order_header[0],
					fds_order_header[1],
					fds_order_header[2],
					fds_order_line[0],
					fds_order_line[1],
					fds_order_line[2]);
			/*write to socket*/
			if(operation_to_perform == NEW_SORD){
				memset(succ,0,1024);
				if(copy_to_string(succ,1024,"{ \"message\" : \"order nr %d, created!\"}",key) == -1){
					/*log error*/
					return -1;
				}

				if(write(data_sock,succ,sizeof(succ)) == -1) goto error;

			}else if(operation_to_perform == UPDATE_SORD){
				memset(succ,0,1024);
				if(copy_to_string(succ,1024,"{ \"message\" : \"order nr %d, updated!\"}",key) == -1){
					/*log error*/
					goto error;
				}
				if(write(data_sock,succ,sizeof(succ)) == -1) goto error;
			}


			close_arena();
			close(data_sock);
			data_sock = -1;
			continue;
error:
			close_arena();
			memset(err,0,1024);
			write(data_sock,err,sizeof(err));
			close(data_sock);
			data_sock = -1;
			continue;
		}	
		case S_ORD:
		{
			/*get the all keys for the sales order file*/
			int fds[3];
			memset(fds,-1,sizeof(int)*3);
			char files[3][1024] = {0};
			if(open_files(SALES_ORDERS_H,fds, files, ONLY_INDEX) == -1) goto error_s_ord;

			char *keys = get_all_keys_for_file(fds,0);
			if(!keys){
				/*log errors*/	
				char erro_message[] = "{\"message\": \"there are no orders\"}";
				memset(err,0,1024);
				strncpy(err,erro_message,strlen(erro_message));
				write(data_sock,err,sizeof(err));
				close_arena();
				close(fds[0]);
				continue;
			}

			size_t l = strlen(keys);
			size_t mes_l = strlen("{\"message\" : ") + l + strlen(" }");
			if((mes_l) >= 1024) {

				d_buff = (char *)ask_mem(mes_l+1);
				if(!d_buff) goto error_s_ord;

				memset(d_buff, 0,mes_l+1);
				if(copy_to_string(d_buff,mes_l+1,"{ \"message\" : %s}",keys) == -1) goto error_s_ord;

				if(write(data_sock,d_buff,strlen(d_buff)) == -1) goto error_s_ord;
				//cancel_memory(NULL,keys,strlen(key));
				
				close_arena();
				close(data_sock);
				close(fds[0]);
				continue;
			}else{

				memset(succ,0,1024);
				if(copy_to_string(succ,mes_l,"{ \"message\" : %s}",keys) == -1) goto error_s_ord;

				if(write(data_sock,succ,strlen(succ)) == -1) goto error_s_ord;
				
				close_arena();
				close(data_sock);
				close(fds[0]);
				continue;
			}


error_s_ord:
					close(fds[0]);
					memset(err,0,1024);
					write(data_sock,err,sizeof(err));
					close_arena();
					close(fds[0]);
					continue;
		}

		default:
		continue;
		}
	}

	kill(getppid(),SIGINT);
	exit(1);
}
