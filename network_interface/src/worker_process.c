#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>

/*my libs*/
#include <crud.h>
#include <str_op.h>
#include <types.h>
#include <memory.h>
#include <file.h>
#include <freestand.h>
#include "end_points.h"
#include "key.h"
#include "handlesig.h"
#include "load.h"
#include "common.h"


static int data_to_json(char **buffer, struct Record_f *rec,int end_point);

int work_process(int sock)
{
	char err[1024];
	char succ[1024];
	int data_sock = -1;
	char buffer[EIGTH_Kib];
	char *d_buff = NULL;

	if(init_prog_memory() == -1){
		kill(getppid(),SIGINT);
		sys_exit(1);
	}

	set_memory(buffer,0,EIGTH_Kib);
	for(;;){
		/*accept connection*/
		if((data_sock = accept(sock,NULL,NULL)) == -1){
			break;
		}

		set_memory(buffer,0,EIGTH_Kib);
		if(read(data_sock,buffer,sizeof(buffer)) == -1){
			break;
		}

		buffer[sizeof(buffer) - 1] = '\0';

		int operation_to_perform = (int)buffer[0] - '0';	
		switch(operation_to_perform){
			case NEW_SORD:
			case UPDATE_SORD:
				{
					/* when insert new order
					 * if the customer is known, the program should check
					 * if it's past due, if it's on credit hold
					 * */

					size_t len = 0;
					char *t = NULL;

					char key_up[1024];
					set_memory(key_up,0,1024);
					if(operation_to_perform == UPDATE_SORD){
						t = tok(&buffer[1],"^");
						if(t){
							len = string_length(t);	
						}else{
							set_memory(err,0,1024);
							write(data_sock,err,sizeof(err));
							close(data_sock);
							data_sock = -1;
							clear_memory();
							continue;
						}

						if(len >= 1024){/*if they length is >= than 1024 we need a code refactor*/
							fprintf(stderr,"code refactor needed %s:%d\n",__FILE__,__LINE__- 10);
							set_memory(err,0,1024);
							write(data_sock,err,sizeof(err));
							close(data_sock);
							data_sock = -1;
							clear_memory();
							continue;

						}
						string_copy(key_up,t,len);
					}

					t = tok(&buffer[1],"^");
					if(t){
						len = string_length(t);	
					}else{
						set_memory(err,0,1024);
						write(data_sock,err,sizeof(err));
						close(data_sock);
						data_sock = -1;
						clear_memory();
						continue;
					}

					char orders_head[len+1];
					set_memory(orders_head,0,len+1);
					string_copy(orders_head,t,len);

					t = tok(NULL,"^");
					if(t){
						len = string_length(t);	
					}else{
						set_memory(err,0,1024);
						write(data_sock,err,sizeof(err));
						close(data_sock);
						data_sock = -1;
						clear_memory();
						continue;
					}

					char orders_line[len+1];
					set_memory(orders_line,0,len+1);
					string_copy(orders_line,t,len);

					/* writing to database*/
					int fds_order_header[3];
					set_memory(fds_order_header,-1,sizeof(int)*3);
					int fds_order_line[3];
					set_memory(fds_order_line,-1,sizeof(int)*3);
					int lock_f = 1;
					char files_order_head[3][1024];
					char files_order_line[3][1024];
					set_memory(files_order_head,0,1024*3);
					set_memory(files_order_line,0,1024*3);

					struct Record_f rec;
					set_memory(&rec,0,sizeof(struct Record_f));
					struct Schema sch_header;
					set_memory(&sch_header,0,sizeof(struct Schema));

					struct Schema sch_line;
					set_memory(&sch_line,0,sizeof(struct Schema));

					struct Header_d hd_head = {0, 0, &sch_header};
					struct Header_d hd_line = {0, 0, &sch_line};

					ui32 key = 0;
					if(operation_to_perform == UPDATE_SORD){
						ui8 type = is_num(key_up);

						switch(type){
							case UINT:
								{
									long l = string_to_long(key_up);
									if(error_value == INVALID_VALUE){
										fprintf(stderr,"cannot convert string to long %s:%d.\n",__FILE__,__LINE__);
										goto error;
									}

									if(l > MAX_KEY){
										fprintf(stderr,"key value is too big for the system %s:%d.\n",__FILE__,__LINE__);
										goto error;
									}

									key = (ui32)l;
									break;
								}
							case STR:
								/*TODO*/
							default:
								clear_memory();
								close(data_sock);
								data_sock = -1;
								continue;
				}
			}


			if(open_files(SALES_ORDERS_H,fds_order_header, files_order_head, -1) == -1){
				clear_memory();
				close(data_sock);
				data_sock = -1;
				continue;
			}

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
			set_memory(&rec,0,sizeof(struct Record_f));

			char *sub_str = NULL;
			ui16 count = 1;
			while((sub_str = get_sub_str("[","]",orders_line))){
				struct String cpy_str;
				set_memory(&cpy_str,0,sizeof(struct String));
				init(&cpy_str,sub_str);

				/*create the key for each line*/
				size_t line_key_sz = number_of_digit(count) + number_of_digit(key) + 1;
				struct String key_each_line;
				set_memory(&key_each_line,0,sizeof(struct String));

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
				set_memory(&rec,0,sizeof(struct Record_f));
				key_each_line.close(&key_each_line);

				/*clear the order_line_array to make sure we get the right result*/
				char *find = find_needle(orders_line,"[");
				if(find) *find = ' ';
				find = find_needle(orders_line,"]");
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
				set_memory(succ,0,1024);
				if(copy_to_string(succ,1024,"{ \"message\" : \"order nr %d, created!\"}",key) == -1){
					/*log error*/
					clear_memory();
					close(data_sock);
					data_sock = -1;
					continue;
				}

				if(write(data_sock,succ,sizeof(succ)) == -1) goto error;

			}else if(operation_to_perform == UPDATE_SORD){
				set_memory(succ,0,1024);
				if(copy_to_string(succ,1024,"{ \"message\" : \"order nr %d, updated!\"}",key) == -1){
					/*log error*/
					goto error;
				}
				if(write(data_sock,succ,sizeof(succ)) == -1) goto error;
			}


			clear_memory();
			close(data_sock);
			data_sock = -1;
			continue;
error:
			clear_memory();
			set_memory(err,0,1024);
			write(data_sock,err,sizeof(err));
			close(data_sock);
			data_sock = -1;
			continue;
		}	
		case CUSTOMER_GET_ALL: 
		case S_ORD:
		{
			/*get the all keys for the sales order file*/
			int fds[3];
			set_memory(fds,-1,sizeof(int)*3);
			char files[3][1024] = {0};
			if(operation_to_perform == S_ORD){
				if(open_files(SALES_ORDERS_H,fds, files, ONLY_INDEX) == -1) goto error_s_ord;
			}else if(operation_to_perform == CUSTOMER_GET_ALL){
				if(open_files(CUSTOMER_FILE,fds, files, ONLY_INDEX) == -1) goto error_s_ord;
			}

			char *keys = 0x0;
			if(operation_to_perform == S_ORD)
				keys = get_all_keys_for_file(fds,0);
			else if (operation_to_perform == CUSTOMER_GET_ALL)
				keys = get_all_keys_for_file(fds,1);

			if(!keys){
				/*log errors*/	
				char *erro_message = 0x0;
				if(operation_to_perform == S_ORD)
					erro_message = "{\"message\": \"there are no orders\"}";
				else if(operation_to_perform == CUSTOMER_GET_ALL)
					erro_message = "{\"message\": \"there are no customers\"}";
				
				set_memory(err,0,1024);
				string_copy(err,erro_message,string_length(erro_message));
				write(data_sock,err,sizeof(err));
				clear_memory();
				close(fds[0]);
				continue;
			}

			size_t l = string_length(keys);
			size_t mes_l = string_length("{\"message\" : ") + l + string_length(" }");
			if((mes_l) >= 1024) {

				d_buff = (char *)ask_mem(mes_l+1);
				if(!d_buff) goto error_s_ord;

				set_memory(d_buff, 0,mes_l+1);
				if(copy_to_string(d_buff,mes_l+1,"{ \"message\" : %s}",keys) == -1) goto error_s_ord;

				if(write(data_sock,d_buff,string_length(d_buff)) == -1) goto error_s_ord;
				
				clear_memory();
				close(data_sock);
				close(fds[0]);
				continue;
			}else{

				set_memory(succ,0,1024);
				if(copy_to_string(succ,mes_l,"{ \"message\" : %s}",keys) == -1) goto error_s_ord;

				if(write(data_sock,succ,string_length(succ)) == -1) goto error_s_ord;
				
				clear_memory();
				close(data_sock);
				close(fds[0]);
				continue;
			}


error_s_ord:
					close(fds[0]);
					set_memory(err,0,1024);
					write(data_sock,err,sizeof(err));
					clear_memory();
					close(fds[0]);
					continue;
		}
		case S_ORD_GET:
		{
			d_buff = NULL;
			ui32 k = 0;
			ui8 type = is_num(&buffer[1]);

			char *key = NULL;
			switch(type){
				case UINT:
					{
						/*convert to number */	
						long l = string_to_long(&buffer[1]);
						if(error_value == INVALID_VALUE){
							/*log error*/
							set_memory(err,0,1024);
							write(data_sock,err,sizeof(err));
							clear_memory();
							close(data_sock);
							continue;
						}
						k = (ui32) l;

						int fds[3];
						set_memory(fds,-1,sizeof(int)*3);
						char files[3][1024];
						set_memory(files,0,3*1024);
						struct Record_f rec;
						set_memory(&rec,0,sizeof(struct Record_f));
						struct Schema sch;
						set_memory(&sch,0,sizeof(struct Schema));
						struct Header_d hd = {0, 0, &sch};

						if(open_files(SALES_ORDERS_H,fds, files, -1) == -1){
							set_memory(err,0,1024);
							write(data_sock,err,sizeof(err));
							clear_memory();
							close(data_sock);
							data_sock = -1;
							continue;
						}
							
						if(is_db_file(&hd,fds) == -1) goto s_ord_get_exit_error; 

						if(get_record(-1,SALES_ORDERS_H,&rec,(void *)&k,type, hd,fds) == -1) goto s_ord_get_exit_error; 

						int field_ix = 0;
						int rec_index = 0;
						struct Record_f *r = &rec;
						if(!get_index_rec_field("lines_nr", &r,1, &field_ix, &rec_index)) goto s_ord_get_exit_error;

						long lines = rec.fields[field_ix].data.l;

						/*stringfy the orders head here*/
						d_buff = (char*)ask_mem(PAGE_SIZE);
						if(!d_buff){
							/*log error*/	
							goto s_ord_get_exit_error;
						}

						/*message formatting*/
						size_t position_in_the_message = string_length("{ ");
						string_copy(d_buff,"{ ",string_length("{ ")+1);
						string_copy(&d_buff[position_in_the_message],"\"message\" : { ",string_length("\"message\" : { ") + 1);
						position_in_the_message += string_length("\"message\" : { ");

						string_copy(&d_buff[position_in_the_message],"\"sales_orders_head\"",string_length("\"sales_orders_head\"") + 1);
						position_in_the_message += string_length("\"sales_orders_head\"");
						string_copy(&d_buff[position_in_the_message]," : { ",string_length(" : { ")+1);
						position_in_the_message += string_length(" : { ");

						if(data_to_json(&d_buff,&rec,S_ORD_GET) == -1) goto s_ord_get_exit_error;

						position_in_the_message = string_length(d_buff);
						string_copy(&d_buff[position_in_the_message],", ",3);
						position_in_the_message += 2;

						string_copy(&d_buff[position_in_the_message],"\"sales_orders_lines\"",string_length("\"sales_orders_lines\"")+1);
						position_in_the_message += string_length("\"sales_orders_lines\"");
						string_copy(&d_buff[position_in_the_message]," : { ",string_length(" : { ")+1);
						position_in_the_message += string_length(" : { ");

						free_record(&rec,rec.fields_num);
						close_file(3,fds[0],fds[1],fds[2]);
						set_memory(&rec,0,sizeof(struct Record_f));
						set_memory(&sch,0,sizeof(struct Schema));
						set_memory(&hd,0,sizeof(struct Header_d));
						set_memory(files,0,3*1024);
						set_memory(fds,-1,sizeof(int)*3);
						hd.sch_d = &sch;

						if(open_files(SALES_ORDERS_L,fds, files, -1) == -1) goto s_ord_get_exit_error;
						if(is_db_file(&hd,fds) == -1) goto s_ord_get_exit_error;

						long i;	
						for(i = 0;i < lines;i++){

							size_t l = number_of_digit(k) + number_of_digit(i+1)+1;
							key = (char*) ask_mem(l+1);
							if(!key){
								fprintf(stderr,"ask_mem() failed, %s:%d.\n",__FILE__,__LINE__-2);
								goto s_ord_get_exit_error;
							}

							char *line_title = (char*)ask_mem(23);
							if(!line_title){
								fprintf(stderr,"ask_mem() failed, %s:%d.\n",__FILE__,__LINE__-2);
								goto s_ord_get_exit_error;
							}
							if(copy_to_string(key,l+1,"%d/%ld",k,i+1) == -1) goto s_ord_get_exit_error;

							if(copy_to_string(line_title,23,"\"line_%ld\"",i+1) == -1)goto s_ord_get_exit_error;


							if(get_record(-1,SALES_ORDERS_L,&rec,
										(void *)key,STR, hd,fds) == -1) goto s_ord_get_exit_error; 

							position_in_the_message = string_length(d_buff);
							string_copy(&d_buff[position_in_the_message],line_title,string_length(line_title));
							position_in_the_message += string_length(line_title);
							string_copy(&d_buff[position_in_the_message]," : { ",string_length(" : { ")+1);
							position_in_the_message += string_length(" : { ");


							/*put the line in the bigger string */
							if(data_to_json(&d_buff,&rec,S_ORD_GET) == -1) goto s_ord_get_exit_error;

							free_record(&rec,rec.fields_num);
							set_memory(&rec,0,sizeof(struct Record_f));
							cancel_memory(NULL,key,l+1);

							if(lines - i > 1){
								position_in_the_message = string_length(d_buff);
								string_copy(&d_buff[position_in_the_message],", ",string_length(", ") + 1);
							}
						}

						close_file(3,fds[0],fds[1],fds[2]);
						/*close the message*/
						position_in_the_message = string_length(d_buff);
						string_copy(&d_buff[position_in_the_message],"}}}",string_length("}}}")+1);
						position_in_the_message+=2;
						set_memory(&d_buff[string_length(d_buff)],0,(1024*4) - string_length(d_buff));	


						if(write(data_sock,d_buff,string_length(d_buff)) == -1 ) goto s_ord_get_exit_error;
						/*printf("%s\nsize is %ld\nlast char is '%c'\n",message,string_length(message),message[string_length(message)-1]);*/
 						
						clear_memory();
						close(data_sock);
						continue;
s_ord_get_exit_error:
						set_memory(err,0,1024);
						write(data_sock,err,2);
						close_file(3,fds[0],fds[1],fds[2]);
						clear_memory();
						close(data_sock);
						continue;

				}
				default:
					set_memory(err,0,1024);
					write(data_sock,err,sizeof(err));
					close(data_sock);
					continue;
			}
		}
		default:
		set_memory(err,0,1024);
				write(data_sock,err,sizeof(err));
					close(data_sock);
		continue;
		}

	}
		close_prog_memory();
		kill(getppid(),SIGINT);
		sys_exit(1);
		return 0;/*unrechable*/
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
