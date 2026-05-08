#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

/*my libs*/
#include <crud.h>
#include <key.h>
#include <str_op.h>
#include <types.h>
#include <file.h>
#include "end_points.h"
#include "handlesig.h"
#include "load_db.h"
#include "common.h"
#include "lua_start.h"
#include "string_utilities.h"

static int data_to_json(char **buffer, struct Record_f *rec,int end_point);

#define EIGTH_Kib 1024*8
int work_process(int sock)
{
	char err[1024];
	char succ[1024];
	int data_sock = -1;
	char buffer[EIGTH_Kib] = {0};
	char *d_buff = NULL;

	init_lua(); /*start the Lua interpreter*/

	for(;;){
		/*accept connection*/
		check_config_file();
		if((data_sock = accept(sock,NULL,NULL)) == -1){
			break;
		}

		memset(buffer,0,EIGTH_Kib);
		int r = 0;
		if((r=read(data_sock,buffer,sizeof(buffer))) == -1){
			close(data_sock);
			break;
		}


		if(r == 0)
			continue;

		buffer[sizeof(buffer) - 1] = '\0';
		int operation_to_perform = (int)(*((ui16*)buffer));	

		switch(operation_to_perform){
		case NEW_CUST:
		{
			char *cust_data = &buffer[2];

			long long res = -1, key = -1;
			if(execute_lua_function("write_customers","s>ii",cust_data,&res,&key) == -1 || res == 2 ){
				/*send error and resume*/
				goto new_cust_error;
			}
			clear_lua_stack();

			memset(succ,0,1024);
			if(copy_to_string(succ,
								1024,
								"{\"message\":\"customer nr %d, created!\"}",
								key) == -1) goto new_cust_error;

			if(write(data_sock,succ,sizeof(succ)) == -1) goto new_cust_error;

			close(data_sock);
			data_sock = -1;
			continue;

new_cust_error:
			memset(err,0,1024);
			write(data_sock,err,sizeof(err));
			close(data_sock);
			data_sock = -1;
			continue;
		}
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
			memset(key_up,0,1024);
			if(operation_to_perform == UPDATE_SORD){
				/*get the key of the record that we have to update*/
				t = tok(&buffer[2],"^");
				if(t){
					len = strlen(t);	
				}else{
					memset(err,0,1024);
					write(data_sock,err,sizeof(err));
					close(data_sock);
					data_sock = -1;
					continue;
				}

				if(len >= 1024){/*if the length is >= than 1024 we need a code refactor*/
					fprintf(stderr,"code refactor needed %s:%d\n",__FILE__,__LINE__- 10);
					memset(err,0,1024);
					write(data_sock,err,sizeof(err));
					close(data_sock);
					data_sock = -1;
					continue;
				}
				strncpy(key_up,t,len);
			}

			t = tok(&buffer[2],"^");
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

			fprintf(stdout,"%s\n",orders_head);
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

			fprintf(stdout,"%s\n",orders_line);

			long long key_ord = -1;
			if(operation_to_perform == NEW_SORD){
				if(execute_lua_function("write_orders","ss>i",orders_head,orders_line,&key_ord) == -1){
					/*send error and resume*/
					clear_lua_stack();
					goto new_up_ords_err;
				}
			}else{
				int key_type = is_num(key_up);
				switch(key_type){
				case UINT:
				{
					error_value = -1;
					long l = string_to_long(key_up);
					if(error_value == INVALID_VALUE) goto new_up_ords_err;

					ui32 key = (ui32)l;
					long long res = -1;
					if(execute_lua_function("update_orders","ssI>i",orders_head,orders_line,key,&res) == -1 || res != 0){
						/*send error and resume*/
						clear_lua_stack();
						goto new_up_ords_err;
					}
					break;	
				}
				case STR:
				{
					long long res = -1;
					if(execute_lua_function("update_orders","sss>i",orders_head,orders_line,key_up,&res) == -1 || res != 0){
						/*send error and resume*/
						clear_lua_stack();
						goto new_up_ords_err;
					}
					break;
				}
				default:
					goto new_up_ords_err;
				}
			}

			clear_lua_stack();

			if(operation_to_perform == NEW_SORD){
				memset(succ,0,1024);
				if(copy_to_string(succ,1024,"{ \"message\" : \"order nr %d, created!\"}",key_ord) == -1){
					/*log error*/
					close(data_sock);
					data_sock = -1;
					continue;
				}

				if(write(data_sock,succ,sizeof(succ)) == -1) goto new_up_ords_err;

			}else if(operation_to_perform == UPDATE_SORD){
				memset(succ,0,1024);
				if(copy_to_string(succ,1024,"{ \"message\" : \"order nr %s, updated!\"}",key_up) == -1){
					/*log error*/
					goto new_up_ords_err;
				}
				if(write(data_sock,succ,sizeof(succ)) == -1) goto new_up_ords_err;
			}


			close(data_sock);
			data_sock = -1;
			continue;

new_up_ords_err:
			memset(err,0,1024);
			write(data_sock,err,sizeof(err));
			close(data_sock);
			data_sock = -1;
			continue;
		}	
		case ITEM_GET_ALL:
		case CUSTOMER_GET_ALL: 
		case S_ORD:
		{
			/*get the all keys for the sales order file or the CUSTOMER*/
			int fds[3];
			memset(fds,-1,sizeof(int)*3);
			char files[3][256] = {0};

			char *keys = 0x0;
			switch(operation_to_perform){
			case S_ORD:
				if(open_files(SALES_ORDERS_H,fds, files, ONLY_INDEX) == -1) goto error_s_ord;
				keys = get_all_keys_for_file(fds,0,0);
				break;
			case CUSTOMER_GET_ALL:
				if(open_files(CUSTOMER_FILE,fds, files, ONLY_INDEX) == -1) goto error_s_ord;
				keys = get_all_keys_for_file(fds,2,MAKE_KEY_JS_STRING);
				break;
			case ITEM_GET_ALL:
				if(open_files(ITEM_FILE,fds, files, ONLY_INDEX) == -1) goto error_s_ord;
				keys = get_all_keys_for_file(fds,1,MAKE_KEY_JS_STRING);
				break;
			default:
				break;
			}


			if(!keys){
				/*log errors*/	
				char *erro_message = 0x0;
				switch(operation_to_perform){
				case S_ORD:
					erro_message = "{\"message\": \"there are no orders\"}";
					break;
				case CUSTOMER_GET_ALL:
					erro_message = "{\"message\": \"there are no customers\"}";
					break;
				case ITEM_GET_ALL:
					erro_message = "{\"message\": \"there are no items\"}";
					break;
				default:
					goto error_s_ord;
				}

				if(operation_to_perform == S_ORD)
					erro_message = "{\"message\": \"there are no orders\"}";
				else if(operation_to_perform == CUSTOMER_GET_ALL)
					erro_message = "{\"message\": \"there are no customers\"}";

				memset(err,0,1024);
				strncpy(err,erro_message,strlen(erro_message));
				write(data_sock,err,sizeof(err));
				close(fds[0]);
				close(data_sock);
				continue;
			}

			size_t l = strlen(keys);
			size_t mes_l = strlen("{\"message\" : ") + l + strlen(" }");
			if((mes_l) >= 1024) {

				d_buff = (char *)malloc(mes_l+1);
				if(!d_buff){
					fprintf(stderr,"malloc() failed. %s:%d.\n",__FILE__,__LINE__-2);
					goto error_s_ord;
				}

				memset(d_buff, 0,mes_l+1);
				if(copy_to_string(d_buff,mes_l+1,"{ \"message\" : %s}",keys) == -1) {
					free(d_buff);
					free(keys);
					goto error_s_ord;
				}

				free(keys);
				keys = NULL;
				if(write(data_sock,d_buff,strlen(d_buff)) == -1) {
					free(d_buff);
					goto error_s_ord;
				}

				close(data_sock);
				close(fds[0]);
				free(d_buff);
				continue;
			}else{

				memset(succ,0,1024);
				if(copy_to_string(succ,mes_l,"{ \"message\" : %s}",keys) == -1) goto error_s_ord;

				if(write(data_sock,succ,strlen(succ)) == -1) goto error_s_ord;

				free(keys);
				keys = NULL;
				close(data_sock);
				close(fds[0]);
				continue;
			}
error_s_ord:
			if(keys)
				free(keys);

			close(fds[0]);
			memset(err,0,1024);
			write(data_sock,err,sizeof(err));
			continue;
		}
		case S_ORD_CUSTOMER_GET:
		case CUSTOMER_GET:
		case S_ORD_GET:
		case ITEM_GET:
		{
			ui32 k = 0;
			ui8 type = is_num(&buffer[2]);
			switch(type){
			case UINT:
			{
				/*convert to number */	
				long l = string_to_long(&buffer[2]);
				if(error_value == INVALID_VALUE){
					/*log error*/
					memset(err,0,1024);
					write(data_sock,err,sizeof(err));
					close(data_sock);
					continue;
				}

				k = (ui32) l;

				char *json = NULL;
				switch(operation_to_perform){
				case ITEM_GET:
					if(execute_lua_function("get_item","I>s",k,&json) == -1){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					if(!json){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					break;
				case S_ORD_GET:
					if(execute_lua_function("get_order","I>s",k,&json) == -1){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					if(!json){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					break;
				case CUSTOMER_GET:
					if(execute_lua_function("get_customer","I>s",k,&json) == -1){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					if(!json){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					break;
				case S_ORD_CUSTOMER_GET:
					if(execute_lua_function("get_customer_for_new_sales_order","I>s",k,&json) == -1){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					if(!json){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					break;
				default:
					fprintf(stderr,"database endpoint not supported\n");
					goto s_ord_get_exit_error;
				}

				/*copy the json string from lua to memory*/
				size_t size_json = strlen(json);
				char *msg = (char*) malloc(size_json+1);
				if(!msg){
					fprintf(stderr,"malloc() failed. %s:%d.\n",__FILE__,__LINE__-2);
					clear_lua_stack();
					goto s_ord_get_exit_error;
				}
				memset(msg,0,size_json+1);
				memcpy(msg,json,size_json);
				clear_lua_stack();
				json = NULL;

				if(write(data_sock,msg,size_json) == -1 ) goto s_ord_get_exit_error;
				/*printf("%s\nsize is %ld\nlast char is '%c'\n",message,strlen(message),message[string_length(message)-1]);*/

				close(data_sock);
				continue;
s_ord_get_exit_error:
				memset(err,0,1024);
				write(data_sock,err,2);
				close(data_sock);
				continue;
			}
			case STR:
			{
				char *json = NULL;
				switch(operation_to_perform){
				case ITEM_GET:
					if(execute_lua_function("get_item","s>s",&buffer[2],&json) == -1){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					if(!json){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					break;
				case S_ORD_GET:
					if(execute_lua_function("get_order","s>s",&buffer[2],&json) == -1){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					if(!json){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					break;
				case CUSTOMER_GET:
					if(execute_lua_function("get_customer","s>s",&buffer[2],&json) == -1){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					if(!json){
						clear_lua_stack();
						goto s_ord_get_exit_error;
				}
					break;
				case S_ORD_CUSTOMER_GET:
					if(execute_lua_function("get_customer_for_new_sales_order","s>s",&buffer[2],&json) == -1){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					if(!json){
						clear_lua_stack();
						goto s_ord_get_exit_error;
					}
					break;
				default:
					fprintf(stderr,"database endpoint not supported\n");
					goto s_ord_get_exit_error;
				}

				/*copy the json string from lua to memory*/
				size_t size_json = strlen(json);
				char *msg = (char*) malloc(size_json+1);
				if(!msg){
					fprintf(stderr,"malloc() failed. %s:%d.\n",__FILE__,__LINE__-2);
					clear_lua_stack();
					goto s_ord_get_exit_error;
				}
				memset(msg,0,size_json+1);
				memcpy(msg,json,size_json);
				clear_lua_stack();
				json = NULL;

				if(write(data_sock,msg,size_json) == -1 ) {
					free(msg);
					goto s_ord_get_exit_error;
				}

				free(msg);
				close(data_sock);
				continue;
			}
			default:
				memset(err,0,1024);
				write(data_sock,err,sizeof(err));
				close(data_sock);
				continue;
			}
		}
		default:
			memset(err,0,1024);
			write(data_sock,err,sizeof(err));
			close(data_sock);
			continue;
		}

	}
	close_lua();
	pid_t p = getppid();
	if(p != -1)
		kill(p,SIGINT);
	exit(1);
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
