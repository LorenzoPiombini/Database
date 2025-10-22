#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <sys/wait.h>

/*mine libs*/
#include <memory.h>
#include <freestand.h>
#include "network.h"
#include "monitor.h"
#include "request.h"
#include "response.h"
#include "handlesig.h"
#include "work_process.h"

#define USE_FORK 1

int main()
{
	unsigned short port = 5043;
	int con = -1;
	if((con = listen_port_80(&port)) == -1){
		fprintf(stderr,"can't connect to port '%d'\n",port);
		return -1;
	}

	fprintf(stderr,"listening on %d\n",port);

	if(start_monitor(con) == -1) {
		/*log the error*/
		stop_listening(con);
		return -1;
	}

	hdl_sock  = con;
	if(handle_sig() == -1) {
		stop_listening(con);
		return -1;
	}

	int cli_sock = -1;
	struct Response res;
	set_memory(&res,0,sizeof(struct Response));

	struct Request req;
	set_memory(&req,0,sizeof(struct Request));
	req.method = -1;
	int work_proc_data_sock = -1;
	int parent_data_sock = -1;

	pid_t work_proc_pid = fork();

	if(work_proc_pid == -1){
		fprintf(stderr,"architecture cannot be implemented");
		stop_listening(con);
		return -1;
	}


	if(work_proc_pid == 0){
		/* start DB handle process */	
		worker = work_proc_pid;
		if((work_proc_data_sock = listen_UNIX_socket()) == -1) exit(1);

		work_process(work_proc_data_sock);
		stop_listening(con);
		return -1;
	}else{
		

		uint8_t disconnect = 0;
		for(;;){
			if((nfds = monitor_events()) == -1) break;
			if(nfds == EINTR) continue;

			int i;
			for(i = 0; i < nfds; i++){

				if(events[i].data.fd == con){
					int r = 0;		
					/*allocate arena for request handling*/
					if(create_arena(FIVE_H_Kib) == -1){
						fprintf(stderr,"could not create arena\n");
						break;
					}

					clear_request(&req);
					if((r = wait_for_connections(con,&cli_sock,&req)) == -1) {
						printf("\nwait_for_connections failed and everything gets cleaned\n");
						remove_socket_from_monitor(cli_sock);
						stop_listening(cli_sock);
						close_arena();
						clear_request(&req);
						break;
					}

					if(r == EAGAIN || r == EWOULDBLOCK) {
						close_arena();
						req.d_req = NULL;
						req.size = 0;
						continue;
					}

					if(r == BAD_REQ){
						/*debug statement to be removed*/
						print_request(req);

						if(generate_response(&res,400,NULL,&req) == -1) break;

						clear_request(&req);
						int w = 0;
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) continue;

						stop_listening(cli_sock);
						clear_response(&res);
						close_arena();
						break;
					}

					if (r == BDY_MISS){
						printf("\nrequest is %s and is %ld bytes long\n",req.d_req,req.size);
						continue;
					}

					close_arena();	
					req.d_req = NULL;
					req.size = 0;
#if USE_FORK
					pid_t child = fork();
#else
					pid_t child = 0;
#endif
					if(child == -1){
						/*parent*/
						/*server error*/
						if(generate_response(&res,500,NULL,&req) == -1) break;

						clear_request(&req);
						int w = 0;
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
							clear_request(&req);
							continue;
						}

						remove_socket_from_monitor(cli_sock);

						clear_response(&res);

						continue;
					}

					if(child == 0){
						parent_data_sock = connect_UNIX_socket();
						if(parent_data_sock == -1){
							fprintf(stderr,"architecture cannot be implemented");
							stop_listening(con);
							exit(1);
						}

						/*send response*/
						if(create_arena(FIVE_H_Kib) == -1){
							/*log error*/
							stop_listening(cli_sock);
							exit(1);
						}

						struct Content cont;
						set_memory(&cont,0,sizeof(struct Content));

						switch(req.method){
							case OPTIONS:
								{
										if(string_compare(req.origin,ORIGIN_DEF,string_length(ORIGIN_DEF)) != 0){
											/*send a bed request response*/
											printf("first brach 400 response ORIGIN FAILED\n");
											if(generate_response(&res,400,NULL,&req) == -1) break;

											clear_request(&req);
											int w = 0;
											if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
											if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK 
												uint8_t ws = 0;
												while((w = write_cli_sock(cli_sock,&res) != -1)){
													if(w == EAGAIN || w == EWOULDBLOCK) continue;

													ws = 1;
													break;
												}
												if(ws){
													//clear_response(&res);
													stop_listening(cli_sock);
													close_arena();
													exit(0);
												}
												//clear_response(&res);
												stop_listening(cli_sock);
												close_arena();
												exit(1);

#else
												continue;
#endif
											}

#if USE_FORK
											//clear_request(&req);
											//clear_response(&res);
											stop_listening(cli_sock);
											close_arena();
											exit(0);
#else
											clear_request(&req);
											clear_response(&res);
											remove_socket_from_monitor(cli_sock);
											break;
#endif
										}
										/*send a response to the options request*/
										if(generate_response(&res,200,NULL,&req) == -1) break;

										clear_request(&req);
										int w = 0;
										if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
										if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK 
											uint8_t ws = 0;
											while((w = write_cli_sock(cli_sock,&res) != -1)){
												if(w == EAGAIN || w == EWOULDBLOCK) continue;

												ws = 1;
												break;
											}
											if(ws){
												//clear_response(&res);
												stop_listening(cli_sock);
												close_arena();
												exit(0);
											}
											//clear_response(&res);
											//stop_listening(cli_sock);
											close_arena();
											exit(1);

#else
											continue;
#endif
										}

#if USE_FORK
										stop_listening(cli_sock);
										//clear_response(&res);
										close_arena();
										exit(0);
#else
										remove_socket_from_monitor(cli_sock);
										clear_response(&res);
										break;
#endif
									break;	
								}
							case GET:
							case POST:
								{
									if(load_resource(&req,&cont,parent_data_sock) == -1){
										print_request(req);
										/*send a bad request response*/
										printf("first brach 400 response for load_resource GET or POST\n");
										if(generate_response(&res,400,NULL,&req) == -1) break;

										//clear_request(&req);
										int w = 0;
										if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
										if(w == EAGAIN || w == EWOULDBLOCK){
#if USE_FORK 
											uint8_t ws = 0;
											while((w = write_cli_sock(cli_sock,&res) != -1)){
												if(w == EAGAIN || w == EWOULDBLOCK) continue;

												ws = 1;
												break;
											}
											if(ws){
												//	clear_response(&res);
												close_arena();
												stop_listening(cli_sock);
												exit(0);
											}
											//clear_response(&res);
											//stop_listening(cli_sock);
											close_arena();
											exit(1);

#else
											continue;
#endif
										}
#if USE_FORK
										stop_listening(cli_sock);

										//clear_content(&cont);
										//clear_response(&res);
										close_arena();
										exit(0);
#else
										clear_response(&res);
										clear_content(&cont);
										remove_socket_from_monitor(cli_sock);
										break;
#endif
									}	
									/*send a response to the client request*/
									int status = 0;

									if(req.method == GET){
										status = 200;
										if(generate_response(&res,status,&cont,&req) == -1) break;
									}else if((req.method == POST)){
										status = 201;
										if(generate_response(&res,status,&cont,&req) == -1) {
											fprintf(stderr,"cannot create response");
											break;
										}
									}

									clear_request(&req);
									if(res.body.d_cont)
										printf("response body is \n %s\n",res.body.d_cont);
									else
										printf("response body is \n %s\n",res.body.content);
										
									int w = 0;
									if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
									if(w == EAGAIN || w == EWOULDBLOCK){
#if USE_FORK 
										uint8_t ws = 0;
										while((w = write_cli_sock(cli_sock,&res) != -1)){
											if(w == EAGAIN || w == EWOULDBLOCK) continue;

											ws = 1;
											break;
										}
										if(ws){
											//clear_response(&res);
											//clear_content(&cont);
											stop_listening(cli_sock);
											close_arena();
											exit(0);
										}
										//clear_response(&res);
										//clear_content(&cont);
										//stop_listening(cli_sock);
										close_arena();
										exit(1);

#else
										continue;
#endif
									}

#if USE_FORK
									stop_listening(cli_sock);
									//clear_response(&res);
									//clear_content(&cont);
									close_arena();
									exit(0);
#else
									clear_response(&res);
									clear_content(&cont);
									remove_socket_from_monitor(cli_sock);
									break;
#endif
								}
							case PUT:
								break;
							default:
								if(generate_response(&res,404,NULL,&req) == -1) break;

								clear_content(&cont);
								int w = 0;
								if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
								while(w == EAGAIN || w == EWOULDBLOCK){
#if USE_FORK 
									uint8_t ws = 0;
									while((w = write_cli_sock(cli_sock,&res) != -1)){
										if(w == EAGAIN || w == EWOULDBLOCK) continue;

										ws = 1;
										break;
									}
									if(ws){
										//clear_response(&res);
										close_arena();
										stop_listening(cli_sock);
										exit(0);
									}
									//clear_response(&res);
									//stop_listening(cli_sock);
									close_arena();
									exit(1);

#else
									continue;
#endif
								}


#if USE_FORK
								stop_listening(cli_sock);
								//clear_request(&req);
								//clear_response(&res);
								exit(0);
#else
								clear_request(&req);
								clear_response(&res);
								remove_socket_from_monitor(cli_sock);
								break;
#endif
						}
					}
#if USE_FORK
					/*parent process*/
					remove_socket_from_monitor(cli_sock);
					stop_listening(cli_sock);
					printf("parent close the socket %d\n",cli_sock);
					clear_request(&req);
#endif
				}else{ /*SECOND BRANCH*/
					int r = 0;
					if(events[i].events == EPOLLIN) {

						printf("second branch line %d\nreading from socket number %d.\n",__LINE__
								,events[i].data.fd);
						if(create_arena(FIVE_H_Kib) == -1){
							fprintf(stderr,"could not create arena\n");
							break;
						}
						if((r = read_cli_sock(events[i].data.fd,&req)) == -1){ 
							remove_socket_from_monitor(events[i].data.fd);
							disconnect = 0;
							break;
						}


						if(r == EAGAIN || r == EWOULDBLOCK) {
							if(disconnect == 20){
								remove_socket_from_monitor(events[i].data.fd);
								disconnect = 0;
								break;
							}
							disconnect++;
							continue;
						}


						close_arena();
						req.d_req = NULL;
						req.size = 0;
#if USE_FORK
						pid_t child = fork();
#else
						pid_t child = 0;
#endif
						if(child == -1){
							/*send a server error response*/
							clear_request(&req);

							continue;
						}
						if(child == 0){

							parent_data_sock = connect_UNIX_socket();
							if(parent_data_sock == -1){
								fprintf(stderr,"architecture cannot be implemented");
								stop_listening(con);
								exit(1);
							}

							if(create_arena(FIVE_H_Kib) == -1){
								/*log error*/
								/*NOT SURE ABOUT REMOVE HERE*/
								//remove_socket_from_monitor(events[i].data.fd);
								exit(1);
							}

							if(r == BAD_REQ) {
								/*send a bed request response*/
								//remove_socket_from_monitor(events[i].data.fd);
								//stop_listening(cli_sock);
								close_arena();
								exit(1);
							}

							struct Content cont;
							set_memory(&cont,0,sizeof(struct Content));
							switch(req.method){
								case OPTIONS:
									{

										if(string_compare(req.origin,ORIGIN_DEF,string_length(ORIGIN_DEF)) != 0){

											printf("\n\n\n\n\norigin failed\n\n\n");
											if(generate_response(&res,400,NULL,&req) == -1) break;
											//clear_request(&req);

											int w = 0;
											if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
											if(w == EAGAIN || w == EWOULDBLOCK){
#if USE_FORK 
												uint8_t ws = 0;
												while((w = write_cli_sock(events[i].data.fd,&res) != -1)){
													if(w == EAGAIN || w == EWOULDBLOCK) continue;

													ws = 1;
													break;
												}
												if(ws){
													//clear_response(&res);
													close_arena();
													//remove_socket_from_monitor(events[i].data.fd);
													//	stop_listening(cli_sock);
													exit(0);

												}
												clear_response(&res);
												//	remove_socket_from_monitor(events[i].data.fd);
												//	stop_listening(cli_sock);
												close_arena();
												exit(1);

#else
												continue;
#endif
											}
										}

										/*send a response to the options request*/
										if(generate_response(&res,200,NULL,&req) == -1) break;


										//clear_request(&req);

										int w = 0;
										if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
										if(w == EAGAIN || w == EWOULDBLOCK){
#if USE_FORK 
											uint8_t ws = 0;
											while((w = write_cli_sock(events[i].data.fd,&res) != -1)){
												if(w == EAGAIN || w == EWOULDBLOCK) continue;

												ws = 1;
												break;
											}
											if(ws){
												//clear_response(&res);
												//remove_socket_from_monitor(events[i].data.fd);
												stop_listening(events[i].data.fd);
												close_arena();
												exit(0);
											}
											//	clear_response(&res);
											//	remove_socket_from_monitor(events[i].data.fd);
											stop_listening(events[i].data.fd);
											close_arena();
											exit(1);

#else
											continue;
#endif
										}

#if USE_FORK

										//clear_response(&res);
										stop_listening(events[i].data.fd);
										close_arena();
										exit(0);
#else
										clear_response(&res);
										remove_socket_from_monitor(events[i].data.fd);
										continue;
#endif
									}
								case GET:
								case POST:
									{
										if(load_resource(&req,&cont,parent_data_sock) == -1){

											printf("\n\n\n\nsecond branch load function failed\n\n\n\n\n");
											/*send a bed request response*/
											if(generate_response(&res,400,NULL,&req) == -1) break;

											//	clear_request(&req);
											//	clear_content(&cont);

											int w = 0;
											if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
											if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK 
												uint8_t ws = 0;
												while((w = write_cli_sock(events[i].data.fd,&res) != -1)){
													if(w == EAGAIN || w == EWOULDBLOCK) continue;

													ws = 1;
													break;
												}
												if(ws){
													//clear_response(&res);
													stop_listening(events[i].data.fd);
													close_arena();
													exit(0);
												}
												//clear_response(&res);
												stop_listening(events[i].data.fd);
												close_arena();
												exit(1);

#else
												continue;
#endif
											}
										}	
										/*send a response*/
										int status = 0;
										if(req.method == GET){
											status = 200;
											if(generate_response(&res,status,&cont,&req) == -1) break;
										}else if((req.method == POST)){
											status = 201;
											if(generate_response(&res,status,&cont,&req) == -1) break;
										}

										//clear_content(&cont);
										//clear_request(&req);
										int w = 0;
										if((w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
										if(w == EAGAIN || w == EWOULDBLOCK){
#if USE_FORK 
											uint8_t ws = 0;
											while((w = write_cli_sock(events[i].data.fd,&res) != -1)){
												if(w == EAGAIN || w == EWOULDBLOCK) continue;

												ws = 1;
												break;
											}
											if(ws){
												//clear_response(&res);
												stop_listening(events[i].data.fd);
												close_arena();
												exit(0);
											}
											//clear_response(&res);
											stop_listening(events[i].data.fd);
											close_arena();
											exit(1);
#else
											continue;
#endif
										}
#if USE_FORK
										//clear_response(&res);
										close_arena();
										stop_listening(events[i].data.fd);
										printf("child try to remove sock nr %d, from monitor.\n",
												events[i].data.fd);
										exit(0);
#else
										clear_response(&res);
										remove_socket_from_monitor(events[i].data.fd);
										continue;
#endif
									}
								default:
									/*send a bad or not found request response*/
									if(generate_response(&res,404,NULL,&req) == -1) break;

									//clear_content(&cont);
									//clear_request(&req);
									int w =	 0;
									if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
									if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK 
										uint8_t ws = 0;
										while((w = write_cli_sock(events[i].data.fd,&res) != -1)){
											if(w == EAGAIN || w == EWOULDBLOCK) continue;

											ws = 1;
											break;
										}
										if(ws){
											//clear_response(&res);
											stop_listening(events[i].data.fd);
											close_arena();
											exit(0);
										}
										//clear_response(&res);
										stop_listening(events[i].data.fd);
										close_arena();
										exit(1);

#else
										continue;
#endif
									}
							}

							/*since thne socket are NON BLOCKING this is needed*/
							/* send response */
							int w = 0;
							if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
							if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK 
								uint8_t ws = 0;
								while((w = write_cli_sock(events[i].data.fd,&res) != -1)){
									if(w == EAGAIN || w == EWOULDBLOCK) continue;

									ws = 1;
									break;
								}
								if(ws){
									//clear_response(&res);
									stop_listening(events[i].data.fd);
									close_arena();
									exit(0);
								}
								//clear_response(&res);
								stop_listening(events[i].data.fd);
								close_arena();
								exit(1);

#else
								continue;
#endif
							}
#if USE_FORK
							//clear_response(&res);
							stop_listening(events[i].data.fd);
							close_arena();
							exit(0);
#else
							clear_response(&res);
							remove_socket_from_monitor(events[i].data.fd);
							continue;
#endif

						}
						/*parent*/
						printf("in the parent\n");
						printf("parent try to remove the socket nr %d from monitor line %d\n",
								events[i].data.fd,
								__LINE__);
						remove_socket_from_monitor(events[i].data.fd);
					}else if(events[i].events == EPOLLOUT) {
						int w = 0;
						if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;	

						if(w == EAGAIN || w == EWOULDBLOCK) {
							/*you should fork right here*/
#if USE_FORK 
							uint8_t ws = 0;
							while((w = write_cli_sock(events[i].data.fd,&res) != -1)){
								if(w == EAGAIN || w == EWOULDBLOCK) continue;

								ws = 1;
								break;
							}
							if(ws){
								clear_response(&res);
								remove_socket_from_monitor(events[i].data.fd);
								exit(0);
							}
							clear_response(&res);
							//remove_socket_from_monitor(events[i].data.fd);
							exit(1);

#else
							continue;
#endif
						}

#if USE_FORK
						clear_response(&res);
						remove_socket_from_monitor(events[i].data.fd);
						exit(0);
#else
						clear_response(&res);
						remove_socket_from_monitor(events[i].data.fd);
						continue;

#endif

					}
				}
			}
		}

		stop_listening(con);
		stop_monitor();
		return 0;
	}
}
