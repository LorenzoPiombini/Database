#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <memory.h>
#include "network.h"
#include "monitor.h"
#include "request.h"
#include "response.h"
#include "handlesig.h"

#define USE_FORK 1

int main()
{
	unsigned short port = 5043;
	int con = -1;
	if((con = listen_port_80(&port)) == -1){
		fprintf(stderr,"can't connect to port '%d': %s.\n",port,strerror(errno));
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
	memset(&res,0,sizeof(struct Response));

	struct Request req;
	memset(&req,0,sizeof(struct Request));
	req.method = -1;
	
	for(;;){
		if((nfds = monitor_events()) == -1) break;
		if(nfds == EINTR) continue;

		int i;
		for(i = 0; i < nfds; i++){


			if(events[i].data.fd == con){
				int r = 0;		
				if((r = wait_for_connections(con,&cli_sock,&req)) == -1) break;

				if(r == EAGAIN || r == EWOULDBLOCK) continue;

				if(r == BAD_REQ){
						if(generate_response(&res,400,NULL,&req) == -1) break;

						clear_request(&req);
						int w = 0;
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) continue;

						stop_listening(cli_sock);
						clear_response(&res);
						break;
				}

				if (r == BDY_MISS){
					printf("\nrequest is %s and is %ld bytes long\n",req.req,req.size);
					continue;
				}
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
					/*send response*/
					if(init_prog_memory() == -1){
						/*log error*/
						stop_listening(cli_sock);
						exit(1);
					}

				
					struct Content cont;
					memset(&cont,0,sizeof(struct Content));

					switch(req.method){
					case OPTIONS:
					{
						size_t s = strlen(req.origin);
						if(s == strlen(ORIGIN_DEF)){
							if(strncmp(req.origin,ORIGIN_DEF,strlen(ORIGIN_DEF)) != 0){
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
										close_prog_memory();
										exit(0);
									}
									//clear_response(&res);
									stop_listening(cli_sock);
									close_prog_memory();
									exit(1);

#else
									continue;
#endif
								}

#if USE_FORK
								//clear_request(&req);
								//clear_response(&res);
								stop_listening(cli_sock);
								close_prog_memory();
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
									close_prog_memory();
									exit(0);
								}
								//clear_response(&res);
								//stop_listening(cli_sock);
								close_prog_memory();
								exit(1);

#else
								continue;
#endif
							}

#if USE_FORK
							stop_listening(cli_sock);
							//clear_response(&res);
							close_prog_memory();
							exit(0);
#else
							remove_socket_from_monitor(cli_sock);
							clear_response(&res);
							break;
#endif
						}
					break;	
					}
					case GET:
					case POST:
					{
						if(load_resource(&req,&cont) == -1){
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
								close_prog_memory();
								stop_listening(cli_sock);
								exit(0);
							}
							//clear_response(&res);
							//stop_listening(cli_sock);
							close_prog_memory();
							exit(1);

#else
							continue;
#endif
						}
#if USE_FORK
							stop_listening(cli_sock);

							//clear_content(&cont);
							//clear_response(&res);
							close_prog_memory();
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
								close_prog_memory();
								exit(0);
							}
							//clear_response(&res);
							//clear_content(&cont);
							//stop_listening(cli_sock);
							close_prog_memory();
							exit(1);

#else
							continue;
#endif
						}

#if USE_FORK
						stop_listening(cli_sock);
						//clear_response(&res);
						//clear_content(&cont);
						close_prog_memory();
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
							close_prog_memory();
							stop_listening(cli_sock);
							exit(0);
						}
						//clear_response(&res);
						//stop_listening(cli_sock);
						close_prog_memory();
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
					
				int wstatus;
				if(waitpid(child, &wstatus, WNOHANG) == -1)
					fprintf(stderr,"cannot wait for child process\n");

				if(WIFEXITED(wstatus)){
					if(WEXITSTATUS(wstatus) == 0){
						stop_listening(cli_sock);
						printf("parent close the socket %d\n",cli_sock);
					}
				}
				clear_request(&req);
#endif
			}else{ /*SECOND BRANCH*/
				int r = 0;
				if(events[i].events == EPOLLIN) {

					printf("second branch line %d\nreading from socket number %d.\n",__LINE__
							,events[i].data.fd);
					if((r = read_cli_sock(events[i].data.fd,&req)) == -1) break;
					if(r == EAGAIN || r == EWOULDBLOCK) continue;


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
						if(init_prog_memory() == -1){
							/*log error*/
							/*NOT SURE ABOUT REMOVE HERE*/
							//remove_socket_from_monitor(events[i].data.fd);
							exit(1);
						}
						if(r == BAD_REQ) {
							/*send a bed request response*/
							//remove_socket_from_monitor(events[i].data.fd);
							//stop_listening(cli_sock);
							close_prog_memory();
							exit(1);
						}

						struct Content cont;
						memset(&cont,0,sizeof(struct Content));
						switch(req.method){
						case OPTIONS:
						{
							size_t s = strlen(req.origin);
							if(s != strlen(ORIGIN_DEF)) goto bad_request;

							if(strncmp(req.origin,ORIGIN_DEF,strlen(ORIGIN_DEF)) != 0){

								bad_request:
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
										close_prog_memory();
										//remove_socket_from_monitor(events[i].data.fd);
									//	stop_listening(cli_sock);
										exit(0);

									}
									clear_response(&res);
								//	remove_socket_from_monitor(events[i].data.fd);
								//	stop_listening(cli_sock);
									close_prog_memory();
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
									close_prog_memory();
									exit(0);
								}
							//	clear_response(&res);
							//	remove_socket_from_monitor(events[i].data.fd);
								stop_listening(events[i].data.fd);
								close_prog_memory();
								exit(1);

#else
								continue;
#endif
							}

#if USE_FORK

							//clear_response(&res);
							stop_listening(events[i].data.fd);
							close_prog_memory();
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
							if(load_resource(&req,&cont) == -1){

							printf("\n\n\n\nssecond branch load function failed\n\n\n\n\n");
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
										close_prog_memory();
										exit(0);
									}
									//clear_response(&res);
									stop_listening(events[i].data.fd);
									close_prog_memory();
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
									close_prog_memory();
									exit(0);
								}
								//clear_response(&res);
								stop_listening(events[i].data.fd);
								close_prog_memory();
								exit(1);
#else
								continue;
#endif
							}
#if USE_FORK
							//clear_response(&res);
							close_prog_memory();
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
									close_prog_memory();
									exit(0);
								}
								//clear_response(&res);
								stop_listening(events[i].data.fd);
								close_prog_memory();
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
								close_prog_memory();
								exit(0);
							}
							//clear_response(&res);
							stop_listening(events[i].data.fd);
							close_prog_memory();
							exit(1);

#else
							continue;
#endif
						}
#if USE_FORK
						//clear_response(&res);
						stop_listening(events[i].data.fd);
						close_prog_memory();
						exit(0);
#else
						clear_response(&res);
						remove_socket_from_monitor(events[i].data.fd);
						continue;
#endif

					}
					/*parent*/
					printf("in the parent\n");
					int wstatus;
					if(waitpid(child, &wstatus, WNOHANG) == -1)
						fprintf(stderr,"cannot wait for child process\n");

					if(WIFEXITED(wstatus)){
						remove_socket_from_monitor(events[i].data.fd);
						printf("parent try to remove the socket nr %d from monitor line %d\n",
								events[i].data.fd,
								__LINE__);
					}
					clear_request(&req);
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
