#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include "network.h"
#include "monitor.h"
#include "request.h"
#include "response.h"
#include "handlesig.h"
#include "memory.h"

#define USE_FORK 0

int main()
{
	if (init_prog_memory() == -1){
		fprintf(stderr,"cannot initialize memory for db inner working.\n");
		return -1;
	}

	unsigned short port = 5043;
	int con = -1;
	if((con = listen_port_80(&port)) == -1){
		fprintf(stderr,"can't connect to port '%d': %s.\n",port,strerror(errno));
		close_prog_memory();
		return -1;
	}

	fprintf(stderr,"listening on %d\n",port);

	if(start_monitor(con) == -1) {
		/*log the error*/
		stop_listening(con);
		close_prog_memory();
		return -1;
	}
	
	hdl_sock  = con;
	if(handle_sig() == -1) {
		stop_listening(con);
		close_prog_memory();
		return -1;
	}

	int cli_sock = -1;
	struct Response res;
	memset(&res,0,sizeof(struct Response));

	for(;;){
		if((nfds = monitor_events()) == -1) break;
		if(nfds == EINTR) continue;

		int i;
		for(i = 0; i < nfds; i++){

			struct Request req;
			memset(&req,0,sizeof(struct Request));
			req.method = -1;

			if(events[i].data.fd == con){
				int r = 0;		
				if((r = wait_for_connections(con,&cli_sock,&req)) == -1) break;

				if(r == EAGAIN || r == EWOULDBLOCK) continue;

				/*fork here*/
#if USE_FORK
				pid_t child = fork();
#else
				pid_t child = 0;
#endif
				if(child == -1){
					/*server error*/
					if(generate_response(&res,500,NULL,&req) == -1) break;

					int w = 0;
					if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
					if(w == EAGAIN || w == EWOULDBLOCK) {
						clear_request(&req);
						continue;
					}

					remove_socket_from_monitor(cli_sock);

					clear_request(&req);
					clear_response(&res);
					
					continue;
				}

				if(child == 0){
					/*send response*/
					if(r == BAD_REQ){
						if(generate_response(&res,400,NULL,&req) == -1) break;

						clear_request(&req);
						int w = 0;
#if USE_FORK
						uint8_t close_sok = 1;
#endif
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK
							int fds = -1;
							int exit = 0;
							while((fds = monitor_events()) != -1){
								int j;
								for(j = 0; j < fds;j++){
									if(events[j].events == EPOLLIN){
										if(( w = write_cli_sock(events[j].data.fd,&res)) == -1) break;
										if(w == EAGAIN || w == EWOULDBLOCK) {
											continue;
										}else{
											remove_socket_from_monitor(events[j].data.fd);
											exit = 1;
											close_sok = 0;
											break;
										}
									}
								}
								if(exit)break;
							}
#else 
							continue;
#endif
						}

#if USE_FORK
						clear_request(&req);
						clear_response(&res);
						if(close_sok) stop_listening(cli_sock);
						stop_monitor();
						exit(0);
#else
						clear_request(&req);
						clear_response(&res);
						stop_listening(cli_sock);
						break;
#endif
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
								if(generate_response(&res,400,NULL,&req) == -1) break;

								clear_request(&req);
								int w = 0;
#if USE_FORK
								uint8_t close_sok = 1;
#endif
								if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
								if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK
									int fds = -1;
									int exit = 0;
									while((fds = monitor_events()) != -1){
										int j;
										for(j = 0; j < fds;j++){
											if(events[j].events == EPOLLIN){
												if(( w = write_cli_sock(
													events[j].data.fd,&res)) == -1) break;
												if(w == EAGAIN || w == EWOULDBLOCK) {
													continue;
												}else{
													remove_socket_from_monitor(
															events[j].data.fd);
													exit = 1;
													close_sok = 0;
													break;
												}
											}
										}
										if(exit)break;
									}
#else
									continue;
#endif
								}

#if USE_FORK
								clear_request(&req);
								clear_response(&res);
								if(close_sok) stop_listening(cli_sock);
								stop_monitor();
								exit(0);
#else
								clear_request(&req);
								clear_response(&res);
								stop_listening(cli_sock);
								break;
#endif
							}
							/*send a response to the options request*/
							if(generate_response(&res,200,NULL,&req) == -1) break;

							clear_request(&req);
							int w = 0;
#if USE_FORK
							uint8_t close_sok = 1;
#endif
							if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
							if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK
								int fds = -1;
								int exit = 0;
								while((fds = monitor_events()) != -1){
									int j;
									for(j = 0; j < fds;j++){
										if(events[j].events == EPOLLIN){
											if(( w = write_cli_sock(
												events[j].data.fd,&res)) == -1) break;
											if(w == EAGAIN || w == EWOULDBLOCK) {
												continue;
											}else{
												remove_socket_from_monitor(
													events[j].data.fd);
												exit = 1;
												close_sok= 0;
												break;
											}
										}
									}
									if(exit)break;
								}
#else
								continue;
#endif
							}

#if USE_FORK
							if(close_sok)stop_listening(cli_sock);
							clear_response(&res);
							stop_monitor();
							exit(0);
#else
							stop_listening(cli_sock);
							clear_response(&res);
							break;
#endif
						}

						/*send a bed request response*/
						if(generate_response(&res,400,NULL,&req) == -1) break;

						clear_request(&req);
						int w = 0;
#if USE_FORK
						uint8_t close_sok = 1;
#endif
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK
							int fds = -1;
							int exit = 0;
							while((fds = monitor_events()) != -1){
								int j;
								for(j = 0; j < fds;j++){
									if(events[j].events == EPOLLIN){
										if(( w = write_cli_sock(
												events[j].data.fd,&res)) == -1) break;
										if(w == EAGAIN || w == EWOULDBLOCK) {
											continue;
										}else{
											remove_socket_from_monitor(events[j].data.fd);
											exit = 1;
											close_sok = 0;
											break;
										}
									}
								}
								if(exit)break;
							}
#else
							continue;
#endif
						}

#if USE_FORK
						if(close_sok)stop_listening(cli_sock);
						clear_request(&req);
						clear_response(&res);
						stop_monitor();
						exit(0);
#else
						clear_request(&req);
						clear_response(&res);
						stop_listening(cli_sock);
						break;
#endif
					}
					case GET:
					case POST:
					{
						if(load_resource(&req,&cont) == -1){
							/*send a bed request response*/
							if(generate_response(&res,400,NULL,&req) == -1) break;

							clear_request(&req);
							int w = 0;
#if USE_FORK
							uint8_t close_sok = 1;
#endif
							if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
							if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK
								int fds = -1;
								int exit = 0;
								while((fds = monitor_events()) != -1){
									int j;
									for(j = 0; j < fds;j++){
										if(events[j].data.fd == EPOLLIN){
											if(( w = write_cli_sock(
												events[j].data.fd,&res)) == -1) break;
											if(w == EAGAIN || w == EWOULDBLOCK) {
												continue;
											}else{
												remove_socket_from_monitor(
													events[j].data.fd);
												exit = 1;
												close_sok = 0;
												break;
											}
										}
									}
									if(exit)break;
								}
#else
								continue;
#endif
							}
#if USE_FORK
							if(close_sok) stop_listening(cli_sock);

							clear_content(&cont);
							clear_response(&res);
							stop_monitor();
							exit(0);
#else
							stop_listening(cli_sock);
							clear_response(&res);
							clear_content(&cont);
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
#if USE_FORK
						uint8_t close_sok= 1;
#endif
						int w = 0;
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK
							int fds = -1;
							int exit = 0;
							while((fds = monitor_events()) != -1){
								int j;
								for(j = 0; j < fds;j++){
									if(events[j].data.fd == EPOLLIN){
										if(( w = write_cli_sock(
											events[j].data.fd,&res)) == -1) break;
										if(w == EAGAIN || w == EWOULDBLOCK) {
											continue;
										}else{
											remove_socket_from_monitor(events[j].data.fd);
											exit = 1;
											close_sok = 0;	
											break;
										}
									}
								}
								if(exit)break;
							}
#else
							continue;
#endif
						}

#if USE_FORK
						if(close_sok) stop_listening(cli_sock);
						clear_response(&res);
						clear_content(&cont);
						stop_monitor();
						exit(0);
#else
						stop_listening(cli_sock);
						clear_response(&res);
						clear_content(&cont);
						break;
#endif
					}
					case PUT:
					break;
					default:
						if(generate_response(&res,404,NULL,&req) == -1) break;
						
						clear_content(&cont);
#if USE_FORK
						uint8_t close_sok = 1;
#endif
						int w = 0;
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						while(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK
							int fds = -1;
							int exit = 0;
							while((fds = monitor_events()) != -1){
								int j;
								for(j = 0; j < fds;j++){
									if(events[j].events == EPOLLIN){
										if(( w = write_cli_sock(
												events[j].data.fd,&res)) == -1) break;
										if(w == EAGAIN || w == EWOULDBLOCK) {
											continue;
										}else{
											remove_socket_from_monitor(events[j].data.fd);
											exit = 1;
											close_sok = 0;
											break;
										}
									}
								}
								if(exit)break;
							}
#else
							continue;
#endif
						}


#if USE_FORK
						if(close_sok) stop_listening(cli_sock);
						clear_request(&req);
						clear_response(&res);
						stop_monitor();
						exit(0);
#else
						stop_listening(cli_sock);
						clear_request(&req);
						clear_response(&res);

						break;
#endif
					}
				}
#if USE_FORK
				/*parent process*/
				stop_listening(cli_sock);
#endif
			}else{
				int r = 0;
				if(events[i].events == EPOLLIN) {

					if((r = read_cli_sock(events[i].data.fd,&req)) == -1) break;
					if(r == EAGAIN || r == EWOULDBLOCK) continue;


#if USE_FORK
					/*fork here*/
					pid_t child = fork();
#else
					pid_t child = 0;
#endif
					if(child == -1){
						/*send a bed request response*/

					}
					if(child == 0){
						if(r == BAD_REQ) {
							/*send a bed request response*/
						}

						struct Content cont= {0};
						switch(req.method){
						case OPTIONS:
						{
							size_t s = strlen(req.origin);
							if(s != strlen(ORIGIN_DEF)) goto bad_request;

							if(strncmp(req.origin,ORIGIN_DEF,strlen(ORIGIN_DEF)) != 0){

								bad_request:
								/*send a bed request response*/
								if(generate_response(&res,400,NULL,&req) == -1) break;
								clear_request(&req);

								int w = 0;
#if USE_FORK
								uint8_t close_sok = 1;
#endif
								if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
								if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK
									int fds = -1;
									int exit = 0;
									while((fds = monitor_events()) != -1){
										int j;
										for(j = 0; j < fds;j++){
											if(events[j].events == EPOLLIN){
												if(( w = write_cli_sock(
													events[j].data.fd,&res)) == -1) break;
												if(w == EAGAIN || w == EWOULDBLOCK) {
													continue;
												}else{
													remove_socket_from_monitor(events[j].data.fd);
													exit = 1;
													close_sok = 0;
													break;
												}
											}
										}
										if(exit)break;
									}
#else 
									continue;
#endif
								}
#if USE_FORK

								clear_request(&req);
								clear_response(&res);
								if(close_sok) 
									remove_socket_from_monitor(events[i].data.fd);
								stop_monitor();
								exit(0);
#else 

								clear_request(&req);
								clear_response(&res);
								remove_socket_from_monitor(events[i].data.fd);
								break;
#endif
							}

							/*send a response to the options request*/
							if(generate_response(&res,200,NULL,&req) == -1) break;


							int w = 0;
#if USE_FORK
							uint8_t close_sok = 1;
#endif
							if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
							if(w == EAGAIN || w == EWOULDBLOCK){
#if USE_FORK
								int fds = -1;
								int exit = 0;
								while((fds = monitor_events()) != -1){
									int j;
									for(j = 0; j < fds;j++){
										if(events[j].events == EPOLLIN){
											if(( w = write_cli_sock(
												events[j].data.fd,&res)) == -1) break;
											if(w == EAGAIN || w == EWOULDBLOCK) {
												continue;
											}else{
												remove_socket_from_monitor(events[j].data.fd);
												exit = 1;
												close_sok = 0;
												break;
											}
										}
									}
									if(exit)break;
								}
#else
								continue;
#endif
							}
#if USE_FORK
							if(close_sok)
								if(remove_socket_from_monitor(events[i].data.fd) == -1){
									clear_request(&req);
									clear_response(&res);
									stop_monitor();
									exit(1);
								}

							clear_request(&req);
							clear_response(&res);
							stop_monitor();
							exit(0);
#else
							clear_request(&req);
							clear_response(&res);
							if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

							break;
#endif
						}
						case GET:
						case POST:
						{
							if(load_resource(&req,&cont) == -1){
								/*send a bed request response*/
								if(generate_response(&res,400,NULL,&req) == -1) break;

								int w = 0;
#if USE_FORK
								uint8_t close_sok = 1;
#endif
								if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
								if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK
									int fds = -1;
									int exit = 0;
									while((fds = monitor_events()) != -1){
										int j;
										for(j = 0; j < fds;j++){
											if(events[j].events == EPOLLIN ){
												if(( w = write_cli_sock(
													events[j].data.fd,&res)) == -1) break;
												if(w == EAGAIN || w == EWOULDBLOCK) {
													continue;
												}else{
													remove_socket_from_monitor(events[j].data.fd);
													exit = 1;
													close_sok = 0;
													break;
												}
											}
										}
										if(exit)break;
									}
#else
									continue;
#endif

								}
#if USE_FORK
								if(close_sok)
									if(remove_socket_from_monitor(events[i].data.fd) == -1){
										clear_request(&req);
										clear_response(&res);
										clear_content(&cont);
										stop_monitor();
										exit(1);
									}

								clear_request(&req);
								clear_response(&res);
								clear_content(&cont);
								stop_monitor();
								exit(0);
#else
								if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

								clear_request(&req);
								clear_response(&res);
								clear_content(&cont);
								break;
#endif
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

							printf("in the second branch response body is \n %s\n",res.body.content);
							clear_content(&cont);
							int w = 0;
#if USE_FORK
							uint8_t close_sok = 1;
#endif
							if((w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
							if(w == EAGAIN || w == EWOULDBLOCK) {
#if USE_FORK
								int fds = -1;
								int exit = 0;
								while((fds = monitor_events()) != -1){
									int j;
									for(j = 0; j < fds;j++){
										if(events[j].events == EPOLLIN){
											if(( w = write_cli_sock(
												events[j].data.fd,&res)) == -1) break;
											if(w == EAGAIN || w == EWOULDBLOCK) {
												continue;
											}else{
												remove_socket_from_monitor(events[j].data.fd);
												exit = 1;
												close_sok = 0;
												break;
											}
										}
									}
									if(exit)break;
								}
#else
								continue;
#endif
							}
#if USE_FORK
							if(close_sok)
								if(remove_socket_from_monitor(events[i].data.fd) == -1) {
									clear_request(&req);
									clear_response(&res);
									stop_monitor();
									exit(1);
								}

							clear_request(&req);
							clear_response(&res);
							stop_monitor();
							exit(0);
#else 
							if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

							clear_request(&req);
							clear_response(&res);
							break;
#endif
						}
						default:
						/*send a bad or not found request response*/
						if(generate_response(&res,404,NULL,&req) == -1) break;

						clear_content(&cont);
						int w = 0;
#if USE_FORK
						uint8_t close_sok = 1;
#endif
						if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK){
#if USE_FORK
							int fds = -1;
							int exit = 0;
							while((fds = monitor_events()) != -1){
								int j;
								for(j = 0; j < fds;j++){
									if(events[j].events == EPOLLIN){
										if(( w = write_cli_sock(events[j].data.fd,&res)) == -1) break;
										if(w == EAGAIN || w == EWOULDBLOCK) {
											continue;
										}else{
											remove_socket_from_monitor(events[i].data.fd);
											exit = 1;
											close_sok = 0;
											break;
										}
									}
								}
								if(exit)break;
							}
#else
							continue;
#endif
						}

#if USE_FORK
						if(close_sok)
							if(remove_socket_from_monitor(events[i].data.fd) == -1){
								clear_request(&req);
								clear_response(&res);
								stop_monitor();
								exit(1);
							}

						clear_request(&req);
						clear_response(&res);
						stop_monitor();
						exit(0);
#else
						if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

						clear_request(&req);
						clear_response(&res);
						break;
#endif
						}

						/*since thne socket are NON BLOCKING this is needed*/
						/* send response */
						int w = 0;
						if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;

#if USE_FORK
						uint8_t close_sok = 1;
#endif
						if(w == EAGAIN || w == EWOULDBLOCK){
#if USE_FORK
							int fds = -1;
							int exit = 0;
							while((fds = monitor_events()) != -1){
								int j;
								for(j = 0; j < fds;j++){
									if(events[j].events == EPOLLIN){
										if(( w = write_cli_sock(events[j].data.fd,&res)) == -1) break;
										if(w == EAGAIN || w == EWOULDBLOCK) {
											continue;
										}else{
											remove_socket_from_monitor(events[i].data.fd);
											exit = 1;
											close_sok = 0;
											break;
										}
									}
								}
								if(exit)break;
							}
#else
							continue;
#endif
						}
#if USE_FORK
						if(close_sok)
							if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

						clear_request(&req);
						clear_response(&res);
						stop_monitor();
						exit(0);
#else
						if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

						clear_request(&req);
						clear_response(&res);
						break;
#endif
					}
					/*parent*/
					if(remove_socket_from_monitor(events[i].data.fd)== -1) break;
				}else if(events[i].events == EPOLLOUT) {
					/*the child process never gets here*/
					int w = 0;
					if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;	

					if(w == EAGAIN || w == EWOULDBLOCK) continue;

					if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

					clear_request(&req);
					clear_response(&res);
				}
			}
		}
	}

	stop_listening(con);
	stop_monitor();
	close_prog_memory();
	return 0;
}
