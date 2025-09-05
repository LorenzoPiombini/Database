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
		return -1;
	}
	fprintf(stderr,"listening on %d\n",port);

	if(start_monitor(con) == -1) {
		/*log the error*/
		stop_listening(con);
		return -1;
	}
	
	hdl_sock  = con;
	if(handle_sig() == -1) return -1;

	int cli_sock = -1;
	struct Response res = {0};
	for(;;){
		if((nfds = monitor_events()) == -1) break;
		if(nfds == EINTR) continue;

		int i;
		for(i = 0; i < nfds; i++){

			struct Request req = {0};
			req.method = -1;
			if(events[i].data.fd == con){
				int r = 0;		
				if((r = wait_for_connections(con,&cli_sock,&req)) == -1) break;

				if(r == EAGAIN || r == EWOULDBLOCK) continue;

				/*fork here*/
				pid_t child = fork();
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
						uint8_t close_sok = 1;
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
							int fds = -1;
							int exit = 0;
							while((fds = monitor_events()) != -1){
								int j;
								for(j = 0; j < fds;j++){
								if(events[i].data.fd == cli_sock){
									if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
									if(w == EAGAIN || w == EWOULDBLOCK) {
										continue;
									}else{
										remove_socket_from_monitor(cli_sock);
										exit = 1;
										close_sok = 0;
										break;
									}
								}
								}
								if(exit)break;
							}
						}

						clear_request(&req);
						clear_response(&res);
						if(close_sok) stop_listening(cli_sock);
						exit(0);
					}

					struct Content cont = {0};
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
								uint8_t close_sok = 1;
								if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
								if(w == EAGAIN || w == EWOULDBLOCK) {
									int fds = -1;
									int exit = 0;
									while((fds = monitor_events()) != -1){
										int j;
										for(j = 0; j < fds;j++){
											if(events[i].data.fd == cli_sock){
												if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
												if(w == EAGAIN || w == EWOULDBLOCK) {
													continue;
												}else{
													remove_socket_from_monitor(cli_sock);
													exit = 1;
													close_sok = 0;
													break;
												}
											}
										}
										if(exit)break;
									}
								}

								clear_request(&req);
								clear_response(&res);

								if(close_sok) stop_listening(cli_sock);
								exit(0);
							}
							/*send a response to the options request*/
							if(generate_response(&res,200,NULL,&req) == -1) break;

							clear_request(&req);
							int w = 0;
							uint8_t close_sok = 1;
							if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
							if(w == EAGAIN || w == EWOULDBLOCK) {
								int fds = -1;
								int exit = 0;
								while((fds = monitor_events()) != -1){
									int j;
									for(j = 0; j < fds;j++){
										if(events[i].data.fd == cli_sock){
											if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
											if(w == EAGAIN || w == EWOULDBLOCK) {
												continue;
											}else{
												remove_socket_from_monitor(cli_sock);
												exit = 1;
												close_sok= 0;
												break;
											}
										}
									}
									if(exit)break;
								}
							}

							if(close_sok)stop_listening(cli_sock);
							clear_response(&res);
							exit(0);
						}

						/*send a bed request response*/
						if(generate_response(&res,400,NULL,&req) == -1) break;

						clear_request(&req);
						int w = 0;
						uint8_t close_sok = 1;
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
							int fds = -1;
							int exit = 0;
							while((fds = monitor_events()) != -1){
								int j;
								for(j = 0; j < fds;j++){
								if(events[i].data.fd == cli_sock){
									if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
									if(w == EAGAIN || w == EWOULDBLOCK) {
										continue;
									}else{
										remove_socket_from_monitor(cli_sock);
										exit = 1;
										close_sok = 0;
										break;
									}
								}
								}
								if(exit)break;
							}
						}

						clear_request(&req);
						clear_response(&res);

						if(close_sok)stop_listening(cli_sock);
						exit(0);
					}
					case GET:
					case POST:
					{
						if(load_resource(&req,&cont) == -1){
							/*send a bed request response*/
							if(generate_response(&res,400,NULL,&req) == -1) break;

							clear_request(&req);
							int w = 0;
							uint8_t close_sok = 1;
							if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
							if(w == EAGAIN || w == EWOULDBLOCK) {
								int fds = -1;
								int exit = 0;
								while((fds = monitor_events()) != -1){
									int j;
									for(j = 0; j < fds;j++){
										if(events[i].data.fd == cli_sock){
											if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
											if(w == EAGAIN || w == EWOULDBLOCK) {
												continue;
											}else{
												remove_socket_from_monitor(cli_sock);
												exit = 1;
												close_sok = 0;
												break;
											}
										}
									}
									if(exit)break;
								}
							}

							if(close_sok) stop_listening(cli_sock);

							clear_content(&cont);
							clear_response(&res);
							exit(0);
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

						printf("response body is \n %s\n",res.body.content);
						uint8_t close_sok= 1;
						int w = 0;
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
							int fds = -1;
							int exit = 0;
							while((fds = monitor_events()) != -1){
								int j;
								for(j = 0; j < fds;j++){
									if(events[j].data.fd == cli_sock){
										if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
										if(w == EAGAIN || w == EWOULDBLOCK) {
											continue;
										}else{
											remove_socket_from_monitor(cli_sock);
											exit = 1;
											close_sok = 0;	
											break;
										}
									}
								}
								if(exit)break;
							}
						}

						if(close_sok) stop_listening(cli_sock);
						clear_request(&req);
						clear_response(&res);
						exit(0);
					}
					case PUT:
					break;
					default:
						if(generate_response(&res,404,NULL,&req) == -1) break;
						
						clear_content(&cont);
						uint8_t close_sok = 1;
						int w = 0;
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						while(w == EAGAIN || w == EWOULDBLOCK) {
							int fds = -1;
							int exit = 0;
							while((fds = monitor_events()) != -1){
								int j;
								for(j = 0; j < fds;j++){
									if(events[i].data.fd == cli_sock){
										if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
										if(w == EAGAIN || w == EWOULDBLOCK) {
											continue;
										}else{
											remove_socket_from_monitor(cli_sock);
											exit = 1;
											close_sok = 0;
											break;
										}
									}
								}
								if(exit)break;
							}
						}


						if(close_sok) stop_listening(cli_sock);
						clear_request(&req);
						clear_response(&res);

						exit(0);
					}
				}
				/*parent process*/

				/* 
				 * TODO:
				 * YOU HAVE TO HADLE THE CHILD 
				 * to avoid ZOMBIES
				 * */

				stop_listening(cli_sock);
			}else{
				int r = 0;
				if(events[i].events == EPOLLIN) {

					if((r = read_cli_sock(events[i].data.fd,&req)) == -1) break;
					if(r == EAGAIN || r == EWOULDBLOCK) continue;

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

							int w = 0;
							if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
							if(w == EAGAIN || w == EWOULDBLOCK) {
								continue;
							}

							if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

							clear_request(&req);
							clear_response(&res);

							continue;
						}

						/*send a response to the options request*/
						if(generate_response(&res,200,NULL,&req) == -1) break;


						int w = 0;
						if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK){
							continue;
						}

						if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

						clear_request(&req);
						clear_response(&res);

						continue;
					}
					case GET:
					case POST:
					{
						if(load_resource(&req,&cont) == -1){
							/*send a bed request response*/
							if(generate_response(&res,400,NULL,&req) == -1) break;

							int w = 0;
							if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
							if(w == EAGAIN || w == EWOULDBLOCK) {
								continue;
							}

							if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

							clear_request(&req);
							clear_response(&res);

							continue;
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
						
						clear_content(&cont);
						int w = 0;
						if((w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
							continue;
						}

						if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

						clear_request(&req);
						clear_response(&res);
						continue;
					}
					default:
						/*send a bad or not found request response*/
						if(generate_response(&res,404,NULL,&req) == -1) break;

						int w = 0;
						if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK){
							clear_content(&cont);
							continue;
						}


						if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

						clear_request(&req);
						clear_response(&res);
						continue;
					}

					/*since thne socket are NON BLOCKING this is needed*/
					/* send response */
					int w = 0;
					if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;


					if(w == EAGAIN || w == EWOULDBLOCK) continue;

					if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

					clear_request(&req);
					clear_response(&res);

				}else if(events[i].events == EPOLLOUT) {
					int w = 0;
					if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;

					if(w == EAGAIN || w == EWOULDBLOCK) {
						continue;
					}

					if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

					clear_request(&req);
					clear_response(&res);
				}
			}
		}
	}

	stop_listening(con);
	close_prog_memory();
	return 0;
}
