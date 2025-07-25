#include <stdio.h>
#include <errno.h>
#include <string.h>
#include "network.h"
#include "monitor.h"
#include "request.h"
#include "response.h"

int main()
{

	unsigned short port = 5043;
	int con = -1;
	if((con = listen_port_80(&port)) == -1) return -1;
	fprintf(stderr,"listening on %d\n",port);

	if(start_monitor(con) == -1) {
		/*log the error*/
		stop_listening(con);
		return -1;
	}
	
	int cli_sock = -1;
	struct Response res = {0};
	for(;;){
		if((nfds = monitor_events()) == -1) break;
		if(nfds == EINTR) continue;

		for(int i = 0; i < nfds; i++){

			struct Request req = {0};
			req.method = -1;
			if(events[i].data.fd == con){
				int r = 0;		
				if((r = wait_for_connections(con,&cli_sock,&req)) == -1) break;

				if(r == EAGAIN || r == EWOULDBLOCK) continue;

				/*send response*/
				if(r == BAD_REQ){
											
				}

				printf("%s\n",req.req);
				struct Content cont = {0};
				switch(req.method){
				case OPTIONS:
				{
					size_t s = strlen(req.origin);
					if(s == strlen(ORIGIN_DEF)){
						if(strncmp(req.origin,ORIGIN_DEF,strlen(ORIGIN_DEF)) != 0){
							/*send a bed request response*/
							if(generate_response(&res,400,NULL,&req) == -1) break;

							int w = 0;
							if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
							if(w == EAGAIN || w == EWOULDBLOCK) {
								clear_request(&req);
								continue;
							}

							clear_request(&req);
							clear_response(&res);

							stop_listening(cli_sock);
							continue;
						}
						/*send a response to the options request*/
						if(generate_response(&res,200,NULL,&req) == -1) break;

						int w = 0;
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
							clear_request(&req);
							continue;
						}

						clear_request(&req);
						clear_response(&res);

						stop_listening(cli_sock);
						continue;

					}
					/*send a bed request response*/
					if(generate_response(&res,400,NULL,&req) == -1) break;

					int w = 0;
					if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
					if(w == EAGAIN || w == EWOULDBLOCK) {
						clear_request(&req);
						continue;
					}


					clear_request(&req);
					clear_response(&res);

					stop_listening(cli_sock);
					continue;
				}
				case GET:
					break;
				case POST:
					break;
				case PUT:
					break;
				default:
					if(generate_response(&res,404,NULL,&req) == -1) break;

					int w = 0;
					if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
					if(w == EAGAIN || w == EWOULDBLOCK) {
						clear_request(&req);
						clear_content(&cont);
						continue;
					}

					stop_listening(cli_sock);

					clear_request(&req);
					clear_response(&res);
					continue;
				}
			}else{
				int r = 0;
				if(events[i].events == EPOLLIN) {

					if((r = read_cli_sock(events[i].data.fd,&req)) == -1) break;
					if(r == EAGAIN || r == EWOULDBLOCK) continue;

					if(r == BAD_REQ) {
						/*send a bed request response*/
					}

					printf("%s\n",req.req);
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
								clear_request(&req);
								continue;
							}

							clear_request(&req);
							clear_response(&res);

							if(remove_socket_from_monitor(events[i].data.fd) == -1) break;
							continue;
						}

						/*send a response to the options request*/
						if(generate_response(&res,200,NULL,&req) == -1) break;

						int w = 0;
						if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
							clear_request(&req);
							continue;
						}

						clear_request(&req);
						clear_response(&res);

						if(remove_socket_from_monitor(events[i].data.fd) == -1) break;
						continue;
					}
					case GET:
						/* Load content */	

						break;
					default:
						/*send a bed request response*/

						if(generate_response(&res,404,NULL,&req) == -1) break;

						int w = 0;
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
							clear_request(&req);
							clear_content(&cont);
							continue;
						}

						stop_listening(cli_sock);

						clear_request(&req);
						clear_response(&res);
						continue;

					}

					/* send response */

					clear_content(&cont);
					int w = 0;
					if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;


					printf("after writing to client.\n");
					if(w == EAGAIN || w == EWOULDBLOCK) continue;

					if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

					if(req.d_req)
						fprintf(stdout,"%s\n",req.d_req);
					else
						fprintf(stdout,"%s\n",req.req);

					clear_response(&res);
					clear_request(&req);
				}else if(events[i].events == EPOLLOUT) {
					int w = 0;
					if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;

					if(w == EAGAIN || w == EWOULDBLOCK) {
						clear_request(&req);
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
	return 0;
}
