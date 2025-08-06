#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "network.h"
#include "monitor.h"
#include "request.h"
#include "response.h"
#include "handlesig.h"

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
	if(handle_sig() == -1) return -1;

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
				
#if 0
				if(strncmp(req.connection,KEEP_ALIVE,strlen(req.connection)) == 0){
					/*set keep alive*/
					int keep_al = 60;
					if(setsockopt(cli_sock,SOL_SOCKET,SO_KEEPALIVE,&keep_al,sizeof(keep_al)) == -1){
						/*log error*/	
						clear_request(&req);
						clear_response(&res);
						continue;
					}

					/*check if cli_sock is in the monitor*/
					if(!is_sock_in_monitor(cli_sock)){
						if(add_socket_to_monitor(cli_sock,EPOLLIN) == -1){
							clear_request(&req);
							clear_response(&res);
							continue;
						}
					}
				}
#endif	
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
				case POST:
				{
					if(load_resource(&req,&cont) == -1){
						/*send a bed request response*/
						if(generate_response(&res,400,NULL,&req) == -1) break;

						int w = 0;
						if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
							clear_request(&req);
							continue;
						}

						clear_request(&req);
						clear_content(&cont);
						clear_response(&res);
						stop_listening(cli_sock);

						continue;
					}	
					/*send a response to the client request*/
					int status = 0;

					if(req.method == GET){
						status = 200;
						if(generate_response(&res,status,&cont,&req) == -1) break;
					}else if((req.method == POST)){
						status = 201;
						if(cont.cnt_dy){
							if(resource_created_response(&res,status,&req,cont.cnt_dy) == -1) break;
						}else{
							if(resource_created_response(&res,status,&req,cont.cnt_st) == -1) break;
						}
					}

					clear_content(&cont);
					int w = 0;
					if(( w = write_cli_sock(cli_sock,&res)) == -1) break;
					if(w == EAGAIN || w == EWOULDBLOCK) {
						clear_request(&req);
						continue;
					}

					clear_request(&req);
					clear_response(&res);
					stop_listening(cli_sock);

					break;
				}
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


					clear_request(&req);
					clear_response(&res);
					stop_listening(cli_sock);
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
#if 0
					if(strncmp(req.connection,KEEP_ALIVE,strlen(req.connection)) == 0){
						/*check keep alive*/
						int is_alive = 0;
						socklen_t optlen = sizeof(is_alive);
						if(getsockopt(events[i].data.fd,
									SOL_SOCKET, 
									SO_KEEPALIVE,
									&is_alive,
									&optlen) == 1){
							/*log error*/	
							clear_request(&req);
							clear_response(&res);
							continue;
						}

						if(!is_alive){
							int keep_al = 1;
							if(setsockopt(events[i].data.fd,
										SOL_SOCKET,
										SO_KEEPALIVE,
										&keep_al,
										sizeof(keep_al)) == -1){
								/*log error*/	
								clear_request(&req);
								clear_response(&res);
								continue;
							}
						}
					}
#endif
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
						if(w == EAGAIN || w == EWOULDBLOCK){
							clear_request(&req);
							continue;
						}

						clear_request(&req);
						clear_response(&res);
						if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

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
								clear_request(&req);
								continue;
							}


							clear_request(&req);
							clear_response(&res);
							if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

							continue;
						}	
						/*send a response*/
						int status = 0;
						if(req.method == GET){
							status = 200;
							if(generate_response(&res,status,&cont,&req) == -1) break;
						}else if((req.method == POST)){
							status = 201;
							if(cont.cnt_dy){
								if(resource_created_response(&res,status,&req,cont.cnt_dy) == -1) break;
							}else{
								if(resource_created_response(&res,status,&req,cont.cnt_st) == -1) break;
							}
						}
						
						clear_content(&cont);
						int w = 0;
						if((w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK) {
							clear_request(&req);
							continue;
						}

						clear_request(&req);
						clear_response(&res);
						if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

						continue;
					}
					default:
						/*send a bad or not found request response*/
						if(generate_response(&res,404,NULL,&req) == -1) break;

						int w = 0;
						if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;
						if(w == EAGAIN || w == EWOULDBLOCK){
							clear_request(&req);
							clear_content(&cont);
							continue;
						}


						clear_request(&req);
						clear_response(&res);
						if(remove_socket_from_monitor(events[i].data.fd) == -1) break;
						continue;
					}

					/*since thne socket are NON BLOCKING this is needed*/
					/* send response */
					int w = 0;
					if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;


					if(w == EAGAIN || w == EWOULDBLOCK) continue;

					if(remove_socket_from_monitor(events[i].data.fd) == -1) break;

					clear_response(&res);
					clear_request(&req);

				}else if(events[i].events == EPOLLOUT) {
					int w = 0;
					if(( w = write_cli_sock(events[i].data.fd,&res)) == -1) break;

					if(w == EAGAIN || w == EWOULDBLOCK) {
						clear_request(&req);
						continue;
					}

					clear_request(&req);
					clear_response(&res);
					if(remove_socket_from_monitor(events[i].data.fd) == -1) break;
				}
			}
		}
	}

	stop_listening(con);
	return 0;
}
