#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <str_op.h>
#include "request.h"
#include "end_points.h"
#include "memory.h"

static char prog[] = "net_interface";
static int get_headers_block(struct Request *req);
static int parse_header(char *head, struct Request *req);
static int get_method(char *method);
static int map_content_type(struct Request *req);

int handle_request(struct Request *req)
{
	int h_end = 0;
	if((h_end = get_headers_block(req)) == -1) return BAD_REQ;	

	char head[h_end+1];
	memset(head,0,h_end+1);
	if(req->d_req)
		strncpy(head,req->d_req,h_end);
	else
		strncpy(head,req->req,h_end);

	if(parse_header(head, req) == -1) return BAD_REQ;

	map_content_type(req);
	if(req->method == POST){
		/*we should have a body*/
		if((req->size - h_end) == 0) return BDY_MISS;
		if(req->d_req){
			if((req->size - h_end) < STD_REQ_BDY_CNT){
				strncpy(req->req_body.content,&req->d_req[h_end],(req->size - h_end) - 1 );
				return 0;
			}else{
				req->req_body.d_cont = (char *)ask_mem(req->size - h_end);
				if(!req->req_body.d_cont){
					fprintf(stderr,"(%s): ask_mem() failed, %s:%d\n",
							prog,__FILE__,__LINE__-2);
					return BAD_REQ;
				}
				strncpy(req->req_body.d_cont, &req->d_req[h_end],(req->size -h_end) - 1);
				req->req_body.size = req->size - h_end;
				return 0;
			}
		}	

		strncpy(req->req_body.content,&req->req[h_end],(req->size - h_end) - 1);
	}
	return 0;
}


int set_up_request(ssize_t bytes,struct Request *req)
{	
	ssize_t n_size = bytes * 2;
	req->d_req = (char *)ask_mem(n_size);
	if(!req->d_req){
		fprintf(stderr,"(%s): cannot allocate memory for request.\n",prog);
		return -1;
	}
	strncpy(req->d_req,req->req,req->size);	
	memset(req->req,0,BASE);
	req->size= n_size;
	return 0;
}

void clear_request(struct Request *req)
{
	if(req->d_req) cancel_memory(NULL,req->d_req,req->size);
	if(req->req_body.d_cont) cancel_memory(NULL,req->req_body.d_cont,req->req_body.size);
	memset(req,0,sizeof(struct Request));
	
}


static int parse_header(char *head, struct Request *req)
{
	char *crlf = NULL;
	int start = 0;
	while((crlf = strstr(&head[start],"\r"))){
		if (start > 0){
			int end = crlf - head;
			size_t s = end - start;
			char t[s+1];
			memset(t,0,s+1);
			strncpy(t,&head[start],s);

			char *b = NULL;
			if((b = strstr(t,"Host:"))){	
				b += strlen("Host: ");
				strncpy(req->host,b,strlen(b));
				*crlf = ' ';
				start = end + 2;
				continue;
			}
			
			if((b = strstr(t,"Transfer-Encoding:"))){
				b += strlen("Transfer-Encoding: ");
				strncpy(req->transfer_encoding,b,strlen(b));
				*crlf = ' ';
				start = end + 2;
				continue;
			}

			if((b = strstr(t,"Access-Control-Request-Method:"))){
				b += strlen("Access-Control-Request-Method: ");
				strncpy(req->access_control_request_method,b,strlen(b));
				*crlf = ' ';
				start = end + 2;
				continue;
			}

			if((b = strstr(t,"Access-Control-Request-Headers:"))){
				b += strlen("Access-Control-Request-Headers: ");
				strncpy(req->access_control_request_headers,b,strlen(b));
				*crlf = ' ';
				start = end + 2;
				continue;
			}

			if((b = strstr(t,"Origin:"))){
				b += strlen("Origin: ");
				strncpy(req->origin,b,strlen(b));
				*crlf = ' ';
				start = end + 2;
				continue;
			}

			if((b = strstr(t,"Connection:"))){
				b += strlen("Connection: ");
				strncpy(req->connection,b,strlen(b));
				*crlf = ' ';
				start = end + 2;
				continue;
			}

			if((b = strstr(t,"Content-Type:"))){
				b += strlen("Content-Type: ");
				strncpy(req->cont_type,b,strlen(b));
				*crlf = ' ';
				start = end + 2;
				continue;
			}

			if((b = strstr(t,"Content-Length:"))){
				b += strlen("Content-Length: ");
				char *endp;
				errno = 0;
				req->cont_length = (size_t)strtol(b,&endp,10);
				if(errno == EINVAL || errno == LONG_MAX){
					printf("string to number conversion failed\n");
					req->cont_length = 0;
				}
				*crlf = ' ';
				start = end + 2;
				continue;
			}
			*crlf = ' ';
			start = end + 2;
			continue;
		}	

		int end = crlf - head;		
		char t[end+1];
		memset(t,0,end+1);
		strncpy(t,head,end);
		char *tok = strtok(t," ");
		if ((req->method = get_method(tok)) == -1 ) return BAD_REQ;

		tok = strtok(NULL, " ");
		size_t tok_l = strlen(tok);
		if(strlen(tok) > STD_LT_RESOURCE){
			/* TODO handle this case*/
		}else{
			strncpy(req->resource,tok,tok_l);
		}
		tok = strtok(NULL," ");
		tok_l = strlen(tok);
		if (strncmp(tok,DEFAULT,tok_l) == 0){
			strncpy(req->protocol,tok,tok_l);
			if(strstr(head,"Host:") == NULL) return BAD_REQ;
		}else{
			/*as for now we support only HTTP/1.1*/
			return BAD_REQ;
		}	
		*crlf = ' ';
		start = end + 2;
	}
	return 0;
}

int find_headers_end(char *buffer, size_t size)
{
	char *start = buffer;
	int c = 0;
	while(*buffer != '\r' && ((size_t)(buffer - start) < size)) {
		buffer++;
		if(*buffer != '\r') continue; 

		while(*buffer == '\r' || *buffer == '\n'){
			if((size_t)(buffer - start) == size) break;
			c++;
			buffer++;
		}

		if( c == 4) break;
		c = 0;
	}

	if(c < 4) return -1;
	if(c == 4) return (int)(buffer - start);

	return -1;
}

int write_request_to_pipe(int pipefd,struct Request *req)
{
	size_t buffer_size = 0;
	if(!req->d_req && req->req[0] != '\0') 
		buffer_size += strlen(req->req) + 1;
	else
		buffer_size += strlen(req->d_req) + 1;

	if(req->size > 0) buffer_size += number_of_digit(req->size) + 1;
	if(req->method != -1 ) buffer_size += 2;
	if(req->protocol[0] != '\0') buffer_size += strlen(req->protocol) + 1;
	if(req->host[0] != '\0') buffer_size += strlen(req->host) + 1;
	if(req->resource[0] != '\0') buffer_size += strlen(req->resource) + 1;
	if(req->connection[0] != '\0') buffer_size += strlen(req->connection) + 1;
	if(req->cont_type[0] != '\0') buffer_size += strlen(req->cont_type) + 1;
	if(req->cont_length != 0) buffer_size += number_of_digit(req->cont_length) + 1;
	if(req->transfer_encoding[0] != '\0') buffer_size += strlen(req->transfer_encoding) + 1;
	if(req->access_control_request_headers[0] != '\0') buffer_size += strlen(req->access_control_request_headers) + 1;
	if(req->access_control_request_method[0] != '\0') buffer_size += strlen(req->access_control_request_method) + 1;
	if(req->origin[0] != '\0') buffer_size += strlen(req->origin) + 1;
	if(!req->req_body->d_cont && req->req_body->content[0] != '\0')
		buffer_size += strlen(req->req_body->content) + 1;
	else
		buffer_size += strlen(req->req_body->d_cont) + 1;
	
	if(req->req_body->size > 0) buffer_size += number_of_digit(req->size) + 1;

	buffer_size += number_of_digit(buffer_size) + 1;
	char buff[buffer_size+1];
	memset(buff,0,buffer_size+1);

	/*copy data to buffer*/
	char *p = buff;

	int number_size = number_of_digit(buffer_size);
	if(copy_to_string(p,number_size < 4 ? 4 : number_size + 1,"%ld#",buffer_size) == -1){
		fprintf(stderr,"(%s): copy_to_string failed, %s:%d\n",__FILE__,__LINE__-1);
		return -1;
	}

	p += number_size + 1;

	if(!req->d_req && req->req[0] != '\0'){
		size_t s = strlen(req->req);
		strncpy(p,req->req,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;
	}else{
		size_t s = strlen(req->d_req);
		strncpy(p,req->d_req,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;
	}


	if(req->size > 0){
		number_size = number_of_digit(req->size);
		if(copy_to_string(p,number_size < 4 ? 4 : number_size + 1,"%ld#",req->size) == -1){
				fprintf(stderr,"(%s): copy_to_string failed, %s:%d\n",__FILE__,__LINE__-1);
				return -1;
		}
		p += number_size + 1;
	}
	if(req->method != -1 ){
		if(copy_to_string(p,3,"%d#",req->method) == -1){
			fprintf(stderr,"(%s): copy_to_string failed, %s:%d\n",prog,__FILE__,__LINE__-1);
			return -1;
		}
		p += 2;
	}

	if(req->protocol[0] != '\0'){ 
		size_t s = strlen(req->protocol);
		strncpy(p,req->protocol,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;
	}
	if(req->host[0] != '\0') {
		size_t s = strlen(req->host);
		strncpy(p,req->host,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;
	}	
	if(req->resource[0] != '\0') {
		size_t s = strlen(req->resource);
		strncpy(p,req->resource,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;
	}
	if(req->connection[0] != '\0'){
		size_t s = strlen(req->connection);
		strncpy(p,req->connection,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;
	}

	if(req->cont_type[0] != '\0') {
		size_t s = strlen(req->cont_type);
		strncpy(buff,req->cont_type,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;
	}

	if(req->cont_length != 0) {
		number_size = number_of_digit(req->cont_length);
		if(copy_to_string(p,number_size < 4 ? 4 : number_size + 1,"%ld#",req->cont_length) == -1){
			fprintf(stderr,"(%s): copy_to_string failed, %s:%d\n",prog,__FILE__,__LINE__-1);
			return -1;
		}
		p += (number_size + 1);
	}
	if(req->transfer_encoding[0] != '\0'){ 
		size_t s = strlen(req->transfer_encoding);
		strncpy(p,req->transfer_encoding,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;
	}

	if(req->access_control_request_headers[0] != '\0'){
		size_t s = strlen(req->access_control_request_headers);
		strncpy(p,req->access_control_request_headers,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;
	}

	if(req->access_control_request_method[0] != '\0'){
		size_t s = strlen(req->access_control_request_method);
		strncpy(p,req->access_control_request_method,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;

	}

	if(req->origin[0] != '\0'){
		size_t s = strlen(req->origin);
		strncpy(p,req->origin,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;
	}

	if(!req->req_body.d_cont && req->req_body.content[0] != '\0'){
		size_t s = strlen(req->req_body.content);
		strncpy(p,req->req_body.content,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;
	}else{
		size_t s = strlen(req->req_body.d_cont);
		strncpy(p,req->req_body.d_cont,s);
		p += s;
		strncpy(p,"#",2);
		p += 1;
	}

	if(req->req_body.size != 0) {
		number_size = number_of_digit(req->req_body.size);
		if(copy_to_string(p,number_size < 4 ? 4 : number_size + 1,"%ld#",req->req_body.size) == -1){
			fprintf(stderr,"(%s): copy_to_string failed, %s:%d\n",prog,__FILE__,__LINE__-1);
			return -1;
		}
		p += (number_size + 1);
	}

	/*write to pipe*/
	if(write(pipefd,buff,buffer_size) == -1){
		fprintf(stderr,"(%s): cannot write to pipe %s:%d\n",prog,__FILE__,__LINE__-1);
		return -1;
	}

	return 0;
}


int read_request_from_pipe(int pipefd,struct Request *req)
{
	char *buffer = (char *)ask_mem(READ_PIPE_FX_SIZE);
	if(!buffer){
		fprintf(stderr,"(%s) ask_mem failed %s:%d.\n",prog,__FILE__,__LINE__-2);	
		return -1;
	}

	if(read(pipefd,buffer,READ_PIPE_FX_SIZE) == -1){
		fprintf(stderr,"(%s) read failed %s:%d.\n",prog,__FILE__,__LINE__-1);	
		return -1;
	}

	char *t = tok(buffer,"#");
	size_t buffer_size = 0;
	if(t){
		errno = 0;
		buffer_size = (size_t)string_to_long(t);
		if(errno == EINVAL){
			fprintf(stderr,"(%s): cannot read proper data from pipe buffer\n",prog,__FILE__,__LINE__-2);
			cancel_memory(NULL,buffer,READ_PIPE_FX_SIZE);
			return -1;
		}
	}

	if(buffer_size > READ_PIPE_FX_SIZE){
		/*expand the memory*/
		/*TODO: 
		 * read the pipe again
		 * put the remaining data at the end of the buffer that you already read*/

	}


	while((t = tok(NULL,"#"))){
		/*parse the request*/

	}


}
/*=================== STATIC FUNCTIONS DECLARATIONS ===========================*/

static int get_headers_block(struct Request *req)
{
	if(req->d_req) return find_headers_end(req->d_req, req->size);
	
	return find_headers_end(req->req, req->size);
}


static int get_method(char *method)
{
	size_t method_l = strlen(method);
	if(strlen("GET") == method_l)
		if(strncmp("GET",method,method_l) == 0) return GET;
	
	if(strlen("HEAD") == method_l)
		if(strncmp("HEAD",method,method_l) == 0) return HEAD;

	if(strlen("POST") == method_l)
		if(strncmp("POST",method,method_l) == 0) return POST;

	if(strlen("PUT") == method_l)
		if(strncmp("PUT",method,method_l) == 0) return PUT;

	if(strlen("DELETE") == method_l)
		if(strncmp("DELETE",method,method_l) == 0) return DELETE;
	
	if(strlen("CONNECT") == method_l)
		if(strncmp("CONNECT",method,method_l) == 0) return CONNECT;

	if(strlen("OPTIONS") == method_l)
		if(strncmp("OPTIONS",method,method_l) == 0) return OPTIONS;
		
	if(strlen("TRACE") == method_l)
		if(strncmp("TRACE",method,method_l) == 0) return TRACE;

	return -1;
}

static int map_content_type(struct Request *req)
{
	if(strstr(req->resource,".html")) {
		strncpy(req->cont_type,"text/html",MIN_HEAD_FIELD);
		return 0;
	} 

	if(strncmp(req->resource,"/", 2) == 0) {
		strncpy(req->cont_type,"text/html",MIN_HEAD_FIELD);
		return 0;
	} 

	if(strstr(req->resource,".css")) {
		strncpy(req->cont_type,"text/css",MIN_HEAD_FIELD);
		return 0;
	} 
	if(strstr(req->resource,".js")) {
		strncpy(req->cont_type,"text/javascript",MIN_HEAD_FIELD);
		return 0;
	} 
	if(strstr(req->resource,".jpeg")) {
		strncpy(req->cont_type,"image/jpeg",MIN_HEAD_FIELD);
		return 0;
	} 

	if(strstr(req->resource,".png")) {
		strncpy(req->cont_type,"image/png",MIN_HEAD_FIELD);
		return 0;
	} 

	if(strstr(req->resource,".json")) {
		strncpy(req->cont_type,"application/json",MIN_HEAD_FIELD);
		return 0;
	} 

	size_t s = strlen(req->resource);
	if(s == strlen(NEW_S_ORDER)){
		if(strncmp(req->resource,NEW_S_ORDER,s) == 0){
			strncpy(req->cont_type,"application/json",MIN_HEAD_FIELD);
			return 0;
		}
	}
	
	strncpy(req->cont_type,"text/plain",MIN_HEAD_FIELD);
	return 0;
}
