#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "response.h"

static char prog[] = "net_interface";
static int set_up_headers(struct Header *headers, int status, size_t body_size, struct Request *req);
static void set_status_and_phrase(struct Header *headers, uint16_t status);
static char *create_response_message(struct Response *res, int status, struct Content *cont, struct Request *req);
static int parse_body(struct Content *cont, struct Response *res);
static char *month_parser(int month);
static char *day_parser(int day);
static char *second_parser(int second);
static char *date_formatter();
static int map_method_and_status(int method,int status,int body);

int generate_response(struct Response *res, int status, struct Content *cont, struct Request *req){
	char *h = create_response_message(res,status,cont,req);
	if (!h) return -1;

 	strncpy(res->header_str,h,strlen(h));
	if(cont)
		if(parse_body(cont,res) == -1) return -1;

	return 0;
}

void clear_response(struct Response *res)
{
	if(res->body.d_cont) free(res->body.d_cont);

	memset(res,0,sizeof(struct Response));
}

static char *create_response_message(struct Response *res, int status, struct Content *cont, struct Request *req)
{
	static char h[STD_HD_L] = {0};

	if(set_up_headers(&res->headers,status,cont == NULL ? 0 : cont->size,req) == -1) return NULL;

	/* string the header*/
	int response_type = map_method_and_status(req->method,status,cont != NULL ? cont->size > 0 : 0);
	switch(response_type)
	{
	case SERVER_ERROR: 
	{
		if(snprintf(h,STD_HD_L,"%s %d %s\r\n"\
				"Content-Type: %s\r\n"\
				"Content-lenght: %ld\r\n\r\n%s",
				"HTTP/1.1", 500, "Internal server error",
				"application/json",
				strlen(SERVER_ER_MES),SERVER_ER_MES) == -1){
			fprintf(stderr,"(%s): cannot form BAD RESPONSE.",prog);
			return NULL;
		}
		break;
	}
	case BAD_REQ_RESPONSE:
		if(snprintf(h,STD_HD_L,"%s %d %s\r\n"\
				"Content-Type: %s\r\n"\
				"Content-lenght: %ld\r\n\r\n%s","HTTP/1.1", 400, "Bad request",
				"application/json",strlen(BAD_REQ_MES),BAD_REQ_MES) == -1){
			fprintf(stderr,"(%s): cannot form BAD RESPONSE.",prog);
			return NULL;
		}
		break;
	case NOT_FOUND_RESPONSE:
		if(snprintf(h,STD_HD_L,"%s %d %s\r\n"\
					"Date: %s\r\n"\
					"Content-Type: %s\r\n"\
					"Connection: %s\r\n"\
					"\r\n",res->headers.protocol_vs, 404, "Not Found",res->headers.date,
					req->cont_type,res->headers.connection) == -1){

			return NULL;
		}
		break;
	case OK_POST_RESPONSE:/*TODO*/
	case OK_CREATED_POST_RESPONSE:
			if(snprintf(h,STD_HD_L,"%s %d %s\r\n"\
						"Content-Type: %s\r\n"\
						"Location: %s\r\n"\
						"Access-Control-Allow-Origin: %s\r\n"
						"Keep-Alive: %s\r\n"
						"\r\n",
						res->headers.protocol_vs,
						status,
						res->headers.reason_phrase,
						req->cont_type,
						req->host,ORIGIN_DEF,
						res->headers.keep_alive) == -1){
				fprintf(stderr,"(%s): snprintf() failed %s:%d",prog,__FILE__,__LINE__-7);
				return NULL;
			}

		break;
	case OK_GET_RESPONSE:
		if(snprintf(h,STD_HD_L,"%s %d %s\r\n"\
						"Content-Type: %s\r\n"\
						"Location: %s\r\n"\
						"Access-Control-Allow-Origin: %s\r\n"
						"Keep-Alive: %s\r\n"
						"\r\n",
						res->headers.protocol_vs,
						status,
						res->headers.reason_phrase,
						req->cont_type,
						req->host,
						ORIGIN_DEF,
						res->headers.keep_alive) == -1){
			fprintf(stderr,"(%s): snprintf() failed %s:%d",prog,__FILE__,__LINE__-7);
			return NULL;
		}

		break;
	case OK_GET_RESPONSE_BODY:
		if(snprintf(h,STD_HD_L,"%s %d %s\r\n"\
						"Content-Type: %s\r\n"\
						"Location: %s\r\n"\
						"Access-Control-Allow-Origin: %s\r\n"
						"Keep-Alive: %s\r\n"
						"\r\n",
						res->headers.protocol_vs,
						status,
						res->headers.reason_phrase,
						req->cont_type,
						req->host,
						ORIGIN_DEF,
						res->headers.keep_alive) == -1){
			fprintf(stderr,"(%s): snprintf() failed %s:%d",prog,__FILE__,__LINE__-7);
			return NULL;
		}
		break;
	case OPTIONS_RESPONSE:
			if(snprintf(h,STD_HD_L,"%s %d %s\r\n"\
				"Access-Control-Allow-Origin: %s\r\n"\
				"Access-Control-Allow-Methods: %s,\r\n"\
				"Access-Control-Allow-Headers: %s,\r\n"\
				"Access-Control-Max-Age: %u\r\n\r\n",
				res->headers.protocol_vs,
				status,
				res->headers.reason_phrase,
				ORIGIN_DEF,
				ALLOWED_METHODS,
				res->headers.access_control_allow_headers,
				res->headers.access_control_max_age) == -1){
				fprintf(stderr,"(%s): cannot form BAD RESPONSE.",prog);
				return NULL;
			}

		break;
	default:
		break;
	}
	return &h[0];
}


static int set_up_headers(struct Header *headers, int status, size_t body_size, struct Request *req)
{
	set_status_and_phrase(headers,(uint16_t)status);
		
	if(strncmp(req->protocol,STD_PTC,STD_LEN_PTC) == 0)
		strncpy(headers->protocol_vs,STD_PTC,STD_LEN_PTC);
	else
		strncpy(headers->protocol_vs,req->protocol,strlen(req->protocol));

	char *date = date_formatter();
	if(!date) return -1;
	strncpy(headers->date,date,50); 
	if(strncmp(req->connection,KEEP_ALIVE,strlen(KEEP_ALIVE)) == 0){
		strncpy(headers->connection,req->connection,50);
		strncpy(headers->keep_alive,"timeout=3600, max=200",strlen("timeout=3600, max=200")+1);
	}

	if(status != 400 || status != 404)
		strncpy(headers->cont_type,req->cont_type,strlen(req->cont_type));

	if(req->method == OPTIONS){
		strncpy(headers->access_control_allow_methods,ALLOWED_METHODS,strlen(ALLOWED_METHODS)+1);
		strncpy(headers->access_control_allow_headers,req->access_control_request_headers,strlen(req->access_control_request_headers));
		strncpy(headers->access_control_allow_origin,req->origin,strlen(req->origin));
		headers->access_control_max_age = SECONDS_IN_A_DAY;
	}
	
	if (body_size > 0) headers->content_lenght = body_size;

	return 0;
}

static void set_status_and_phrase(struct Header *headers, uint16_t status)
{
	switch(status){
	case 100:
		headers->status = status;
		strncpy(headers->reason_phrase,"Continue",50);
		break;
	case 101  :
		headers->status = status;
	       strncpy(headers->reason_phrase,"Switching Protocols",50);
	       break;
	case 200:   
		headers->status = status;
		strncpy(headers->reason_phrase,"OK",50);
		break;
	case 201:
		headers->status = status;
		strncpy(headers->reason_phrase,"Created",50);
		break;
		       
	case 202  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Accepted",50);
		break;
		       
	case 203  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Non-Authoritative Information",50);
		break;
		       
	case 204  :
		headers->status = status;
		strncpy(headers->reason_phrase,"No Content",50);
		break;
	 	      
	case 205  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Reset Content",50);
		break;
		      
	case 206  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Partial Content",50);
		break;
		       
	case 300  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Multiple Choices",50);
		break;
		       
	case 301  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Moved Permanently",50);
		break;
		       
	case 302  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Found",50);
		break;
		       
	case 303  :
		headers->status = status;
		strncpy(headers->reason_phrase,"See Other",50);
		break;
		      
	case 304  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Not Modified",50);
		break;
		       
	case 305  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Use Proxy",50);
		break;
		       
	case 307  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Temporary Redirect",50);
		break;
		       
	case 400  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Bad Request",50);
		break;
		       
	case 401  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Unauthorized",50);
		break;
		      
	case 402  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Payment Required ",50);
		break;
		      
	case 403  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Forbidden",50);
		break;
		      
	case 404  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Not Found",50);
		break;
		       
	case 405  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Method Not All",50);
		break;
	 	      
	case 406  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Not Acceptabl",50);
		break;
	       		
	case 407  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Proxy Authen",50);
		break;
	       		
	case 408  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Request Tim",50);
		break;
	       
	case 409  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Conflict",50);
		break;
	       
	case 410  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Gone",50);
		break;
	       
	case 411  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Length R",50);
		break;
	       
	case 412  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Precond",50);
		break;
		       
	case 413  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Payloa",50);
		break;
	       
	case 414  :
		headers->status = status;
		strncpy(headers->reason_phrase,"URI T",50);
		break;
	       
	case 415  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Unsupported Media Type",50);
		break;
	      
	case 416  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Range Not Satisfiable",50);
		break;
	       
	case 417  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Expectation Failed",50);
		break;
	       
	case 426  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Upgrade Required",50);
		break;
	       
	case 500  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Internal Server Error",50);
		break;
	       
	case 501  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Not Implemented",50);
		break;
	       
	case 502  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Bad Gateway",50);
		break;
	       
	case 503  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Service Unavailable",50);
		break;
	       
	case 504  :
		headers->status = status;
		strncpy(headers->reason_phrase,"Gateway Timeout",50);
		break;
	      
	case 505  :
		headers->status = status;
		strncpy(headers->reason_phrase,"HTTP Version Not Supported",50);
		break;
	       
	default:
		break;
	}
}

static int parse_body(struct Content *cont, struct Response *res)
{
	if(cont->size < STD_BDY_CNT){
		if(cont->cnt_dy)
			strncpy(res->body.content,cont->cnt_dy,cont->size);
		else
			strncpy(res->body.content,cont->cnt_st,cont->size);
		
		res->body.size = cont->size;
		return 0;
	}
	/*TODO handle cont->size > STD_BDY_CNT */
	return 0;
}

static char *date_formatter()
{
 	static char date [50] = {0};

	time_t t =0;
	if((t = time(NULL)) == -1) return NULL;
	struct tm *d = gmtime(&t);
	if(!d) return NULL;

	
	if(snprintf(date,50,"%s, %d %s %d %d:%d:%s GMT",day_parser(d->tm_wday),
				d->tm_mday,month_parser(d->tm_mon), d->tm_year + 1900,
				d->tm_hour,d->tm_min,second_parser(d->tm_sec)) == -1) return NULL;
	
	return date;

}

static char *day_parser(int day)
{
	switch(day){
	case 0: return "Sun";
	case 1: return "Mon";
	case 2: return "Tue";
	case 3: return "Wed";
	case 4: return "Thu";
	case 5: return "Fri";
	case 6: return "Sat";
	default: return NULL;
	}
}
static char *month_parser(int month)
{
	switch(month){
	case 0: return "Jan";
	case 1: return "Feb";
	case 2: return "Mar";
	case 3: return "Apr";
	case 4: return "May";
	case 5: return "Jun";
	case 6: return "Jul";
	case 7: return "Aug";
	case 8: return "Sep";
	case 9: return "Oct";
	case 10: return "Nov";
	case 11: return "Dec";
	default: return NULL;
	}

}
static char *second_parser(int second)
{
	switch(second){
	case 0 : return "00";
	case 1 : return "01";
	case 2 : return "02";
	case 3 : return "03";
	case 4 : return "04";
	case 5 : return "05";
	case 6 : return "06";
	case 7 : return "07";
	case 8 : return "08";
	case 9 : return "09";
	default:
	{
		static char num [3] = {0};
		if(snprintf(num,3,"%d",second) == -1 ) return NULL;
		return num;
	}
	}
}
static int map_method_and_status(int method,int status, int body)
{
	if(status == 400) return BAD_REQ_RESPONSE;
	if(status == 404) return NOT_FOUND_RESPONSE;
	if(status == 500) return SERVER_ERROR;
	if(method == OPTIONS && status == 200) return OPTIONS_RESPONSE;
	if(method == GET && status == 200 && body) return OK_GET_RESPONSE_BODY;
	if(method == GET && status == 200) return OK_GET_RESPONSE;
	if(method == POST && status == 200 && body) return OK_POST_RESPONSE;
	if(method == POST && status == 201 && body) return OK_CREATED_POST_RESPONSE;

	return BAD_REQ_RESPONSE;
}
