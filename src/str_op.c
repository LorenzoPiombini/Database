#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdint.h>
#include <limits.h>
#include <errno.h>
#include <assert.h>
#include "str_op.h"
#include "common.h"
#include "globals.h"
#include "debug.h"

static char prog[] = "db";
static char *strstr_last(char *src, char delim);
static int is_target_db_type(char *target);
static uint32_t power(uint32_t n, int pow);

#define BASE 247

static const char *base_247[] = {"_A","_B","_C" ,"_D","_E","_F","_G","_H","_I","_J","_K","_L","_M","_N","_O","_P","_Q","_R","_S","_T","_U",
				"_V","_W","_X","_Y","_Z","_[","_\\","_]","_^","__"," ","!","\"","#","$","%","&","'","(",")","*","+",
				",","-",".","0","1","2","3","4","5","6","7","8","9",":",";","<","=","?","@","A","B","C","D","E","F",
				"G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z","[","]","_","`","a",
				"b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z",
				"_x7f","_x80","_x81","_x82","_x83","_x84","_x85","_x86","_x87","_x88","_x89","_x8a","_x8b","_x8c","_x8d",
				"_x8e","_x8f","_x90","_x91","_x92","_x93","_x94","_x95","_x96","_x97","_x98","_x99","_x9a","_x9b","_x9c",
				"_x9d","_x9e","_x9f","_xa0","_xa1","_xa2","_xa3","_xa4","_xa5","_xa6","_xa7","_xa8","_xa9","_xaa","_xab",
				"_xac","_xad","_xae","_xaf","_xb0","_xb1","_xb2","_xb3","_xb4","_xb5","_xb6","_xb7","_xb8","_xb9","_xba",
				"_xbb","_xbc","_xbd","_xbe","_xbf","_xc0","_xc1","_xc2","_xc3","_xc4","_xc5","_xc6","_xc7","_xc8","_xc9",
				"_xca","_xcb","_xcc","_xcd","_xce","_xcf","_xd0","_xd1","_xd2","_xd3","_xd4","_xd5","_xd6","_xd7","_xd8",
				"_xd9","_xda","_xdb","_xdc","_xdd","_xde","_xdf","_xe0","_xe1","_xe2","_xe3","_xe4","_xe5","_xe6","_xe7",
				"_xe8","_xe9","_xea","_xeb","_xec","_xed","_xee","_xef","_xf0","_xf1","_xf2","_xf3","_xf4","_xf5","_xf6",
				"_xf7","_xf8","_xf9","_xfa","_xfb","_xfc","_xfd","_xfe","_xff"};
char *get_sub_str(char *start_delim, char *end_delim, char *str)
{
	char *start_d = NULL;
	char *end_d = NULL;
	static char sub_str[1024] = {0};

	if((start_d = strstr(str,start_delim))){
		if((end_d = strstr(str,end_delim))){
			int sx = start_d - str;
			int size = ((--end_d - str) - sx)+1;

			strncpy(sub_str,&str[sx +1],size);
			return &sub_str[0];
		}
	}
	return NULL;
}

int check_handle_input_mode(char *buffer, int op)
{

	/*look for TYPE_FILE*/
	int av_ct = 0;
	int av_cT = 0;
	/*
	 * __IMPORT_EZ is a global variable that will we true
	 * only if we import from an EZgen system
	 * in that case we do not need to check for file type
	 * */

	if(!__IMPORT_EZ){
		char *sub_str = get_sub_str("[", "]", buffer);
		if(sub_str){
			/*count T_ and TYPE_ that we have to ignore*/
			av_ct = count_fields(buffer,T_); 
			av_cT = count_fields(buffer,TYPE_);

		}
	}
	int c_t = count_fields(buffer,T_) - av_ct; 
	int c_T = count_fields(buffer,TYPE_) - av_cT; 
	int c_delim =  count_fields(buffer,":");
	
	if(c_T == 0 && c_t == 0 && c_delim > 0) return NO_TYPE;
	
	/*for each t_ or TYPE_ delim we need to account for one ':' */
	int v = c_T  + c_t ;
	switch(op){ 
	case FCRT:
	{

		if(c_T == 0 && c_t == 0 && c_delim == 0 && (buffer && *buffer != '\0')) return NO_TYPE;
		int additional_c_delim_count = v > 1 ? v-1:0;
		if((c_delim - v -additional_c_delim_count) > 0) return HYB;
		if((c_delim - v -additional_c_delim_count) == 0) return TYPE;
		if(c_delim == v) return TYPE;
		break;
	}
	case FWRT:
	{
		if(c_T == 0 && c_t == 0 && c_delim == 0) return -1;
		int additional_c_delim_count = v == 1 ? 2 : v *2;
		if((c_delim - v -additional_c_delim_count) > 0) return HYB;
		if((c_delim - v -additional_c_delim_count) == 0) return TYPE;
		if((c_delim - v -additional_c_delim_count) < 0) return TYPE;
		break;
	}
	default:
		break;	
	}

	return -1;
}

int get_name_types_hybrid(int mode,char *buffer, char names[][MAX_FILED_LT],int *types_i)
{
	size_t size = strlen(buffer) + 1;
	char cbuf[size];
	memset(cbuf,0,size);
	strncpy(cbuf,buffer,size);


	replace('@','^',cbuf);
	int i = 0;

	/* detect files types*/	
	char *file_start = NULL;
	if(!__IMPORT_EZ){
		while((file_start = strstr(cbuf,"["))){
			char *end_file = strstr(file_start,"]");
			if(end_file){ 
				int end = end_file - cbuf;
				char *delim = NULL;
				while((delim = strstr(file_start,":"))){
					if((delim - cbuf) < end) 
						*delim = '@';
					else
						break;
				}
			}
			*file_start = '@';
		}
	}

	int reset_mistype_index[10];
	memset(reset_mistype_index,-1,sizeof(int)*10);
	int rmi = 0;
	if(strstr(cbuf,T_)){
		char *pos_t = NULL;
		while((pos_t = strstr(cbuf,T_))){
			if(is_target_db_type(pos_t) == -1){
				/* this is not a type*/
				*pos_t = '@';			
				reset_mistype_index[rmi] = pos_t - cbuf;
				rmi++;
				continue;
			}
			int start = pos_t - cbuf;

			/*this change the delimeter so it won't
			 * be found in the next iteration*/
			*pos_t= '@';
			char *end_t = (strstr(pos_t,":"));
			if(!end_t){
				size_t sz = strlen(++pos_t) + 1;
				char type[sz];
				memset(type,0,sz);
				strncpy(type,pos_t,sz);
				types_i[i] = get_type(type);

				char *p = &cbuf[start] - 1;
				while(*p !=':' && p != &cbuf[0] && *p != '@') p--;

				sz = (--pos_t - cbuf) - (p - cbuf) + 1;
				if(sz-2 > MAX_FIELD_LT) return -1;

				if(p != &cbuf[0])
					strncpy(names[i],++p, sz -2);
				else
					strncpy(names[i],p, sz -1);

				i++;
				break;
			}


			/*get the type*/
			int l = end_t - pos_t;
			char type[l];
			memset(type,0,l);
			strncpy(type,++pos_t,l-1);
			types_i[i] = get_type(type);

			char *p = &cbuf[start] - 1;
			while(*p !=':' && p != &cbuf[0] && *p != '@') p--;
			
			l = pos_t - p;
			if(l-2 > MAX_FIELD_LT) return -1;

			if(p != &cbuf[0]){
				strncpy(names[i],++p, l -2);
			}else{
				strncpy(names[i],p, l -1);
			}
				
			if(types_i[i] == TYPE_FILE){
				char *file_block = NULL;
				if((file_block = strstr(p,"["))){
					char *d = NULL;
					char *end = strstr(file_block,"]");
					while((d = strstr(&file_block[1],T_)) && 
							(((d = strstr(&file_block[1],T_)) - cbuf) < (end - cbuf))){ 
						*d = '@';	
					}
				}
			}

			i++;
		}
	}

	
	rmi = 0;
	while(reset_mistype_index[rmi] != -1){
		cbuf[reset_mistype_index[rmi]] = ':';
		rmi++;
	}
	if (strstr(cbuf,TYPE_)){
		char *pos_T = NULL;
		while((pos_T = strstr(cbuf,TYPE_))){
			int start = pos_T - cbuf;
			/*this change the delimeter so it won't
			 * be found in the next iteration*/
			*pos_T= '@';
			char *end_T = (strstr(pos_T,":"));
			if(!end_T){
				size_t sz = strlen(++pos_T) + 1;
				char type[sz];
				memset(type,0,sz);
				strncpy(type,pos_T,sz);
				types_i[i] = get_type(type);

				char *p = &cbuf[start] - 1;
				while(*p !=':' && p != &cbuf[0] && *p != '@') p--;

				sz = pos_T - p;
				if(sz-2 > MAX_FIELD_LT) return -1;

				if(p != &cbuf[0])
					strncpy(names[i],++p, sz -2);
				else
					strncpy(names[i],p, sz -1);
				i++;
				break;
			}

			/*get the type*/
			int l = end_T - pos_T;
			char type[l];
			memset(type,0,l);
			strncpy(type,++pos_T,l-1);
			types_i[i] = get_type(type);


			char *p = &cbuf[start] - 1;
			while(*p !=':' && p != &cbuf[0] && *p != '@') p--;
			
			l = pos_T - p;
			if(l-2 > MAX_FIELD_LT) return -1;

			if(p != &cbuf[0])
				strncpy(names[i],++p, l -2);
			else
				strncpy(names[i],p, l -1);

			if(types_i[i] == TYPE_FILE){
				char *file_block = NULL;
				if((file_block = strstr(p,"["))){
					char *d = NULL;
					char *end = strstr(file_block,"]");
					while((d = strstr(&file_block[1],TYPE_)) && 
							(((d = strstr(&file_block[1],TYPE_)) - cbuf) < (end - cbuf))){ 
						*d = '@';	
					}
				}

			}
			i++;
		}
	} 
	char names_s[MAX_FIELD_NR - i][MAX_FILED_LT];
	memset(names_s,0,(MAX_FIELD_NR - i)*MAX_FILED_LT);
	

	int s = 0;
	switch(mode){
	case HYB_DF:
		char *t = NULL;
		while((t = strstr(cbuf,"@t_"))) {
			char *d = strstr(t,":");	
			if(d) *d = '@';
			if(d) d = strstr(d,":");
			if(d) *d = '@';
			t[1] = '@';
		}
		
		char *T = NULL;
		while((T = strstr(cbuf,"@TYPE_"))) {
			char *d = strstr(T,":");	
			if(d) *d = '@';
			if(d) d = strstr(d,":");
			if(d) *d = '@';
			T[1] = '@';
		}

		s = get_fields_name_with_no_type(cbuf, names_s);
		break;
	case HYB_WR:	
	{ 		
		char *t = NULL;
		while((t = strstr(cbuf,"@t_"))) {
			char *d = strstr(t,":");	
			if(d) *d = '@';
			if(d) d = strstr(d,":");
			if(d) *d = '@';
			t[1] = '@';
		}
		
		char *T = NULL;
		while((T = strstr(cbuf,"@TYPE_"))) {
			char *d = strstr(T,":");	
			if(d) *d = '@';
			if(d) d = strstr(d,":");
			if(d) *d = '@';
			T[1] = '@';
		}
		s = get_names_with_no_type_skip_value(cbuf,names_s);
			
		break;
	}
	default:
		fprintf(stderr,"mode not supported, %s:%d.\n",__FILE__,__LINE__);
		return -1;
	}
	
	int skip = -1;
	for(int j = 0; j < s; j++){
		int b = 0;
		for(int x = 0; x < i; x++){
			size_t sz = strlen(names_s[j]);
			if(sz != strlen(names[x])) continue;
			if(strncmp(names_s[j],names[x],sz) == 0){
				skip = j;
				b = 1;
				break;
			}
		}
		if (b) break;
	}

	for(int x = 0; names_s[x][0] != '\0'; x++){
		if (x == skip) continue;
		strncpy(names[i],names_s[x],strlen(names_s[x]));
		i++;
	}
	return i;
}

int get_array_values(char *src, char ***values)
{
	int items = count_fields(src, ",");
	*values = calloc(items, sizeof(char *));
	if (!(*values))
	{
		__er_calloc(F, L - 2);
		return -1;
	}

	char *t = strtok(src, ",");
	if (!t)
	{
		fprintf(stderr, "strtok() failed, %s:%d.\n", F, L - 2);
		free(*values);
		return -1;
	}

	(*values)[0] = strdup(t);
	if (!(*values)[0])
	{
		fprintf(stderr, "strdup() failed, %s:%d.\n", F, L - 2);
		free(*values);
		return -1;
	}

	for (int i = 1; ((t = strtok(NULL, ",")) != NULL) && (i < items); i++)
	{
		(*values)[i] = strdup(t);
		if (!(*values)[i])
		{
			fprintf(stderr, "strdup() failed, %s:%d.\n", F, L - 2);
			free(*values);
			return -1;
		}
	}

	return 0;
}

int is_num(char *key)
{
	unsigned char uint = 2;
	unsigned char str = 1;

	for (; *key != '\0'; key++)
		if (isalpha(*key) || ispunct(*key))
			return str;

	return uint;
}

void *key_converter(char *key, int *key_type)
{
	void *converted = NULL;
	*key_type = is_num(key);
	switch (*key_type) {
	case 2:
	{
		char *end;
		errno = 0;
		long l = strtol(key, &end, 10);
		if (errno == ERANGE || errno == EINVAL) return NULL;

		if (l == MAX_KEY) {
			fprintf(stderr, "key value out fo range.\n");
			return NULL;
		}

		converted = calloc(1, sizeof(uint32_t));
		if (!converted) {
			__er_calloc(F, L - 2);
			return NULL;
		}

		memcpy(converted, (uint32_t *)&l, sizeof(uint32_t));
		break;
	}
	case 1:
		return NULL;
	default:
		return NULL;
	}

	return converted;
}

static char *strstr_last(char *src, char delim)
{

	int last = 0 ;
	for(char *p = src; *p != '\0'; p++){
		if(*p == delim) last = p - src;		
	}
	
	if(last == 0) return NULL;
	return &src[last];
}
int get_names_with_no_type_skip_value(char *buffer, char names[][MAX_FIELD_LT])
{
	/*copy the buffer*/
	size_t l = strlen(buffer);
	char cpy[l+1];
	memset(cpy,0,l+1);
	strncpy(cpy,buffer,l);
	

	if(strstr(buffer,"@") && !strstr(buffer,"@@")) replace('@','^',cpy);

	/*exclude file syntax*/
	if(!__IMPORT_EZ){
		char *f_st = NULL;
		char *f_en = NULL;
		while((f_st = strstr(cpy,"[")) && (f_en = strstr(cpy,"]"))){
			*f_st = '\n';
			for(; *f_st != ']'; f_st++){
				if(*f_st == ':') *f_st = '@';
			}
			*f_en = '\r';
		}

		if(strstr(cpy,"\n")) replace('\n','[',cpy);
		if(strstr(cpy,"\r")) replace('\r',']',cpy);
	}
	

	char *delim = ":";
	char *first = NULL;
	char *last = NULL;
	
	int i = 0;
	char *p = cpy;
	char *p2 = cpy;
	while(((first = strstr(p2,delim)) != NULL) && ((last=strstr(&p[(first+1) - cpy],delim)) != NULL)){
		char *move_back = (first - 1); 
		while(move_back != &cpy[0] && *move_back != '@' && *move_back != ':') --move_back;   
			
		int size = 0;
		if(move_back == &cpy[0])
			size = (first - cpy) - ( move_back - cpy) +1;
		else
			size = (first - cpy) - ( move_back - cpy);
		
		int next_start = last - p2;
		if(move_back == &cpy[0])
			strncpy(names[i],move_back,size-1);
		else
			strncpy(names[i],++move_back,size-1);

		p2 += next_start+1;
		i++;
	}

	if(first){
		if(*(first + 1) != '\0') {
			char *cf = (first - 1);		
			while(*cf != ':' && cf != &cpy[0] && *cf != '@') cf--;
			int size = 0; 
			if(cf != &cpy[0])	 
				size = first - (++cf) +1;
			else 
				size = first - cf +1;	
			
			strncpy(names[i],cf,size-1);
			i++;
		}
	}else if(strstr(cpy,delim) == NULL && !strstr(cpy,"@")){
		if(buffer){
			strncpy(names[i],cpy,strlen(buffer));
			i++;
		}
	}else if ((first = strstr(cpy,delim))){
		if(first[1] == '['){
			if((last = strstr(&first[1],delim))){
				if(last[strlen(last)-1] == ']') return i;
			}
		}

	}else if((last = strstr_last(cpy,'@'))){
		++last;
		strncpy(names[i],last,strlen(last));
	}else {
		return -1;
	}

	return i;
}


int three_file_path(char *file_path, char files[][MAX_FILE_PATH_LENGTH])
{
	char *dat = ".dat";
	char *ind = ".inx";
	char *sch = ".sch";

	size_t len = strlen(file_path) + strlen(ind) + 1;

	if(len > MAX_FILE_PATH_LENGTH) return EFLENGTH;
	
	strncpy(files[0],file_path,len);
	strncat(files[0],ind,strlen(ind));
	strncpy(files[1],file_path,len);
	strncat(files[1],dat,strlen(dat));
	strncpy(files[2],file_path,len);
	strncat(files[2],sch,strlen(dat));
	
	return 0;
}

int count_fields(char *fields, const char *user_target)
{
	int c = 0;
	const char *p = fields;
	if(user_target){
		while ((p = strstr(p, user_target)) != NULL) {
			c++;
			p += strlen(user_target);
		}
		return c;
	}
	

	char *target = TYPE_;
	if(strstr(p,target) == NULL){
		char *new_target = T_;
		if(strstr(p,new_target) != NULL){
			while ((p = strstr(p, new_target)) != NULL) {
				if(is_target_db_type((char *)p) == -1){
					p += strlen(new_target);
					continue;
				}
				c++;
				p += strlen(new_target);
			}
		}
	} else {
		char *second_target = T_;
		if(strstr(p,second_target) != NULL){

			while ((p = strstr(p, second_target)) != NULL) {
				if(is_target_db_type((char *)p) == -1){
					p += strlen(second_target);
					continue;
				}
				c++;
				p += strlen(second_target);
			}
		}

		p = fields;
		while ((p = strstr(p, target)) != NULL) {
				c++;
				p += strlen(target);
		}

	}

	if(c == 0){
		p = fields;
		int pos = 0;
		char *new_target = ":";
		while((p = strstr(p,new_target))){
			pos = p - fields;
			c++;
			p += strlen(new_target);
		}

		if(fields[pos+1] != '\0') c++;

	}
	
	

	return c;
}

int get_type(char *s){

	if (strcmp(s, "TYPE_INT") == 0) {
		return 0;
	} else if (strcmp(s, "TYPE_LONG") == 0){
		return 1;
	} else if (strcmp(s, "TYPE_FLOAT") == 0) {
		return 2;
	}
	else if (strcmp(s, "TYPE_STRING") == 0)
	{
		return 3;
	}
	else if (strcmp(s, "TYPE_BYTE") == 0)
	{
		return 4;
	}
	else if(strcmp(s, "TYPE_PACK") == 0)
	{
		return 5;
	}
	else if (strcmp(s, "TYPE_DOUBLE") == 0)
	{
		return 6;
	}
	else if (strcmp(s, "t_i") == 0)
	{
		return 0;
	}
	else if (strcmp(s, "t_l") == 0)
	{
		return 1;
	}
	else if (strcmp(s, "t_f") == 0)
	{
		return 2;
	}
	else if (strcmp(s, "t_s") == 0)
	{
		return 3;
	}
	else if (strcmp(s, "t_b") == 0)
	{
		return 4;
	}
	else if (strcmp(s, "t_pk") == 0)
	{
		return 5;
	}
	else if (strcmp(s, "t_d") == 0)
	{
		return 6;
	}
	else if (strcmp(s, "TYPE_ARRAY_INT") == 0)
	{
		return 7;
	}
	else if (strcmp(s, "TYPE_ARRAY_LONG") == 0)
	{
		return 8;
	}
	else if (strcmp(s, "TYPE_ARRAY_FLOAT") == 0)
	{
		return 9;
	}
	else if (strcmp(s, "TYPE_ARRAY_STRING") == 0)
	{
		return 10;
	}
	else if (strcmp(s, "TYPE_ARRAY_BYTE") == 0)
	{
		return 11;
	}
	else if (strcmp(s, "TYPE_ARRAY_DOUBLE") == 0)
	{
		return 12;
	}
	else if (strcmp(s, "TYPE_FILE") == 0)
	{
		return 13;
	}
	else if (strcmp(s, "t_ai") == 0)
	{
		return 7;
	}
	else if (strcmp(s, "t_al") == 0)
	{
		return 8;
	}
	else if (strcmp(s, "t_af") == 0)
	{
		return 9;
	}
	else if (strcmp(s, "t_as") == 0)
	{
		return 10;
	}
	else if (strcmp(s, "t_ab") == 0)
	{
		return 11;
	}
	else if (strcmp(s, "t_ad") == 0)
	{
		return 12;
	}
	else if (strcmp(s, "t_fl") == 0)
	{
		return 13;
	}

	return -1;
}

int get_fields_name_with_no_type(char *fields_name, char names[][MAX_FILED_LT])
{
	
	char *p = fields_name;
	char *delim = NULL;
	int j = 0;
	int pos = 0;
	if(strstr(p,":") == NULL){
		if(p){
			strncpy(names[j],p,strlen(p));
			j++;
			return j;
		}
	}
		
	while((delim = strstr(p,":"))){
		pos = delim - fields_name;
		*delim = '@';
		delim--;
		int i = 0;
		for( i = 1;*delim != ':' && *delim != '@' && delim != &p[0]; i++, delim--);
		
		if(delim != &p[0]) delim++, i--;

		strncpy(names[j],delim,i);
		p += (i+1);
		j++;
	}

	if(fields_name[pos + 1] != '\0') {
		strncpy(names[j],&fields_name[pos+1],strlen(&fields_name[pos + 1]));
		j++;
	}

	return j;
}

int get_fileds_name(char *fields_name, int fields_count, int steps, char names[][MAX_FILED_LT])
{
	if (fields_count == 0) return -1;

	int j = 0;
	char *s = NULL;

	char *cp_fv = fields_name;
	while ((s = strtok_r(cp_fv, ":", &cp_fv)) != NULL && j < fields_count) {
		strncpy(names[j],s,strlen(s));

		j++;

		strtok_r(NULL, ":", &cp_fv);

		if (steps == 3 && j < fields_count)
			strtok_r(NULL, ":", &cp_fv);
	}

	return 0;
}

unsigned char check_fields_integrity(char names[][MAX_FILED_LT], int fields_count)
{
	int c = 0;

	if(fields_count == 0) return 0;
	for (int i = 0; i < fields_count; i++) {
		if ((fields_count - i) == 1)
			break;

		for (int j = 0; j < fields_count; j++) {
			if (((fields_count - i) > 1) && ((fields_count - j) > 1))
			{
				if (j == i)
					continue;

				int l_i = strlen(names[i]);
				int l_j = strlen(names[j]);
				if(names[j][0] == '\0') c++;
				if(c == fields_count) return 0;

				if (l_i == l_j)
				{
					if (strncmp(names[i], names[j], l_j) == 0)
						return 0;
				}
			}
		}
	}
	return 1;
}

int check_array_input(char *value)
{
	size_t size = strlen(value) + 1;
	char cbuff[size];
	memset(cbuff,0,size);
	strncpy(cbuff,value,size);

	char *p = NULL;
	int elements = 0;
	int d = 0;
	int i = 0;
	int pos = 0;
	while((p = strstr(cbuff,","))){
		if(p[1] == '\0') break;

		elements++;
		int start = p - cbuff;
		while((p != &cbuff[0] && *p != '@')) p--;

		if(*p == '@') ++p;

		int l = (&cbuff[start] - p)+1;
		pos = start ;
		char cpy[l];
		memset(cpy, 0, l);

		strncpy(cpy,p,l-1);

		if(is_floaintg_point(cpy)){
			if(is_number_in_limits(cpy)) d++;
		}else if(is_integer(cpy)){
			if(is_number_in_limits(cpy)) i++;
		}
		
		cbuff[start] = '@';
	}

	if(cbuff[pos] != '\0' && p == NULL){
		elements++;
		char *last = &cbuff[pos];
		size_t size = strlen(++last) + 1;
		char n[size];
		memset(n,0,size);
		
		strncpy(n,last,size-1);
		if(is_floaintg_point(n)){
			if(is_number_in_limits(n)) d++;
		}else if(is_integer(n)){
			if(is_number_in_limits(n)) i++;
		}

	}
		
	if(i == elements) return TYPE_ARRAY_LONG;
	if(d == elements) return TYPE_ARRAY_DOUBLE;

	return -1;


}
int assign_type(char *value)
{

	if(!value) return -1;

	if(value[0] == '[' && value[strlen(value) - 1] == ']') return TYPE_FILE;

	if(strstr(value,",")){
		/*ARRAY case*/
		int result = 0;
		if((result = check_array_input(value)) == - 1) return TYPE_ARRAY_STRING;
			
		return result;	
	}


	if(is_floaintg_point(value)) {
		if(is_number_in_limits(value)) return TYPE_DOUBLE;

		return -1;
	}

	if(is_integer(value)){
		if(is_number_in_limits(value)) return TYPE_LONG;

		return -1;
	}

	return TYPE_STRING;
}



int get_value_types(char *fields_input, int fields_count, int steps, int *types)
{

	if (fields_count == 0) return -1;

	int i = steps == 3 ? 0 : 1;
	int j = 0;

	char *s = NULL;
	char *copy_fv = NULL;

	s = strtok_r(fields_input, ":", &copy_fv);

	s = strtok_r(NULL, ":", &copy_fv);

	if (s)
	{
		int result = get_type(s);
		if (result == -1) return -1;
		types[j] = result;
		i++;
	}else {
		if(fields_count > 1){
			printf("type token not found in get_value_types().\n");
			return -1;
		}
		return 0;
	}

	while ((s = strtok_r(NULL, ":", &copy_fv)) != NULL) {
		if ((i % 3 == 0) && (j < fields_count - 1)) {
			j++;
			int r  = get_type(s);
			if (r == -1){
				printf("check input format: fieldName:TYPE:value.\n");
				return -1;
			}
			types[j] = r;
		}

		if (steps != 3 && i == 2) {
			i++;
		}else if (steps != 3 && i == 3) {
			i--;
		}else{
			i++;
		}
	}
	return 0;
}

int get_values_hyb(char *buff,char ***values,  int fields_count)
{	
	size_t size = strlen(buff)+1;
	char cbuff[size];
	memset(cbuff,0,size);
	strncpy(cbuff,buff,size);

	*values = calloc(fields_count, sizeof(char *));
	if (!(*values)) {
		perror("memory get values");
		return -1;
	}

	/* detect files types - ONLY IF NOT IMPORTING FROM EZgen*/	
	char *file_start = NULL;
	int count = 0;
	if(!__IMPORT_EZ){
		
		while((file_start = strstr(cbuff,"["))){
			char *end_file = strstr(file_start,"]");
			if(end_file){ 
				int end = end_file - cbuff;
				char *delim = NULL;
				while((delim = strstr(file_start,":"))){
					if((delim - cbuff) < end) 
						*delim = '&';
					else
						break;
				}
			}

			*file_start = '@';
			count++;
		}
		if(count){
			replace('@','[',cbuff);
			replace('&','@',cbuff);
		}
	}

	int i = 0;
	int reset_mistype_index[10];
	memset(reset_mistype_index,-1,sizeof(int)*10);
	int rmi = 0;
	if(strstr(cbuff,T_)){
		char *pos_t = NULL;
		while((pos_t = strstr(cbuff,T_))){
			/*this change the delimeter so it won't
			 * be found in the next iteration*/
			if(is_target_db_type(pos_t) == -1){
				*pos_t = '@';
				reset_mistype_index[rmi] = pos_t - cbuff;
				rmi++;
				continue;
			}
			*pos_t= '@';
			char *end_t = (strstr(pos_t,":"));
			assert(end_t != NULL); 

			*end_t = '@';
			char *next = strstr(end_t,":");
			if(!next){
				++end_t;
				if(count > 0){
					/*clean the value*/
					replace('@',':',end_t);
				}
				(*values)[i] = strdup(end_t);
				if(!(*values)[i]){
					fprintf(stderr,"strdup() failed, %s:%d\n",__FILE__,__LINE__-2);
					if(i == 0)
						free(*values);
					else
						free_strs(i,1,values);

					return -1;
				}
				i++;
				break;
			}
			*next = '@';
			size_t len = (next - cbuff) - (++end_t - cbuff) + 1;
			char cpy[len];
			memset(cpy,0,len);
			strncpy(cpy,end_t,len-1);

			if(count > 0){
				/*clean the value*/
				replace('@',':',cpy);
			}
			(*values)[i] = strdup(cpy);
			if(!(*values)[i]){
				fprintf(stderr,"strdup() failed, %s:%d\n",__FILE__,__LINE__-2);
				if(i == 0)
					free(*values);
				else
					free_strs(i,1,values);
				return -1;
			}
			i++;
		}		
	}
 	rmi = 0;
	while(reset_mistype_index[rmi] != -1){
		cbuff[reset_mistype_index[rmi]] = ':';
		rmi++;
	}
	

	if(strstr(cbuff,TYPE_)){
		char *pos_T = NULL;
		while((pos_T = strstr(cbuff,TYPE_))){
			/*this change the delimeter so it won't
			 * be found in the next iteration*/
			*pos_T= '@';
			char *end_T = (strstr(pos_T,":"));
			assert(end_T != NULL); 

			*end_T = '@';
			char *next = strstr(end_T+1,":");
			if(!next){
				++end_T;
				if(count > 0){
					/*clean the value*/
					replace('@',':',end_T);
				}
				(*values)[i] = strdup(end_T);
				if(!(*values)[i]){
					fprintf(stderr,"strdup() failed, %s:%d\n",__FILE__,__LINE__-2);
					if(i == 0)
						free(*values);
					else
						free_strs(i,1,values);

					return -1;
				}
				i++;
				break;
			}

			*next = '@';
			size_t len = (next - cbuff) - (++end_T- cbuff) + 1;
			char cpy[len];
			memset(cpy,0,len);
			strncpy(cpy,end_T,len-1);

			if(count > 0){
				/*clean the value*/
				replace('@',':',cpy);
			}
			(*values)[i] = strdup(cpy);
			if(!(*values)[i]){
				fprintf(stderr,"strdup() failed, %s:%d\n",__FILE__,__LINE__-2);
				if(i == 0)
					free(*values);
				else
					free_strs(i,1,values);

				return -1;
			}
			i++;
		}		
	}

	if(strstr(cbuff,":")){
		char *pos_d = NULL;
		while((pos_d = strstr(cbuff,":"))){
			/*this change the delimeter so it won't
			 * be found in the next iteration*/
			*pos_d= '@';
			char *end_d = strstr(pos_d,":");
			if(!end_d){
				++pos_d;		
				if(count > 0){
					/*clean the value*/
					replace('@',':',pos_d);
				}
				(*values)[i] = strdup(pos_d);
				if(!(*values)[i]){
					fprintf(stderr,"strdup() failed, %s:%d\n",__FILE__,__LINE__-2);
					if(i == 0)
						free(*values);
					else
						free_strs(i,1,values);

					return -1;
				}
				break;
			}

			size_t len = end_d - (++pos_d) + 1;
			char cpy[len];
			memset(cpy,0,len);
			*end_d = '@';
			strncpy(cpy,pos_d,len-1);

			if(count > 0){
				/*clean the value*/
				replace('@',':',cpy);
			}
			(*values)[i] = strdup(cpy);
			if(!(*values)[i]){
				fprintf(stderr,"strdup() failed, %s:%d\n",__FILE__,__LINE__-2);
				free_strs(i,1,values);
				return -1;
			}
			i++;
		}		
	}

	return 0;
}
char ** get_values_with_no_types(char *buff,int fields_count)
{
	size_t l = strlen(buff);
	char cpy[l+1];
	memset(cpy,0,l+1);
	strncpy(cpy,buff,l);

	char *delim = ":";
	char *first = NULL;
	char *last = NULL;
	
	char **values = calloc(fields_count, sizeof(char *));
	if (!values) {
		perror("memory get values");
		return NULL;
	}

	if(strstr(buff,"@")) replace('@','^',cpy);

	/*exclude file syntax*/
	char *f_st = NULL;
	char *f_en = NULL;
	while((f_st = strstr(cpy,"[")) && (f_en = strstr(cpy,"]"))){
		*f_st = '\n';
		for(; *f_st != ']'; f_st++){
			if(*f_st == ':') *f_st = '@';
		}
		*f_en = '\r';
	}

	if(strstr(cpy,"\n")) replace('\n','[',cpy);
	if(strstr(cpy,"\r")) replace('\r',']',cpy);
	

	int i = 0;
	char *p2 = cpy;
	char *p = cpy;
	while((first = strstr(p2,delim)) != NULL && (last = strstr(&p[(first+1) - cpy],delim)) != NULL){
	/*	if(first[1] == '[' && ((file_block = strstr(&first[1],"]")))){
			int start = (first + 1) - buff;
			int size = (file_block - buff) - start +1;
			char cpy[size+1];
			memset(cpy,0,size+1);
			strncpy(cpy,&buff[start],size);
			values[i] = strdup(cpy);
			if(!values[i]){
				fprintf(stderr,"strdup() failed, %s:%d",__FILE__,__LINE__-2);
				return NULL;
			}
			p2 = (first + 1) + size+1;
			i++;
			continue;
		}
		*/
		int start = (first + 1) - cpy;
		int size = (last - cpy) - start;
		char bpy[size+1];
		memset(bpy,0,size+1);
		strncpy(bpy,&buff[start],size);

		values[i] = strdup(bpy);
		if(!values[i]){
			fprintf(stderr,"strdup() failed, %s:%d",__FILE__,__LINE__-2);
			return NULL;
		}

		if(strstr(values[i],"@")) replace('@',':',values[i]);
		if(strstr(values[i],"^")) replace('^','@',values[i]);

		p2 = (first + 1) + size+1;
		i++;
	}

	if(first){
		if(*(first + 1) != '\0') {
			values[i] = strdup(&first[1]);
			if(!values[i]){
				fprintf(stderr,"strdup() failed, %s:%d",__FILE__,__LINE__-2);
				return NULL;
			}
			if(strstr(values[i],"@")) replace('@',':',values[i]);
			if(strstr(values[i],"^")) replace('^','@',values[i]);
		}
	}

	return values;
}

char **get_values(char *fields_input, int fields_count)
{
	int i = 0, j = 0;

	char **values = calloc(fields_count, sizeof(char *));
	if (!values) {
		perror("memory get values");
		return NULL;
	}

	char *s = NULL;
	char *cp_fv = NULL;
	strtok_r(fields_input, ":", &cp_fv);
	strtok_r(NULL, ":", &cp_fv);
	s = strtok_r(NULL, ":", &cp_fv);

	if (s)
	{
		values[j] = strdup(s);
		if (!values[j])
		{
			free(values);
			return NULL;
		}
		i++;
	}
	else
	{
		perror("value token not found in get_values();");
		free(values);
		return NULL;
	}

	while ((s = strtok_r(NULL, ":", &cp_fv)) != NULL)
	{
		if ((i % 3 == 0) && (fields_count - j >= 1))
			j++, values[j] = strdup(s);

		if (!values[j])
		{
			while (j-- > 0)
			{
				free(values[j]);
			}
			free(values);
			return NULL;
		}
		i++;
		s = NULL;
	}

	return values;
}

unsigned char create_blocks_data_to_add(int fields, char dta_src[][500], char dta_blocks[][500])
{

	for (int i = 0; i < fields; i++)
	{
		char *t = strtok(dta_src[i], "!");
		if (t)
		{
			strncpy(dta_blocks[i], t, strlen(t));
		}
	}

	return 1;
}

void free_strs(int fields_num, int count, ...)
{
	va_list args;
	va_start(args, count);

	for (int i = 0; i < count; i++){
		char **str = va_arg(args, char **);
		for (int j = 0; j < fields_num; j++){
			if (str[j]){
				free(str[j]);
			}
		}
		free(str);
	}
}

int is_file_name_valid(char *str)
{
	size_t l = strlen(str);
	for (size_t i = 0; i < l; i++)
	{
		if (ispunct(str[i]))
			if ((str[i] != '-' && str[i] != '/' && str[i] != '_' && str[i] != '.')) return 0;

		if (isspace(str[i]))
			return 0;
	}
	return 1;
}

void strip(const char c, char *str)
{
	size_t l = strlen(str);
	char cpy[l];
	memset(cpy,0,l);
	for (size_t i = 0, j = 0; i < l; i++){
		if (str[i] == c)continue;

		cpy[j] = str[i];
		j++;
	}

	memset(str,0,l);
	strncpy(str,cpy,l);
}

void replace(const char c, const char with, char *str)
{
	size_t l = strlen(str);
	for (size_t i = 0; i < l; i++)
		if (str[i] == c)
			str[i] = with;
}

/*
char return_first_char(char *str)
{
	return str[0];
}
*/

char return_last_char(char *str)
{
	size_t l = strlen(str);
	return str[l - 1];
}

size_t digits_with_decimal(float n)
{
	char buf[20];
	if (snprintf(buf, 20, "%.2f", n) < 0)
	{
		printf("snprintf failed %s:%d.\n", F, L - 2);
		return -1;
	}

	size_t i = 0;
	for (i = 0; i < 20; i++)
	{
		if (buf[i] == '.')
			return ++i;
	}

	return (size_t)-1;
}

unsigned char is_floaintg_point(char *str)
{
	size_t l = strlen(str);
	int point = 0;
	for (size_t i = 0; i < l; i++)
	{
		if (isdigit(str[i]))
		{
			if (str[i] == '.')
				return 1;
		}
		else if (str[i] == '.')
		{
			point++;
		}
		else if (str[i] == '-' && i == 0)
		{
			continue;
		}
		else
		{
			return 0;
		}
	}

	return point == 1;
}

unsigned char is_integer(char *str)
{
	size_t l = strlen(str);
	for (size_t i = 0; i < l; i++){
		if (!isdigit(str[i])){
			if (str[i] == '-' && i == 0){
				continue;
			}else{
				return 0;
			}
		}
	}
	return 1;
}

size_t number_of_digit(int n)
{
	if (n < 10)
	{
		return 1;
	}
	else if (n >= 10 && n < 100)
	{
		return 2;
	}
	else if (n >= 100 && n < 1000)
	{
		return 3;
	}
	else if (n >= 1000 && n < 10000)
	{
		return 4;
	}
	else if (n >= 10000 && n < 100000)
	{
		return 5;
	}
	else if (n >= 100000 && n < 1000000)
	{
		return 6;
	}
	else if (n >= 1000000 && n < 1000000000)
	{
		return 7;
	}
	else if (n >= 1000000000)
	{
		return 10;
	}

	return -1;
}

float __round_alt(float n)
{
	char buff[20];
	if (snprintf(buff, 20, "%f", n) < 0)
		return -1;

	size_t digit = 0;

	if ((digit = digits_with_decimal(n) + 2) > 20)
		return -1;

	int num = buff[digit];
	if (num > 5)
	{
		n += 0.01;
	}

	return n;
}

unsigned char is_number_in_limits(char *value)
{
	if (!value)
		return 0;

	size_t l = strlen(value);
	unsigned char negative = value[0] == '-';
	int ascii_value = 0;

	if (is_integer(value)){

		for (size_t i = 0; i < l; i++)
			ascii_value += (int)value[i];

		if (negative){
			if (-1 * ascii_value < -1 * ASCII_INT_MIN){
				if (-1 * ascii_value < -1 * ASCII_LONG_MIN)
					return 0;
				else
					return IN_LONG;
			}

			return IN_INT;
		}

		if (ascii_value > ASCII_INT_MAX){
			if (ascii_value > ASCII_LONG_MAX)
				return 0;
			else
				return IN_LONG;
		}

		return IN_INT;
	}

	if (is_floaintg_point(value)){
		for (size_t i = 0; i < l; i++)
			ascii_value += (int)value[i];

		if (negative){
			if (-1 * ascii_value < -1 * ASCII_FLT_MAX_NEGATIVE){
				if (-1 * ascii_value < -1 * ASCII_DBL_MAX_NEGATIVE)
					return 0;
				else
					return IN_DOUBLE;
			}

			return IN_FLOAT;
		}

		if (ascii_value > ASCII_FLT_MAX){
			if (ascii_value > ASCII_DBL_MAX)
				return 0;
			else
				return IN_DOUBLE;
		}
		return IN_FLOAT;
	}

	return 0;
}

int find_last_char(const char c, char *src)
{
	size_t l = strlen(src);
	int last = -1;
	for (size_t i = 0; i < l; i++){
		if (src[i] == c)
			last = i;
	}

	return last;
}

int count_delim(char *delim, char *str)
{
	size_t l = strlen(str) + 1;
	char buf[l];
	memset(buf,0,l);
	strncpy(buf,str,l-1);

	int c = 0;
	char *d = NULL;
		
	while((d = strstr(buf,delim))){
		c++;
		*d = '@';	
	}

	return c;
}
int find_double_delim(char *delim, char *str, int *pos, struct Schema sch)
{

	char *c = NULL;
	int f = 0;
	while((c = strstr(str,delim))){
		f = 1;
		if(find_delim_in_fields(":",str,pos,sch) == 0) return 0;
		if(*(c + 2) == '\0'){
			c++;
			*c = '\0';
			break;	
		}
		c++;
		*c = '{';
		if(pos){
			*pos = c - str;
			pos++;
		}
	}
	if(f) return 0;
	
	return -1;
}

int find_delim_in_fields(char *delim, char *str, int *pos, struct Schema sch)
{
	if(strstr(str, delim) == NULL) return -1;
	size_t l = strlen(str) + 1;
	char buf[l];
	memset(buf,0,l);
	strncpy(buf,str,l-1);
	
	/* in the case of UTILITY usage we need to exclude the type file syntax
	 * field_name:[file data]*/
	int16_t f_start[50];
	int16_t f_end[50];
	memset(f_start,-1,sizeof(int16_t)*50);
	memset(f_end,-1,sizeof(int16_t)*50);

	if(__UTILITY){
		char *fs = NULL;
		char *fe = NULL;
		int i = 0;
		for(; (fs = strstr(buf,"[")) && (fe = strstr(buf,"]")); i++){
			if (i == 50){
				fprintf(stderr,"(%s): code refactor needed for %s() %s:%d\n",prog,__func__,__FILE__,__LINE__-10);
				break;
			}

			f_start[i] = fs - buf;
			f_end[i] = fe - buf;
			*fs = '@';
			*fe = '&';
		}

		if (i > 0) {
			replace('@','[',buf);
			replace('&',']',buf);
		}
	}
	/*
	 * -step 1;
	 * erase the field name from the buf string and the attached ':'*/
	for(int i = 0; i < sch.fields_num;i++){
		char *f = NULL;
		if(!(f = strstr(buf,sch.fields_name[i]))) continue;

		
		
		if(f == &buf[0]) {
			while( *f != ':'){
				*f = ' '; 
				f++;
			}
			if(*f == ':') *f = ' ';
			continue;
		}

		f--;
		/* 
		 * if f is not ':' it means that 
		 * this is not a field  or a similar field, i.e name and last_name 
		 * so we need to skip and look again
		 * for the field 
		 * */

		/* 
		 * check for a similir field
		 * example name and last_name
		 * */
		if(*f == ':'){
			*f = ' ';
		}else if(f != &buf[0]){
			while(*f != ':' && f != &buf[0]) f--;
			if(f == &buf[0] || *f == ':'){
				
				if (*f == ':') *f = ' ';
				int size = 0;
				if(f == &buf[0])
					size = strstr(f,":") - f;
				else 
					size = strstr(&f[1],":") - f;

				if (size < 0) continue;

				char cpy[size+1];
				memset(cpy,0,size+1);
				strncpy(cpy,f,size);
				if(strlen(sch.fields_name[i]) == (size_t)size){
					if(strncmp(cpy,sch.fields_name[i],size) == 0){
						while(*f != ':'){
							*f = ' ';
							f++;
						}
						if(*f == ':') *f = ' ';
						continue;
					}
				}

				continue;
			}
		}else{
			f++;
			*f = '@';
			i--;/* this is to recheck on the same field in the next iteration*/
			continue;
		}

		while( *f != ':'){
			*f = ' '; 
			f++;
		}
		if(*f == ':') *f = ' ';
	}

	if(strstr(buf," ") == NULL) return -1;
	char *start = NULL;
	while((start = strstr(buf,"TYPE_"))){
		*start = '@';
		while(*start != ':' && *start != '\0') start++;

		if(*start == ':') *start = ' ';
	}

	while((start = strstr(buf,"t_"))){
		if(is_target_db_type(start) == -1)break;

		*start = '@';
		while(*start != ':' && *start != '\0') start++;

		if(*start == ':') *start = ' ';
	}


	/* at this point only the ':' inside the fields are left*/
	char *delim_in_fields = NULL;
	int i = 0;
	int8_t cont = 0;
	while((delim_in_fields = strstr(buf,delim))){
		
		int p = delim_in_fields - buf;
		if(f_start[0] > -1 && f_end[0] > -1){
			for(int i = 0; f_start[i] != -1 && f_end[i] != -1; i++){
				if (p > f_start[i] && p < f_end[i]){
					*delim_in_fields = '@';
					cont = 1;	
					break;
				}
			}
			if(cont){
				cont = 0;
				continue;
			}
		}

		str[p] = '{';
		if(i < 200){
			pos[i] = p;
			i++;
		}
		*delim_in_fields = '@';
	}

	return 0;
}

char *find_field_to_reset_delim(int *pos, char *buffer)
{
	static char field[MAX_FILED_LT] = {0};
	
	for(int i = 0; i < 200; i++){
		if (pos[i] == -1) break;
		
		char *p = &buffer[pos[i]];
		int count = 0;
		int start = 0;
		int end = 0;
		int variable_steps = 2;
		while(count < variable_steps) {
			p--;
			if (*p == ':') {
				if(count == 0) end = p - buffer;
				if(count == 1) start = p - buffer;
				if(count == 2) start = p - buffer;
				
				if(count == 1){
					char *type = NULL;
					if((type = strstr(p,TYPE_))){
						if(p == type){ 		
							end = p - buffer;
							variable_steps = 3;
						}
					}
				}
				count++;
			}
		}
	
		strncpy(field, &buffer[start+1],end - start -1);
	}

	return field;
}

static int is_target_db_type(char *target)
{
	int size = 0;
	size_t l = strlen(target);
	if(l > 4){
		/*extract the real target*/
		if(*target == ':'){
			char *e = strstr(&target[1],":");
			if(e) size = e - target;
		}else{
			char *e = strstr(target,":");
			if(e) size = e - target;
		}
	
	}

	if(size > 5) return -1;

	int b_size = 0;
	if(*target == ':') b_size += size + 1;
	if(*target != ':') b_size += (size + 2);


	char tg[b_size];
	memset(tg,0,b_size);

	if(size > 0){
		if(*target == ':') {
			strncpy(tg,target,size);
		}else{
			*tg = ':';
			strncpy(&tg[1],target,size);
		}
		if(strlen(tg) == strlen(":t_s")) if(strncmp(tg,":t_s",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_l")) if(strncmp(tg,":t_l",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_b")) if(strncmp(tg,":t_b",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_d")) if(strncmp(tg,":t_d",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_i")) if(strncmp(tg,":t_i",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_f")) if(strncmp(tg,":t_f",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_p")) if(strncmp(tg,":t_p",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_ai")) if(strncmp(tg,":t_ai",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_al")) if(strncmp(tg,":t_al",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_af")) if(strncmp(tg,":t_af",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_ad")) if(strncmp(tg,":t_ad",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_as")) if(strncmp(tg,":t_as",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_ab")) if(strncmp(tg,":t_ab",size) == 0) return 0;
		if(strlen(tg) == strlen(":t_fl")) if(strncmp(tg,":t_fl",size) == 0) return 0;

		return -1;

	}
	if(strstr(target,":t_s")) return 0;
	if(strstr(target,":t_l")) return 0;
	if(strstr(target,":t_b")) return 0;
	if(strstr(target,":t_d")) return 0;
	if(strstr(target,":t_i")) return 0;
	if(strstr(target,":t_f")) return 0;
	if(strstr(target,":t_p")) return 0;
	if(strstr(target,":t_ai")) return 0;
	if(strstr(target,":t_al")) return 0;
	if(strstr(target,":t_as")) return 0;
	if(strstr(target,":t_ad")) return 0;
	if(strstr(target,":t_af")) return 0;
	if(strstr(target,":t_ab")) return 0;
	if(strstr(target,":t_fl")) return 0;

	return -1;

}

void pack(uint32_t n, uint8_t *digits_indexes)
{
	if((n <= (BASE-1))) {
		memset(digits_indexes,255,sizeof(uint8_t) * 5);
		digits_indexes[4] = n;
		return;
	}

	int i = 4;	
	memset(digits_indexes,255,sizeof(uint8_t) * 5);

	uint32_t rm = 0;
	while(n > 0){
		if (i < 0){
			memset(digits_indexes,255,sizeof(uint8_t) * 5);
			return;
		}
		rm = n % BASE;
		n /= BASE;
		digits_indexes[i] = rm;
		i--;
	}
}

void print_pack_str(uint8_t *digits_index)
{
	for(int i = 0; i < 5; i++){
		if(digits_index[i] == 255) continue;
		printf("%s",base_247[digits_index[i]]);
	}
}

static uint32_t power(uint32_t n, int pow)
{
	uint32_t a = n;
	if(pow == 0) return 1;
	
	for(int i = 1; i < pow; i++){
		a *= n;
	}

	return a;
}

/* this is good for internal unpack*/
long long unpack(uint8_t *digits_index)
{
	long long unpacked = 0;
	int pow = 0;
	for(int i = 4; i >= 0; i--){
		if(digits_index[i] == 255) break;
		unpacked += (digits_index[i] * power(BASE,pow));
		pow++;
	}

	return unpacked;
}
