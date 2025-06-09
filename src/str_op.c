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
#include "debug.h"


static char *strstr_last(char *src, char delim);
static int is_target_db_type(char *target);

int is_number_type(int type)
{
	return type == TYPE_LONG || type == TYPE_FLOAT 
		|| type == TYPE_DOUBLE || type == TYPE_INT 
		|| type == TYPE_BYTE;

}

int is_number_array(int type)
{
	return type == TYPE_ARRAY_DOUBLE || type == TYPE_ARRAY_LONG 
		|| type == TYPE_ARRAY_BYTE || type == TYPE_ARRAY_FLOAT
		|| type == TYPE_ARRAY_INT;
}

int check_handle_input_mode(char *buffer, int op)
{
	int c_t = count_fields(buffer,T_); 
	int c_T = count_fields(buffer,TYPE_); 
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


	find_double_delim("::",cbuf,NULL);
	replace('@','^',cbuf);
	int i = 0;

	/* detect files types*/	
	char *file_start = NULL;
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
	if(strstr(cbuf,T_)){
		char *pos_t = NULL;
		while((pos_t = strstr(cbuf,T_))){
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
			if(strstr(names_s[j],names[x])){
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
		if (l == 0 || errno == EINVAL)
			return NULL;

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
	char *delim = ":";
	char *first = NULL;
	char *p = buffer;
	char *p2 = buffer;
	char *last = NULL;
	
	int i = 0;
	char *file_block = NULL;

	while(((first = strstr(p2,delim)) != NULL) && ((last=strstr(&p[(first+1) - buffer],delim)) != NULL)){
		if(first[1] == '[' && ((file_block = strstr(&first[1],"]")))){
			int size = first - p2;
			int next_start = file_block - buffer;
			char cpy[size+1];
			memset(cpy,0,size+1);
			strncpy(names[i],p2,size);

			p2 += next_start+1;
			i++;
			continue;
		}	

		char *move_back = (first - 1); 
		while(move_back != &buffer[0] && *move_back != '@' && *move_back != ':') --move_back;   
			
		int size = 0;
		if(move_back == &buffer[0])
			size = (first - buffer) - ( move_back - buffer) +1;
		else
			size = (first - buffer) - ( move_back - buffer);
		
		int next_start = last - p2;
		char cpy[size];
		memset(cpy,0,size);
		if(move_back == &buffer[0])
			strncpy(names[i],move_back,size-1);
		else
			strncpy(names[i],++move_back,size-1);

		p2 += next_start+1;
		i++;
	}

	if(first){
		if(*(first + 1) != '\0') {
			char *cf = (first - 1);		
			while(*cf != ':' && cf != &buffer[0] && *cf != '@') cf--;
			int size = 0; 
			if(cf != &buffer[0])	 
				size = first - (++cf) +1;
			else 
				size = first - cf +1;	
			
			strncpy(names[i],cf,size-1);
			i++;
		}
	}else if(strstr(buffer,delim) == NULL && !strstr(buffer,"@")){
		if(buffer){
			strncpy(names[i],buffer,strlen(buffer));
			i++;
		}
	}else if ((first = strstr(buffer,delim))){
		if(first[1] == '['){
			if((last = strstr(&first[1],delim))){
				if(last[strlen(last)-1] == ']') return i;
			}
		}

	}else if((last = strstr_last(buffer,'@'))){
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
		int size = delim - p;
		pos = delim - fields_name;
		strncpy(names[j],p,size);
		p += size +1;
		j++;
	}

	if(fields_name[pos + 1] != '\0') {
		strncpy(names[j],p,strlen(p));
		j++;
	}

	return j;
}

int get_fileds_name(char *fields_name, int fields_count, int steps, char names[][MAX_FILED_LT])
{

	if (fields_count == 0) return -1;

	int j = 0;
	char *s = NULL;

	find_double_delim("::",fields_name,NULL);
	char *cp_fv = fields_name;
	while ((s = strtok_r(cp_fv, ":", &cp_fv)) != NULL && j < fields_count) {
		strip('_', s);
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

	replace('@','^',cbuff);
	/* detect files types*/	
	char *file_start = NULL;
	int count = 0;
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

	replace('@','[',cbuff);
	replace('&','@',cbuff);

	int i = 0;
	if(strstr(cbuff,T_)){
		char *pos_t = NULL;
		while((pos_t = strstr(cbuff,T_))){
			/*this change the delimeter so it won't
			 * be found in the next iteration*/
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
	char *delim = ":";
	char *first = NULL;
	char *p = buff;
	char *p2 = buff;
	char *last = NULL;
	
	char **values = calloc(fields_count, sizeof(char *));
	if (!values) {
		perror("memory get values");
		return NULL;
	}

	int i = 0;
	char *file_block = NULL;
	while((first = strstr(p2,delim)) != NULL && (last=strstr(&p[(first+1) - buff],delim)) != NULL){
		if(first[1] == '[' && ((file_block = strstr(&first[1],"]")))){
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
		int start = (first + 1) - buff;
		int size = (last - buff) - start;
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
	}

	if(first){
		if(*(first + 1) != '\0') {
			values[i] = strdup(&first[1]);
			if(!values[i]){
				fprintf(stderr,"strdup() failed, %s:%d",__FILE__,__LINE__-2);
				return NULL;
			}

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
	int i = 0, j = 0;
	va_list args;
	va_start(args, count);

	for (i = 0; i < count; i++)
	{
		char **str = va_arg(args, char **);
		for (j = 0; j < fields_num; j++)
		{
			if (str[j])
			{
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
			if ((str[i] != '/' && str[i] != '_'))
				return 0;

		if (isspace(str[i]))
			return 0;
	}
	return 1;
}

void strip(const char c, char *str)
{
	size_t l = strlen(str);
	for (size_t i = 0; i < l; i++)
		if (str[i] == c)
			str[i] = ' ';
}

void replace(const char c, const char with, char *str)
{
	size_t l = strlen(str);
	for (size_t i = 0; i < l; i++)
		if (str[i] == c)
			str[i] = with;
}
char return_first_char(char *str)
{
	return str[0];
}

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
	for (size_t i = 0; i < l; i++)
	{
		if (!isdigit(str[i]))
		{
			if (str[i] == '-' && i == 0)
			{
				continue;
			}
			else
			{
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

	if (is_integer(value))
	{

		for (size_t i = 0; i < l; i++)
			ascii_value += (int)value[i];

		if (negative)
		{
			if (-1 * ascii_value < -1 * ASCII_INT_MIN)
			{
				if (-1 * ascii_value < -1 * ASCII_LONG_MIN)
					return 0;
				else
					return IN_LONG;
			}

			return IN_INT;
		}

		if (ascii_value > ASCII_INT_MAX)
		{
			if (ascii_value > ASCII_LONG_MAX)
				return 0;
			else
				return IN_LONG;
		}

		return IN_INT;
	}

	if (is_floaintg_point(value))
	{
		for (size_t i = 0; i < l; i++)
			ascii_value += (int)value[i];

		if (negative)
		{
			if (-1 * ascii_value < -1 * ASCII_FLT_MAX_NEGATIVE)
			{
				if (-1 * ascii_value < -1 * ASCII_DBL_MAX_NEGATIVE)
					return 0;
				else
					return IN_DOUBLE;
			}

			return IN_FLOAT;
		}

		if (ascii_value > ASCII_FLT_MAX)
		{
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
	for (size_t i = 0; i < l; i++)
	{
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
int find_double_delim(char *delim, char *str, int *pos)
{

	char *c = NULL;
	while((c = strstr(str,delim))){
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
	
	return 0;
}

int find_delim_in_fields(char *delim, char *str, int *pos)
{
	size_t l = strlen(str) + 1;
	char buf[l];
	memset(buf,0,l);
	strncpy(buf,str,l-1);
	
	int c_T = count_delim(TYPE_,str);
	int c_t = count_delim(T_,str);

	if(c_t == 0 && c_T == 0	) return -1;
	if(c_T > 0 && c_t > 0){
		/* you gotta swap types you have to map 'TYPE_STRING' to 't_s'*/	

	}
	
	char *start = NULL;
	char *end = NULL;

	/*this is to avoid repeating code 
	 * but this is HORRIBLE
	 * i used the thirnary expretion to choose wich string costat i need to use
	 * */
	static char t[10] = {0};
	strncpy(t,(c_t > 0 && c_T == 0) ? T_ : TYPE_,(c_t > 0 && c_T == 0) ? strlen(T_) : strlen(TYPE_)); 
	while((start = strstr(buf,t)) && ((end = strstr(++start,t)))){
		*start = '@';
		end--;
		while(*start != ':') start++;
		while(*end != ':') end--;

		start++;
		int begin = start - buf;
		int size = (end - buf) - (start - buf)+1;	
		char frag[size]; 
		memset(frag,0,size);

		strncpy(frag,&buf[begin],size - 1);

		char *d = strstr(frag,delim);
		if(d){
			char *fr = strstr(str,d);
			int i = fr-str;
			str[i] = '{';
			if(pos){
				*pos = d - str;
				pos++;
			}
		} 
	}
	return 0;
}

static int is_target_db_type(char *target)
{
	if(strstr(target,":t_s")) return 0;
	if(strstr(target,":t_l")) return 0;
	if(strstr(target,":t_b")) return 0;
	if(strstr(target,":t_d")) return 0;
	if(strstr(target,":t_i")) return 0;
	if(strstr(target,":t_f")) return 0;
	if(strstr(target,":t_ai")) return 0;
	if(strstr(target,":t_al")) return 0;
	if(strstr(target,":t_as")) return 0;
	if(strstr(target,":t_ad")) return 0;
	if(strstr(target,":t_af")) return 0;
	if(strstr(target,":t_ab")) return 0;
	if(strstr(target,":t_fl")) return 0;

	return -1;

}
