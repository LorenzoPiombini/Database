#include <stdarg.h>
#include <assert.h>
#include <stdio.h>
#include "str_op.h"
#include "types.h"
#include "freestand.h"
#include "common.h"
#include "globals.h"
#include "debug.h"
#include "memory.h"

static char prog[] = "db";
static char *find_needle_last(char *src, char delim);
static int is_target_db_type(char *target);
static int search_string_for_types(char *str, int *types);

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
	char *start_d = 0x0;
	char *end_d = 0x0;
	static char sub_str[1024] = {0};
	set_memory(sub_str,0,1024);

	if((start_d = find_needle(str,start_delim))){
		if((end_d = find_needle(str,end_delim))){
			int sx = start_d - str;
			int size = ((--end_d - str) - sx)+1;

			string_copy(sub_str,&str[sx + 1],size-1);
			return &sub_str[0];
		}
	}
	return 0x0;
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
	size_t size = string_length(buffer) + 1;
	char cbuf[size];
	set_memory(cbuf,0,size);
	string_copy(cbuf,buffer,size);


	replace('@','^',cbuf);
	int i = 0;

	/* detect files types*/	
	char *file_start = 0x0;
	if(!__IMPORT_EZ){
		while((file_start = find_needle(cbuf,"["))){
			char *end_file = find_needle(file_start,"]");
			if(end_file){ 
				int end = end_file - cbuf;
				char *delim = 0x0;
				while((delim = find_needle(file_start,":"))){
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
	set_memory(reset_mistype_index,-1,sizeof(int)*10);
	int rmi = 0;
	if(find_needle(cbuf,T_)){
		char *pos_t = 0x0;
		while((pos_t = find_needle(cbuf,T_))){
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
			char *end_t = (find_needle(pos_t,":"));
			if(!end_t){
				size_t sz = string_length(++pos_t) + 1;
				char type[sz];
				set_memory(type,0,sz);
				string_copy(type,pos_t,sz);
				types_i[i] = get_type(type);

				char *p = &cbuf[start] - 1;
				while(*p !=':' && p != &cbuf[0] && *p != '@') p--;

				sz = (--pos_t - cbuf) - (p - cbuf) + 1;
				if(sz-2 > MAX_FIELD_LT) return -1;

				if(p != &cbuf[0]){
					string_copy(names[i],++p, sz -2);
					set_memory(p,0x20,sz-2);
				}else{
					string_copy(names[i],p, sz -1);
					set_memory(p,0x20,sz-1);
				}

				i++;
				break;
			}


			/*get the type*/
			int l = end_t - pos_t;
			char type[l];
			set_memory(type,0,l);
			string_copy(type,++pos_t,l-1);
			types_i[i] = get_type(type);

			char *p = &cbuf[start] - 1;
			while(*p !=':' && p != &cbuf[0] && *p != '@') p--;
			
			l = pos_t - p;
			if(l-2 > MAX_FIELD_LT) return -1;

			if(p != &cbuf[0]){
				string_copy(names[i],++p, l -2);
				set_memory(p,0x20,l-2);
			}else{
				string_copy(names[i],p, l -1);
				set_memory(p,0x20,l-1);
			}
				
			if(types_i[i] == TYPE_FILE){
				char *file_block = 0x0;
				if((file_block = find_needle(p,"["))){
					char *d = 0x0;
					char *end = find_needle(file_block,"]");
					while((d = find_needle(&file_block[1],T_)) && 
							(((d = find_needle(&file_block[1],T_)) - cbuf) < (end - cbuf))){ 
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
	if (find_needle(cbuf,TYPE_)){
		char *pos_T = 0x0;
		while((pos_T = find_needle(cbuf,TYPE_))){
			int start = pos_T - cbuf;
			/*this change the delimeter so it won't
			 * be found in the next iteration*/
			*pos_T= '@';
			char *end_T = (find_needle(pos_T,":"));
			if(!end_T){
				size_t sz = string_length(++pos_T) + 1;
				char type[sz];
				set_memory(type,0,sz);
				string_copy(type,pos_T,sz);
				types_i[i] = get_type(type);

				char *p = &cbuf[start] - 1;
				while(*p !=':' && p != &cbuf[0] && *p != '@') p--;

				sz = pos_T - p;
				if(sz-2 > MAX_FIELD_LT) return -1;

				if(p != &cbuf[0]){
					string_copy(names[i],++p, sz -2);
					set_memory(p,0x20,sz-2);
				}else{
					string_copy(names[i],p, sz -1);
					set_memory(p,0x20,sz-1);
				}
				i++;
				break;
			}

			/*get the type*/
			int l = end_T - pos_T;
			char type[l];
			set_memory(type,0,l);
			string_copy(type,++pos_T,l-1);
			types_i[i] = get_type(type);


			char *p = &cbuf[start] - 1;
			while(*p !=':' && p != &cbuf[0] && *p != '@') p--;
			
			l = pos_T - p;
			if(l-2 > MAX_FIELD_LT) return -1;

			if(p != &cbuf[0]){
				string_copy(names[i],++p, l -2);
				set_memory(p,0x20,l-2);
			}else{
				string_copy(names[i],p, l -1);
				set_memory(p,0x20,l-1);
			}

			if(types_i[i] == TYPE_FILE){
				char *file_block = 0x0;
				if((file_block = find_needle(p,"["))){
					char *d = 0x0;
					char *end = find_needle(file_block,"]");
					while((d = find_needle(&file_block[1],TYPE_)) && 
							(((d = find_needle(&file_block[1],TYPE_)) - cbuf) < (end - cbuf))){ 
						*d = '@';	
					}
				}

			}
			i++;
		}
	} 
	char names_s[MAX_FIELD_NR - i][MAX_FILED_LT];
	set_memory(names_s,0,(MAX_FIELD_NR - i)*MAX_FILED_LT);
	

	int s = 0;
	switch(mode){
	case HYB_DF:
	{
		if(search_string_for_types(cbuf,types_i) == -1){
			return -1;
		}

		char buf[string_length(cbuf)+1];
		set_memory(buf,0,string_length(cbuf)+1);

		char *p_buf = &cbuf[0];
		int i;
		for(i = 0; *p_buf != '\0'; p_buf++){
			if(*p_buf == ' ') continue;
			
			if(i == 0 && *p_buf == ':') continue;
			if(i > 0){
				if(buf[i-1] == ':' && *p_buf == ':')continue;
			}
			buf[i] = *p_buf;
			i++;
		}

		s = get_fields_name_with_no_type(buf, names_s);
		break;
	}
	case HYB_WR:	
	{ 		
		if(search_string_for_types(cbuf,types_i) == -1){
			return -1;
		}
		char buf[string_length(cbuf)+1];
		set_memory(buf,0,string_length(cbuf)+1);

		char *p_buf = &cbuf[0];
		int i;
		for(i = 0; *p_buf != '\0'; p_buf++){
			if(*p_buf == ' ') continue;
			
			if(i == 0 && *p_buf == ':') continue;
			if(i > 0){
				if(buf[i-1] == ':' && *p_buf == ':')continue;
			}
			buf[i] = *p_buf;
			i++;
		}

		s = get_names_with_no_type_skip_value(buf,names_s);
			
		break;
	}
	default:
		display_to_stdout("mode not supported, %s:%d.\n",__FILE__,__LINE__);
		return -1;
	}
	
	int skip = -1;
	int j,x;
	for(j = 0; j < s; j++){
		int b = 0;
		for(x = 0; x < i; x++){
			size_t sz = string_length(names_s[j]);
			if(sz != string_length(names[x])) continue;
			if(string_compare(names_s[j],names[x],sz,-1) == 0){
				skip = j;
				b = 1;
				break;
			}
		}
		if (b) break;
	}

	for(x = 0; names_s[x][0] != '\0'; x++){
		if (x == skip) continue;
		string_copy(names[i],names_s[x],string_length(names_s[x]));
		i++;
	}
	return i;
}

static int search_string_for_types(char *str, int *types)
{
	int i;
	for(i = 0;*str != '\0';str++,i++){
		if(types[i] == -1) break;
		switch(types[i]){
		case TYPE_KEY:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_ky:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;

				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			while((p = find_needle(str,"@TYPE_KEY:"))){

				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;

				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}

			break;

		}
		case TYPE_INT:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_i:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;

				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			while((p = find_needle(str,"@TYPE_INT:"))){

				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;

				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}

			break;
		}
		case TYPE_DATE:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_dt:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p);
					int k;
					for(k = 0; k < p_size; k++,p++)
						*p = ' ';

					break;/* break from switch */
				}
				int p_size = (int)(p_end - str) - (p - str);
				if(p_size < 0)return -1;

				int k;
				for(k = 0; k < p_size; k++,p++)
					*p = ' ';

				break;
			}

			while((p = find_needle(str,"@TYPE_DATE:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p);
					int k;
					for(k = 0; k < p_size; k++,p++)
						*p = ' ';

					break;/* break from switch */
				}
				int p_size = (int)(p_end - str) - (p - str);
				if(p_size < 0)return -1;

				int k;
				for(k = 0; k < p_size; k++,p++)
					*p = ' ';

				break;
			}
			break;
		}
		case TYPE_LONG:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_l:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;

				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			while((p = find_needle(str,"@TYPE_LONG:"))){

				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;

				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}

			break;

		}
		case TYPE_FLOAT:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_f:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			while((p = find_needle(str,"@TYPE_FLOAT:"))){

				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			break;

		}
		case TYPE_DOUBLE:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_d:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}

			while((p = find_needle(str,"@TYPE_DOUBLE:"))){

				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			break;
		}
		case TYPE_BYTE:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_b:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			while((p = find_needle(str,"@TYPE_BYTE:"))){

				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			break;
		}
		case TYPE_STRING:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_s:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}

			while((p = find_needle(str,"@TYPE_STRING:"))){

				char *p_end = find_needle(p,":");
				if(!p_end) return -1;

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			break;
		}
		case TYPE_ARRAY_INT:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_ai:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}

			while((p = find_needle(str,"@TYPE_ARRAY_INT:"))){

				char *p_end = find_needle(p,":");
				if(!p_end) return -1;

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			break;
		}
		case TYPE_ARRAY_LONG:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_al:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}

			while((p = find_needle(str,"@TYPE_ARRAY_LONG:"))){

				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			break;
		}
		case TYPE_ARRAY_BYTE:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_ab:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}


				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}

			while((p = find_needle(str,"@TYPE_ARRAY_BYTE:"))){

				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			break;
		}
		case TYPE_ARRAY_FLOAT:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_af:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}

			while((p = find_needle(str,"@TYPE_ARRAY_FLOAT:"))){

				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			break;
		}
		case TYPE_ARRAY_DOUBLE:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_ad:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}

			while((p = find_needle(str,"@TYPE_ARRAY_DOUBLE:"))){

				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;	
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			break;
		}
		case TYPE_ARRAY_STRING:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_ab:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}

			while((p = find_needle(str,"@TYPE_ARRAY_BYTE:"))){

				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			break;
		}
		case TYPE_FILE:
		{
			char *p = 0x0;
			while((p = find_needle(str,"@t_fl:"))){
				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}

			while((p = find_needle(str,"@TYPE_FILE:"))){

				char *p_end = find_needle(p,":");
				if(!p_end){
					int p_size = (int)string_length(p); 

					int k;
					for(k = 0; k < p_size; k++){
						*p = ' ';
						p++;
					}
					break;
				}

				int p_size =(int) (p_end - str) - (p - str); 
				if(p_size < 0) return -1;
		
				int k;
				for(k = 0; k < p_size; k++){
					*p = ' ';
					p++;
				}
			}
			break;
		}
		default:
			display_to_stdout("type not valid. %s:%d.\n",__FILE__,__LINE__);
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
		if (is_alpha(*key) || is_punct(*key))
			return str;

	return uint;
}

void *key_converter(char *key, int *key_type)
{
	void *converted = 0x0;
	*key_type = is_num(key);
	switch (*key_type) {
	case 2:
	{
		long l = string_to_long(key);
		if (l == INVALID_VALUE) return 0x0;

		if (l == MAX_KEY) {
			display_to_stdout( "key value out fo range.\n");
			return 0x0;
		}

		converted = (ui32*)ask_mem(sizeof(ui32));
		if (!converted) {
			display_to_stdout("ask_mem() failed, %s:%d.\n",F, L - 2);
			return 0x0;
		}

		copy_memory(converted, (ui32 *)&l, sizeof(ui32));
		break;
	}
	case 1:
		return 0x0;
	default:
		return 0x0;
	}

	return converted;
}

static char *find_needle_last(char *src, char delim)
{

	int last = 0 ;
	char *p = src;
	for(; *p != '\0'; p++){
		if(*p == delim) last = p - src;		
	}
	
	if(last == 0) return 0x0;
	return &src[last];
}
int get_names_with_no_type_skip_value(char *buffer, char names[][MAX_FIELD_LT])
{
	/*copy the buffer*/
	size_t l = string_length(buffer);
	char cpy[l+1];
	set_memory(cpy,0,l+1);
	string_copy(cpy,buffer,l);
	

	if(find_needle(buffer,"@") && !find_needle(buffer,"@@")) replace('@','^',cpy);

	/*exclude file syntax*/
	if(!__IMPORT_EZ){
		char *f_st = 0x0;
		char *f_en = 0x0;
		while((f_st = find_needle(cpy,"[")) && (f_en = find_needle(cpy,"]"))){
			*f_st = '\n';
			for(; *f_st != ']'; f_st++){
				if(*f_st == ':') *f_st = '@';
			}
			*f_en = '\r';
		}

		if(find_needle(cpy,"\n")) replace('\n','[',cpy);
		if(find_needle(cpy,"\r")) replace('\r',']',cpy);
	}
	

	char *delim = ":";
	char *first = 0x0;
	char *last = 0x0;
	
	int i = 0;
	char *p = cpy;
	char *p2 = cpy;
	while(((first = find_needle(p2,delim)) != 0x0) && ((last=find_needle(&p[(first+1) - cpy],delim)) != 0x0)){
		char *move_back = (first - 1); 
		while(move_back != &cpy[0] && *move_back != '@' && *move_back != ':') --move_back;   
			
		int size = 0;
		if(move_back == &cpy[0])
			size = (first - cpy) - ( move_back - cpy) +1;
		else
			size = (first - cpy) - ( move_back - cpy);
		
		int next_start = last - p2;
		if(move_back == &cpy[0])
			string_copy(names[i],move_back,size-1);
		else
			string_copy(names[i],++move_back,size-1);

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
			
			string_copy(names[i],cf,size-1);
			i++;
		}
	}else if(find_needle(cpy,delim) == 0x0 && !find_needle(cpy,"@")){
		if(buffer){
			string_copy(names[i],cpy,string_length(buffer));
			i++;
		}
	}else if ((first = find_needle(cpy,delim))){
		if(first[1] == '['){
			if((last = find_needle(&first[1],delim))){
				if(last[string_length(last)-1] == ']') return i;
			}
		}

	}else if((last = find_needle_last(cpy,'@'))){
		++last;
		string_copy(names[i],last,string_length(last));
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

	size_t file_p_l= string_length(file_path);

	if(file_p_l > MAX_FILE_PATH_LENGTH) return EFLENGTH;
	if(file_p_l + string_length(dat) > MAX_FILE_PATH_LENGTH) return EFLENGTH;
	
	string_copy(files[0],file_path,file_p_l);
	string_copy(&files[0][file_p_l],ind,string_length(ind));

	string_copy(files[1],file_path,file_p_l);
	string_copy(&files[1][file_p_l],dat,string_length(dat));

	string_copy(files[2],file_path,file_p_l);
	string_copy(&files[2][file_p_l],sch,string_length(dat));
	
	return 0;
}

int count_fields(char *fields, const char *user_target)
{
	int c = 0;
	const char *p = fields;
	if(user_target){
		while ((p = find_needle(p, user_target)) != 0x0) {
			c++;
			p += string_length(user_target);
		}
		return c;
	}
	

	char *target = TYPE_;
	if(find_needle(p,target) == 0x0){
		char *new_target = T_;
		if(find_needle(p,new_target) != 0x0){
			while ((p = find_needle(p, new_target)) != 0x0) {
				if(is_target_db_type((char *)p) == -1){
					p += string_length(new_target);
					continue;
				}
				c++;
				p += string_length(new_target);
			}
		}
	} else {
		char *second_target = T_;
		if(find_needle(p,second_target) != 0x0){

			while ((p = find_needle(p, second_target)) != 0x0) {
				if(is_target_db_type((char *)p) == -1){
					p += string_length(second_target);
					continue;
				}
				c++;
				p += string_length(second_target);
			}
		}

		p = fields;
		while ((p = find_needle(p, target)) != 0x0) {
				c++;
				p += string_length(target);
		}

	}

	if(c == 0){
		p = fields;
		int pos = 0;
		char *new_target = ":";
		while((p = find_needle(p,new_target))){
			pos = p - fields;
			c++;
			p += string_length(new_target);
		}

		if(fields[pos+1] != '\0') c++;

	}
	
	

	return c;
}

int get_type(char *s){

	if (string_compare(s, "TYPE_INT",8,-1) == 0) {
		return 0;
	} else if (string_compare(s, "TYPE_LONG",9,-1) == 0){
		return 1;
	} else if (string_compare(s, "TYPE_FLOAT",10,-1) == 0) {
		return 2;
	}
	else if (string_compare(s, "TYPE_STRING",11,-1) == 0)
	{
		return 3;
	}
	else if (string_compare(s, "TYPE_BYTE",9,-1) == 0)
	{
		return 4;
	}
	else if(string_compare(s, "TYPE_PACK",9,-1) == 0)
	{
		return 5;
	}
	else if (string_compare(s, "TYPE_DOUBLE",11,-1) == 0)
	{
		return 6;
	}else if (string_compare(s, "TYPE_KEY",8,-1) == 0)
	{
		return 15;
	}
	else if (string_compare(s, "t_i",3,-1) == 0)
	{
		return 0;
	}
	else if (string_compare(s, "t_l",3,-1) == 0)
	{
		return 1;
	}
	else if (string_compare(s, "t_f",3,-1) == 0)
	{
		return 2;
	}
	else if (string_compare(s, "t_s",3,-1) == 0)
	{
		return 3;
	}
	else if (string_compare(s, "t_b",3,-1) == 0)
	{
		return 4;
	}
	else if (string_compare(s, "t_pk",4,-1) == 0)
	{
		return 5;
	}
	else if (string_compare(s, "t_d",3,-1) == 0)
	{
		return 6;
	}
	else if (string_compare(s, "TYPE_ARRAY_INT",14,-1) == 0)
	{
		return 7;
	}
	else if (string_compare(s, "TYPE_ARRAY_LONG",15,-1) == 0)
	{
		return 8;
	}
	else if (string_compare(s, "TYPE_ARRAY_FLOAT",16,-1) == 0)
	{
		return 9;
	}
	else if (string_compare(s, "TYPE_ARRAY_STRING",17,-1) == 0)
	{
		return 10;
	}
	else if (string_compare(s, "TYPE_ARRAY_BYTE",15,-1) == 0)
	{
		return 11;
	}
	else if (string_compare(s, "TYPE_ARRAY_DOUBLE",17,-1) == 0)
	{
		return 12;
	}
	else if (string_compare(s, "TYPE_FILE",9,-1) == 0)
	{
		return 13;
	}
	else if (string_compare(s, "TYPE_DATE",9,-1) == 0){
		return 14;
	}else if (string_compare(s, "t_ai",4,-1) == 0)
	{
		return 7;
	}
	else if (string_compare(s, "t_al",4,-1) == 0)
	{
		return 8;
	}
	else if (string_compare(s, "t_af",4,-1) == 0)
	{
		return 9;
	}
	else if (string_compare(s, "t_as",4,-1) == 0)
	{
		return 10;
	}
	else if (string_compare(s, "t_ab",4,-1) == 0)
	{
		return 11;
	}
	else if (string_compare(s, "t_ad",4,-1) == 0)
	{
		return 12;
	}
	else if (string_compare(s, "t_fl",4,-1) == 0)
	{
		return 13;
	}else if (string_compare(s, "t_dt",4,-1) == 0){
		return 14;
	}else if (string_compare(s, "t_ky",4,-1) == 0){
		return 15;
	}

	return -1;
}

int get_fields_name_with_no_type(char *fields_name, char names[][MAX_FILED_LT])
{
	
	char *p = fields_name;
	char *delim = 0x0;
	int j = 0;
	int pos = 0;
	if(find_needle(p,":") == 0x0){
		if(p){
			string_copy(names[j],p,string_length(p));
			j++;
			return j;
		}
	}
		
	while((delim = find_needle(p,":"))){
		pos = delim - fields_name;
		*delim = '@';
		delim--;
		int i = 0;
		for( i = 1;*delim != ':' && *delim != '@' && delim != &p[0]; i++, delim--);
		
		if(delim != &p[0]) delim++, i--;

		string_copy(names[j],delim,i);
		p += (i+1);
		j++;
	}

	if(fields_name[pos + 1] != '\0' && find_needle(fields_name,"@@") == 0x0) {
		string_copy(names[j],&fields_name[pos+1],string_length(&fields_name[pos + 1]));
		j++;
	}

	return j;
}

int get_fileds_name(char *fields_name, int fields_count, int steps, char names[][MAX_FILED_LT])
{
	if (fields_count == 0) return -1;

	int j = 0;
	char *s = 0x0;

	while ((s = tok(fields_name, ":")) != 0x0 && j < fields_count) {
		string_copy(names[j],s,string_length(s));

		j++;

		tok(0x0, ":");

		if (steps == 3 && j < fields_count)
			tok(0x0, ":");
	}

	return 0;
}

unsigned char check_fields_integrity(char names[][MAX_FILED_LT], int fields_count)
{
	int c = 0;

	if(fields_count == 0) return 0;
	int i,j;
	for (i = 0; i < fields_count; i++) {
		if ((fields_count - i) == 1)
			break;

		for (j = 0; j < fields_count; j++) {
			if (((fields_count - i) > 1) && ((fields_count - j) > 1))
			{
				if (j == i)
					continue;

				int l_i = string_length(names[i]);
				int l_j = string_length(names[j]);
				if(names[j][0] == '\0') c++;
				if(c == fields_count) return 0;

				if (l_i == l_j)
				{
					if (string_compare(names[i], names[j], l_j,-1) == 0)
						return 0;
				}
			}
		}
	}
	return 1;
}

int check_array_input(char *value)
{
	size_t size = string_length(value) + 1;
	char cbuff[size];
	set_memory(cbuff,0,size);
	string_copy(cbuff,value,size);

	char *p = 0x0;
	int elements = 0;
	int d = 0;
	int i = 0;
	int pos = 0;
	while((p = find_needle(cbuff,","))){
		if(p[1] == '\0') break;

		elements++;
		int start = p - cbuff;
		while((p != &cbuff[0] && *p != '@')) p--;

		if(*p == '@') ++p;

		int l = (&cbuff[start] - p)+1;
		pos = start ;
		char cpy[l];
		set_memory(cpy, 0, l);

		string_copy(cpy,p,l-1);

		if(is_floaintg_point(cpy)){
			if(is_number_in_limits(cpy)) d++;
		}else if(is_integer(cpy)){
			if(is_number_in_limits(cpy)) i++;
		}
		
		cbuff[start] = '@';
	}

	if(cbuff[pos] != '\0' && p == 0x0){
		elements++;
		char *last = &cbuff[pos];
		size_t size = string_length(++last) + 1;
		char n[size];
		set_memory(n,0,size);
		
		string_copy(n,last,size-1);
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

	if(value[0] == '[' && value[string_length(value) - 1] == ']') return TYPE_FILE;

	if(find_needle(value,",")){
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

	char *s = 0x0;
	s = tok(fields_input, ":");

	s = tok(0x0, ":");

	if (s){
		int result = get_type(s);
		if (result == -1) return -1;
		types[j] = result;
		i++;
	}else {
		if(fields_count > 1){
			display_to_stdout("type token not found in get_value_types().\n");
			return -1;
		}
		return 0;
	}

	while ((s = tok(0x0, ":")) != 0x0) {
		if ((i % 3 == 0) && (j < fields_count - 1)) {
			j++;
			int r  = get_type(s);
			if (r == -1){
				display_to_stdout("check input format: fieldName:TYPE:value.\n");
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
	size_t size = string_length(buff)+1;
	char cbuff[size];
	set_memory(cbuff,0,size);
	string_copy(cbuff,buff,size);

	*values = (char**)ask_mem(fields_count * sizeof(char *));
	if (!(*values)) {
		display_to_stdout("ask_mem() failed, %s:%d.\n",F,L-2);
		return -1;
	}

	/* detect files types - ONLY IF NOT IMPORTING FROM EZgen*/	
	char *file_start = 0x0;
	int count = 0;
	if(!__IMPORT_EZ){
		
		while((file_start = find_needle(cbuff,"["))){
			char *end_file = find_needle(file_start,"]");
			if(end_file){ 
				int end = end_file - cbuff;
				char *delim = 0x0;
				while((delim = find_needle(file_start,":"))){
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
	set_memory(reset_mistype_index,-1,sizeof(int)*10);
	int rmi = 0;
	if(find_needle(cbuff,T_)){
		char *pos_t = 0x0;
		while((pos_t = find_needle(cbuff,T_))){
			/*this change the delimeter so it won't
			 * be found in the next iteration*/
			if(is_target_db_type(pos_t) == -1){
				*pos_t = '@';
				reset_mistype_index[rmi] = pos_t - cbuff;
				rmi++;
				continue;
			}
			*pos_t= '@';
			char *end_t = (find_needle(pos_t,":"));
			assert(end_t != 0x0); 

			*end_t = '@';
			char *next = find_needle(end_t,":");
			if(!next){
				++end_t;
				if(count > 0){
					/*clean the value*/
					replace('@',':',end_t);
				}
				(*values)[i] = duplicate_str(end_t);
				if(!(*values)[i]){
					display_to_stdout("duplicate_str() failed, %s:%d\n",__FILE__,__LINE__-2);
					if(i == 0)
						cancel_memory(0x0,*values,sizeof(char*)*fields_count);
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
			set_memory(cpy,0,len);
			string_copy(cpy,end_t,len-1);

			if(count > 0){
				/*clean the value*/
				replace('@',':',cpy);
			}
			(*values)[i] = duplicate_str(cpy);
			if(!(*values)[i]){
				display_to_stdout("duplicate_str() failed, %s:%d\n",__FILE__,__LINE__-2);
				if(i == 0)
					cancel_memory(0x0,*values,sizeof(char*)*fields_count);
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
	

	if(find_needle(cbuff,TYPE_)){
		char *pos_T = 0x0;
		while((pos_T = find_needle(cbuff,TYPE_))){
			/*this change the delimeter so it won't
			 * be found in the next iteration*/
			*pos_T= '@';
			char *end_T = (find_needle(pos_T,":"));
			assert(end_T != 0x0); 

			*end_T = '@';
			char *next = find_needle(end_T+1,":");
			if(!next){
				++end_T;
				if(count > 0){
					/*clean the value*/
					replace('@',':',end_T);
				}
				(*values)[i] = duplicate_str(end_T);
				if(!(*values)[i]){
					display_to_stdout("duplicate_str() failed, %s:%d\n",__FILE__,__LINE__-2);
					if(i == 0)
						cancel_memory(0x0,*values,sizeof(char*)*fields_count);
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
			set_memory(cpy,0,len);
			string_copy(cpy,end_T,len-1);

			if(count > 0){
				/*clean the value*/
				replace('@',':',cpy);
			}
			(*values)[i] = duplicate_str(cpy);
			if(!(*values)[i]){
				display_to_stdout("duplicate_str() failed, %s:%d\n",__FILE__,__LINE__-2);
				if(i == 0)
					cancel_memory(0x0,*values,sizeof(char*)*fields_count);
				else
					free_strs(i,1,values);

				return -1;
			}
			i++;
		}		
	}

	if(find_needle(cbuff,":")){
		char *pos_d = 0x0;
		while((pos_d = find_needle(cbuff,":"))){
			/*this change the delimeter so it won't
			 * be found in the next iteration*/
			*pos_d= '@';
			char *end_d = find_needle(pos_d,":");
			if(!end_d){
				++pos_d;		
				if(count > 0){
					/*clean the value*/
					replace('@',':',pos_d);
				}
				(*values)[i] = duplicate_str(pos_d);
				if(!(*values)[i]){
					display_to_stdout("duplicate_str() failed, %s:%d\n",__FILE__,__LINE__-2);
					if(i == 0)
						cancel_memory(0x0,*values,sizeof(char*)*fields_count);
					else
						free_strs(i,1,values);

					return -1;
				}
				break;
			}

			size_t len = end_d - (++pos_d) + 1;
			char cpy[len];
			set_memory(cpy,0,len);
			*end_d = '@';
			string_copy(cpy,pos_d,len-1);

			if(count > 0){
				/*clean the value*/
				replace('@',':',cpy);
			}
			(*values)[i] = duplicate_str(cpy);
			if(!(*values)[i]){
				display_to_stdout("duplicate_str() failed, %s:%d\n",__FILE__,__LINE__-2);
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
	size_t l = string_length(buff);
	char cpy[l+1];
	set_memory(cpy,0,l+1);
	string_copy(cpy,buff,l);

	char *delim = ":";
	char *first = 0x0;
	char *last = 0x0;
	
	char **values = (char**)ask_mem(fields_count * sizeof(char *));
	if (!values) {
		display_to_stdout("memory get values %s:%d\n",__FILE__,__LINE__-2);
		return 0x0;
	}

	if(find_needle(buff,"@")) replace('@','^',cpy);

	/*exclude file syntax*/
	char *f_st = 0x0;
	char *f_en = 0x0;
	while((f_st = find_needle(cpy,"[")) && (f_en = find_needle(cpy,"]"))){
		*f_st = '\n';
		for(; *f_st != ']'; f_st++){
			if(*f_st == ':') *f_st = '@';
		}
		*f_en = '\r';
	}

	if(find_needle(cpy,"\n")) replace('\n','[',cpy);
	if(find_needle(cpy,"\r")) replace('\r',']',cpy);
	

	int i = 0;
	char *p2 = cpy;
	char *p = cpy;
	while((first = find_needle(p2,delim)) != 0x0 && (last = find_needle(&p[(first+1) - cpy],delim)) != 0x0){
	/*	if(first[1] == '[' && ((file_block = find_needle(&first[1],"]")))){
			int start = (first + 1) - buff;
			int size = (file_block - buff) - start +1;
			char cpy[size+1];
			set_memory(cpy,0,size+1);
			string_copy(cpy,&buff[start],size);
			values[i] = duplicate_str(cpy);
			if(!values[i]){
				display_to_stdout("duplicate_str() failed, %s:%d",__FILE__,__LINE__-2);
				return 0x0;
			}
			p2 = (first + 1) + size+1;
			i++;
			continue;
		}
		*/
		int start = (first + 1) - cpy;
		int size = (last - cpy) - start;
		char bpy[size+1];
		set_memory(bpy,0,size+1);
		string_copy(bpy,&buff[start],size);

		values[i] = duplicate_str(bpy);
		if(!values[i]){
			display_to_stdout("duplicate_str() failed, %s:%d",__FILE__,__LINE__-2);
			return 0x0;
		}

		if(find_needle(values[i],"@")) replace('@',':',values[i]);
		if(find_needle(values[i],"^")) replace('^','@',values[i]);

		p2 = (first + 1) + size+1;
		i++;
	}

	if(first){
		if(*(first + 1) != '\0') {
			values[i] = duplicate_str(&first[1]);
			if(!values[i]){
				display_to_stdout("duplicate_str() failed, %s:%d",__FILE__,__LINE__-2);
				return 0x0;
			}
			if(find_needle(values[i],"@")) replace('@',':',values[i]);
			if(find_needle(values[i],"^")) replace('^','@',values[i]);
		}
	}

	return values;
}

char **get_values(char *fields_input, int fields_count)
{
	int i = 0, j = 0;

	char **values = (char**)ask_mem(fields_count * sizeof(char *));
	if (!values) {
		display_to_stdout("memory get values %s:%d",__FILE__,__LINE__-2);
		return 0x0;
	}

	char *s = 0x0;
	tok(fields_input, ":");
	tok(0x0, ":");
	s = tok(0x0,":");

	if (s){
		values[j] = duplicate_str(s);
		if (!values[j]){
			cancel_memory(0x0,values,fields_count * sizeof(char*));
			return 0x0;
		}
		i++;
	}
	else
	{
		display_to_stdout("value token not found in get_values();\n");
		cancel_memory(0x0,values,fields_count * sizeof(char*));
		return 0x0;
	}

	while ((s = tok(0x0, ":")) != 0x0)
	{
		if ((i % 3 == 0) && (fields_count - j >= 1)){
			j++;
			values[j] = duplicate_str(s);
		}
	

		if (!values[j]){
			free_strs(j,i,values);
			return 0x0;
		}
		i++;
		s = 0x0;
	}

	return values;
}

void free_strs(int fields_num, int count, ...)
{
	va_list args;
	va_start(args, count);

	int i,j;
	for (i = 0; i < count; i++){
		char **str = va_arg(args, char **);
		for (j = 0; j < fields_num; j++){
			if (str[j]){
				cancel_memory(0x0,str[j],string_length(str[j])+1);
			}
		}
		cancel_memory(0x0,str,sizeof(char*)*fields_num);
	}
}

int is_file_name_valid(char *str)
{
	size_t l = string_length(str);
	size_t i;
	for (i = 0; i < l; i++)
	{
		if (is_punct(str[i]))
			if ((str[i] != '-' && str[i] != '/' && str[i] != '_' && str[i] != '.')) return 0;

		if (is_space(str[i]))
			return 0;
	}
	return 1;
}

void strip(const char c, char *str)
{
	size_t l = string_length(str);
	char cpy[l];
	set_memory(cpy,0,l);

	size_t i,j;
	for (i = 0, j = 0; i < l; i++){
		if (str[i] == c)continue;

		cpy[j] = str[i];
		j++;
	}

	set_memory(str,0,l);
	string_copy(str,cpy,l);
}

void replace(const char c, const char with, char *str)
{
	size_t l = string_length(str);
	size_t i;
	for (i = 0; i < l; i++)
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
	size_t l = string_length(str);
	return str[l - 1];
}

size_t digits_with_decimal(float n)
{
	char buf[20];
	if (copy_to_string(buf, 20, "%.2f", n) < 0)
	{
		display_to_stdout("copy_to_string failed %s:%d.\n", F, L - 2);
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
	size_t l = string_length(str);
	int point = 0;
	size_t i;
	for (i = 0; i < l; i++)
	{
		if (is_digit(str[i]))
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
	size_t l = string_length(str);
	size_t i;
	for (i = 0; i < l; i++){
		if (!is_digit(str[i])){
			if (str[i] == '-' && i == 0){
				continue;
			}else{
				return 0;
			}
		}
	}
	return 1;
}


float __round_alt(float n)
{
	char buff[20];
	if (copy_to_string(buff, 20,"%f", n) < 0)
		return -1;

	size_t digit = 0;

	if ((digit = digits_with_decimal(n) + 2) > 20)
		return -1;

	int num = (int)((int)buff[digit] - '0');
	if (num > 5){
		n += 0.01;
	}

	return n;
}

unsigned char is_number_in_limits(char *value)
{
	if (!value)
		return 0;

	size_t l = string_length(value);
	unsigned char negative = value[0] == '-';
	int ascii_value = 0;

	if (is_integer(value)){

		size_t i;
		for (i = 0; i < l; i++)
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
		size_t i;
		for (i = 0; i < l; i++)
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
	size_t l = string_length(src);
	int last = -1;
	size_t i;
	for (i = 0; i < l; i++){
		if (src[i] == c)
			last = i;
	}

	return last;
}

int count_delim(char *delim, char *str)
{
	size_t l = string_length(str) + 1;
	char buf[l];
	set_memory(buf,0,l);
	string_copy(buf,str,l-1);

	int c = 0;
	char *d = 0x0;
		
	while((d = find_needle(buf,delim))){
		c++;
		*d = '@';	
	}

	return c;
}

int find_double_delim(char *delim, char *str, int *pos, struct Schema sch)
{

	char *c = 0x0;
	int f = 0;
	while((c = find_needle(str,delim))){
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
	if(find_needle(str, delim) == 0x0) return -1;
	size_t l = string_length(str) + 1;
	char buf[l];
	set_memory(buf,0,l);
	string_copy(buf,str,l-1);
	
	/* in the case of UTILITY usage we need to exclude the type file syntax
	 * field_name:[file data]*/
	i16 f_start[50];
	i16 f_end[50];
	set_memory(f_start,-1,sizeof(i16)*50);
	set_memory(f_end,-1,sizeof(i16)*50);

	if(__UTILITY){
		char *fs = 0x0;
		char *fe = 0x0;
		int i = 0;
		for(; (fs = find_needle(buf,"[")) && (fe = find_needle(buf,"]")); i++){
			if (i == 50){
				display_to_stdout("(%s): code refactor needed for %s() %s:%d\n",prog,__func__,__FILE__,__LINE__-10);
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
	int i;
	for(i = 0; i < sch.fields_num;i++){
		char *f = 0x0;
		if(!(f = find_needle(buf,sch.fields_name[i]))) continue;

		
		
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
					size = find_needle(f,":") - f;
				else 
					size = find_needle(&f[1],":") - f;

				if (size < 0) continue;

				char cpy[size+1];
				set_memory(cpy,0,size+1);
				string_copy(cpy,f,size);
				if(string_length(sch.fields_name[i]) == (size_t)size){
					if(string_compare(cpy,sch.fields_name[i],size,-1) == 0){
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

		while( *f != ':' && *f != '\0'){
			*f = ' '; 
			f++;
		}
		if(*f == ':') *f = ' ';
	}

	i = 0;
	if(find_needle(buf," ") == 0x0) return -1;
	char *start = 0x0;
	while((start = find_needle(buf,"TYPE_"))){
		*start = '@';
		while(*start != ':' && *start != '\0') start++;

		if(*start == ':') *start = ' ';
	}

	while((start = find_needle(buf,"t_"))){
		if(is_target_db_type(start) == -1)break;

		*start = '@';
		while(*start != ':' && *start != '\0') start++;

		if(*start == ':') *start = ' ';
	}


	/* at this point only the ':' inside the fields are left*/
	char *delim_in_fields = 0x0;
	i8 cont = 0;
	while((delim_in_fields = find_needle(buf,delim))){
		
		int p = delim_in_fields - buf;
		if(f_start[0] > -1 && f_end[0] > -1){
			int i;
			for(i = 0; f_start[i] != -1 && f_end[i] != -1; i++){
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
	int i;	
	for(i = 0; i < 200; i++){
		if (pos[i] == -1) break;
		
		char *p = &buffer[pos[i]];
		int count = 0;
		int start = 0;
		int end = 0;
		int variable_steps = 2;
		while(count < variable_steps) {
			if(p == &buffer[0]) break;
			p--;
			if (*p == ':') {
				if(count == 0) end = p - buffer;
				if(count == 1) start = p - buffer;
				if(count == 2) start = p - buffer;
				
				if(count == 1){
					char *type = 0x0;
					if((type = find_needle(p,TYPE_))){
						if(p == type){ 		
							end = p - buffer;
							variable_steps = 3;
						}
					}
				}
				count++;
			}
		}
	
		if(p == &buffer[0])
			string_copy(field, &buffer[start],end - start -1);
		else
			string_copy(field, &buffer[start+1],end - start -1);
	}

	return field;
}

static int is_target_db_type(char *target)
{
	int size = 0;
	size_t l = string_length(target);
	if(l > 4){
		/*extract the real target*/
		if(*target == ':'){
			char *e = find_needle(&target[1],":");
			if(e) size = e - target;
		}else{
			char *e = find_needle(target,":");
			if(e) size = e - target;
		}
	
	}

	if(size > 5) return -1;

	int b_size = 0;
	if(*target == ':') b_size += size + 1;
	if(*target != ':') b_size += (size + 2);


	char tg[b_size];
	set_memory(tg,0,b_size);

	if(size > 0){
		if(*target == ':') {
			string_copy(tg,target,size);
		}else{
			*tg = ':';
			string_copy(&tg[1],target,size);
		}
		if(string_length(tg) == string_length(":t_s")) if(string_compare(tg,":t_s",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_l")) if(string_compare(tg,":t_l",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_b")) if(string_compare(tg,":t_b",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_d")) if(string_compare(tg,":t_d",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_i")) if(string_compare(tg,":t_i",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_f")) if(string_compare(tg,":t_f",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_p")) if(string_compare(tg,":t_p",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_ai")) if(string_compare(tg,":t_ai",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_al")) if(string_compare(tg,":t_al",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_af")) if(string_compare(tg,":t_af",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_ad")) if(string_compare(tg,":t_ad",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_as")) if(string_compare(tg,":t_as",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_ab")) if(string_compare(tg,":t_ab",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_fl")) if(string_compare(tg,":t_fl",size,-1) == 0) return 0;
		if(string_length(tg) == string_length(":t_dt")) if(string_compare(tg,":t_dt",size,-1) == 0) return 0;

		return -1;

	}
	if(find_needle(target,":t_s")) return 0;
	if(find_needle(target,":t_l")) return 0;
	if(find_needle(target,":t_b")) return 0;
	if(find_needle(target,":t_d")) return 0;
	if(find_needle(target,":t_i")) return 0;
	if(find_needle(target,":t_f")) return 0;
	if(find_needle(target,":t_p")) return 0;
	if(find_needle(target,":t_ai")) return 0;
	if(find_needle(target,":t_al")) return 0;
	if(find_needle(target,":t_as")) return 0;
	if(find_needle(target,":t_ad")) return 0;
	if(find_needle(target,":t_af")) return 0;
	if(find_needle(target,":t_ab")) return 0;
	if(find_needle(target,":t_fl")) return 0;

	return -1;

}

void pack(ui32 n, ui8 *digits_indexes)
{
	if((n <= (BASE-1))) {
		set_memory(digits_indexes,255,sizeof(ui8) * 5);
		digits_indexes[4] = n;
		return;
	}

	int i = 4;	
	set_memory(digits_indexes,255,sizeof(ui8) * 5);

	ui32 rm = 0;
	while(n > 0){
		if (i < 0){
			set_memory(digits_indexes,255,sizeof(ui8) * 5);
			return;
		}
		rm = n % BASE;
		n /= BASE;
		digits_indexes[i] = rm;
		i--;
	}
}

void print_pack_str(ui8 *digits_index)
{
	int i;
	for(i = 0; i < 5; i++){
		if(digits_index[i] == 255) continue;
		display_to_stdout("%s",base_247[digits_index[i]]);
	}
}

/* this is good for internal unpack*/
long long unpack(ui8 *digits_index)
{
	long long unpacked = 0;
	int pow = 0;
	int i;
	for(i = 4; i >= 0; i--){
		if(digits_index[i] == 255) break;
		unpacked += (digits_index[i] * power(BASE,pow));
		pow++;
	}

	return unpacked;
}



struct tok_handler{
	char *original_tok;
	char *last;
	char delim[300];
	char result[1024];
	ui8 finish;
};

static struct tok_handler t_hndl;

void clear_tok()
{
	t_hndl.finish = 0;
	if(cancel_memory(0x0,t_hndl.original_tok,string_length(t_hndl.original_tok)+1) == -1){
		/*log the error*/
	}
	t_hndl.original_tok = 0x0;
}
char *tok(char *str, char *delim)
{

        if(t_hndl.finish){
			if(!str){
				if(cancel_memory(0x0,t_hndl.original_tok,string_length(t_hndl.original_tok)+1) == -1){
					/*log the error*/
				}
				t_hndl.original_tok = 0x0;
				t_hndl.finish = 0;
				return 0x0;
			}

		
			size_t string_size = string_length(str);	
			if(string_size != string_length(t_hndl.original_tok)){
				if(cancel_memory(0x0,t_hndl.original_tok,string_length(t_hndl.original_tok)+1) == -1){
					/*log the error*/
				}
				t_hndl.original_tok = 0x0;
				t_hndl.finish = 0;
				goto tok_process;
			}else{
				replace('\n',*delim,t_hndl.original_tok);	
				if(string_compare(str,t_hndl.original_tok,string_size,-1) == 0){
					if(cancel_memory(0x0,t_hndl.original_tok,string_length(t_hndl.original_tok)+1) == -1){
						/*log the error*/
					}
					t_hndl.original_tok = 0x0;
					t_hndl.finish = 0;
					return 0x0;
				} else {
					if(cancel_memory(0x0,t_hndl.original_tok,string_length(t_hndl.original_tok)+1) == -1){
						/*log the error*/
					}
					t_hndl.original_tok = 0x0;
					t_hndl.finish = 0;
					goto tok_process;
				}
			}
			t_hndl.finish = 0;
			if(!t_hndl.original_tok)goto tok_process;

			return 0x0;
		}

tok_process:
	if(!t_hndl.original_tok && !str) return 0x0;

	size_t len = 0;
	if(str) len = string_length(str);	

	if(!t_hndl.original_tok){
		set_memory(&t_hndl,0,sizeof(struct tok_handler));
		t_hndl.original_tok = (char*)ask_mem(len + 1);		
		set_memory(t_hndl.original_tok,0,len+1);

		if(!t_hndl.original_tok){
				display_to_stdout("ask_mem failed, %s:%d.\n",__FILE__,__LINE__-2);
				return 0x0;
		}
		
		string_copy(t_hndl.original_tok,str,len);
		string_copy(t_hndl.delim,delim,string_length(delim));
		t_hndl.last = t_hndl.original_tok;
	}
	
	if(string_compare(t_hndl.delim,delim,string_length(t_hndl.delim),-1) != 0) {
		cancel_memory(0x0,t_hndl.original_tok,string_length(t_hndl.original_tok));
		t_hndl.original_tok = 0x0;
		return 0x0;
	}

	char *found = 0x0;
	if((found = find_needle(t_hndl.original_tok,t_hndl.delim))){
		if(*(found + 1) == '\0'){
			set_memory(t_hndl.result,0,1024);
			string_copy(t_hndl.result,t_hndl.last,string_length(t_hndl.last)-1);				
			t_hndl.finish =  1;
			return &t_hndl.result[0];
		}
		ui32 end = found - t_hndl.original_tok;
		ui32 start = t_hndl.last - t_hndl.original_tok;
		set_memory(t_hndl.result,0,1024);
		string_copy(t_hndl.result,t_hndl.last,end - start);				
		*found = '\n';
		t_hndl.last += (end - start) + 1;
	} else {
		set_memory(t_hndl.result,0,1024);
		string_copy(t_hndl.result,t_hndl.last,string_length(t_hndl.last));				
		t_hndl.finish =  1;
		return &t_hndl.result[0];
	}

	return &t_hndl.result[0];
}

char *duplicate_str(char *str)
{
	char *dup = (char*)ask_mem(string_length(str)+1);
	if(!dup){
		display_to_stdout("ask_mem failed, %s:%d.\n",__FILE__,__LINE__-2);
		return 0x0;
	}

	set_memory(dup,0,string_length(str)+1);
	string_copy(dup,str,string_length(str));
	return dup;
}






