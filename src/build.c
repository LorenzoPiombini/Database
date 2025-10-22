#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include "build.h"
#include "file.h"
#include "lock.h"
#include "helper.h"
#include "str_op.h"
#include "debug.h"
#include "crud.h"
#include "common.h"
#include "globals.h"
#include "memory.h"
#include "freestand.h"

static char prog[] = "db";
int get_number_value_from_txt_file(FILE *fp)
{

	char buffer[250];
	ui16 i = 0;
	ui8 is_num = 1;
	if (fgets(buffer, sizeof(buffer), fp))
	{
		buffer[strcspn(buffer, "\n")] = '\0';

		for (i = 0; buffer[i] != '\0'; i++)
		{
			if (!isdigit(buffer[i]))
			{
				is_num = 0;
				break;
			}
		}
	}

	if (!is_num)
	{
		printf("first line is not a number, %s:%d.\n", __FILE__, __LINE__ - 1);
		return 0;
	}

	errno = 0;
	long l = string_to_long(buffer);
	if(errno == EINVAL) 
		return 0;
	else 
		return l;
}

unsigned char create_system_from_txt_file(char *txt_f)
{
	FILE *fp = fopen(txt_f, "r");
	if (!fp)
	{
		printf("failed to open %s. %s:%d.", txt_f, F, L - 3);
		return 0;
	}
	int lines = 0;
	int i = 0;
	int size = return_bigger_buffer(fp, &lines);
	char buffer[size];
	char **files_n = (char**)ask_mem(lines * sizeof(char *));
	char **schemas = (char**)ask_mem(lines * sizeof(char *));
	if(!files_n || !schemas){
		fprintf(stderr,"ask_mem failed, %s:%d.\n",__FILE__,__LINE__-2);
		if(files_n)cancel_memory(NULL,files_n,sizeof(char*)*lines);
		if(schemas)cancel_memory(NULL,schemas,sizeof(char*)*lines);
		return 0;
	}

	int buckets[lines];
	int indexes[lines];
	int file_field[lines];
	memset(buckets,0,lines);
	memset(indexes,0,lines);
	memset(file_field,0,lines);


	while (fgets(buffer, sizeof(buffer), fp))
	{
		buffer[strcspn(buffer, "\n")] = '\0';
		if (buffer[0] == '\0')
			continue;

		char *t = tok(buffer, "|"); 
		if(t){ 
			files_n[i] = duplicate_str(t);
			if(!files_n[i]){
				free_strs(lines, 2, files_n, schemas);
				fprintf(stderr,"wrong file or syntax error\n");
				return 0;
			}
		}else{
			free_strs(lines, 2, files_n, schemas);
			fprintf(stderr,"wrong file or syntax error\n");
			return 0;
		}
			
		
		t = tok(NULL, "|");
		if(t){
			schemas[i] = duplicate_str(t);
			if(!schemas[i]){
				free_strs(lines, 2, files_n, schemas);
				fprintf(stderr,"wrong file or syntax error\n");
				return 0;
			}
		}else{
			free_strs(lines, 2, files_n, schemas);
			fprintf(stderr,"wrong file or syntax error\n");
			return 0;
		}

		t = tok(NULL,"|");
		errno = 0;
		long l = string_to_long(t);
		if(errno == EINVAL)
			buckets[i] = (int) l; 
		else 
			buckets[i] = 0;


		t = tok(NULL, "|");

		errno = 0;
		l = string_to_long(t);
		if(errno == EINVAL)
			indexes[i] = (int) l; 
		else 
			indexes[i] = 0; 

		t = tok(NULL,"|");

		errno = 0;
		l = string_to_long(t);
		if(errno == EINVAL)
			file_field[i] = 0; 
		else 
			file_field[i] = (int) l; 

		i++;
	}

	fclose(fp);

	/* create file */
	int j = 0;
	for (j = 0; j < i; j++)
	{
		char files[3][MAX_FILE_PATH_LENGTH] = {0};
		three_file_path(files_n[j],files);
		int fd_schema = -1;
		int fd_index = -1;
		int fd_data = -1;

		if(file_field[j]){
			fd_schema = create_file(files[2]);

			if(file_error_handler(1,fd_schema) !=0){
				fprintf(stderr, "system already exist!\n");
				free_strs(lines, 2, files_n, schemas);
				return 0;
			}
		}else{
			fd_index = create_file(files[0]);
			fd_data = create_file(files[1]);
			fd_schema = create_file(files[2]);
			if (file_error_handler(3,fd_schema,fd_data,fd_index) !=0){
				fprintf(stderr, "system already exist!\n");
				free_strs(lines, 2, files_n, schemas);
				return 0;
			}
		}


		if (create_file_with_schema(fd_schema, fd_index, schemas[j], buckets[j], indexes[j],file_field[j]) == -1){
			if(file_field[j]){
				delete_file(1,files[2]);
				close_file(1,fd_schema);
			}else{
				delete_file(3, files[0], files[1],files[2]);
				close_file(3, fd_data, fd_index,fd_schema);
			}
			free_strs(lines, 2, files_n, schemas);
			return 0;
		}

		if(file_field[j])
			close_file(1,fd_schema);
		else
			close_file(3, fd_data, fd_index,fd_schema);
	}

	free_strs(lines, 2, files_n, schemas);
	return 1;
}

int return_bigger_buffer(FILE *fp, int *lines)
{
	char buffer[5000];
	int max = 0;
	while (fgets(buffer, sizeof(buffer), fp) != NULL)
	{
		(*lines)++;
		int temp = 0;
		if ((temp = (strcspn(buffer, "\n") + 1)) > max)
			max = temp;
	}
	rewind(fp);
	return max + 10;
}

/* 
 * import_data_to_system 
 * is to be use to import old EZgen systems
 * */
int import_data_to_system(char *data_file)
{
	FILE *fp = fopen(data_file,"r"); 
	if(!fp){
		fprintf(stderr,"cannot open %s.\n",data_file);
		return -1;
	}
	
	if(fseek(fp,0,SEEK_END) == -1) return -1;
	
	file_offset size = ftell(fp);
	rewind(fp);	

	char *content = (char*)ask_mem((size+1)*sizeof(char));
	if(!content){
		fprintf(stderr,"ask_mem failed, %s:%d.\n",__FILE__,__LINE__-2);
		return -1;
	}

	memset(content,0,size+1);
	if(fread(content,size,1,fp) != 1){
		fprintf(stderr,"fread() failed, %s:%d.\n",F,L-1);
		cancel_memory(NULL,content,sizeof(char)*(size+1));
		fclose(fp);
		return -1;
	}

	fclose(fp);

	/*  global variable that change the behavior of the inner
	 *  working of the check_data functions
	 *  it affects how the string functions detect types */
	__IMPORT_EZ = 1;

	int fds[3];
	memset(fds,-1,sizeof(int)*3);
	char files[3][MAX_FILE_PATH_LENGTH] = {0};  
	/* init the Schema structure*/
	struct Schema sch;
	memset(&sch,0,sizeof(struct Schema));
	struct Record_f rec;
	memset(&rec,-1,sizeof(struct Record_f));
	struct Header_d hd = {0, 0, &sch};


	char *delim = NULL;
	file_offset start = 0;
	char file_name[MAX_FILE_PATH_LENGTH] = {0};
	int lock_f = 0;
	while((delim = strstr(&content[start],"\n"))){
		size_t l = 0;
		file_offset end = delim - content;		
		if(start != 0) 
			l = end - start + 1; 
		else
			l = end + 1;

		char buf[l];				
		memset(buf,0,l);
		strncpy(buf,&content[start],l-1);
		/* set the new start */
		start = (delim +1) - content;
	        *delim = ' ';	
		

		if(buf[0] == '@'){
			memset(file_name,0,MAX_FILE_PATH_LENGTH);
			if(open_files(&buf[1],fds,files,0) == -1){
				cancel_memory(NULL,content,sizeof(char)*(size+1));
				return STATUS_ERROR;
			}
			if(is_db_file(&hd,fds) == -1){
				cancel_memory(NULL,content,sizeof(char)*(size+1));
				return STATUS_ERROR;
			}
			strncpy(file_name,&buf[1],strlen(&buf[1]));
			printf("importing '%s' ...\n",file_name);
			continue;
		}

		if(buf[0] == '='){
			int r = 0;
			while(is_locked(3,fds[0],fds[1],fds[2]) == LOCKED);
			while((r = lock(fds[0],WLOCK)) == WTLK);
			if(r == -1){
				fprintf(stderr,"can't acquire or release proper lock.\n");
				close_file(3,fds[0],fds[1],fds[2]);
				if(g_ht) free_ht_array(g_ht,g_index);
				close_ram_file(&ram);
				cancel_memory(NULL,content,sizeof(char)*(size+1));
				return STATUS_ERROR;
			}

			lock_f = 1;

			if(write(fds[1],ram.mem,ram.size) == -1){
				close_file(3,fds[0],fds[1],fds[2]);
				if(g_ht) free_ht_array(g_ht,g_index);
				close_ram_file(&ram);
				cancel_memory(NULL,content,sizeof(char)*(size+1));
				return STATUS_ERROR;
			}

			if(write_index(fds,g_index,g_ht,files[0]) == -1){
				close_file(3,fds[0],fds[1],fds[2]);
				close_ram_file(&ram);
				cancel_memory(NULL,content,sizeof(char)*(size+1));
				return STATUS_ERROR;
			}

			g_index = 0;
			g_ht = NULL;
			memset(&sch,0,sizeof(struct Schema));
			clear_ram_file(&ram);
			if(lock_f) {
				while(lock(fds[0],UNLOCK) == WTLK);
				lock_f = 0;
			}
			close_file(3,fds[0],fds[1],fds[2]);
			continue;
		}

		if(buf[0] == ' ' || buf[0] == '\0' || buf[0] == '\n' || buf[0] == '#') continue;

		/*write to file*/
		char *d = strstr(buf,":{@");
		if(!d){
			fprintf(stderr,"(%s): delim ':{@' not found, import of '%s' aborted.\n",prog,file_name);
			close_file(3,fds[0],fds[1],fds[2]);
			cancel_memory(NULL,content,sizeof(char)*(size+1));
			close_ram_file(&ram);
			return -1;
		}

		/* separating the data from the key*/		
		char *dd = d;
		d += 3;
		*dd = '\0';
		size_t sz = strlen(buf);
		char cpy[sz+1];
		memset(cpy,0,sz+1);
		strncpy(cpy,buf,sz);

		
		size_t key_sz = strlen(d) + 1;
		char key[key_sz];
		memset(key,0,key_sz);
		strncpy(key,d,key_sz -1);
		
		printf("key: '%s' - '%s'\n",key,file_name);
		/*check data (schema) and writing to file*/
		if(check_data(file_name,cpy,fds,files,&rec,&hd,&lock_f,-1) == -1) {
			printf("key value: %s\n",key);
			free_record(&rec,rec.fields_num);
			cancel_memory(NULL,content,sizeof(char)*(size+1));
			close_ram_file(&ram);
			if(lock_f) while(lock(fds[0],UNLOCK) == WTLK);
			close_file(3,fds[0],fds[1],fds[2]);
			return STATUS_ERROR;
		}

		int key_type = STR;
		if(is_integer(key)) key_type = -1;

		if(write_record(fds,(void*)key,key_type,&rec, 0,files,&lock_f,IMPORT) == -1) {
			free_record(&rec,rec.fields_num);
			memset(&rec,0,sizeof(struct Record_f));
			continue;
		}
	
		free_record(&rec,rec.fields_num);
		memset(&rec,0,sizeof(struct Record_f));
	}
	cancel_memory(NULL,content,sizeof(char)*(size+1));
	close_ram_file(&ram);
	__IMPORT_EZ = 0;
	if(lock_f) while(lock(fds[0],UNLOCK) == WTLK);
	return 0;
}
