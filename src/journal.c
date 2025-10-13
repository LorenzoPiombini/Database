#include <stdio.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include "hash_tbl.h"
#include "journal.h"
#include "file.h"
#include "memory.h"
#include "endian.h"
#include "str_op.h"
#include "types.h"

static char p[] ="db";

static void error(char *msg,int line);

int journal(int caller_fd, file_offset offset, void *key, int key_type, int operation)
{
	int create = 0;
	int fd = open_file(JINX,0);
	if(fd == ENOENT){
		create = 1;
		fd = create_file(JINX);
		if(fd == -1){
			fprintf(stderr,"(%s): can't create or open '%s'.\n",p,JINX);
			return -1;
		}
	}

	/*
	 * each journal record will store : 
	 * - timestamp
	 * - operation
	 * - the file name
	 * - file_offset
	 * - key   
	 * */
	
	struct stack index;
	struct Node_stack  node;
	memset(&node,0,sizeof(struct Node_stack));
	memset(&index,0,sizeof(struct stack));

	if(!create){
		if (read_journal_index(fd, &index) == -1) {
			fprintf(stderr,"(%s): read index from '%s' failed, %s:%d",
					p,JINX,__FILE__,__LINE__-1);
			close(fd);
			return -1;
		}
	}
	
	/*get the file name from the caller file descriptor */
	char path[1024];
	memset(path,0,1024);
	char db_dir[1024];
	memset(db_dir,0,1024);

	
	if(copy_to_string(path,1024,PROC_PATH, caller_fd) < 0){
		error("copy_to_string failed.",__LINE__ - 1);
		close(fd);
		return -1;
	}

	struct stat st;
	if(stat(path,&st) == -1){
		error("can't get file info.",__LINE__ -1);
		close(fd);
		return -1;
	}

	char cwd[1024];
	memset(cwd,0,1024);
	if(getcwd(cwd,1024) == NULL){
		error("can't get current directory.",__LINE__ -1);
		close(fd);
		return -1;
	}

	size_t cwd_length = strlen(cwd);
	char *dynamic_db_path = NULL; 
	if(cwd_length + strlen("/db") >= 1024){
		dynamic_db_path = (char*)ask_mem(cwd_length+strlen("/db")+1);
		if(!dynamic_db_path){
			error("ask_mem() failed.",__LINE__ -1);
			close(fd);
			return -1;
		}
		memset(dynamic_db_path,0,cwd_length + strlen("/db")+1);
	}else{
		strncpy(db_dir,cwd,cwd_length);
		strncat(db_dir,"/db",strlen("/db")+1);
	}
	
	
	DIR *database_dir = NULL;
	if(dynamic_db_path){
		if((database_dir = opendir(dynamic_db_path)) == NULL){
			error("can't get current directory.",__LINE__ -1);
			close(fd);
			cancel_memory(NULL,dynamic_db_path,cwd_length+strlen("/db")+1);
			return -1;
		}
	}else{
		if((database_dir = opendir(db_dir)) == NULL){
			error("can't get current directory.",__LINE__ -1);
			close(fd);
			return -1;
		}
	}

	struct dirent *dir_data = NULL;
	char *dynamic_file_name = NULL;
	char file_name[MAX_FILE_NAME];
	memset(file_name,0,MAX_FILE_NAME);

	errno = 0;
	while((dir_data = readdir(database_dir))){
		if(dir_data->d_ino != st.st_ino) continue;

		size_t fl_name_length = strlen(dir_data->d_name);
		if(fl_name_length > 1024){
			dynamic_file_name = (char*)ask_mem(fl_name_length + 1);
			if(!dynamic_file_name){
				error("ask_mem() failed.\n",__LINE__ -1);
				close(fd);
				closedir(database_dir);
				if(dynamic_db_path)
					cancel_memory(NULL,dynamic_db_path,cwd_length+strlen("/db")+1);

				return -1;
			}
			memset(dynamic_file_name,0,fl_name_length+1);
			break;
		}else{
			strncpy(file_name,dir_data->d_name,fl_name_length);
			break;
		}
	}
	
	closedir(database_dir);
	if(dynamic_db_path) cancel_memory(NULL,dynamic_db_path,cwd_length+strlen("/db")+1);

	if(dir_data == NULL && errno == 0){
		error("file name not found.\n",__LINE__ -1);
		close(fd);
		if(dynamic_file_name) cancel_memory(NULL,dynamic_file_name,strlen(dynamic_file_name)+1);
		return -1;
	}	

	if(dynamic_file_name){
		if(strlen(dynamic_file_name) > MAX_FILE_NAME){
			error("code refactor needed for journal operation.",__LINE__);
			close(fd);
			cancel_memory(NULL,dynamic_file_name,strlen(dynamic_file_name));
			return -1;
		}
		strncpy(node.file_name,dynamic_file_name,strlen(dynamic_file_name));
		cancel_memory(NULL,dynamic_file_name,strlen(dynamic_file_name));
	}else{
		strncpy(node.file_name,file_name,strlen(file_name));
	}
	node.offset = offset;
	node.key.type = key_type;
	node.operation = operation;

	switch(key_type){
	case STR:
	{	
		size_t l = strlen((char *) key)+1;
		node.key.k.s = (char *)ask_mem(l);
		if(!node.key.k.s){
			fprintf(stderr,"ask_mem() failed, %s:%d.\n",__FILE__,__LINE__-1);
			return -1;
			
		}
		strncpy(node.key.k.s,(char *)key,l);
		break;
	}
	case UINT:
	{
		if(*(uint32_t *)key < USHRT_MAX){
			node.key.k.n16 = (*(uint32_t *)key);
			node.key.size = 16;
		}else{
			node.key.k.n = (*(uint32_t *)key);
			node.key.size = 32;
		}
		break;
	}
	default:
		close_file(1,fd);
		return -1;
	}


	node.timestamp = time(NULL);
	/*push the node on the stack*/	
	if(push_journal(&index,node) == -1){
		error("failed to push on journal stack",__LINE__-1);
		close(fd);
		return -1;
	}

	/* write the index file */
	if (write_journal_index(&fd, &index) == -1) {
		printf("write to file failed, %s:%d",__FILE__,__LINE__ - 2);
		close(fd);
		return -1;
	}
	
	close(fd);
	return 0;
}

 
int push_journal(struct stack *index, struct Node_stack node)
{
	if(index->capacity == 0){
		index->elements = (struct Node_stack*)ask_mem(sizeof(struct Node_stack));
		if(!index->elements){
			fprintf(stderr,"(%s): ask_mem failed %s:%d.\n",p,__FILE__,__LINE__-2);
			return -1;
		}

		memcpy(index->elements,&node,sizeof(struct Node_stack));

		index->capacity++;
		return 0;
	
	}

	size_t new_size = index->capacity + 1;
	struct Node_stack *new_elements= (struct Node_stack*) reask_mem(index->elements,
							index->capacity*sizeof(struct Node_stack),
							new_size * sizeof(struct Node_stack));
	if(!new_elements){
		fprintf(stderr,"(%s): reask_mem failed %s:%d.\n",p,__FILE__,__LINE__-2);
		return -1;
	}
	
	index->elements = new_elements;
	index->capacity = new_size;

	memcpy(&index->elements[new_size-1],&node,sizeof(struct Node_stack));
	return 0;
}

int pop_journal(struct stack *index)
{
	if(index->capacity == 0) return -1;

	size_t new_size = index->capacity -1;
	struct Node_stack *new_elements = (struct Node_stack*)reask_mem(index->elements,
						index->capacity * sizeof(struct Node_stack),
						new_size * sizeof(struct Node_stack));
	if(!new_elements){
		fprintf(stderr,"(%s): reask_mem failed %s:%d.\n",p,__FILE__,__LINE__-2);
		return -1;
	}

	index->elements = new_elements;
	index->capacity = new_size;
	return 0;
}

int peek_journal(struct stack *index, struct Node_stack *node)
{
	if(index->capacity == 0) return -1;

	memcpy(node,&index->elements[index->capacity-1],sizeof(struct Node_stack));

	return 0;
}

int is_empty(struct stack *index)
{
	return index->capacity == 0;

}

void free_stack(struct stack *index)
{
	cancel_memory(NULL,index->elements,index->capacity*sizeof(struct Node_stack));
}

int write_journal_index(int *fd,struct stack *index)
{
	if(index->capacity == 0) return -1;


	close(*fd);
	*fd = open_file(JINX,1);
	if(file_error_handler(1,*fd) != 0) return 1;

	uint32_t cap_ne = swap32(index->capacity);
	if(write(*fd,&cap_ne,sizeof(cap_ne)) == -1){
		fprintf(stderr,"(%s): can't write journal index file.",p);
		return -1;
	}

	
	int i;
	for(i = 0; i < index->capacity; i++){
		uint64_t ts_ne = swap64((index->elements[i].timestamp));
		if(write(*fd,&ts_ne,sizeof(ts_ne)) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}
		size_t size = strlen(index->elements[i].file_name);
		uint64_t size_ne = swap64(size);
		if(write(*fd,&size_ne,sizeof(size_ne)) == -1 ||
				write(*fd,index->elements[i].file_name,size) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}

		switch(index->elements[i].key.type){ 
		case STR:
		{
			uint32_t type = swap32(index->elements[i].key.type);
			uint64_t key_l = swap64(strlen(index->elements[i].key.k.s));

			if (write(*fd, &type, sizeof(type)) == -1 ||
					write(*fd, &key_l, sizeof(key_l)) == -1 ||
					write(*fd, index->elements[i].key.k.s, strlen(index->elements[i].key.k.s) + 1) == -1 ){
				perror("write index:");
				return 0; 
			}
			break;

		}
		case UINT:
		{
			uint32_t type = swap32(index->elements[i].key.type);
			uint8_t size = (uint8_t)index->elements[i].key.size;

			uint32_t k = 0;
			uint16_t k16 = 0;
			if(index->elements[i].key.size == 32)
				k = swap32(index->elements[i].key.k.n);
			else
				k16 = swap16(index->elements[i].key.k.n16);

			if (write(*fd, &type, sizeof(type)) == -1 ||
					write(*fd, &size, sizeof(size)) == -1) {
				perror("write index:");
				return 0; 
			}

			if(index->elements[i].key.size == 16){
				if (write(*fd, &k16, sizeof(k16)) == -1) {
					perror("write index:");
					return 0; /*false*/
				}
			}else{
				if (write(*fd, &k, sizeof(k)) == -1) {
					perror("write index:");
					return 0; /*false*/
				}
			}

			break;
		}
		default:
			error("key not supported.",__LINE__);
			return -1;
		}

		uint64_t ot_ne = swap64((index->elements[i].offset));
		if(write(*fd,&ot_ne,sizeof(ot_ne)) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}

		uint32_t op_ne = swap32(index->elements[i].operation);
		if(write(*fd,&op_ne,sizeof(op_ne)) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}

	}

	close(*fd);
	*fd = open_file(JINX,0);
	if(file_error_handler(1,*fd) != 0 ) return -1;

	return 0;
}

int read_journal_index(int fd,struct stack *index)
{
	uint32_t cap_ne = 0;
	if(read(fd,&cap_ne,sizeof(cap_ne)) == -1){
		fprintf(stderr,"(%s): can't write journal index file.",p);
		return -1;
	}


	int cap = (int)swap32(cap_ne);

	index->elements = (struct Node_stack*)ask_mem(cap *sizeof(struct Node_stack));
	if(!index->elements){
		error("ask_mem() failed.",__LINE__-2);
		return 0;
	}
	index->capacity = cap;

	int i;
	for(i = 0; i < cap; i++){
		
		uint64_t ts_ne = 0;
		if(read(fd,&ts_ne,sizeof(ts_ne)) == -1){
			error("read journal index failed",__LINE__-1);
			return -1;
		}
		
		index->elements[i].timestamp = (time_t) swap64(ts_ne);
		
		uint64_t size_ne = 0;
		if(read(fd,&size_ne,sizeof(size_ne)) == -1){
			error("read journal index failed",__LINE__-1);
			return -1;
		}

		size_t size = swap64(size_ne);
		char buff[size + 1];
		memset(buff,0,size + 1);

		if(read(fd,buff,size) == -1){
			error("read journal index failed",__LINE__-1);
			return -1;
		}

		strncpy(index->elements[i].file_name,buff,size);

		uint32_t kt_ne = 0;
		if(read(fd,&kt_ne,sizeof(kt_ne)) == -1){
			error("read journal index failed",__LINE__-1);
			return -1;
		}
		
		index->elements[i].key.type = (int)swap32(kt_ne);

		switch(index->elements[i].key.type){
		case STR:
		{
			uint64_t size_ne = 0;
			if(read(fd,&size_ne,sizeof(size_ne)) == -1){
				error("read journal index failed",__LINE__-1);
				return -1;
			}

			size_t size = swap64(size_ne) + 1;
			char buff[size];
			memset(buff,0,size);

			if(read(fd,buff,size) == -1){
				error("read journal index failed",__LINE__-1);
				return -1;
			}

			index->elements[i].key.k.s = (char *)ask_mem(size);
			if(!index->elements[i].key.k.s){
				error("ask_mem() failed",__LINE__-1);
				return -1;
			}

			strncpy(index->elements[i].key.k.s ,buff,size);
			break;
		}
		case UINT:
		{
			uint8_t k_size = 0;
			if(read(fd,&k_size,sizeof(k_size)) == -1){
				error("read journal index failed",__LINE__-1);
				return -1;
			}

			if(k_size == 16){
				uint16_t k_ne = 0;
				if(read(fd,&k_ne,sizeof(k_ne)) == -1){
					error("read journal index failed",__LINE__-1);
					return -1;
				}

				index->elements[i].key.k.n16 = swap16(k_ne);
			}else{
				uint32_t k_ne = 0;
				if(read(fd,&k_ne,sizeof(k_ne)) == -1){


					error("read journal index failed",__LINE__-1);
					return -1;
				}

				index->elements[i].key.k.n = (uint32_t)swap32(k_ne);
			}
			break;
		}
		default:
			break;
		}

		uint64_t os_ne = 0;
		if(read(fd,&os_ne,sizeof(os_ne)) == -1){
			error("read journal index failed",__LINE__-1);
			return -1;
		}
		
		index->elements[i].offset = (file_offset) swap64(os_ne);
		uint32_t op_ne = 0;
		if(read(fd,&op_ne,sizeof(op_ne)) == -1){
			error("read journal index failed",__LINE__-1);
			return -1;
		}
		
		index->elements[i].operation = (int) swap32(op_ne);
	}

	return 0;
}


int show_journal()
{
	struct stack index;
	memset(&index,0,sizeof(struct stack));

	int fd = open_file(JINX,0);
	if(fd == -1 || fd == ENOENT){
		error("cannot open the journal file.\n",__LINE__-2);
		return -1;
	}

	if(read_journal_index(fd,&index)){
		error("cannot read journal file",__LINE__-1);	
		close(fd);
		return -1;
	}			
	
	int i;		
	for(i = 0;i < index.capacity; i++){
		char *date = ctime(&index.elements[i].timestamp);	
		char dt[strlen(date)+1];
		memset(dt,0,strlen(date)+1);
		strncpy(dt,date,strlen(date)-1);
		printf("%s, ",dt);
		printf("%s, record key: ",index.elements[i].file_name);
		switch(index.elements[i].key.type){
		case STR:
			printf("%s, offset: ",index.elements[i].key.k.s);
			break;
		case UINT:
			if(index.elements[i].key.size == 16)
				printf("%d, offset: ",index.elements[i].key.k.n16);	
			else
				printf("%d, offset: ",index.elements[i].key.k.n);	
			break;
		default:
			break;
		}
		printf("%ld, operation: ",index.elements[i].offset);
		switch(index.elements[i].operation){
		case J_DEL:
			printf("DEL.\n");
		default:
			break;
		}
	}


	free_stack(&index);
	close(fd);
	return 0;
}
static void error(char *msg,int line)
{
	fprintf(stderr,"(%s): %s, %s:%d.\n",p,msg,__FILE__,line);
}
