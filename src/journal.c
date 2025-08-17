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

int journal(int caller_fd, off_t offset, void *key, int key_type, int operation)
{
	int create = 0;
	int fd = open_file(JINX,0);
	if(fd == -1){
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
	 * - the file path
	 * - off_t
	 * - key   
	 * */
	
	static struct stack index;
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
		}else{
			strncpy(file_name,dir_data->d_name,fl_name_length);
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
	node.key_type = key_type;
	node.operation = operation;

	switch(key_type){
	case STR:
	{	
		size_t l = strlen((char *) key)+1;
		strncpy(node.key.s,(char *)key,l);
		break;
	}
	case UINT:
		node.key.n = (*(uint32_t *)key);
		break;
	default:
		close_file(1,fd);
		return -1;
	}


	/*push the node ont the stack*/	
	node.timestamp = time(NULL);
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
	if(index->capacity < MAX_STACK_CAP ){
		index->elements[index->capacity] = node; 
		index->capacity++;
		return 0;
	}
	
	if(index->dynamic_capacty == 0) {
		index->dynamic_elements = (struct Node_stack*)ask_mem(sizeof(struct Node_stack));
		if(!index->dynamic_elements){
			fprintf(stderr,"(%s): ask_mem failed %s:%d.\n",p,__FILE__,__LINE__-2);
			return -1;
		}

		index->dynamic_elements->timestamp = node.timestamp;
		strncpy(index->dynamic_elements->file_name,node.file_name,strlen(node.file_name));
		index->dynamic_elements->key_type = node.key_type;
		switch(node.key_type){
		case STR:
			strncpy(index->dynamic_elements->key.s,node.key.s,strlen(node.key.s));
			break;
		case UINT:
			index->dynamic_elements->key.n= node.key.n;
		default:
			break;
		}
		index->dynamic_elements->offset = node.offset;
		index->dynamic_elements->operation = node.operation;
		index->dynamic_capacty++;
		return 0;
	}
	
	struct Node_stack *nd = (struct Node_stack*) ask_mem(sizeof(struct Node_stack));
	if(!nd){
		fprintf(stderr,"(%s): ask_mem failed %s:%d.\n",p,__FILE__,__LINE__-2);
		return -1;
	}
	
	nd->timestamp = node.timestamp;
	strncpy(nd->file_name,node.file_name,strlen(node.file_name));
	nd->key_type = node.key_type;
	switch(node.key_type){
	case STR:
		strncpy(nd->key.s,node.key.s,strlen(node.key.s));
		break;
	case UINT:
		nd->key.n= node.key.n;
		break;
	default:
		break;
	}
	
	nd->offset = node.offset;
	nd->operation = node.operation;
	index->dynamic_elements->next = nd;
	index->dynamic_capacty++;
	return 0;
}

int pop_journal(struct stack *index)
{
	if(index->capacity <= MAX_STACK_CAP && index->dynamic_capacty == 0){
		memset(&index->elements[index->capacity - 1],0,sizeof(struct Node_stack));
		index->capacity--;
		return 0;
	}
	

	struct Node_stack *temp = index->dynamic_elements;
	while(temp) {

		if(!temp->next->next) {
			struct Node_stack *temp_s = temp; 
			temp_s->next = NULL;
		}

		temp = temp->next;
	}

	cancel_memory(NULL,temp,sizeof(struct Node_stack));
	index->dynamic_capacty--;
	return 0;
}

int peek_journal(struct stack *index, struct Node_stack *node)
{
	if(index->capacity < MAX_STACK_CAP){
		*node = index->elements[index->capacity-1];
		return 0;
	}
	
	struct Node_stack *temp = index->dynamic_elements;
	while(temp) {
		if(!temp->next->next) break;

		temp = temp->next;
	}

	node->timestamp = temp->timestamp;
	strncpy(node->file_name,temp->file_name,strlen(temp->file_name));
	node->key_type = temp->key_type;
	switch(node->key_type){
	case STR:
		strncpy(node->key.s,temp->key.s,strlen(temp->key.s));
		break;
	case UINT:
		node->key.n = temp->key.n;
		break;
	default:
		break;
	}

	node->offset = temp->offset;
	node->operation = temp->operation;

	return 0;
}

int is_empty(struct stack *index)
{
	return index->capacity == 0;

}

void free_stack(struct stack *index)
{
	memset(index,0,sizeof(*index));
	if(index->dynamic_capacty == 0 ) return;
	
	while(index->dynamic_capacty > 0) pop_journal(index);
}

int write_journal_index(int *fd,struct stack *index)
{
	if(index->capacity == 0) return -1;
	/*  
	 * check if capacity is bigger then MAX_STACK_CAP
	 * if true, you have to 
	 * write 400 entry to an HISTORY-index
	 * and write the most recent 100 to a new truncated (so you erase the content)
	 * journal index file,
	 * so you can keep the system efficient, and avoid to use memory allocations
	 * extensevely
	 * */

	if(index->capacity == MAX_STACK_CAP){
		int fd_hst = open_file(JHST,0);
		if(fd_hst == -1){
			fd_hst = create_file(JHST);
			if(fd_hst == -1){
				error("can't create archive file.",__LINE__-2);
				return -1;
			}
		}

		int hst_cap = 0;
		int nw_cap = 0;
		if(index->dynamic_capacty == 0) hst_cap = index->capacity -100;

		if(index->dynamic_capacty > 0){
			nw_cap = 100 + index->dynamic_capacty;
			hst_cap = index->capacity-100;
		}

		uint32_t cap_ne = swap32(hst_cap);
		if(write(fd_hst,&cap_ne,sizeof(cap_ne)) == -1) {
			error("can't write to journal index file.",__LINE__-1);
			close(fd_hst);
			return -1;
		}

		int i;
		for(i = 0; i < hst_cap;i++){
			uint64_t ts_ne = swap64((index->elements[i].timestamp));
			if(write(fd_hst,&ts_ne,sizeof(ts_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				close(fd_hst);
				return -1;
			}
			
			size_t size = strlen(index->elements[i].file_name);
			uint64_t size_ne = swap64(size);
			if(write(fd_hst,&size_ne,sizeof(size_ne)) == -1 ||
				write(fd_hst,index->elements[i].file_name,size) == -1){
				error("can't write to journal index file.",__LINE__-1);
				close(fd_hst);
				return -1;
			}

			uint32_t kt_ne = swap32(index->elements[i].key_type);
			if(write(fd_hst,&kt_ne,sizeof(kt_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				close(fd_hst);
				return -1;
			}
			switch(index->elements[i].key_type){ 
			case STR:
			{
				size_t size = strlen(index->elements[i].key.s);
				uint64_t size_ne = swap64(size);
				if(write(fd_hst,&size_ne,sizeof(size_ne)) == -1 ||
						write(fd_hst,index->elements[i].key.s,size) == -1){
					error("can't write to journal index file.",__LINE__-1);
					close(fd_hst);
					return -1;
				}

				break;
			}
			case UINT:
			{
				uint32_t k_ne = swap32(index->elements[i].key.n);
				if(write(fd_hst,&k_ne,sizeof(k_ne)) == -1){
					error("can't write to journal index file.",__LINE__-1);
					close(fd_hst);
					return -1;
				}
				break;
			}
			default:
				error("key not supported.",__LINE__);
				close(fd_hst);
				return -1;
			}

			uint64_t ot_ne = swap64((index->elements[i].offset));
			if(write(fd_hst,&ot_ne,sizeof(ot_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				close(fd_hst);
				return -1;
			}

			uint32_t op_ne = swap32(index->elements[i].operation);
			if(write(fd_hst,&op_ne,sizeof(op_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				close(fd_hst);
				return -1;
			}
		}

		close_file(2,*fd,fd_hst);
		if(nw_cap == 0)
			nw_cap = 100;

		/*open with O_TRUNC*/
		*fd = open_file(JINX,1); 
		if(file_error_handler(1,*fd) > 0) return -1;

		uint32_t nw_cap_ne = swap32(nw_cap);
		if(write(*fd,&nw_cap_ne,sizeof(nw_cap_ne)) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}
		
		if(nw_cap == 100)
			nw_cap = MAX_STACK_CAP;


		for(i = hst_cap ; i < nw_cap;i++){
			
			if(i >= MAX_STACK_CAP){
				struct Node_stack *temp = index->dynamic_elements;
				while(temp){
					uint64_t ts_ne = swap64((temp->timestamp));
					if(write(*fd,&ts_ne,sizeof(ts_ne)) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}

					size_t size = strlen(temp->file_name);
					uint64_t size_ne = swap64(size);
					if(write(*fd,&size_ne,sizeof(size_ne)) == -1 ||
							write(*fd,temp->file_name,size) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}

					uint32_t kt_ne = swap32(temp->key_type);
					if(write(*fd,&kt_ne,sizeof(kt_ne)) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}
					switch(temp->key_type){ 
						case STR:
							{
								size_t size = strlen(temp->key.s);
								uint64_t size_ne = swap64(size);
								if(write(*fd,&size_ne,sizeof(size_ne)) == -1 ||
										write(*fd,temp->key.s,size) == -1){
									error("can't write to journal index file.",__LINE__-1);
									return -1;
								}

								break;
							}
						case UINT:
							{
								uint32_t k_ne = swap32(temp->key.n);
								if(write(*fd,&k_ne,sizeof(k_ne)) == -1){
									error("can't write to journal index file.",__LINE__-1);
									return -1;
								}
								break;
							}
						default:
							error("key not supported.",__LINE__);
							return -1;
					}

					uint64_t ot_ne = swap64((temp->offset));
					if(write(*fd,&ot_ne,sizeof(ot_ne)) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}

					uint32_t op_ne = swap32(temp->operation);
					if(write(*fd,&op_ne,sizeof(op_ne)) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}

					temp = temp->next;
				}
				close(*fd);
				*fd = open_file(JINX,0);
				if(file_error_handler(1,*fd) != 0) return -1;

				return 0;
			}

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

			uint32_t kt_ne = swap32(index->elements[i].key_type);
			if(write(*fd,&kt_ne,sizeof(kt_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				return -1;
			}
			switch(index->elements[i].key_type){ 
			case STR:
			{
				size_t size = strlen(index->elements[i].key.s);
				uint64_t size_ne = swap64(size);
				if(write(*fd,&size_ne,sizeof(size_ne)) == -1 ||
						write(*fd,index->elements[i].key.s,size) == -1){
					error("can't write to journal index file.",__LINE__-1);
					return -1;
				}

				break;
			}
			case UINT:
			{
				uint32_t k_ne = swap32(index->elements[i].key.n);
				if(write(*fd,&k_ne,sizeof(k_ne)) == -1){
					error("can't write to journal index file.",__LINE__-1);
					return -1;
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
		if(file_error_handler(1,*fd) != 0) return -1;

		return 0;
	}

	/*
	 * this is what gets executed most of the time 
	 * index->camacity is smaller than MAX_STACK_CAP 
	 * */
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

		uint32_t kt_ne = swap32(index->elements[i].key_type);
		if(write(*fd,&kt_ne,sizeof(kt_ne)) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}
		switch(index->elements[i].key_type){ 
			case STR:
				{
					size_t size = strlen(index->elements[i].key.s);
					uint64_t size_ne = swap64(size);
					if(write(*fd,&size_ne,sizeof(size_ne)) == -1 ||
							write(*fd,index->elements[i].key.s,size) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}

					break;
				}
			case UINT:
				{
					uint32_t k_ne = swap32(index->elements[i].key.n);
					if(write(*fd,&k_ne,sizeof(k_ne)) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
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
	if(file_error_handler(1,*fd) != 0) return -1;

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
	if(cap == 0 || cap >= MAX_STACK_CAP) {
		return cap == 0 ? -1 : EJCAP;
	}

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

		size_t size = swap64(size_ne) + 1;
		char buff[size];
		memset(buff,0,size);

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
		
		index->elements[i].key_type = (int)swap32(kt_ne);

		switch(index->elements[i].key_type){
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

			strncpy(index->elements[i].key.s ,buff,size);
			break;
		}
		case UINT:
		{
			uint32_t k_ne = 0;
			if(read(fd,&k_ne,sizeof(k_ne)) == -1){
				error("read journal index failed",__LINE__-1);
				return -1;
			}
			
			index->elements[i].key.n = (uint32_t)swap32(k_ne);
			
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
		
		index->elements[i].offset = (off_t) swap64(os_ne);
		uint32_t op_ne = 0;
		if(read(fd,&op_ne,sizeof(op_ne)) == -1){
			error("read journal index failed",__LINE__-1);
			return -1;
		}
		
		index->elements[i].operation = (int) swap32(op_ne);
		index->capacity++;
	}

	return 0;
}
static void error(char *msg,int line)
{
	fprintf(stderr,"(%s): %s, %s:%d.\n",p,msg,__FILE__,line);
}
