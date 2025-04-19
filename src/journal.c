#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <byteswap.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include "hash_tbl.h"
#include "journal.h"
#include "file.h"

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
	
	static struct stack index = {0};
	struct Node_stack  node = {0};
	if(!create){
		if (read_journal_index(fd, &index) == -1) {
			fprintf(stderr,"(%s): read index from '%s' failed, %s:%d",
					p,JINX,__FILE__,__LINE__-1);
			close(fd);
			return -1;
		}
	}
	
	/*get the file name from the caller file descriptor */
	ssize_t buf_size = 0;
	char path[1024] = {0};
	char file_name[1024] = {0};
	
	if(snprintf(path,1024,PROC_PATH, caller_fd) < 0){
		error("snpritnf() failed",__LINE__ - 1);
		close(fd);
		return -1;
	}

	if((buf_size = readlink(path,file_name,1024-1)) == -1){
		error("cannot get full path.",__LINE__-1);
		close(fd);
		return -1;
	}
	
	if(buf_size == 1024){
		error("file name is not completed",__LINE__-7);
		close(fd);
		return -1;
	}
	
	strncpy(node.file_name,file_name,strlen(file_name));
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
	if(push(&index,node) == -1){
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

 
int push(struct stack *index, struct Node_stack node)
{
	if(index->capacity < MAX_STACK_CAP ){
		index->elements[index->capacity] = node; 
		index->capacity++;
		return 0;
	}
	
	if(index->dynamic_capacty == 0) {
		index->dynamic_elements = calloc(1,sizeof(struct Node_stack));
		if(!index->dynamic_elements){
			fprintf(stderr,"(%s): calloc failed.\n",p);
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
	
	struct Node_stack *nd = calloc(1,sizeof(struct Node_stack));
	if(!nd){
		fprintf(stderr,"(%s): calloc failed.\n",p);
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

int pop(struct stack *index)
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

	free(temp);
	index->dynamic_capacty--;
	return 0;
}

int peek(struct stack *index, struct Node_stack *node)
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
	
	while(index->dynamic_capacty > 0) pop(index);
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

		uint32_t cap_ne = htonl(hst_cap);
		if(write(fd_hst,&cap_ne,sizeof(cap_ne)) == -1) {
			error("can't write to journal index file.",__LINE__-1);
			close(fd_hst);
			return -1;
		}

		for(int i = 0; i < hst_cap;i++){
			uint64_t ts_ne = bswap_64((index->elements[i].timestamp));
			if(write(fd_hst,&ts_ne,sizeof(ts_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				close(fd_hst);
				return -1;
			}
			
			size_t size = strlen(index->elements[i].file_name);
			uint64_t size_ne = bswap_64(size);
			if(write(fd_hst,&size_ne,sizeof(size_ne)) == -1 ||
				write(fd_hst,index->elements[i].file_name,size) == -1){
				error("can't write to journal index file.",__LINE__-1);
				close(fd_hst);
				return -1;
			}

			uint32_t kt_ne = htonl(index->elements[i].key_type);
			if(write(fd_hst,&kt_ne,sizeof(kt_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				close(fd_hst);
				return -1;
			}
			switch(index->elements[i].key_type){ 
			case STR:
			{
				size_t size = strlen(index->elements[i].key.s);
				uint64_t size_ne = bswap_64(size);
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
				uint32_t k_ne = htonl(index->elements[i].key.n);
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

			uint64_t ot_ne = bswap_64((index->elements[i].offset));
			if(write(fd_hst,&ot_ne,sizeof(ot_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				close(fd_hst);
				return -1;
			}

			uint32_t op_ne = htonl(index->elements[i].operation);
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

		uint32_t nw_cap_ne = htonl(nw_cap);
		if(write(*fd,&nw_cap_ne,sizeof(nw_cap_ne)) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}
		
		if(nw_cap == 100)
			nw_cap = MAX_STACK_CAP;

		for(int i = hst_cap ; i < nw_cap;i++){
			
			if(i >= MAX_STACK_CAP){
				struct Node_stack *temp = index->dynamic_elements;
				while(temp){
					uint64_t ts_ne = bswap_64((temp->timestamp));
					if(write(*fd,&ts_ne,sizeof(ts_ne)) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}

					size_t size = strlen(temp->file_name);
					uint64_t size_ne = bswap_64(size);
					if(write(*fd,&size_ne,sizeof(size_ne)) == -1 ||
							write(*fd,temp->file_name,size) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}

					uint32_t kt_ne = htonl(temp->key_type);
					if(write(*fd,&kt_ne,sizeof(kt_ne)) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}
					switch(temp->key_type){ 
						case STR:
							{
								size_t size = strlen(temp->key.s);
								uint64_t size_ne = bswap_64(size);
								if(write(*fd,&size_ne,sizeof(size_ne)) == -1 ||
										write(*fd,temp->key.s,size) == -1){
									error("can't write to journal index file.",__LINE__-1);
									return -1;
								}

								break;
							}
						case UINT:
							{
								uint32_t k_ne = htonl(temp->key.n);
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

					uint64_t ot_ne = bswap_64((temp->offset));
					if(write(*fd,&ot_ne,sizeof(ot_ne)) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}

					uint32_t op_ne = htonl(temp->operation);
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

			uint64_t ts_ne = bswap_64((index->elements[i].timestamp));
			if(write(*fd,&ts_ne,sizeof(ts_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				return -1;
			}
				
			size_t size = strlen(index->elements[i].file_name);
			uint64_t size_ne = bswap_64(size);
			if(write(*fd,&size_ne,sizeof(size_ne)) == -1 ||
				write(*fd,index->elements[i].file_name,size) == -1){
				error("can't write to journal index file.",__LINE__-1);
				return -1;
			}

			uint32_t kt_ne = htonl(index->elements[i].key_type);
			if(write(*fd,&kt_ne,sizeof(kt_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				return -1;
			}
			switch(index->elements[i].key_type){ 
			case STR:
			{
				size_t size = strlen(index->elements[i].key.s);
				uint64_t size_ne = bswap_64(size);
				if(write(*fd,&size_ne,sizeof(size_ne)) == -1 ||
						write(*fd,index->elements[i].key.s,size) == -1){
					error("can't write to journal index file.",__LINE__-1);
					return -1;
				}

				break;
			}
			case UINT:
			{
				uint32_t k_ne = htonl(index->elements[i].key.n);
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

			uint64_t ot_ne = bswap_64((index->elements[i].offset));
			if(write(*fd,&ot_ne,sizeof(ot_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				return -1;
			}

			uint32_t op_ne = htonl(index->elements[i].operation);
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
	uint32_t cap_ne = htonl(index->capacity);
	if(write(*fd,&cap_ne,sizeof(cap_ne)) == -1){
		fprintf(stderr,"(%s): can't write journal index file.",p);
		return -1;
	}

	
	for(int i = 0; i < index->capacity; i++){
		uint64_t ts_ne = bswap_64((index->elements[i].timestamp));
		if(write(*fd,&ts_ne,sizeof(ts_ne)) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}
		size_t size = strlen(index->elements[i].file_name);
		uint64_t size_ne = bswap_64(size);
		if(write(*fd,&size_ne,sizeof(size_ne)) == -1 ||
				write(*fd,index->elements[i].file_name,size) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}

		uint32_t kt_ne = htonl(index->elements[i].key_type);
		if(write(*fd,&kt_ne,sizeof(kt_ne)) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}
		switch(index->elements[i].key_type){ 
			case STR:
				{
					size_t size = strlen(index->elements[i].key.s);
					uint64_t size_ne = bswap_64(size);
					if(write(*fd,&size_ne,sizeof(size_ne)) == -1 ||
							write(*fd,index->elements[i].key.s,size) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}

					break;
				}
			case UINT:
				{
					uint32_t k_ne = htonl(index->elements[i].key.n);
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

		uint64_t ot_ne = bswap_64((index->elements[i].offset));
		if(write(*fd,&ot_ne,sizeof(ot_ne)) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}

		uint32_t op_ne = htonl(index->elements[i].operation);
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


	int cap = (int)ntohl(cap_ne);
	if(cap == 0 || cap >= MAX_STACK_CAP) {
		return cap == 0 ? -1 : EJCAP;
	}

	for(int i = 0; i < cap; i++){
		
		uint64_t ts_ne = 0;
		if(read(fd,&ts_ne,sizeof(ts_ne)) == -1){
			error("read journal index failed",__LINE__-1);
			return -1;
		}
		
		index->elements[i].timestamp = (time_t) bswap_64(ts_ne);
		
		uint64_t size_ne = 0;
		if(read(fd,&size_ne,sizeof(size_ne)) == -1){
			error("read journal index failed",__LINE__-1);
			return -1;
		}

		size_t size = bswap_64(size_ne) + 1;
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
		
		index->elements[i].key_type = (int)ntohl(kt_ne);

		switch(index->elements[i].key_type){
		case STR:
		{
			uint64_t size_ne = 0;
			if(read(fd,&size_ne,sizeof(size_ne)) == -1){
				error("read journal index failed",__LINE__-1);
				return -1;
			}

			size_t size = bswap_64(size_ne) + 1;
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
			
			index->elements[i].key.n = (uint32_t)ntohl(k_ne);
			
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
		
		index->elements[i].offset = (off_t) bswap_64(os_ne);
		uint32_t op_ne = 0;
		if(read(fd,&op_ne,sizeof(op_ne)) == -1){
			error("read journal index failed",__LINE__-1);
			return -1;
		}
		
		index->elements[i].operation = (int) ntohl(op_ne);
		index->capacity++;
	}

	return 0;
}
static void error(char *msg,int line)
{
	fprintf(stderr,"(%s): %s, %s:%d.\n",p,msg,__FILE__,line);
}
