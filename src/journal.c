#include <stdio.h>
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

int journal_del(off_t offset, void *key, int key_type)
{

	int create = 0;
	int fd = open_file(J_DEL,0);
	int fd_inx = open_file(JINX,0);
	if(fd == -1 && fd_inx == -1){
		create = 1;
		fd = create_file(J_DEL);
		fd_inx = create_file(JINX);
		if(fd == -1 || fd_inx == -1){
			fprintf(stderr,"(%s): can't create or open '%s'.\n",p,J_DEL);
			return -1;
		}
	} else if (fd == -1 || fd_inx == -1){
		fprintf(stderr,"(%s): can't create or open '%s'.\n",p,J_DEL);
		return -1;
	}

	off_t eof = go_to_EOF(fd);
	if(eof == -1){
		fprintf(stderr,"(%s): can't reach EOF for '%s'.\n",p,J_DEL);
		close_file(2,fd,fd_inx);
		return -1;
	}
	
	/*
	 * each journal record will store : 
	 * - 8 bit flag ( if 0 record is in the original file
	 *   		  if 1 record is in delete archive)
	 * - off_t
	 * - key   
	 * */
	

	uint8_t flag = DEL_ORIG;
	if(write(fd,&flag,sizeof(flag)) == -1){
		fprintf(stderr,
				"(%s): write to '%s' failed, %s:%d.\n",
				p,J_DEL,__FILE__,__LINE__-1);
		close_file(2,fd,fd_inx);
		return -1;
	}

	uint64_t offset_ne = bswap_64(offset);
	if(write(fd,&offset_ne,sizeof(offset_ne)) == -1){
		fprintf(stderr,
				"(%s): write to '%s' failed, %s:%d.\n",
				p,J_DEL,__FILE__,__LINE__-1);
		close_file(1,fd);
		return -1;
	}

	switch(key_type){
	case STR:
	{	
		size_t l = strlen((char *) key)+1;
		uint64_t l_ne = bswap_64(l);
		char buff[l];
		memset(buff,0,l);
		strncpy(buff,(char *)key,l);

		if(write(fd,&l_ne,sizeof(l_ne)) == -1 ||
			write(fd,buff,l) == -1){
			fprintf(stderr,
				"(%s): write to '%s' failed, %s:%d.\n",
				p,J_DEL,__FILE__,__LINE__-1);
			close_file(2,fd,fd_inx);
			return -1;
		}
		break;
	}
	case UINT:
		uint32_t k_ne = htonl(*(uint32_t *)key);
		if(write(fd,&k_ne,sizeof(k_ne)) == -1){
			fprintf(stderr,
				"(%s): write to '%s' failed, %s:%d.\n",
				p,J_DEL,__FILE__,__LINE__-1);
			close_file(2,fd,fd_inx);
			return -1;
		}
		break;
	default:
		close_file(2,fd,fd_inx);
		return -1;
	}

	/*write timestamp and offt of the new journal entry */
	if(create){
		/*  write the index file */
		int bucket = 7;
		int index_num = 5;

		if (!write_index_file_head(fd_inx, index_num)) {
			fprintf(stderr,"(%s): write to '%s' failed, %s:%d",
				p,JINX,__FILE__,__LINE__-1);
			close_file(2,fd,fd_inx);
			return -1;
		}

		for (int i = 0; i < index_num; i++) {
			HashTable ht = {0};
			ht.size = bucket;
			ht.write = write_ht;

			if (i == DEL_INX) {

				time_t t = time(NULL);	
				int key_type = UINT;
				if (!set((void*)&t, key_type, offset, &ht)) {
					close_file(2,fd,fd_inx);
					return -1;
				}

			}

			if (!write_index_body(fd_inx, i, &ht)) {
				fprintf(stderr,"(%s): write to '%s' failed, %s:%d",
				p,JINX,__FILE__,__LINE__-1);
				destroy_hasht(&ht);
				close_file(2,fd,fd_inx);
				return -1;
			}

			destroy_hasht(&ht);
		}

	
	}else{
		HashTable *ht = NULL;
		int index = 0;
		int *p_index = &index;
		/* load all indexes in memory */
		if (!read_all_index_file(fd_inx, &ht, p_index)) {
			fprintf(stderr,"(%s): read index from '%s' failed, %s:%d",
			p,JINX,__FILE__,__LINE__-1);
			destroy_hasht(ht);
			close_file(2,fd,fd_inx);
			return -1;
		}

		time_t t = time(NULL);	
		int key_type = UINT;
		if (!set((void *)&t, key_type, offset, &ht[0])) {
			close_file(2,fd,fd_inx);
			return -1;
		}

		close_file(1, fd_inx);
		fd_inx = open_file(JINX, 1); // opening with o_trunc

		/*  write the index file */
		if (!write_index_file_head(fd_inx, index)) {
			printf("write to file failed, %s:%d",__FILE__,__LINE__ - 2);
			free_ht_array(ht,index);
			return -1;
		}

		for (int i = 0; i < index; i++){

			if (!write_index_body(fd_inx, i, &ht[i])) {
				printf("write to file failed. %s:%d.\n",__FILE__,__LINE__ - 2);
				free_ht_array(ht,index);
				return -1;
			}

			destroy_hasht(&ht[i]);
		}


	}	

	return 0;
}

 
int push(struct stack *index, struct Node_stack node);
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
		index->dynamic_elements->timestemp = node->timestemp;
		index->dynamic_elements->offset = node->offset;
		index->dynamic_capacty++;
		return 0;
	}
	
	struct Node_stack nd = calloc(s,sizeof(struct Node_stack));
	if(!nd){
		fprintf(stderr,"(%s): calloc failed.\n",p);
		return -1;
	}
	
	
	nd->timestemp = node->timestemp;
	nd->offset = node->offset;
	index->dynamic_elements->next = nd;
	index->dynamic_capacty++;
	return 0;
}
int pop(struct stack *index){
	if(index->capacity < MAX_STACK_CAP){
		memset(&index->elements[index->capacity - 1],0,sizeof(struct Node_stack));
		index->capacity--;
		return 0;
	}
	
	if(index->dynamic_capacty == 0) return -1;

	struct Node_stack *temp = index->dynamic_elements;
	while(temp) {

		if(!temp->next->next) {
			struct Node_stack temp_s = temp; 
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
	while(temp) temp = temp->next;

	node->timestemp = temp->timestemp;
	node->offset = temp->offset;

	return 0;
}

int is_empty(struct stack *index)
{
	return index->capacity == 0;

}

int free_stack(struct stack *index)
{
	if(index->dynamic_capacty == 0 ) return;
	
	while(index->dynamic_capacty > 0) pop(index);
	
}

int write_journal_index(int fd,struct stack *index)
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
		if(fd_hsp == -1){
			fd_hst = create_file(JHST,0);
			if(fd_hsp == -1){
				error("can't create history file.",__LINE__-2);
				return -1;
			}
		}

		int hst_cap = 0;
		int nw_cap = 0;
		if(index->dynamic_capacty == 0){

			hst_cap = index->capacity -100;
			if(index->dynamic_capacty > 0)
				nw_cap = 100 + index->dynamic_capacty;
		}	

		uint32_t cap_ne = htonl(hst_cap);
		if(write(fd_hsp,&cap_ne,sizeof(cap_ne)) == -1){
			error("can't write to journal index file.",__LINE__-1);
			close(fd_hst);
			return -1;
		}

		for(int i = 0; i < hst_cap;i++){

			uint64_t ts_ne = bswap_64((index->elements[i].timestemp));
			if(write(fd_hst,&ts_ne,sizeof(ts_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				close(fd_hst);
				return -1;
			}
				
			uint64_t ot_ne = bswap_64((index->elements[i].offset));
			if(write(fd_hst,&ot_ne,sizeof(ot_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				close(fd_hst);
				return -1;
			}
		}

		close(fd_hst);
		if(nw_cap == 0)
			nw_cap = 100;
			
		uint32_t nw_cap_ne = htonl(nw_cap);
		if(write(fd,&nw_cap_ne,sizeof(nw_cap_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				return -1;
		}
			
		
		if(nw_cap == 100)
			nw_cap = MAX_STACK_CAP;

		for(int i = hst_cap ; i < nw_cap;i++){
			
			if(i >= MAX_STACK_CAP){
				struct Node_stack *temp = index->dynamic_elements;
				while(temp){
					uint64_t ts_ne = bswap_64((temp->dynamic_elements->timestemp));
					if(write(fd,&ts_ne,sizeof(ts_ne)) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}

					uint64_t ot_ne = bswap_64((temp->dynamic_elements[i].offset));
					if(write(fd,&ot_ne,sizeof(ot_ne)) == -1){
						error("can't write to journal index file.",__LINE__-1);
						return -1;
					}

					temp = temp->next;
				}
			}

			uint64_t ts_ne = bswap_64((index->elements[i].timestemp));
			if(write(fd,&ts_ne,sizeof(ts_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				return -1;
			}
				
			uint64_t ot_ne = bswap_64((index->elements[i].offset));
			if(write(fd,&ot_ne,sizeof(ot_ne)) == -1){
				error("can't write to journal index file.",__LINE__-1);
				return -1;
			}
		}

		return 0;
			
	}


	uint32_t cap_ne = htonl(index->capacity);
	if(write(fd,&cap_ne,sizeof(cap_ne)) == -1){
		fprintf(stderr,"(%s): can't write journal index file.",p);
		return -1;
	}

	
	for(int i = 0; i < index->capacity; i++){
		uint64_t ts_ne = bswap_64((index->elements[i].timestemp));
		if(write(fd,&ts_ne,sizeof(ts_ne)) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}

		uint64_t ot_ne = bswap_64((index->elements[i].offset));
		if(write(fd,&ot_ne,sizeof(ot_ne)) == -1){
			error("can't write to journal index file.",__LINE__-1);
			return -1;
		}
	}
	return 0;

}

static void error(char *msg,int line)
{
	fprintf(stderr,"(%s): %s, %s:%d.\n",p,msg,__FILE__,line);
}
