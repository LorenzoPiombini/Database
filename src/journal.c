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

char p[] ="db";


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
	{	size_t l = strlen((char *) key)+1;
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
		if (!set((void*)&t, key_type, offset, &ht[0])) {
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
int journal_add(off_t offset, void *key);
