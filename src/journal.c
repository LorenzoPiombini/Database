#include <stdio.h>
#include <sys/types>
#include <byteswap.h>
#include <stdint.h>
#include "hash_tbl.h"
#include "journal.h"
#include "file.h"

char prog[] ="db";


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
			fprintf(stderr,"(%s): can't create or open '%s'.\n",prog,JDEL);
			return -1;
		}
	} else if (fd == -1 || fd_inx == -1){
		fprintf(stderr,"(%s): can't create or open '%s'.\n",prog,JDEL);
		return -1;
	}

	off_t eof = go_to_EOF(fd);
	if(eof == -1){
		fprintf(stderr,"(%s): can't reach EOF for '%s'.\n",prog,J_DEL);
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
		fprint(stderr,
				"(%s): write to '%s' failed, %s:%d.\n",
				prog,J_DEL,__FILE__,__LINE__-1);
		close_file(1,fd);
		return -1;
	}

	uint64_t offset_ne = bswap_64(offset);
	if(write(fd,&offset_ne,sizeof(offset_ne)) == -1){
		fprint(stderr,
				"(%s): write to '%s' failed, %s:%d.\n",
				prog,J_DEL,__FILE__,__LINE__-1);
		close_file(1,fd);
		return -1;
	}

	switch(key_type){
	case STR:
		size_t l = strlen((char *) key)+1;
		uint64_t l_ne = bswap_64(l);
		char buff[l];
		memset(buff,0,l);
		strncpy(buff,(char *)key,l);

		if(write(fd,&l_ne,sizeof(l_ne)) == -1 ||
			write(fd,buff,l) == -1){
			fprint(stderr,
				"(%s): write to '%s' failed, %s:%d.\n",
				prog,J_DEL,__FILE__,__LINE__-1);
			close_file(1,fd);
			return -1;
		}
		break;
	case UINT:
		uint32_t kn_ne = htonl(key);
		if(write(fd,&k_ne,sizeof(k_ne)) == -1){
			fprint(stderr,
				"(%s): write to '%s' failed, %s:%d.\n",
				prog,J_DEL,__FILE__,__LINE__-1);
			close_file(1,fd);
			return -1;
		}
		break
	default:
		close_file(1,fd);
		return -1;
	}

	/*write timestamp and offt of the new journal entry */
	

	return 0;
}
int journal_add(off_t offset, void *key);
