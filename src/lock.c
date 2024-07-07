#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <stdio.h>
#include "lock.h"

/*
 *struct flock {
               ...
               short l_type;     Type of lock: F_RDLCK,
                                   F_WRLCK, F_UNLCK 
               short l_whence;   How to interpret l_start:
                                   SEEK_SET, SEEK_CUR, SEEK_END
               off_t l_start;    Starting offset for lock 
               off_t l_len;      Number of bytes to lock 
               pid_t l_pid;      PID of process blocking our lock
                                   (set by F_GETLK and F_OFD_GETLK) 
               ...
           };


 */

int lock_record(int fd, off_t record_offset,off_t record_size, int lock_type){
	struct flock lock;
	
	lock.l_type = lock_type;
	lock.l_whence = SEEK_SET;
	lock.l_start = record_offset;
	lock.l_len = record_size;
	lock.l_pid = F_GETLK;

	if(fcntl(fd,F_SETLK, &lock) == -1){
		perror("fcntl - locking failed");
		return  0; // false
	}

	return 1; // true;


}

int unlock_record(int fd, off_t record_offset, off_t record_size){
	struct flock lock;

	lock.l_type = F_UNLCK;
	lock.l_whence = SEEK_SET;
	lock.l_start = record_offset;
	lock.l_len = record_size;

	if(fcntl(fd, F_SETLK, &lock) == -1){
		perror("fcntl - unlocking failed");
		return 0; // false
	}

	return 1;//true
}



