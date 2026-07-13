#ifndef LOCK_H
#define LOCK_H


#define LOCK_FROM_LUA 0x00000100 
#define GET_TYPE_LOCK(n) ((n) & 0x000000ff)
#define IS_LOCK_FROM_LUA(n) (((n) & 0x00000100) != 0)
enum lock_type{
	STD_LOCK,
	LOCK_DATA_FILE,
	LOCK_SCHEMA_FILE,
};

#define FCNTL_ERR 2
#define SH_ILOCK "/lock_info_memory"
#define SEM_MILOCK "/sem_lock_info_memory"
#define MAX_NR_FILE_LOCKABLE 20
#define MAX_LOCK_IN_FILE 20
#define WTLK 6
#define MAX_WTLK 7
#define WT_RSLK 8
#define RLOCK 9
#define WLOCK 10
#define UNLOCK 11
#define LOCKED 12


/*type of operations performed on a file in the system */
typedef enum
{
    RD_HEADER,
    WR_HEADER,
    RD_REC,
    WR_REC,
    RD_IND,
    WR_IND
} mode;

int release_lock(int *fds,int mode);
int acquire_lock(int *fds,int mode);

#endif /* lock.h */
