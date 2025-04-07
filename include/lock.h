#ifndef LOCK_H
#define LOCK_H

#include <sys/types.h>
#include <semaphore.h>
#include <fcntl.h>

#define FCNTL_ERR 2
#define SH_ILOCK "/lock_info_memory"
#define SEM_MILOCK "/sem_lock_info_memory"
#define MAX_NR_FILE_LOCKABLE 20
#define MAX_LOCK_IN_FILE 20
#define WTLK 6
#define MAX_WTLK 7
#define WT_RSLK 8
#define LOCK 9
#define UNLOCK 10

/* lock info for a file */
typedef struct
{
    struct flock lock[MAX_LOCK_IN_FILE];
    unsigned char lock_num;
    char file_name[256];
} lock_info;

extern lock_info *shared_locks;

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

typedef size_t compute_bytes(void *); /*pointer to a fucntion that computes the byte to lock in a file*/
unsigned char free_memory_object(char *smo_name);
unsigned char set_memory_obj(lock_info **shared_locks);
unsigned char acquire_lock_smo(lock_info **shared_locks, int *lock_pos, int *lock_pos_arr,
                               char *file_n, off_t start, off_t rec_len, int mode, int fd_data);
unsigned char release_lock_smo(lock_info **shared_locks, int *lock_pos, int *lock_pos_arr);
unsigned char is_locked(int fd, off_t rec_offset, off_t rec_size);
unsigned char lock_record(int fd, off_t rec_offset, off_t rec_size, int lock_type);
unsigned char unlock_record(int fd, off_t rec_offset, off_t rec_size);
int lock(int fd, int flag);

#endif /* lock.h */
