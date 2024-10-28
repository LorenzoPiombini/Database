#ifndef LOCK_H
#define LOCK_H

#include <sys/types.h>
#include <semaphore.h>
#include <fcntl.h>

#define FCNTL_ERR 2
#define SH_ILOCK "/lock_info_memory"
#define SEM_MILOCK "/sem_lock_info_memory"
#define MAX_NR_FILE_LOCKABLE 20

/* lock info for a file */
/*--! to be used in case you need a shared memory object !--*/
typedef struct
{
    struct flock lock;
    char file_name[256];
} lock_info;

typedef enum
{
    RD_HEADER,
    WR_HEADER,
    RD_REC,
    WR_REC
} mode;

typedef size_t compute_bytes(void *); /*pointer to a fucntion that computes the byte to lock in a file*/
unsigned char set_memory_obj(lock_info **shared_locks, sem_t **sem);
unsigned char aquire_lock_smo(lock_info **shared_locks, int *lock_pos,
                              char *file_n, off_t start, compute_bytes cb, int mode, int fd_data);
unsigned char is_locked(int fd, off_t rec_offset, off_t rec_size);
unsigned char lock_record(int fd, off_t rec_offset, off_t rec_size, int lock_type);
unsigned char unlock_record(int fd, off_t rec_offset, off_t rec_size);

#endif
