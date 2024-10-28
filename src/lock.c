#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <stdio.h>
#include <string.h>
#include "lock.h"
#include "debug.h"
#include "common.h"
#include "parse.h"

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

/* this fucntion makes the lock info avaiable to different processes
	by creating a shared memory object(SMO) and map a pointer to it,
	if the function succeed, shared_locks will be used to write and read data from SMO.
	A semaphore will also be created to allow synchronized access to the SMO.
	this fucntion is ment to be called only from the main program and only once to set SMO.
	SMO will be accessed from the utililty program to check if there is any lock on the file
	that is about to read from or write to */

unsigned char set_memory_obj(lock_info **shared_locks, sem_t **sem)
{
	int fd = shm_open(SH_ILOCK, O_CREAT | O_RDWR, 0666);
	if (fd == -1)
	{
		perror("shared memory object:");
		printf("%s:%d.\n", F, L - 4);
		return 0;
	}

	/* define the size of the shared memory */
	if (ftruncate(fd, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE) == STATUS_ERROR)
	{
		perror("ftruncate(): ");
		printf(" %s:%d.\n", F, L - 3);
		if (shm_unlink(SH_ILOCK) == STATUS_ERROR)
		{
			perror("can`t unlink shared memory:");
			printf(" %s:%d.\n", F, L - 3);
			return 0;
		}
		return 0;
	}

	*sem = sem_open(SEM_MILOCK, O_CREAT, 0644, 1);
	if (*sem == SEM_FAILED)
	{
		perror("semaphore creation failed: ");
		printf("%s:%d.\n", F, L - 4);
		if (shm_unlink(SH_ILOCK) == STATUS_ERROR)
		{
			perror("can`t unlink shared memory:");
			printf(" %s:%d.\n", F, L - 3);
			return 0;
		}
		return 0;
	}

	*shared_locks = mmap(NULL, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (*shared_locks == MAP_FAILED)
	{
		perror("mmap failed: ");
		printf("%s:%d.\n", F, L - 4);
		if (shm_unlink(SH_ILOCK) == STATUS_ERROR)
		{
			perror("can`t unlink shared memory:");
			printf(" %s:%d.\n", F, L - 3);
			return 0;
		}
		return 0;
	}

	/*set the values of the struct arrays to 0*/
	memset(*shared_locks, 0, sizeof(lock_info) * MAX_NR_FILE_LOCKABLE);

	return 1;
}

/* aquire_lock_smo :
	check if there is a lock for the file_n that we want to read or write to.
	this function will try to wait for the semaphore, if the shared memory object(smo)
	is locked from another process, the function will fail (return 0)

	@parameters:
		- shared_locks, double pointer to lock_info struct (smo access)
		- lock_pos, pointer to an integer, if the function aquire a lock this param will
			return to the caller level the position of the lock in the lock_info array
			you will use this position to free the lock.
		- file_n, a string rapresenting the file name where to place the lock
		- start, offset position in the file where the function will acquire the lock
		- compute_bytes, pointer to a fucntion used to compute the bytes to lock in the file
		- mode, define which operation we want to perform on the file, and it is used to
			compute how many bytes to lock in the file.
		- fd_data, file descriptor for the data file.

	@return:
		aquire_lock_smo() return 1 if it succeeds, 0 if it fails.
*/
unsigned char aquire_lock_smo(lock_info **shared_locks, int *lock_pos,
							  char *file_n, off_t start, compute_bytes cb, int mode, int fd_data)
{

	if (!*shared_locks)
		return 0;

	sem_t *sem = sem_open(SEM_MILOCK, 0);
	if (sem == SEM_FAILED)
	{
		perror("sem_open failed: ");
		printf("%s:%d.\n", F, L - 4);
		return 0;
	}

	if (sem_trywait(sem) == -1)
	{
		perror("sem_trywait() failed: ");
		printf("%s() can't acces the shared memory object, %s:%d.\n", __func__, F, L - 3);
		sem_close(sem);
		return 0;
	}

	/* we can safely access to the shared memory object */
	for (int i = 0; i < MAX_NR_FILE_LOCKABLE; i++)
	{
		if ((*shared_locks)[i].file_name[0] != '\0')
		{
			size_t l = strlen(file_n);
			if (strncmp((*shared_locks)[i].file_name, file_n, l) != 0)
				continue;

			if ((*shared_locks)[i].lock.l_type == F_UNLCK)
			{
				switch (mode)
				{
				case RD_HEADER:
				{
					(*shared_locks)[i].lock.l_type = F_RDLCK;
					(*shared_locks)[i].lock.l_whence = SEEK_SET;
					(*shared_locks)[i].lock.l_start = start;
					(*shared_locks)[i].lock.l_len = MAX_HD_SIZE;
					break;
				}
				default:
					printf("unknown mode.\n");
					break;
				}

				*lock_pos = i;
				break;
			}
		}
	}

	if (*lock_pos == 0)
	{
		(*shared_locks)[*lock_pos].lock.l_type = F_RDLCK;
		(*shared_locks)[*lock_pos].lock.l_whence = SEEK_SET;
		(*shared_locks)[*lock_pos].lock.l_start = start;
		(*shared_locks)[*lock_pos].lock.l_len = MAX_HD_SIZE;
		if (snprintf((*shared_locks)[*lock_pos].file_name, strlen(file_n), "%s", file_n) < 0)
		{
			printf("snprintf() failed, %s:%d.\n", F, L - 3);
			sem_post(sem);
			sem_close(sem);
			return 0;
		}
	}

	sem_post(sem);
	sem_close(sem);
	return 1;
}

unsigned char is_locked(int fd, off_t rec_offset, off_t rec_size)
{
	struct flock lock;
	lock.l_type = F_WRLCK;
	lock.l_whence = SEEK_SET;
	lock.l_start = rec_offset;
	lock.l_len = rec_size;

	if (fcntl(fd, F_GETLK, &lock) == -1)
	{
		perror("fcntl failed:");
		return FCNTL_ERR;
	}

	if (lock.l_type == F_UNLCK)
	{
		return 0;
	}

	return 1;
}

unsigned char lock_record(int fd, off_t rec_offset, off_t rec_size, int lock_type)
{
	struct flock lock;

	lock.l_type = lock_type;
	lock.l_whence = SEEK_SET;
	lock.l_start = rec_offset;
	lock.l_len = rec_size;

	if (fcntl(fd, F_SETLK, &lock) == -1)
	{
		perror("fcntl - locking failed");
		return FCNTL_ERR; // false
	}

	return 1; // true;
}

unsigned char unlock_record(int fd, off_t rec_offset, off_t rec_size)
{
	struct flock lock;

	lock.l_type = F_UNLCK;
	lock.l_whence = SEEK_SET;
	lock.l_start = rec_offset;
	lock.l_len = rec_size;

	if (fcntl(fd, F_SETLK, &lock) == -1)
	{
		perror("fcntl - unlocking failed");
		return FCNTL_ERR; // false
	}

	return 1; // true
}
