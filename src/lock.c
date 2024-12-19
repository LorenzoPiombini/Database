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
#include "parse.h"
#include "common.h"

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

lock_info *shared_locks = NULL;

unsigned char free_memory_object(char *smo_name)
{
	if (shm_unlink(smo_name) == STATUS_ERROR)
	{
		perror("shm_uinlink failed: ");
		printf(" %s:%d.\n", F, L - 3);
		return 0;
	}

	if (sem_unlink(SEM_MILOCK) == STATUS_ERROR)
	{
		perror("sem_uinlink failed: ");
		printf(" %s:%d.\n", F, L - 3);
		return 0;
	}

	return 1;
}

/* this fucntion makes the lock info avaiable to different processes
	by creating a shared memory object(SMO) and map a pointer to it,
	if the function succeed, shared_locks will be used to write and read data from SMO.
	A semaphore will also be created to allow synchronized access to the SMO.
	this fucntion is ment to be called only from the main program and only once to set SMO.
	SMO will be accessed from the utililty program to check if there is any lock on the file
	that is about to read from or write to */

unsigned char set_memory_obj(lock_info **shared_locks)
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
		if (!free_memory_object(SH_ILOCK))
		{
			perror("can`t unlink shared memory:");
			printf(" %s:%d.\n", F, L - 3);
			return 0;
		}
		return 0;
	}

	sem_t *sem = sem_open(SEM_MILOCK, O_CREAT, 0644, 1);
	if (sem == SEM_FAILED)
	{
		perror("semaphore creation failed: ");
		printf("%s:%d.\n", F, L - 4);
		if (!free_memory_object(SH_ILOCK))
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
		if (!free_memory_object(SH_ILOCK))
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

/* acquire_lock_smo :
	check if there is a lock for the file_n that we want to read or write to.
	this function will try to wait for the semaphore, if the shared memory object(smo)
	is locked from another process, the function will fail (return 0)

	@parameters:
		- shared_locks, double pointer to lock_info struct (smo access)
		- lock_pos, pointer to an integer, if the function aquire a lock this param will
			return to the caller level the position of the lock in the lock_info array
			you will use this position to free the lock.
		- lock_pos_arr, pointer to an integer, if the function aquire a lock this param will
			return to the caller level the position of the lock like so:
			lock_info[i].lock[lock_pos_arr].
		- file_n, a string rapresenting the file name where to place the lock
		- start, offset position in the file where the function will acquire the lock
		- rec_len, record length that you wish to lock (record or bytes)
		- mode, define which operation we want to perform on the file, and it is used to
			compute how many bytes to lock in the file.
		- fd, file descriptor for file, can be fd_index or fd_data.

	@return:
		aquire_lock_smo() return 1 if it succeeds, 0 if it fails.

		WTLK:
			another process holds either a read or write lock in the same or overallaping region
			in the file, or the semaphore is not acquired.
		MAX_WTLK:
			we reach the lockable file limit for the system.
*/
unsigned char acquire_lock_smo(lock_info **shared_locks, int *lock_pos, int *lock_pos_arr,
							   char *file_n, off_t start, off_t rec_len, int mode, int fd)
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
		sem_close(sem);
		return WTLK;
	}

	/* we can safely access the shared memory object */
	for (int i = 0; i < MAX_NR_FILE_LOCKABLE; i++)
	{
		if ((*shared_locks)[i].file_name[0] != '\0')
		{
			size_t l = strlen(file_n);
			if (strncmp((*shared_locks)[i].file_name, file_n, l) != 0)
				continue;

			if ((*shared_locks)[i].lock_num == 0)
			{
				switch (mode)
				{
				case RD_HEADER:
				case RD_REC:
				case RD_IND:
				{
					(*shared_locks)[i].lock[0].l_type = F_RDLCK;
					(*shared_locks)[i].lock[0].l_whence = SEEK_SET;
					(*shared_locks)[i].lock[0].l_start = start;
					(*shared_locks)[i].lock[0].l_len = rec_len;
					(*shared_locks)[i].lock[0].l_pid = getpid();
					break;
				}
				case WR_HEADER:
				case WR_REC:
				case WR_IND:
				{
					(*shared_locks)[i].lock[0].l_type = F_WRLCK;
					(*shared_locks)[i].lock[0].l_whence = SEEK_SET;
					(*shared_locks)[i].lock[0].l_start = start;
					(*shared_locks)[i].lock[0].l_len = rec_len;
					(*shared_locks)[i].lock[0].l_pid = getpid();
					break;
				}
				default:
					printf("unknown mode.\n");
					break;
				}

				*lock_pos = i;
				*lock_pos_arr = 0;
				(*shared_locks)[i].lock_num++;
				break;
			}
			else if ((*shared_locks)[i].lock_num < MAX_LOCK_IN_FILE)
			{
				/* check if one of the process is reading or writing the same region */
				/* read is a non destructive operation, we can allowe different processes
					to read the same region, HOWEVER, a write Lock may be acquired
					only if there are no locks whatsoever on the file */
				for (int y = 0; y < MAX_LOCK_IN_FILE; y++)
				{
					if ((*shared_locks)[i].lock[y].l_type != F_RDLCK &&
						(*shared_locks)[i].lock[y].l_type != F_WRLCK)
						continue;

					if ((*shared_locks)[i].lock[y].l_start == start)
					{
						if ((*shared_locks)[i].lock[y].l_type == F_RDLCK &&
							(mode == RD_HEADER || mode == RD_REC || mode == RD_IND))
						{
							sem_post(sem);
							sem_close(sem);
							return 1;
						}

						if (!((*shared_locks)[i].lock[y].l_pid == getpid()))
						{
							sem_post(sem);
							sem_close(sem);
							return WTLK;
						}

						sem_post(sem);
						sem_close(sem);
						return 1;
					}

					if (((*shared_locks)[i].lock[y].l_start < start) &&
						(((*shared_locks)[i].lock[y].l_len +
						  (*shared_locks)[i].lock[y].l_start) > start))
					{
						sem_post(sem);
						sem_close(sem);
						return WTLK;
					}

					if (((*shared_locks)[i].lock[y].l_start > start) &&
						((start + rec_len) < ((*shared_locks)[i].lock[y].l_len +
											  (*shared_locks)[i].lock[y].l_start)))
					{
						sem_post(sem);
						sem_close(sem);
						return WTLK;
					}

					/*we need to ensure there is no locks are present
							if we want to write to the file */
					if (mode == WR_HEADER || mode == WR_REC || mode == WR_IND)
					{
						sem_post(sem);
						sem_close(sem);
						return WTLK;
					}
				}

				for (int j = 0; j < MAX_LOCK_IN_FILE; j++)
				{
					if ((*shared_locks)[i].lock[j].l_type == F_UNLCK)
					{
						switch (mode)
						{
						case RD_HEADER:
						case WR_HEADER:
						case RD_REC:
						{
							(*shared_locks)[i].lock[j].l_type = F_RDLCK;
							(*shared_locks)[i].lock[j].l_whence = SEEK_SET;
							(*shared_locks)[i].lock[j].l_start = start;
							(*shared_locks)[i].lock[j].l_len = rec_len;
							(*shared_locks)[i].lock[j].l_pid = getpid();
							break;
						}
						case WR_REC:
						case RD_IND:
						case WR_IND:
						{
							(*shared_locks)[i].lock[j].l_type = F_WRLCK;
							(*shared_locks)[i].lock[j].l_whence = SEEK_SET;
							(*shared_locks)[i].lock[j].l_start = start;
							(*shared_locks)[i].lock[j].l_len = rec_len;
							(*shared_locks)[i].lock[j].l_pid = getpid();
							break;
						}
						default:
							printf("unknown mode.\n");
							break;
						}

						*lock_pos = i;
						*lock_pos_arr = j;
						(*shared_locks)[i].lock_num++;
						break;
					}
				}
			}
			else
			{ /* we reach the max nr of lockable records in a file */
				sem_post(sem);
				sem_close(sem);
				return MAX_WTLK;
			}
		}
	}

	if (*lock_pos == 0)
	{
		unsigned char file_lockable = 0;
		for (int i = 0; i < MAX_NR_FILE_LOCKABLE; i++)
		{
			if ((*shared_locks)[i].file_name[0] == '\0')
			{
				*lock_pos = i;
				file_lockable = 1;
				break;
			}
		}

		if (!file_lockable)
		{
			sem_post(sem);
			sem_close(sem);
			return MAX_WTLK;
		}

		switch (mode)
		{
		case RD_HEADER:
		case RD_REC:
		case RD_IND:
		{
			(*shared_locks)[*lock_pos].lock[0].l_type = F_RDLCK;
			(*shared_locks)[*lock_pos].lock[0].l_whence = SEEK_SET;
			(*shared_locks)[*lock_pos].lock[0].l_start = start;
			(*shared_locks)[*lock_pos].lock[0].l_len = rec_len;
			(*shared_locks)[*lock_pos].lock[0].l_pid = getpid();

			if (snprintf((*shared_locks)[*lock_pos].file_name, strlen(file_n) + 1, "%s", file_n) < 0)
			{
				printf("snprintf() failed, %s:%d.\n", F, L - 3);
				sem_post(sem);
				sem_close(sem);
				return 0;
			}
			break;
		}
		case WR_HEADER:
		case WR_REC:
		case WR_IND:
		{
			(*shared_locks)[*lock_pos].lock[0].l_type = F_WRLCK;
			(*shared_locks)[*lock_pos].lock[0].l_whence = SEEK_SET;
			(*shared_locks)[*lock_pos].lock[0].l_start = start;
			(*shared_locks)[*lock_pos].lock[0].l_len = MAX_HD_SIZE;
			(*shared_locks)[*lock_pos].lock[0].l_pid = getpid();

			if (snprintf((*shared_locks)[*lock_pos].file_name, strlen(file_n) + 1, "%s", file_n) < 0)
			{
				printf("snprintf() failed, %s:%d.\n", F, L - 3);
				sem_post(sem);
				sem_close(sem);
				return 0;
			}
			break;
		}
		default:
			printf("unknown mode.\n");
			break;
		}

		*lock_pos_arr = 0;
		(*shared_locks)[*lock_pos].lock_num++;
	}

	sem_post(sem);
	sem_close(sem);
	return 1;
}

unsigned char release_lock_smo(lock_info **shared_locks, int *lock_pos, int *lock_pos_arr)
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

	/*try to acquire the semaphore */
	if (sem_trywait(sem) == -1)
	{
		sem_close(sem);
		return WTLK;
	}

	/* it is safe to read the smo*/

	/*if the process owns the lock then release it */
	if ((*shared_locks)[*lock_pos].lock[*lock_pos_arr].l_pid == getpid())
	{
		(*shared_locks)[*lock_pos].lock[*lock_pos_arr].l_type = F_UNLCK;
		(*shared_locks)[*lock_pos].lock[*lock_pos_arr].l_whence = SEEK_SET;
		/* decrease the lock number on the file*/
		(*shared_locks)[*lock_pos].lock_num--;

		if ((*shared_locks)[*lock_pos].lock_num == 0)
		{
			memset((*shared_locks)[*lock_pos].file_name, 0,
				   sizeof((*shared_locks)[*lock_pos].file_name));
		}

		sem_post(sem);
		sem_close(sem);
		/*reset the pointers reference, this make sure the subsequent locks are aquired correctly*/
		*lock_pos = 0;
		*lock_pos_arr = 0;
		return 1;
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
