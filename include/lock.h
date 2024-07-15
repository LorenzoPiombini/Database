#ifndef LOCK_H
#define LOCK_H

#include <sys/types.h>

int lock_record(int fd, off_t record_offset, off_t record_size, int lock_type);

int unlock_record(int fd, off_t record_offset, off_t record_size);

#endif
