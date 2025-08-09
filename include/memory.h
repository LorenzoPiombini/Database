#ifndef _MEMORY_H
#define _MEMORY_H

#include "types.h"

#define PAGE_SIZE 4096
#define MEM_SIZE PAGE_SIZE*1000 /* 4 Mib */

struct Mem{
	void *p;
	uint64_t size;
};/*16 bytes*/

enum types{
	INT,
	STRING,
	LONG,
	DOUBLE,
	FLOAT
};

int create_memory(struct Mem *memory, uint64_t size, int type);
int cancel_memory(struct Mem *memory);
void *ask_mem(size_t size);
int return_mem(void *start, size_t size);
void init_prog_memory();
void close_prog_memory();
int push(struct Mem *memory,void* value,int type);
void *value_at_index(uint64_t index,struct Mem *memory,int type);

#endif
