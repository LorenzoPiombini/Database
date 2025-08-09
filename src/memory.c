#if defined(__linux__)
	#include <sys/mman.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "memory.h"
#include "types.h"

static int8_t mem_safe = 0;
static int8_t *prog_mem = NULL;
static int8_t *last_addr = NULL;
static struct Mem *free_memory = NULL;

/*static functions*/
static int is_free(void *mem, size_t size);
int e = -1;
void init_prog_memory()
{
#if defined(__linux__)
	prog_mem = (int8_t *)mmap(NULL,sizeof (int8_t) * (MEM_SIZE + PAGE_SIZE), PROT_READ | PROT_WRITE,MAP_SHARED | MAP_ANONYMOUS,-1,0);
	if(prog_mem == MAP_FAILED){
		printf("mmap, failed, fallback malloc used.\n");
		prog_mem = NULL;
		prog_mem = malloc(MEM_SIZE*sizeof(int8_t));
		assert(prog_mem != NULL);
		memset(prog_mem,0,MEM_SIZE * sizeof(int8_t));

		free_memory = malloc(sizeof(struct Mem) * (PAGE_SIZE / sizeof (struct Mem)));
		assert(free_memory != NULL);
		memset(free_memory,0,sizeof(struct Mem) * (PAGE_SIZE / sizeof(struct Mem)));
		return;
	}
	/*we insert a page after the memory so we get a SIGSEGV if we overflow*/
	if(mprotect(prog_mem + MEM_SIZE,PAGE_SIZE,PROT_NONE) == -1){
		printf("memory protection failed.\n");
	}
	free_memory = malloc(sizeof(struct Mem) * (PAGE_SIZE / sizeof(struct Mem)));
	assert(free_memory != NULL);
	memset(free_memory,0,sizeof(struct Mem) * (PAGE_SIZE / sizeof(struct Mem)));
	mem_safe = 1;
#else
	prog_mem = malloc(MEM_SIZE*sizeof(int8_t));
	assert(prog_mem != NULL);
	memset(prog_mem,0,MEM_SIZE * sizeof(int8_t));

	free_memory = malloc(sizeof(struct Mem) * (PAGE_SIZE / sizeof (struct Mem)));
	assert(free_memory != NULL);
	memset(free_memory,0,sizeof(struct Mem) * (PAGE_SIZE / sizeof(struct Mem)));
#endif
}

void close_prog_memory()
{
	if(mem_safe){
		memset(free_memory,0,sizeof (struct Mem) * (PAGE_SIZE / sizeof(struct Mem)));
		if(munmap(prog_mem,MEM_SIZE+4096) == -1){
			printf("could not unmap the memory");
			return;
		}

		free(free_memory);
	}else{
		memset(free_memory,0,sizeof (struct Mem) * (PAGE_SIZE / sizeof(struct Mem)));
		free(prog_mem);
		free(free_memory);
	}
}


int create_memory(struct Mem *memory, uint64_t size, int type)
{
	if(!memory) return -1;

	switch(type){
	case INT:
		if((size % sizeof(int)) != 0) return -1;
		memory->p = (int*)ask_mem(size);
		if(!memory->p) return -1;
		break;
	case STRING:
		memory->p = (char*)ask_mem(size);
		if(!memory->p) return -1;
		break;
	case LONG:
		if((size % sizeof(long)) != 0) return -1;
		memory->p = (long*)ask_mem(size);
		if(!memory->p) return -1;
		break;
	case DOUBLE:
		if((size % sizeof(double)) != 0) return -1;
		memory->p = (double*)ask_mem(size);
		if(!memory->p) return -1;
		break;
	case FLOAT:
		if((size % sizeof(float)) != 0) return -1;
		memory->p = (float*)ask_mem(size);
		if(!memory->p) return -1;
		break;
	default:
		break;
	}
	memory->size = size;

	return 0;
}

int cancel_memory(struct Mem *memory){
	if(!memory->p) return -1;
	if(memory->size == 0 || memory->size >= MEM_SIZE) return -1;

	if(return_mem(memory->p,memory->size) == -1) return -1;

	return 0;
}

int push(struct Mem *memory,void* value,int type){
	
	switch(type){
	case INT:
		if(is_free((void*)memory->p,memory->size) == -1){
			if((memory->size / sizeof(int)) > 1){
				/*find the first empty memory slot*/
				uint64_t i;
				for(i = 0; i < (memory->size / sizeof(int));i++){
					if(*(int*)memory->p == 0) break;
						
					memory->p = (int*)memory->p + 1;
				}	
				memcpy(memory->p,value,sizeof(int));
				memory->p= (int*)memory->p - i;	
				break;
			}
		}
		memcpy(memory->p,value,sizeof(int));
		break;
	case STRING:
		if(is_free((void*)memory->p,memory->size) == -1){
			if((memory->size / sizeof(char)) > 1){
				/*find the first empty memory slot*/
				uint64_t i;
				for(i = 0; i < (memory->size / sizeof(char));i++){
					if(*(char*)memory->p == '\0') break;
						
					memory->p = (char*)memory->p + 1;
				}	
				memcpy(memory->p,value,sizeof(char));
				memory->p= (char*)memory->p - i;	
				break;
			}
		}
		memcpy(memory->p,value,sizeof(char));
		break;
	case LONG:
		*(long*)memory->p = *(long*)value;
		if((memory->size / sizeof(long)) > 1) memory->p = (long*)memory->p + 1;
		break;
	case DOUBLE:
		*(double*)memory->p = *(double*)value;
		if((memory->size / sizeof(double)) > 1) memory->p = (double*)memory->p + 1;
		break;
	case FLOAT:
		*(float*)memory->p = *(float*)value;
		if((memory->size / sizeof(float)) > 1) memory->p = (float*)memory->p + 1;
		break;
	default:
		return -1;
	}

	return 0;
}
	
void *value_at_index(uint64_t index,struct Mem *memory,int type)
{
	switch(type){
	case INT:
		if((int)((memory->size / sizeof(int)) - (int)index ) <= 0 ) return (void*) &e;
		return (void*)((int*)memory->p + index);
	case STRING:
		if((int)((memory->size / sizeof(char)) - index ) <= 0 ) return (void*) &e;
		return (void*)((char*)memory->p + index);
	default:
		return (void*)&e;
	}
}
void *ask_mem(size_t size){
	if(!prog_mem) return NULL;
	if(size > (MEM_SIZE - 1)) return NULL;

	/*we have enough space from the start of the memory block*/
	if(!last_addr){
		/*this is the first allocation*/
		last_addr = (prog_mem + size) - 1;
		return (void*) prog_mem;
	}

	/*look for freed blocks*/
	uint64_t i;
	for(i = 0;i < (PAGE_SIZE / sizeof (struct Mem));i++){
		if(!free_memory[i].p) continue;
		
		if(free_memory[i].size == size){
			last_addr = free_memory[i].p + size -1;
			void *found = free_memory[i].p;
			memset(&free_memory[i],0,sizeof(struct Mem));
			return found;
		}

		if(free_memory[i].size > size){
			last_addr = free_memory[i].p + size -1;
			void *found = free_memory[i].p;
			free_memory[i].p += size;
			free_memory[i].size -= size;
			return found;
		}
	}
	
	/*look for space in the memory block*/
	int8_t *p = NULL;
	if(last_addr) 
		p = last_addr + 1;
	else
		p = prog_mem;

	while(is_free((void*)p,size) == -1) {
		if(((p + size) - prog_mem) > (MEM_SIZE -1)) {
			return NULL;
		}
		p += size;
	}

	/*here we know we have a big enolugh block*/
	last_addr = (p + size) - 1;
	return (void*) p;
}

int return_mem(void *start, size_t size){
	if(!start) return -1;
	if(((int8_t *)start - prog_mem) < 0) return -1;

	uint64_t s = (uint64_t)((int8_t *)start - prog_mem);
	uint64_t i;
	for(i = 0; i < (PAGE_SIZE / sizeof(struct Mem)); i++){
		if(free_memory[i].p){
			continue;
		}else{
			free_memory[i].p = (void*)&prog_mem[s];
			free_memory[i].size = size;
			memset(&prog_mem[s],0,sizeof(int8_t)*size);
			break;
		}
		/*
		 * in this case we do not have space to register the free block
		 * but we 'free' the memory anyway.
		 * */

		memset(&prog_mem[s],0,sizeof(int8_t)*size);
		break;
	}

	return 0;
}


static int is_free(void *mem, size_t size){
	char cbuf[size+1];
	memset(cbuf,0,size+1);
	memcpy(cbuf,(char*)mem,size);

	/*
	 * if the memory is 'free' each byte in the block is set to 0
	 * the loop try to find any ascii charater (besides '\0') in the block,
	 * if any char is found, the block is not free
	 * */
	if(cbuf[0] !=  '\0') return -1;
	int i;
	for(i = 1; i < 128;i++){
		if(strstr(cbuf,(char *)&i) != NULL) return -1;
	}

	return 0;
}
