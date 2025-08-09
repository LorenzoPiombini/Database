#ifndef _JOURNAL_H_
#define _JOURNAL_H_

#include "hash_tbl.h"

#define JINX "journal.inx"
#define JHST "jarchive.inx"


/* operations */
#define J_DEL 0
#define J_ADD 1

#define DEL_REG 1 	/*0000 0001*/
#define DEL_ORIG 0 	/*0000 0000*/

#define DEL_INX 0

#define PROC_PATH "/proc/self/fd/%d"
#define MAX_FILE_NAME 1024
#define MAX_KEY_STRING 1024


/*errors*/
#define EJCAP 2 /*index file has MAX_STACK_CAP elements*/



/*stack for journal index file */

#define MAX_STACK_CAP 500
struct Node_stack{
	time_t timestamp;
	char file_name[MAX_FILE_NAME];
	int key_type;
	union{
		char s[MAX_KEY_STRING];
		uint32_t n;
	}key;
	off_t offset;
	int operation;
	struct Node_stack *next;
};

struct stack{
	int capacity;
	int dynamic_capacty;
	struct Node_stack elements[MAX_STACK_CAP];
	struct Node_stack *dynamic_elements;
};


int push_journal(struct stack *index, struct Node_stack node);
int pop_journal(struct stack *index);
int peek_journal(struct stack *index, struct Node_stack *node);
int is_empty(struct stack *index);

int journal(int caller_fd, off_t offset, void *key, int key_type, int operation);
int write_journal_index(int *fd,struct stack *index);
int read_journal_index(int fd,struct stack *index);
void free_stack(struct stack *index);

#endif
