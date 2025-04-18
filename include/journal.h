#ifndef _JOURNAL_H_
#define _JOURNAL_H_

#include "hash_tbl.h"

#define JINX "journal.inx"
#define JHST "journal_history.db"
#define J_DEL "journal_del.db"
#define J_ADD "journal_add.db"

#define DEL_REG 1 	/*0000 0001*/
#define DEL_ORIG 0 	/*0000 0000*/

#define DEL_INX 0

#define PROC_PATH "/proc/self/fd/%d"

/*errors*/
#define EJCAP 2 /*index file has MAX_STACK_CAP elements*/



/*stack for journal index file */

#define MAX_STACK_CAP 500

struct Node_stack{
	time_t timestamp;
	off_t offset;
	struct Node_stack *next;
};

struct stack{
	int capacity;
	int dynamic_capacty;
	struct Node_stack elements[MAX_STACK_CAP];
	struct Node_stack *dynamic_elements;
};


int push(struct stack *index, struct Node_stack node);
int pop(struct stack *index);
int peek(struct stack *index, struct Node_stack *node);
int is_empty(struct stack *index);

int journal_del(off_t offset, void *key, int key_type);
int write_journal_index(int *fd,struct stack *index);
int read_journal_index(int fd,struct stack *index);
void free_stack(struct stack *index);

#endif
