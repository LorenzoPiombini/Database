#ifndef _JOURNAL_H_
#define _JOURNAL_H_

#include "hash_tbl.h"

#define JINX "journal.inx"
#define J_DEL "journal_del.db"
#define J_ADD "journal_add.db"

#define DEL_REG 1 	/*0000 0001*/
#define DEL_ORIG 0 	/*0000 0000*/

#define DEL_INX 0

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
	struct Node_stack dynamic_elements;
};


int push(struct stack *index);
int pop(struct stack *index);
int peek(struct stack *index);
int is_empty(struct stack *index);

int journal_del(off_t offset, void *key, int key_type);
int write_index_journal(struct stack *index);

#endif
