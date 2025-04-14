#ifndef _JOURNAL_H_
#define _JOURNAL_H_

#include "hash_tbl.h"

#define JINX "journal.inx"
#define J_DEL "jornal_del.db"
#define J_ADD "journal_add.db"

#define DEL_REG 1 	/*0000 0001*/
#define DEL_ORIG 0 	/*0000 0000*/

#define DEL_INX 0

int journal_del(off_t offset, void *key, int key_type);
int journal_add(off_t offset, void *key);


#endif
