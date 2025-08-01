#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include "key.h"
#include "record.h"
#include "end_points.h"
#include "hash_tbl.h"
#include "file.h"


#define ORDER_BASE 100

uint32_t generate_numeric_key(int *fds, int end_point)
{
	uint32_t key = 0;
	switch(end_point){
	case NEW_SORD: 
	{
		HashTable ht = {0};
		HashTable *p_ht = &ht;
		if(!read_index_nr(0,fds[0],&p_ht)){
			/*log failure*/
			return 0;
		}

		int elements = len(ht);
		key = elements + ORDER_BASE;

		destroy_hasht(p_ht);
		break;
	}
	default:
		return 0;
	}

	return key;
}
