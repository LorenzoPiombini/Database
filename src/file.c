#if defined(__linux__)
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#endif /*linux*/

#include <string.h>
#include <stdarg.h>
#include <math.h>
#include <errno.h>
#include "file.h"
#include "str_op.h"
#include "common.h"
#include "endian.h"
#include "debug.h"
#include "memory.h"
#include "lock.h"

static char prog[] = "db";
static int is_array_last_block(int fd, struct Ram_file *ram, int element_nr, size_t bytes_each_element, int type);
static size_t get_string_size(int fd, struct Ram_file *ram);
static size_t get_disk_size_record(struct Record_f *rec);
static int init_ram_file(struct Ram_file *ram, size_t size);
static void move_ram_file_ptr(struct Ram_file *ram,size_t size);
#if defined(_WIN32)
static off_t seek_file_win(HANDLE file_handle,long long offset, DWORD file_position);
#endif

#if defined(__linux__)
int open_file(char *fileName, int use_trunc)
{
	int fd = 0;
	errno = 0;
	if (!use_trunc) {
		fd = open(fileName, O_RDWR , S_IRWXU);
	} else {
		fd = open(fileName, O_WRONLY | O_TRUNC, S_IRWXU);
	}

	if ( errno != 0) return errno;			
	return fd;
}

int create_file(char *fileName)
{
	int fd = open(fileName, O_RDONLY);
	if (fd != STATUS_ERROR) {
		printf("File already exist.\n");
		close(fd);
		return STATUS_ERROR;
	}

	fd = open(fileName, O_RDWR | O_CREAT, S_IRWXU);

	if (fd == STATUS_ERROR) {
		perror("open");
		return STATUS_ERROR;
	}

	return fd;
}

void close_file(int count, ...)
{
	int sum = 0;
	short i = 0;

	va_list args;
	va_start(args, count);

	for (i = 0; i < count; i++) {
		int fd = va_arg(args, int);
		if (close(fd) != 0)
			sum++;
	}

	if (sum > 0)
		perror("close");
}

void delete_file(unsigned short count, ...)
{
	int sum = 0;
	short i = 0;
	va_list args;
	va_start(args, count);

	for (i = 0; i < count; i++) {
		char *file_name = va_arg(args, char *);
		if (unlink(file_name) != 0)
			sum++;
	}

	if (sum > 0)
		perror("unlink");
}

off_t begin_in_file(int fd)
{

	off_t pos = lseek(fd, 0, SEEK_SET);
	if (pos == STATUS_ERROR) {
		perror("set begin in file");
		return pos;
	}

	return pos;
}

off_t get_file_offset(int fd)
{

	off_t offset = lseek(fd, 0, SEEK_CUR);

	if (offset == STATUS_ERROR)
	{
		perror("get offset: ");
		return offset;
	}

	return offset;
}

off_t go_to_EOF(int fd)
{
	off_t eof = lseek(fd, 0, SEEK_END);
	if (eof == STATUS_ERROR)
	{
		perror("could not find end of file");
		return eof;
	}

	return eof;
}

off_t find_record_position(int fd, off_t offset)
{
	off_t pos = lseek(fd, offset, SEEK_SET);
	if (pos == STATUS_ERROR)
	{
		perror("seeking offset.");
		return pos;
	}

	return pos;
}

/*
 * move_in_file_bytes will change the file pointer of offset bytes
 *	example:
 *		move_in_file_bytes(fd, -4); will move the file pointer backwords of 4 bytes
 *
 * */
off_t move_in_file_bytes(int fd, off_t offset)
{
	off_t current_p = get_file_offset(fd);
	off_t move_to = current_p + offset;
	off_t pos = 0;
	if ((pos = lseek(fd, move_to, SEEK_SET)) == STATUS_ERROR) {
		perror("seeking offset.");
		return pos;
	}

	return pos;
}

off_t get_file_size(int fd, char *file_name)
{
	struct stat st;
	if (!file_name)
	{
		if (fstat(fd, &st) == -1)
		{
			perror("stat failed :");
			printf("%s:%d.\n", F, L - 3);
			return -1;
		}

		return (off_t)st.st_size;
	}
	else
	{
		if (stat(file_name, &st) == -1)
		{
			perror("stat failed :");
			printf("%s:%d.\n", F, L - 3);
			return -1;
		}

		return (off_t)st.st_size;
	}
}
#endif /*linux*/

#if defined(__linux__)
	unsigned char write_index_file_head(int fd, int index_num)
#elif defined(_WIN32)
	unsigned char write_index_file_head(HANDLE file_handle,int index_num)
#endif
{

	int i = 0;
	off_t pos = 0;

	uint32_t in = swap32(index_num);
#if defined(__linux__)
	if (write(fd, &in, sizeof(in)) == -1){
#elif defined(_WIN32)
	DWORD written = 0;
	if(!WriteFile(file_handle,&in,sizeof(uint32_t),&written,NULL)){
#endif
		printf("write to file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	for (i = 0; i < index_num; i++)
	{
		uint64_t p_n = swap64(pos);
#if defined(__linux__)
		if (write(fd, &p_n, sizeof(p_n)) == -1){
#elif defined(_WIN32)
		written = 0;
		if(!WriteFile(file_handle,&p_n,sizeof(uint64_t),&written,NULL)){
#endif
			printf("write to file failed, %s:%d.\n", F, L - 2);
			return 0;
		}
	}

	return 1;
}
#if defined(__linux__)
unsigned char write_index_body(int fd, int i, HashTable *ht)
#elif defined(_WIN32)
unsigned char write_index_body(HANDLE file_handle, int i, HashTable *ht)
#endif
{
	off_t pos = 0;

	uint32_t i_n = swap32(i);
#if defined(__linux__)
	if (write(fd, &i_n, sizeof(i_n)) == -1){
#elif defined(_WIN32)
	DWORD written = 0;
	if (WriteFile(file_handle,&i_n,sizeof(i_n),&written,NULL)){
#endif
		printf("write to file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	if ((pos = get_file_offset(fd)) == -1)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	if (!ht->write(fd, ht))
	{
		printf("write to file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	if (begin_in_file(fd) == -1)
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}
#if defined(__linux__)
	if (find_record_position(fd, sizeof(int)) == -1) {
#elif defined(_WIN32)
	if (find_record_position(file_handle, sizeof(int)) == -1) {
#endif
		__er_file_pointer(F, L - 2);
		return 0;
	}

	if (i != 0) {
#if defined(__linux__)
		if (move_in_file_bytes(fd, i * sizeof(pos)) == -1) {
#elif defined(_WIN32)
		if (move_in_file_bytes(file_handle, i * sizeof(pos)) == -1) {
#endif
			__er_file_pointer(F, L - 2);
			return 0;
		}
	}

	uint64_t p_n = swap64(pos);
#if defined(__linux__)
	if (write(fd, &p_n, sizeof(p_n)) == -1){
#elif defined(_WIN32)
	written = 0;
	if (WriteFile(file_handle,&p_n,sizeof(p_n),&written,NULL)){
#endif
		printf("write to file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

#if defined(__linux__)
	if (go_to_EOF(fd) == STATUS_ERROR)
#elif defined(_WIN32)
	if (go_to_EOF(file_handle) == STATUS_ERROR)
#endif
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}
	return 1;
}

#if defined(__linux__)
unsigned char read_index_nr(int i_num, int fd, HashTable **ht)
#elif defined(_WIN32) 
unsigned char read_index_nr(int i_num, HANDLE file_handle, HashTable **ht)
#endif
{
#if defined(__linux__)
	if (begin_in_file(fd) == STATUS_ERROR)
#elif defined(_WIN32) 
	if (begin_in_file(file_handle) == STATUS_ERROR)
#endif
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	uint32_t a_s = 0;
#if defined(__linux__)
	if (read(fd, &a_s, sizeof(a_s)) == STATUS_ERROR)
#elif defined(_WIN32) 
	DWORD written = 0;
	if (!ReadFile(file_handle,&a_s,sizeof(a_s),&written,NULL))
#endif
	{
		fprintf(stderr,"read from file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	int array_size = (int)swap32(a_s);
	if (array_size == 0)
	{
		printf("wrong reading from file, check position. %s:%d.\n", F, L - 8);
		return 0;
	}

	if (array_size <= i_num)
	{
		printf("index number out of bound.\n");
		return 0;
	}

	off_t move_to = i_num * sizeof(off_t);
#if defined(__linux__)
	if (move_in_file_bytes(fd, move_to) == STATUS_ERROR)
#elif defined(_WIN32) 
	if (move_in_file_bytes(file_handle, move_to) == STATUS_ERROR)
#endif
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

	uint64_t i_p = 0;
#if defined(__linux__)
	if (read(fd, &i_p, sizeof(i_p)) == STATUS_ERROR)
#elif defined(_WIN32)
	written = 0;
	if (!ReadFile(file_handle,&a_s,sizeof(a_s),&written,NULL))
#endif
	{
		printf("read from fiel failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	off_t index_pos = (off_t)swap64(i_p);
	if (index_pos == 0)
	{
		printf("wrong reading from file, check position. %s:%d.\n", F, L - 8);
		return 0;
	}
#if defined(__linux__)
	if (find_record_position(fd, index_pos) == STATUS_ERROR)
#elif defined(_WIN32)
	if (find_record_position(file_handle, index_pos) == STATUS_ERROR)
#endif
	{
		__er_file_pointer(F, L - 2);
		return 0;
	}

#if defined(__linux__)
	if (!read_index_file(fd, *ht)){
#elif defined(_WIN32)
	if (!read_index_file(file_handle, *ht)){
#endif
		printf("read from file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	return 1;
}

#if defined(__linux__)
unsigned char indexes_on_file(int fd, int *p_i_nr)
#elif defined(_WIN32)
unsigned char indexes_on_file(HANDLE file_handle, int *p_i_nr)
#endif
{

#if defined(__linux__)
	if (begin_in_file(fd) == STATUS_ERROR) {
#elif defined(_WIN32)
	if (begin_in_file(file_handle) == STATUS_ERROR) {
#endif
		__er_file_pointer(F, L - 2);
		return 0;
	}

	uint32_t a_s = 0;
#if defined(__linux__)
	if (read(fd, &a_s, sizeof(a_s)) == STATUS_ERROR) {
#elif defined(_WIN32)
	DWORD written = 0;
	if (!ReadFile(file_handle,&a_s,sizeof(a_s),&written,NULL)){
#endif
		printf("read from file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	int array_size = (int)swap32(a_s);
	if (array_size == 0) {
		printf("wrong reading from file, check position. %s:%d.\n", F, L - 8);
		return 0;
	}

	*p_i_nr = array_size;
	return 1;
}

#if defined(__linux__)
unsigned char nr_bucket(int fd, int *p_buck)
#elif defined(_WIN32)
unsigned char nr_bucket(HANDLE file_handle, int *p_buck)
#endif
{
	HashTable ht = {0};
	HashTable *pht = &ht;
#if defined(__linux__)
	if (!read_index_nr(0, fd, &pht)) {
#elif defined(_WIN32)
	if (!read_index_nr(0,file_handle, &pht)) {
#endif
		printf("read from file failed. %s:%d.\n", F, L - 2);
		if (ht.size > 0) {
			destroy_hasht(&ht);
		}
		return 0;
	}

	*p_buck = ht.size;
	destroy_hasht(&ht);
	return 1;
}

#if defined(__linux__)
unsigned char read_all_index_file(int fd, HashTable **ht, int *p_index)
#elif defined(_WIN32)
unsigned char read_all_index_file(HANDLE file_handle, HashTable **ht, int *p_index)
#endif
{

#if defined(__linux__)
	if (begin_in_file(fd) == STATUS_ERROR) {
#elif defined(_WIN32)
	if (begin_in_file(file_handle) == STATUS_ERROR) {
#endif
		__er_file_pointer(F, L - 2);
		return 0;
	}

	uint32_t a_s = 0;
#if defined(__linux__)
	if (read(fd, &a_s, sizeof(a_s)) == STATUS_ERROR) {
#elif defined(_WIN32)
	if (!ReadFile(file_handle,&a_s,sizeof(a_s),&written,NULL)){
#endif
		printf("read from file failed. %s:%d.\n", F, L - 2);
		return 0;
	}

	int array_size = (int)swap32(a_s);
	if (array_size == 0) {
		printf("wrong reading from file, check position. %s:%d.\n", F, L - 8);
		return 0;
	}

	*p_index = array_size;

	*ht = (HashTable*)ask_mem(array_size * sizeof(HashTable));
	if (!*ht) {
		printf("ask_mem failed. %s:%d.\n", F, L - 3);
		return 0;
	}

	int i = 0;
	for (i = 0; i < array_size; i++)
		(*ht)[i].write = write_ht;

	off_t move_to = (array_size * sizeof(off_t)) + sizeof(int);
#if defined(__linux__)
	if (move_in_file_bytes(fd, move_to) == STATUS_ERROR) {
#elif defined(_WIN32)
	if (move_in_file_bytes(file_handle, move_to) == STATUS_ERROR) {
#endif
		__er_file_pointer(F, L - 2);
		cancel_memory(NULL,ht,array_size * sizeof *ht);
		return 0;
	}

	for (i = 0; i < array_size; i++)
	{
#if defined(__linux__)
		if (!read_index_file(fd, &((*ht)[i])))
#elif defined(_WIN32)
		if (!read_index_file(file_handle, &((*ht)[i])))
#endif
		{
			printf("read from file failed. %s:%d.\n", F, L - 2);
			free_ht_array(*ht, i);
			return 0;
		}

		if ((array_size - i) > 1)
		{
#if defined(__linux__)
			if (move_in_file_bytes(fd, sizeof(int)) == STATUS_ERROR)
#elif defined(_WIN32)
			if (move_in_file_bytes(file_handle, sizeof(int)) == STATUS_ERROR)
#endif
			{
				__er_file_pointer(F, L - 2);
				free_ht_array(*ht, i);
				return 0;
			}
		}
	}
	return 1;
}


#if defined(__linux__)
unsigned char read_index_file(int fd, HashTable *ht)
#elif defined(_WIN32)
unsigned char read_index_file(HANDLE file_handle, HashTable *ht)
#endif
{
	uint32_t s_n = 0;
#if defined(__linux__)
	if (read(fd, &s_n, sizeof(s_n)) < 0)
#elif defined(_WIN32)
	DWORD written = 0;
	if (!ReadFile(file_handle,&s_n,sizeof(s_n),&written,NULL))
#endif
	{
		perror("reading Index file");
		return 0;
	}

	ht->size = (int)swap32(s_n); 

	uint32_t ht_ln = 0;
#if defined(__linux__)
	if (read(fd, &ht_ln, sizeof(ht_ln)) == STATUS_ERROR) {
#elif defined(_WIN32)
	written = 0;
	if (!ReadFile(file_handle,&ht_ln,sizeof(ht_ln),&written,NULL))
#endif
		perror("reading ht length");
		return 0;
	}
	
	int ht_l = (int)swap32(ht_ln);
	register int i = 0;
	for (i = 0; i < ht_l; i++) {
		uint32_t type = 0;
#if defined(__linux__)
		if (read(fd, &type, sizeof(type)) == -1) 
#elif defined(_WIN32)
		written = 0;
		if (!ReadFile(file_handle,&type,sizeof(type),&written,NULL))
#endif
		{
			fprintf(stderr, "can't read key type, %s:%d.\n",
					F, L - 3);
			free_nodes(ht->data_map, ht->size);
			return 0;
		}

		int key_type = (int)swap32(type);

		switch (key_type) {
		case STR:
		{
			uint64_t key_l = 0;
#if defined(__linux__)
			if (read(fd, &key_l, sizeof(key_l)) > 0) {
#elif defined(_WIN32)
			written = 0;
			if (!ReadFile(file_handle,&key_l,sizeof(key_l),&written,NULL))
#endif
				size_t size = (size_t)swap64(key_l);
				char *key = (char*)ask_mem((size + 1)*sizeof(char));
				if (!key) {
					fprintf(stderr,"(%s): ask_mem() failed, %s:%d.\n",prog,F,L-2);		
					free_nodes(ht->data_map, ht->size);
					return 0;
				}

				uint64_t v_n = 0l;
#if defined(__linux__)
				if (read(fd, key, size + 1) == -1 ||
					read(fd, &v_n, sizeof(v_n)) == -1) 
#elif defined(_WIN32)
				written = 0;
				if (!ReadFile(file_handle,&key,sizeof(key),&written,NULL) ||
					 !ReadFile(file_handle,&v_n,sizeof(v_n),&written,NULL))
#endif
				{


					fprintf(stderr,"(%s): read key failed, %s:%d.\n",prog,F,L-2);		
					free_nodes(ht->data_map, ht->size);
					cancel_memory(NULL,key,(size+1)*sizeof(char));
					return 0;
				}

				off_t value = (off_t)swap64(v_n);
				key[size] = '\0';
				Node *newNode = (Node*)ask_mem(1*sizeof(Node));
				if (!newNode)
				{
					perror("memory for node");
					free_nodes(ht->data_map, ht->size);
					cancel_memory(NULL,key,(size+1)*sizeof(char));
					return 0;
				}

				newNode->key.k.s = duplicate_str(key);
				if (!newNode->key.k.s){
					fprintf(stderr,"strdup() failed, %s:%d.\n",F, L - 3);
					free_nodes(ht->data_map, ht->size);
					cancel_memory(NULL,key,(size+1)*sizeof(char));
					return 0;
				}
				cancel_memory(NULL,key,(size+1)*sizeof(char));
				newNode->value = value;
				newNode->next = NULL;
				newNode->key.type = key_type;

				int bucket = hash((void *)newNode->key.k.s, ht->size, key_type);
				if (ht->data_map[bucket]){
					Node *current = ht->data_map[bucket];
					while (current->next != NULL)
					{
						current = current->next;
					}
					current->next = newNode;
				}
				else{
					ht->data_map[bucket] = newNode;
				}
			}
			else
			{
				fprintf(stderr,"read index failed, %s:%d\n", F, L - 2);
				free_nodes(ht->data_map, ht->size);
				return 0;
			}
			break;
		}
		case UINT:
		{
			uint8_t size = 0;
			uint32_t k = 0;
			uint16_t k16 = 0;
			uint64_t value = 0;
#if defined(__linux__)
			if (read(fd, &size, sizeof(size)) == -1)
#elif defined(_WIN32)
			written = 0;
			if (!ReadFile(file_handle,&size,sizeof(size),&written,NULL))
#endif
			{
				fprintf(stderr, "read index failed.\n");
				free_nodes(ht->data_map, ht->size);
				return 0;
			}
			if(size == 16){
#if defined(__linux__)
				if (read(fd, &k16, sizeof(k16)) == -1)
#elif defined(_WIN32)
				written = 0;
				if (!ReadFile(file_handle,&k16,sizeof(k16),&written,NULL))
#endif
				{	
					fprintf(stderr, "read index failed.\n");
					free_nodes(ht->data_map, ht->size);
					return 0;
				}
			}else{
#if defined(__linux__)
				if (read(fd, &k, sizeof(k)) == -1){
#elif defined(_WIN32)
				written = 0;
				if (!ReadFile(file_handle,&k,sizeof(k),&written,NULL))
#endif
				{
					fprintf(stderr, "read index failed.\n");
					free_nodes(ht->data_map, ht->size);
					return 0;
				}

			}

#if defined(__linux__)
			if (read(fd, &value, sizeof(value)) == -1)
#elif defined(_WIN32)
			written = 0;
			if (!ReadFile(file_handle,&value,sizeof(value),&written,NULL))
#endif
			{
				fprintf(stderr, "read index failed.\n");
				free_nodes(ht->data_map, ht->size);
				return 0;
			}

			Node *new_node = (Node*)ask_mem(sizeof(Node));
			if (!new_node){
				fprintf(stderr,"ask_mem failed, %s:%d.\n",F,L-2);
				free_nodes(ht->data_map, ht->size);
				return 0;
			}
			memset(new_node,0,sizeof *new_node);
			new_node->key.type = key_type;
			if(size == 16)
				new_node->key.k.n16 = swap16(k16);
			else
				new_node->key.k.n = swap32(k);

			new_node->value = (off_t)swap64(value);
			new_node->next = NULL;
			new_node->key.size = size;

			int index = hash((void *)&new_node->key.k.n, ht->size, key_type);
			if (index == -1){
				fprintf(stderr, "read index failed.%s:%d\n", F, L - 2);
				free_nodes(ht->data_map, ht->size);
				free_ht_node(new_node);
				return 0;
			}

			if (ht->data_map[index]){
				Node *current = ht->data_map[index];
				if(current->next){
					while (current->next) current = current->next;
					current->next = new_node;
				}else{
					current->next = new_node;
				}
			}
			else{
				ht->data_map[index] = new_node;
			}

			break;
		}
		default:
			fprintf(stderr, "key type not supported.\nsize ht is %d\nht_l is %d\n",ht->size,ht_l);
			free_nodes(ht->data_map, ht->size);
			return 0;
		}
	}

	return 1; /*true*/
}

size_t record_size_on_disk(void *rec_f)
{
	size_t rec_size = 0;
	struct Record_f *rec = (struct Record_f *)rec_f;

	rec_size += sizeof(rec->fields_num);
	/*each field name length*/
	rec_size += (sizeof(uint64_t) * rec->fields_num);

	/*each field type*/
	rec_size += (sizeof(uint32_t) * rec->fields_num);

	int i;
	for (i = 0; i < rec->fields_num; i++)
	{
		/*actual name length wrote to disk*/
		rec_size += strlen(rec->fields[i].field_name);

		switch (rec->fields[i].type)
		{
		case TYPE_INT:
		case TYPE_FLOAT:
		case TYPE_DATE:
			rec_size += sizeof(uint32_t);
			break;
		case TYPE_LONG:
		case TYPE_DOUBLE:
			rec_size += sizeof(uint64_t);
			break;
		case TYPE_BYTE:
			rec_size += sizeof(uint16_t);
			break;
		case TYPE_STRING:
			rec_size += (sizeof(uint64_t) * 3);
			rec_size += (strlen(rec->fields[i].data.s) * 2) + 1;
			break;
		default:
			printf("unknown type");
			return -1;
		}
	}

	/*any eventual update position*/
	rec_size += sizeof(uint64_t);
	return rec_size;
}

int write_file(int fd, struct Record_f *rec, off_t update_off_t, unsigned char update)
{
	off_t go_back_to = 0;
	        
	
	/* ----------these variables are used to handle the strings-------- */
	/* now each string fields can be updated regardless the string size */
	/* ----------some realities might required such a feature----------- */

	size_t lt = 0, new_lt = 0;
	off_t str_loc = 0, af_str_loc_pos = 0, eof = 0;
	size_t buff_update = 0;
	size_t __n_buff_update = 0;
	off_t move_to = 0, bg_pos = 0;

	/*--------------------------------------------------*/

	/*count the active fields in the record*/
	uint8_t count = 0;

	int i;
	for(i = 0; i < rec->fields_num; i++){
		if (rec->field_set[i] == 1)  count++;
	}

	if(count == 0) return NTG_WR;

	/*
	 * writes the number of fields active 
	 * that are going to be written to the file
	 * */

	if (write(fd, &count, sizeof(count)) < 0) {
		perror("could not write fields number");
		return 0;
	}


	for(i = 0; i < rec->fields_num; i++){
		if (rec->field_set[i] == 0) continue;

		/*position in the field_set array*/
		uint8_t i_ne = (uint8_t)i;
		if (write(fd, &i_ne, sizeof(i_ne)) < 0) {
			perror("could not write fields number");
			return 0;
		}
	}

	for (i = 0; i < rec->fields_num; i++) {
		if(rec->field_set[i] == 0) continue;

		switch (rec->fields[i].type){
		case TYPE_INT:
		{
			uint32_t i_ne = swap32(rec->fields[i].data.i);
			if (write(fd, &i_ne, sizeof(i_ne)) < 0){
				perror("error in writing int type to file.\n");
				return 0;
			}
			break;
		}
		case TYPE_LONG: 
		{
			uint64_t l_ne = swap64((uint64_t)rec->fields[i].data.l);
			if (write(fd, &l_ne, sizeof(l_ne)) < 0) {
				perror("error in writing long type to file.\n");
				return 0;
			}
			break;
		}
		case TYPE_FLOAT:
		{
			uint32_t f = htonf(rec->fields[i].data.f);
			if (write(fd, &f, sizeof(f)) < 0)
			{
				perror("error in writing float type.\n");
				return 0;
			}
			break;
		}
		case TYPE_STRING:
		{
			if (!update)
			{
				lt = strlen(rec->fields[i].data.s);
				buff_update = lt * 2;

				/* adding 1 for '\0'*/
				lt++, buff_update++;
				char buff_w[buff_update];
				memset(buff_w,0,buff_update);

				strncpy(buff_w, rec->fields[i].data.s, lt - 1);

				uint16_t bu_ne = swap16((uint16_t)buff_update);
				uint32_t str_loc_ne = swap32((uint32_t)str_loc);

				if (write(fd, &str_loc_ne, sizeof(str_loc_ne)) < 0 ||
					write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
					write(fd, buff_w, buff_update) < 0){
					perror("write file failed: ");
					printf(" %s:%d", F, L - 2);
					return 0;
				}
			}
			else
			{

				__n_buff_update = 0;
				eof = 0;
				/*save the starting offset for the string record*/
				if ((bg_pos = get_file_offset(fd)) == STATUS_ERROR){
					__er_file_pointer(F, L - 2);
					return 0;
				}

				/*read pos of new str if any*/
				uint32_t str_loc_ne = 0;
				if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == STATUS_ERROR)
				{
					perror("can't read string location: ");
					printf(" %s:%d", F, L - 3);
					return 0;
				}

				str_loc = (off_t)swap32(str_loc_ne);

				/*store record beginning pos*/
				if ((af_str_loc_pos = get_file_offset(fd)) == STATUS_ERROR)
				{
					__er_file_pointer(F, L - 2);
					return 0;
				}

				uint16_t bu_ne = 0;
				if (read(fd, &bu_ne, sizeof(bu_ne)) < 0){
					perror("can't read safety buffer before writing string.\n");
					printf("%s:%d", F, L - 3);
					return 0;
				}

				buff_update = (off_t)swap16(bu_ne);

				/*save the end offset of the first string record */
				if((go_back_to = get_file_offset(fd)) == STATUS_ERROR)
				{
					__er_file_pointer(F, L - 2);
					return 0;
				}

				/*add the buffer size to this file off_t*/
				/*so we can reposition right after the 1st string after the writing */
				go_back_to += buff_update;
				if (str_loc > 0)
				{
					/*set the file pointer to str_loc*/
					if (find_record_position(fd, str_loc) == STATUS_ERROR)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
					/*
					 * in the case of a regular buffer update we have
					 *  to save the off_t to get back to it later
					 * */
					if ((move_to = get_file_offset(fd)) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}

					uint16_t bu_ne = 0;
					if (read(fd, &bu_ne, sizeof(bu_ne)) < 0){
						perror("read file.\n");
						printf("%s:%d", F, L - 3);
						return 0;
					}

					buff_update = (off_t)swap16(bu_ne);
				}

				new_lt = strlen(rec->fields[i].data.s) + 1; /*get new str length*/

				if (new_lt > buff_update) {
					/*
					 * if the new length is bigger then the buffer,
					 * set the file pointer to th of the file
					 * to write the new data
					 * */
					if ((eof = go_to_EOF(fd)) == STATUS_ERROR){
						__er_file_pointer(F, L - 2);
						return 0;
					}

					/*expand the buff_update only for the bytes needed*/
					buff_update += (new_lt - buff_update);
				}
				char buff_w[buff_update];
				memset(buff_w,0,buff_update);

				strncpy(buff_w, rec->fields[i].data.s, new_lt - 1);
				/*
				 * if we did not move to another position
				 * set the file pointer back to the begginning of the string record
				 * to overwrite the data accordingly
				 * */
				if (str_loc == 0 && (__n_buff_update == 0))
				{
					if (find_record_position(fd, af_str_loc_pos) == -1)
					{
						__er_file_pointer(F, L - 3);
						return 0;
					}
				}
				else if (str_loc > 0 && (__n_buff_update == 0))
				{
					if (find_record_position(fd, move_to) == STATUS_ERROR)
					{
						__er_file_pointer(F, L - 3);
						return 0;
					}
				}

				/*
				 * write the data to file --
				 * the file pointer is always pointing to the
				 * right position at this point */
				bu_ne = swap16((uint16_t)buff_update);

				if (write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
					write(fd, buff_w, buff_update) < 0){
					perror("error in writing type string (char *)file.\n");
					return 0;
				}


				/*
				 * if eof is bigger than 0 means we updated the string
				 * we need to save the off_t of the new written data
				 * at the start of the original.
				 * */
				if (eof > 0)
				{
					/*go at the beginning of the str record*/
					if (find_record_position(fd, bg_pos) == STATUS_ERROR)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}

					/*update new string position*/
					uint32_t eof_ne = swap32((uint32_t)eof);
					if (write(fd, &eof_ne, sizeof(eof_ne)) == STATUS_ERROR)
					{
						perror("write file: ");
						printf(" %s:%d", F, L - 3);
						return 0;
					}

					/*set file pointer to the end of the 1st string rec*/
					/*this step is crucial to avoid losing data        */

					if (find_record_position(fd, go_back_to) == STATUS_ERROR)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
				else if (str_loc > 0)
				{
					/*
					 * Make sure that in all cases
					 * we go back to the end of the 1st record
					 * */
					if (find_record_position(fd, go_back_to) == STATUS_ERROR){
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}
			break;
		}
		case TYPE_BYTE:
		{
			if (write(fd, &rec->fields[i].data.b, sizeof(unsigned char)) < 0){
				perror("error in writing type byte to file.\n");
				return 0;
			}
			break;
		}
		case TYPE_PACK:
		{
			uint32_t p_ne = swap32(rec->fields[i].data.p);
			if (write(fd, &p_ne, sizeof(p_ne)) < 0)
			{
				perror("error in writing type byte to file.\n");
				return 0;
			}
			break;
		}
		case TYPE_DOUBLE:
		{
			uint64_t d_ne = htond(rec->fields[i].data.d);
			if (write(fd, &d_ne, sizeof(d_ne)) < 0)
			{
				perror("error in writing double to file.\n");
				return 0;
			}
			break;
		}
		case TYPE_ARRAY_INT:
		{
			if (!update)
			{
				/*write the size of the array */
				uint32_t size_ne = swap32((uint32_t)rec->fields[i].data.v.size);
				if (write(fd, &size_ne, sizeof(size_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				uint32_t padding_ne = swap32(0);
				if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				int k;
				for (k = 0; k < rec->fields[i].data.v.size; k++)
				{
					if (!rec->fields[i].data.v.elements.i[k])
						continue;

					uint32_t num_ne = swap32(rec->fields[i].data.v.elements.i[k]);
					if (write(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				uint64_t upd_ne = 0;
				if (write(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}
			}
			else
			{
				/* update branch*/
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int step = 0;
				int sz = 0;
				int k = 0;
				int padding_value = 0;
				do
				{
					/* check the size */
					uint32_t sz_ne = 0;
					if (read(fd, &sz_ne, sizeof(sz_ne)) == -1)
					{
						fprintf(stderr, "can't read int array size.\n");
						return 0;
					}

					sz = (int)swap32(sz_ne);
					if (rec->fields[i].data.v.size < sz ||
						rec->fields[i].data.v.size == sz)
						break;

					/*read the padding data*/
					uint32_t pd_ne = 0;
					if (read(fd, &pd_ne, sizeof(pd_ne)) == -1)
					{
						fprintf(stderr, "can't read padding array.\n");
						return 0;
					}

					padding_value = (int)swap32(pd_ne);

					if (step >= sz)
					{
						int array_last = 0;
						int exit = 0;
						if ((array_last = is_array_last_block(fd,NULL, sz, sizeof(int),0)) == -1)
						{
							fprintf(stderr, "can't verify array last block %s:%d.\n", F, L - 1);
							return 0;
						}

						if (rec->fields[i].data.v.size < (sz + step) && array_last)
						{
							int pad_value = sz - (rec->fields[i].data.v.size - step);
							padding_value += pad_value;

							sz = rec->fields[i].data.v.size - step;
							exit = 1;

							if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
							{
								__er_file_pointer(F, L - 1);
								return 0;
							}

							/* write the updated size of the array */
							uint32_t new_sz = swap32((uint32_t)sz);
							if (write(fd, &new_sz, sizeof(new_sz)) == -1)
							{
								perror("error in writing remaining size int array.\n");
								return 0;
							}

							/* write the updated padding value */
							uint32_t new_pd = swap32((uint32_t)padding_value);
							if (write(fd, &new_pd, sizeof(new_pd)) == -1)
							{
								perror("error in writing new pading value int array.\n");
								return 0;
							}
						}
						else if (rec->fields[i].data.v.size == (sz + step) && array_last)
						{
							exit = 1;
						}

						while (sz)
						{
							if (step < rec->fields[i].data.v.size)
							{
								uint32_t num_ne = swap32(rec->fields[i].data.v.elements.i[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
							sz--;
						}

						if (exit)
						{
							/*write the epty update offset*/
							uint64_t empty_offset = swap64(0);
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}
					else
					{

						int exit = 0;
						for (k = 0; k < sz; k++)
						{
							if (step > 0 && k == 0)
							{
								if ((step + sz) > rec->fields[i].data.v.size)
								{
									int pad = sz - (rec->fields[i].data.v.size - step);
									padding_value += pad;

									sz = rec->fields[i].data.v.size - step;
									exit = 1;

									if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
									{
										__er_file_pointer(F, L - 1);
										return 0;
									}

									/* write the updated size of the array */
									uint32_t new_sz = swap32((uint32_t)sz);
									if (write(fd, &new_sz, sizeof(new_sz)) == -1)
									{
										perror("error in writing remaining size int array.\n");
										return 0;
									}

									/*write padding */
									uint32_t new_pd = swap32((uint32_t)padding_value);
									if (write(fd, &new_pd, sizeof(new_pd)) == -1)
									{
										perror("error in writing new padd int array.\n");
										return 0;
									}
								}
							}

							if (step < rec->fields[i].data.v.size)
							{
								if (!rec->fields[i].data.v.elements.i[step])
									continue;

								uint32_t num_ne = swap32(rec->fields[i].data.v.elements.i[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
								if(!(step < rec->fields[i].data.v.size)) exit = 0;
							}
						}

						if (exit)
						{

							if (padding_value > 0)
							{
								if (move_in_file_bytes(fd, padding_value * sizeof(int)) == -1)
								{
									__er_file_pointer(F, L - 1);
									return 0;
								}
							}
							/*write the epty update offset*/
							uint64_t empty_offset = swap64(0);
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}

					if (padding_value > 0)
					{
						if (move_in_file_bytes(fd, padding_value * sizeof(int)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
					}

					uint64_t update_off_ne = 0;
					off_t go_back_to = get_file_offset(fd);

					if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
					{
						perror("failed read update off_t int array.\n");
						return 0;
					}

					if (go_back_to_first_rec == 0)
						go_back_to_first_rec = go_back_to + sizeof(update_off_ne);

					update_pos = (off_t)swap64(update_off_ne);
					if (update_pos == 0)
					{
						/*go to EOF*/
						if ((update_pos = go_to_EOF(fd)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
						/* write the size of the array */
						int size_left = rec->fields[i].data.v.size - step;
						uint32_t size_left_ne = swap32((uint32_t)size_left);
						if (write(fd, &size_left_ne, sizeof(size_left_ne)) == -1)
						{
							perror("error in writing remaining size int array.\n");
							return 0;
						}

						uint32_t padding_ne = 0;
						if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						int j;
						for(j = 0; j < size_left; j++){
							if (step < rec->fields[i].data.v.size)
							{
								if (!rec->fields[i].data.v.elements.i[step])
									continue;

								uint32_t num_ne = swap32(rec->fields[i].data.v.elements.i[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
						}

						/*write the empty update offset*/
						uint64_t empty_offset = swap64(0);
						if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						if (find_record_position(fd, go_back_to) == -1){
							__er_file_pointer(F, L - 1);
							return 0;
						}

						update_off_ne = (uint64_t)swap64((uint64_t)update_pos);
						if (write(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
						{
							fprintf(stderr, "can't write update position int array, %s:%d.\n",
									F, L - 1);
							return 0;
						}

						break;
					}

					if (find_record_position(fd, update_pos) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

				} while (update_pos > 0);

				if (rec->fields[i].data.v.size < sz)
				{

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/*write the size of the array */
					uint32_t size_ne = swap32((uint32_t)rec->fields[i].data.v.size);
					if (write(fd, &size_ne, sizeof(size_ne)) == -1)
					{
						perror("error in writing size array to file.\n");
						return 0;
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					pd_he += (sz - rec->fields[i].data.v.size);

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/* write the padding to apply after the  array */
					pad_ne = swap32((uint32_t)pd_he);
					if (write(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int j;
					for (j = step; j < rec->fields[i].data.v.size; j++)
					{
						if (!rec->fields[i].data.v.elements.i[j])
							continue;

						uint32_t num_ne = swap32(rec->fields[i].data.v.elements.i[j]);
						if (write(fd, &num_ne, sizeof(num_ne)) == -1)
						{
							perror("failed write int array to file");
							return 0;
						}
					}

					/*
					 * move the file pointer after the array
					 * as much as the pad
					 * */
					if (move_in_file_bytes(fd, pd_he * sizeof(int)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					uint64_t update_arr_ne = swap64(0);
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}
				else if (rec->fields[i].data.v.size == sz)
				{
					/*
					 * the sizes are the same
					 * we simply write the array.
					 * */
					if (step > 0)
					{
						if (move_in_file_bytes(fd, -sizeof(sz)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						int size_left = rec->fields[i].data.v.size - step;
						uint32_t sz_ne = swap32((uint32_t)size_left);
						if (write(fd, &sz_ne, sizeof(sz_ne)) == -1)
						{
							fprintf(stderr, "write failed %s:%d.\n", F, L - 1);
							return 0;
						}
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					int j;
					for(j = 0; j < rec->fields[i].data.v.size; j++)
					{
						if (step < rec->fields[i].data.v.size)
						{
							if (!rec->fields[i].data.v.elements.i[step])
								continue;

							uint32_t num_ne = swap32(rec->fields[i].data.v.elements.i[step]);
							if (write(fd, &num_ne, sizeof(num_ne)) == -1)
							{
								perror("failed write int array to file");
								return 0;
							}
							step++;
						}
					}

					/*
					 * move the file pointer
					 * as much as the padding value
					 * if it si bigger than 0
					 * */
					if (pd_he > 0)
					{
						if (move_in_file_bytes(fd, pd_he * sizeof(int)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}
					}

					uint64_t update_arr_ne = 0;
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				if (go_back_to_first_rec > 0)
				{
					if (find_record_position(fd, go_back_to_first_rec) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}

			break;
		}
		case TYPE_ARRAY_LONG:
		{
			if (!update)
			{
				/*write the size of the array */
				uint32_t size_ne = swap32((uint32_t)rec->fields[i].data.v.size);
				if (write(fd, &size_ne, sizeof(size_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				uint32_t padding_ne = swap32(0);
				if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				int j;
				for ( j = 0; j < rec->fields[i].data.v.size; j++)
				{
					uint64_t num_ne = swap64(rec->fields[i].data.v.elements.l[j]);
					if (write(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				uint64_t upd_ne = 0;
				if (write(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}
			}
			else
			{
				/* update branch*/
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int step = 0;
				int sz = 0;
				int k = 0;
				int padding_value = 0;
				do
				{
					/* check the size */
					uint32_t sz_ne = 0;
					if (read(fd, &sz_ne, sizeof(sz_ne)) == -1)
					{
						fprintf(stderr, "can't read int array size.\n");
						return 0;
					}

					sz = (int)swap32(sz_ne);
					if (rec->fields[i].data.v.size < sz ||
						rec->fields[i].data.v.size == sz)
						break;

					/*read the padding data*/
					uint32_t pd_ne = 0;
					if (read(fd, &pd_ne, sizeof(pd_ne)) == -1)
					{
						fprintf(stderr, "can't read padding array.\n");
						return 0;
					}

					padding_value = (int)swap32(pd_ne);

					if (step >= sz)
					{
						int array_last = 0;
						int exit = 0;
						if ((array_last = is_array_last_block(fd,NULL, sz, sizeof(long),0)) == -1)
						{
							fprintf(stderr, "can't verify array last block %s:%d.\n", F, L - 1);
							return 0;
						}

						if (rec->fields[i].data.v.size < (sz + step) && array_last)
						{
							int pad_value = sz - (rec->fields[i].data.v.size - step);
							padding_value += pad_value;

							sz = rec->fields[i].data.v.size - step;
							exit = 1;

							if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
							{
								__er_file_pointer(F, L - 1);
								return 0;
							}

							/* write the updated size of the array */
							uint32_t new_sz = swap32((uint32_t)sz);
							if (write(fd, &new_sz, sizeof(new_sz)) == -1)
							{
								perror("error in writing remaining size int array.\n");
								return 0;
							}

							/* write the updated padding value */
							uint32_t new_pd = swap32((uint32_t)padding_value);
							if (write(fd, &new_pd, sizeof(new_pd)) == -1)
							{
								perror("error in writing new pading value int array.\n");
								return 0;
							}
						}
						else if (rec->fields[i].data.v.size == (sz + step) && array_last)
						{
							exit = 1;
						}

						while (sz)
						{
							if (step < rec->fields[i].data.v.size)
							{
								uint64_t num_ne = swap64(rec->fields[i].data.v.elements.l[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
							sz--;
						}

						if (exit)
						{
							/*write the epty update offset*/
							uint64_t empty_offset = 0;
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}
					else
					{

						int exit = 0;
						for (k = 0; k < sz; k++)
						{
							if (step > 0 && k == 0)
							{
								if ((step + sz) > rec->fields[i].data.v.size)
								{
									int pad = sz - (rec->fields[i].data.v.size - step);
									padding_value += pad;

									sz = rec->fields[i].data.v.size - step;
									exit = 1;

									if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
									{
										__er_file_pointer(F, L - 1);
										return 0;
									}

									/* write the updated size of the array */
									uint32_t new_sz = swap32((uint32_t)sz);
									if (write(fd, &new_sz, sizeof(new_sz)) == -1)
									{
										perror("error in writing remaining size int array.\n");
										return 0;
									}

									/*write padding */
									uint32_t new_pd = swap32((uint32_t)padding_value);
									if (write(fd, &new_pd, sizeof(new_pd)) == -1)
									{
										perror("error in writing new padd int array.\n");
										return 0;
									}
								}
							}

							if (step < rec->fields[i].data.v.size)
							{
								uint64_t num_ne = swap64(rec->fields[i].data.v.elements.l[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
								if(!(step < rec->fields[i].data.v.size)) exit = 1;
							}
						}

						if (exit)
						{
							if (padding_value > 0)
							{
								if (move_in_file_bytes(fd, padding_value * sizeof(long)) == -1)
								{
									__er_file_pointer(F, L - 1);
									return 0;
								}
							}
							/*write the epty update offset*/
							uint64_t empty_offset = 0;
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}

					if (padding_value > 0)
					{
						if (move_in_file_bytes(fd, padding_value * sizeof(long)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
					}

					uint64_t update_off_ne = 0;
					off_t go_back_to = get_file_offset(fd);

					if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1){
						perror("failed read update off_t int array.\n");
						return 0;
					}

					if (go_back_to_first_rec == 0)
						go_back_to_first_rec = go_back_to + sizeof(update_off_ne);

					update_pos = (off_t)swap64(update_off_ne);
					if (update_pos == 0)
					{
						/*go to EOF*/
						if ((update_pos = go_to_EOF(fd)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
						/* write the size of the array */
						int size_left = rec->fields[i].data.v.size - step;
						uint32_t size_left_ne = swap32((uint32_t)size_left);
						if (write(fd, &size_left_ne, sizeof(size_left_ne)) == -1){
							perror("error in writing remaining size int array.\n");
							return 0;
						}

						uint32_t padding_ne = swap32(0);
						if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						int j;
						for (j = 0; j < size_left; j++)
						{
							if (step < rec->fields[i].data.v.size)
							{
								uint64_t num_ne = swap64(rec->fields[i].data.v.elements.l[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
						}

						/*write the empty update offset*/
						uint64_t empty_offset = 0;
						if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						if (find_record_position(fd, go_back_to) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}

						update_off_ne = (uint64_t)swap64((uint64_t)update_pos);
						if (write(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
						{
							fprintf(stderr, "can't write update position int array, %s:%d.\n",
									F, L - 1);
							return 0;
						}

						break;
					}

					if (find_record_position(fd, update_pos) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

				} while (update_pos > 0);

				if (rec->fields[i].data.v.size < sz)
				{

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/*write the size of the array */
					uint32_t size_ne = swap32((uint32_t)rec->fields[i].data.v.size);
					if (write(fd, &size_ne, sizeof(size_ne)) == -1)
					{
						perror("error in writing size array to file.\n");
						return 0;
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					pd_he += (sz - rec->fields[i].data.v.size);

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/* write the padding to apply after the  array */
					pad_ne = swap32((uint32_t)pd_he);
					if (write(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int k;
					for (k = step; k < rec->fields[i].data.v.size; k++)
					{
						uint64_t num_ne = swap64(rec->fields[i].data.v.elements.l[k]);
						if (write(fd, &num_ne, sizeof(num_ne)) == -1)
						{
							perror("failed write int array to file");
							return 0;
						}
					}

					/*
					 * move the file pointer after the array
					 * as much as the pad
					 * */
					if (move_in_file_bytes(fd, pd_he * sizeof(long)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					uint64_t update_arr_ne = 0;
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}
				else if (rec->fields[i].data.v.size == sz)
				{
					/*
					 * the sizes are the same
					 * we simply write the array.
					 * */
					if (step > 0)
					{
						if (move_in_file_bytes(fd, -sizeof(sz)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						int size_left = rec->fields[i].data.v.size - step;
						uint32_t sz_ne = swap32((uint32_t)size_left);
						if (write(fd, &sz_ne, sizeof(sz_ne)) == -1)
						{
							fprintf(stderr, "write failed %s:%d.\n", F, L - 1);
							return 0;
						}
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					int j;
					for (j = 0; k < rec->fields[i].data.v.size; j++)
					{
						if (step < rec->fields[i].data.v.size)
						{
							uint64_t num_ne = swap64(rec->fields[i].data.v.elements.l[step]);
							if (write(fd, &num_ne, sizeof(num_ne)) == -1)
							{
								perror("failed write int array to file");
								return 0;
							}
							step++;
						}
					}

					/*
					 * move the file pointer
					 * as much as the padding value
					 * if it si bigger than 0
					 * */
					if (pd_he > 0)
					{
						if (move_in_file_bytes(fd, pd_he * sizeof(long)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}
					}

					uint64_t update_arr_ne = 0;
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				if (go_back_to_first_rec > 0)
				{
					if (find_record_position(fd, go_back_to_first_rec) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}
			break;
		}
		case TYPE_ARRAY_FLOAT:
		{
			if (!update)
			{
				/*write the size of the array */
				uint32_t size_ne = swap32((uint32_t)rec->fields[i].data.v.size);
				if (write(fd, &size_ne, sizeof(size_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				uint32_t padding_ne = 0;
				if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				int k;
				for (k = 0; k < rec->fields[i].data.v.size; k++)
				{
					uint32_t num_ne = htonf(rec->fields[i].data.v.elements.f[k]);
					if (write(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				uint64_t upd_ne = 0;
				if (write(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}
			}
			else
			{
				/* update branch*/
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int step = 0;
				int sz = 0;
				int k = 0;
				int padding_value = 0;
				do
				{
					/* check the size */
					uint32_t sz_ne = 0;
					if (read(fd, &sz_ne, sizeof(sz_ne)) == -1)
					{
						fprintf(stderr, "can't read int array size.\n");
						return 0;
					}

					sz = (int)swap32(sz_ne);
					if (rec->fields[i].data.v.size < sz ||
						rec->fields[i].data.v.size == sz)
						break;

					/*read the padding data*/
					uint32_t pd_ne = 0;
					if (read(fd, &pd_ne, sizeof(pd_ne)) == -1)
					{
						fprintf(stderr, "can't read padding array.\n");
						return 0;
					}

					if(pd_ne != 0)
						padding_value = (int)swap32(pd_ne);

					if (step >= sz)
					{
						int array_last = 0;
						int exit = 0;
						if ((array_last = is_array_last_block(fd,NULL, sz, sizeof(float),0)) == -1)
						{
							fprintf(stderr, "can't verify array last block %s:%d.\n", F, L - 1);
							return 0;
						}

						if (rec->fields[i].data.v.size < (sz + step) && array_last)
						{
							int pad_value = sz - (rec->fields[i].data.v.size - step);
							padding_value += pad_value;

							sz = rec->fields[i].data.v.size - step;
							exit = 1;

							if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
							{
								__er_file_pointer(F, L - 1);
								return 0;
							}

							/* write the updated size of the array */
							uint32_t new_sz = swap32((uint32_t)sz);
							if (write(fd, &new_sz, sizeof(new_sz)) == -1)
							{
								perror("error in writing remaining size int array.\n");
								return 0;
							}

							/* write the updated padding value */
							uint32_t new_pd = swap32((uint32_t)padding_value);
							if (write(fd, &new_pd, sizeof(new_pd)) == -1)
							{
								perror("error in writing new pading value int array.\n");
								return 0;
							}
						}
						else if (rec->fields[i].data.v.size == (sz + step) && array_last)
						{
							exit = 1;
						}

						while (sz)
						{
							if (step < rec->fields[i].data.v.size)
							{
								uint32_t num_ne = htonf(rec->fields[i].data.v.elements.f[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
							sz--;
						}

						if (exit)
						{
							/*write the epty update offset*/
							uint64_t empty_offset = swap64(0);
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}
					else
					{

						int exit = 0;
						for (k = 0; k < sz; k++)
						{
							if (step > 0 && k == 0)
							{
								if ((step + sz) > rec->fields[i].data.v.size)
								{
									int pad = sz - (rec->fields[i].data.v.size - step);
									padding_value += pad;

									sz = rec->fields[i].data.v.size - step;
									exit = 1;

									if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
									{
										__er_file_pointer(F, L - 1);
										return 0;
									}

									/* write the updated size of the array */
									uint32_t new_sz = swap32((uint32_t)sz);
									if (write(fd, &new_sz, sizeof(new_sz)) == -1)
									{
										perror("error in writing remaining size int array.\n");
										return 0;
									}

									/*write padding */
									uint32_t new_pd = swap32((uint32_t)padding_value);
									if (write(fd, &new_pd, sizeof(new_pd)) == -1)
									{
										perror("error in writing new padd int array.\n");
										return 0;
									}
								}
							}

							if (step < rec->fields[i].data.v.size)
							{
								uint32_t num_ne = htonf(rec->fields[i].data.v.elements.f[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
								if(!(step < rec->fields[i].data.v.size)) exit = 1;
							}
						}

						if (exit)
						{

							if (padding_value > 0)
							{
								if (move_in_file_bytes(fd, padding_value * sizeof(float)) == -1)
								{
									__er_file_pointer(F, L - 1);
									return 0;
								}
							}
							/*write the epty update offset*/
							uint64_t empty_offset = 0;
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}

					if (padding_value > 0)
					{
						if (move_in_file_bytes(fd, padding_value * sizeof(float)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
					}

					uint64_t update_off_ne = 0;
					off_t go_back_to = get_file_offset(fd);

					if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
					{
						perror("failed read update off_t int array.\n");
						return 0;
					}

					if (go_back_to_first_rec == 0)
						go_back_to_first_rec = go_back_to + sizeof(update_off_ne);

					update_pos = (off_t)swap64(update_off_ne);
					if (update_pos == 0)
					{
						/*go to EOF*/
						if ((update_pos = go_to_EOF(fd)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
						/* write the size of the array */
						int size_left = rec->fields[i].data.v.size - step;
						uint32_t size_left_ne = swap32((uint32_t)size_left);
						if (write(fd, &size_left_ne, sizeof(size_left_ne)) == -1)
						{
							perror("error in writing remaining size int array.\n");
							return 0;
						}

						uint32_t padding_ne = 0;
						if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						int j;
						for (j = 0; j < size_left; j++)
						{
							if (step < rec->fields[i].data.v.size)
							{
								uint32_t num_ne = htonf(rec->fields[i].data.v.elements.f[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
						}

						/*write the empty update offset*/
						uint64_t empty_offset = 0;
						if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						if (find_record_position(fd, go_back_to) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}

						update_off_ne = (uint64_t)swap64((uint64_t)update_pos);
						if (write(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
						{
							fprintf(stderr, "can't write update position int array, %s:%d.\n",
									F, L - 1);
							return 0;
						}

						break;
					}

					if (find_record_position(fd, update_pos) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

				} while (update_pos > 0);

				if (rec->fields[i].data.v.size < sz)
				{

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/*write the size of the array */
					uint32_t size_ne = swap32((uint32_t)rec->fields[i].data.v.size);
					if (write(fd, &size_ne, sizeof(size_ne)) == -1)
					{
						perror("error in writing size array to file.\n");
						return 0;
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					pd_he += (sz - rec->fields[i].data.v.size);

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/* write the padding to apply after the  array */
					pad_ne = swap32((uint32_t)pd_he);
					if (write(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int k;
					for (k = step; k < rec->fields[i].data.v.size; k++)
					{

						uint32_t num_ne = htonf(rec->fields[i].data.v.elements.f[k]);
						if (write(fd, &num_ne, sizeof(num_ne)) == -1)
						{
							perror("failed write int array to file");
							return 0;
						}
					}

					/*
					 * move the file pointer after the array
					 * as much as the pad
					 * */
					if (move_in_file_bytes(fd, pd_he * sizeof(float)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					uint64_t update_arr_ne = 0;
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}
				else if (rec->fields[i].data.v.size == sz)
				{
					/*
					 * the sizes are the same
					 * we simply write the array.
					 * */
					if (step > 0)
					{
						if (move_in_file_bytes(fd, -sizeof(sz)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						int size_left = rec->fields[i].data.v.size - step;
						uint32_t sz_ne = swap32((uint32_t)size_left);
						if (write(fd, &sz_ne, sizeof(sz_ne)) == -1)
						{
							fprintf(stderr, "write failed %s:%d.\n", F, L - 1);
							return 0;
						}
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					int j;
					for (j = 0; j < rec->fields[i].data.v.size; j++)
					{
						if (step < rec->fields[i].data.v.size)
						{
							uint32_t num_ne = swap32(rec->fields[i].data.v.elements.f[step]);
							if (write(fd, &num_ne, sizeof(num_ne)) == -1)
							{
								perror("failed write int array to file");
								return 0;
							}
							step++;
						}
					}

					/*
					 * move the file pointer
					 * as much as the padding value
					 * if it si bigger than 0
					 * */
					if (pd_he > 0)
					{
						if (move_in_file_bytes(fd, pd_he * sizeof(float)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}
					}

					uint64_t update_arr_ne = 0;
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				if (go_back_to_first_rec > 0)
				{
					if (find_record_position(fd, go_back_to_first_rec) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}

			break;
		}
		case TYPE_ARRAY_STRING:
		{
			if (!update)
			{
				/*write the size of the array */
				uint32_t size_ne = swap32((uint32_t)rec->fields[i].data.v.size);
				if (write(fd, &size_ne, sizeof(size_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				uint32_t padding_ne = 0;
				if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}
				int j;
				for (j = 0; j < rec->fields[i].data.v.size; j++)
				{
					if (!rec->fields[i].data.v.elements.s[j])
						continue;

					lt = strlen(rec->fields[i].data.v.elements.s[j]);
					buff_update = lt * 2;

					/* adding 1 for '\0'*/
					lt++, buff_update++;
					char buff_w[buff_update];
					memset(buff_w,0,buff_update);

					strncpy(buff_w, rec->fields[i].data.v.elements.s[j], lt - 1);

					uint16_t bu_ne = swap16((uint16_t)buff_update);
					uint32_t str_loc_ne = swap32((uint32_t)str_loc);

					if (write(fd, &str_loc_ne, sizeof(str_loc_ne)) < 0 ||
						write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
						write(fd, buff_w, buff_update) < 0){
						perror("write file failed: ");
						printf(" %s:%d", F, L - 2);
						return 0;
					}

				}

				uint64_t upd_ne = 0;
				if (write(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}
			}
			else
			{
				/* update branch*/
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int step = 0;
				int sz = 0;
				int k = 0;
				int padding_value = 0;
				do
				{
					/* check the size */
					uint32_t sz_ne = 0;
					if (read(fd, &sz_ne, sizeof(sz_ne)) == -1)
					{
						fprintf(stderr, "can't read int array size.\n");
						return 0;
					}

					sz = (int)swap32(sz_ne);
					if (rec->fields[i].data.v.size < sz ||
						rec->fields[i].data.v.size == sz)
						break;

					/*read the padding data*/
					uint32_t pd_ne = 0;
					if (read(fd, &pd_ne, sizeof(pd_ne)) == -1)
					{
						fprintf(stderr, "can't read padding array.\n");
						return 0;
					}

					if(pd_ne != 0)
						padding_value = (int)swap32(pd_ne);

					if (step >= sz)
					{
						int array_last = 0;
						int exit = 0;
						if ((array_last = is_array_last_block(fd,NULL, sz, 0,TYPE_ARRAY_STRING)) == -1)
						{
							fprintf(stderr, "can't verify array last block %s:%d.\n", F, L - 1);
							return 0;
						}

						if (rec->fields[i].data.v.size < (sz + step) && array_last)
						{
							int pad_value = sz - (rec->fields[i].data.v.size - step);
							padding_value += pad_value;

							sz = rec->fields[i].data.v.size - step;
							exit = 1;

							if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
							{
								__er_file_pointer(F, L - 1);
								return 0;
							}

							/* write the updated size of the array */
							uint32_t new_sz = swap32((uint32_t)sz);
							if (write(fd, &new_sz, sizeof(new_sz)) == -1)
							{
								perror("error in writing remaining size int array.\n");
								return 0;
							}

							/* write the updated padding value */
							uint32_t new_pd = swap32((uint32_t)padding_value);
							if (write(fd, &new_pd, sizeof(new_pd)) == -1)
							{
								perror("error in writing new pading value int array.\n");
								return 0;
							}
						}
						else if (rec->fields[i].data.v.size == (sz + step) && array_last)
						{
							exit = 1;
						}

						while (sz)
						{
							if (step < rec->fields[i].data.v.size)
							{
								/*string update process*/
								/*save the starting offset for the string record*/
								if ((bg_pos = get_file_offset(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								/*read pos of new str if any*/
								uint32_t str_loc_ne = 0;
								if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == STATUS_ERROR)
								{
									perror("can't read string location: ");
									printf(" %s:%d", F, L - 3);
									return 0;
								}
								
								if(str_loc_ne != 0)
									str_loc = (off_t)swap32(str_loc_ne);

								/*store record  beginning pos*/
								if ((af_str_loc_pos = get_file_offset(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								uint16_t bu_ne = 0;
								if (read(fd, &bu_ne, sizeof(bu_ne)) < 0){
									perror("can't read safety buffer before writing string.\n");
									printf("%s:%d", F, L - 3);
									return 0;
								}

								buff_update = (off_t)swap16(bu_ne);

								/*save the end offset of the first string record */
								if ((go_back_to = get_file_offset(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								/*add the buffer size to this file off_t*/
								/*so we can reposition right after the 1st string after the writing */
								go_back_to += buff_update;
								if (str_loc > 0)
								{
									/*set the file pointer to str_loc*/
									if (find_record_position(fd, str_loc) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}
									/*
									 * in the case of a regular buffer update we have
									 *  to save the off_t to get back to it later
									 * */
									if ((move_to = get_file_offset(fd)) == -1)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}

									uint16_t bu_ne = 0;
									if (read(fd, &bu_ne, sizeof(bu_ne)) < 0){
										perror("read file.\n");
										printf("%s:%d", F, L - 3);
										return 0;
									}

									buff_update = (off_t)swap16(bu_ne);
								}

								new_lt = strlen(rec->fields[i].data.v.elements.s[step]) + 1; /*get new str length*/

								if (new_lt > buff_update)
								{
									/*
									 * if the new length is bigger then the buffer,
									 * set the file pointer to the end of the file
									 * to write the new data
									 * */
									if ((eof = go_to_EOF(fd)) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}

									/*expand the buff_update only for the bytes needed*/
									__n_buff_update = buff_update;
									__n_buff_update += (new_lt - buff_update);
									buff_update = __n_buff_update;

								}
								char buff_w[buff_update];
								memset(buff_w,0,buff_update);

								strncpy(buff_w, rec->fields[i].data.v.elements.s[step], new_lt - 1);
								/*
								 * if we did not move to another position
								 * set the file pointer back to the begginning of the string record
								 * to overwrite the data accordingly
								 * */
								if (str_loc == 0 && (__n_buff_update == 0))
								{
									if (find_record_position(fd, af_str_loc_pos) == -1)
									{
										__er_file_pointer(F, L - 3);
										return 0;
									}
								}
								else if (str_loc > 0 && (__n_buff_update == 0))
								{
									if (find_record_position(fd, move_to) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 3);
										return 0;
									}
								}

								/*
								 * write the data to file --
								 * the file pointer is always pointing to the
								 * right position at this point */
								bu_ne = swap16((uint16_t)buff_update);

								if (write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
									write(fd, buff_w, buff_update) < 0){
									perror("error in writing type string (char *)file.\n");
									return 0;
								}


								/*
								 * if eof is bigger than 0 means we updated the string
								 * we need to save the off_t of the new written data
								 * at the start of the original.
								 * */
								if (eof > 0)
								{
									/*go at the beginning of the str record*/
									if (find_record_position(fd, bg_pos) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}

									/*update new string position*/
									uint32_t eof_ne = swap32((uint32_t)eof);
									if (write(fd, &eof_ne, sizeof(eof_ne)) == STATUS_ERROR)
									{
										perror("write file: ");
										printf(" %s:%d", F, L - 3);
										return 0;
									}

									/*set file pointer to the end of the 1st string rec*/
									/*this step is crucial to avoid losing data        */

									if (find_record_position(fd, go_back_to) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}
								}
								else if (str_loc > 0)
								{
									/*
									 * Make sure that in all cases
									 * we go back to the end of the 1st record
									 * */
									if (find_record_position(fd, go_back_to) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}
								}
								__n_buff_update = 0;
								eof = 0;
								str_loc = 0;
								step++;
							}
							sz--;
						}

						if (exit){
							/*write the epty update offset*/
							uint64_t empty_offset = 0;
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}
					else
					{

						int exit = 0;
						for (k = 0; k < sz; k++)
						{
							if (step > 0 && k == 0)
							{
								if ((step + sz) > rec->fields[i].data.v.size)
								{
									int pad = sz - (rec->fields[i].data.v.size - step);
									padding_value += pad;

									sz = rec->fields[i].data.v.size - step;
									exit = 1;

									if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
									{
										__er_file_pointer(F, L - 1);
										return 0;
									}

									/* write the updated size of the array */
									uint32_t new_sz = swap32((uint32_t)sz);
									if (write(fd, &new_sz, sizeof(new_sz)) == -1)
									{
										perror("error in writing remaining size int array.\n");
										return 0;
									}

									/*write padding */
									uint32_t new_pd = swap32((uint32_t)padding_value);
									if (write(fd, &new_pd, sizeof(new_pd)) == -1)
									{
										perror("error in writing new padd int array.\n");
										return 0;
									}
								}
							}

							if (step < rec->fields[i].data.v.size)
							{
								if (!rec->fields[i].data.v.elements.s[step])
									continue;

								/*string update process*/
								/*save the starting offset for the string record*/
								if ((bg_pos = get_file_offset(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								/*read pos of new str if any*/
								uint32_t str_loc_ne = 0;
								if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == STATUS_ERROR)
								{
									perror("can't read string location: ");
									printf(" %s:%d", F, L - 3);
									return 0;
								}
								str_loc = (off_t)swap32(str_loc_ne);

								/*store record  beginning pos*/
								if ((af_str_loc_pos = get_file_offset(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								uint16_t bu_ne = 0;
								if (read(fd, &bu_ne, sizeof(bu_ne)) < 0){
									perror("can't read safety buffer before writing string.\n");
									printf("%s:%d", F, L - 3);
									return 0;
								}

								buff_update = (off_t)swap16(bu_ne);

								/*save the end offset of the first string record */
								if ((go_back_to = get_file_offset(fd)) == STATUS_ERROR){
									__er_file_pointer(F, L - 2);
									return 0;
								}

								/*add the buffer size to this file off_t*/
								/*so we can reposition right after the 1st string after the writing */
								go_back_to += buff_update;
								if (str_loc > 0)
								{
									/*set the file pointer to str_loc*/
									if (find_record_position(fd, str_loc) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}
									/*
									 * in the case of a regular buffer update we have
									 *  to save the off_t to get back to it later
									 * */
									if ((move_to = get_file_offset(fd)) == -1)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}

									uint16_t bu_ne = 0;
									if (read(fd, &bu_ne, sizeof(bu_ne)) < 0){
										perror("read file.\n");
										printf("%s:%d", F, L - 3);
										return 0;
									}

									buff_update = (off_t)swap16(bu_ne);
								}

								new_lt = strlen(rec->fields[i].data.v.elements.s[step]) + 1; /*get new str length*/

								if (new_lt > buff_update){
									/*
									 * if the new length is bigger then the buffer,
									 * set the file pointer to the end of the file
									 * to write the new data
									 * */
									if ((eof = go_to_EOF(fd)) == STATUS_ERROR){
										__er_file_pointer(F, L - 2);
										return 0;
									}

									/*expand the buff_update only for the bytes needed*/
									__n_buff_update = buff_update;
									__n_buff_update += (new_lt - buff_update);
									buff_update = __n_buff_update;
								}
								char buff_w[buff_update];
								memset(buff_w,0,buff_update);

								strncpy(buff_w, rec->fields[i].data.v.elements.s[step], new_lt - 1);
								/*
								 * if we did not move to another position
								 * set the file pointer back to the begginning of the string record
								 * to overwrite the data accordingly
								 * */
								if (str_loc == 0 && (__n_buff_update == 0))
								{
									if (find_record_position(fd, af_str_loc_pos) == -1)
									{
										__er_file_pointer(F, L - 3);
										return 0;
									}
								}
								else if (str_loc > 0 && (__n_buff_update == 0))
								{
									if (find_record_position(fd, move_to) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 3);
										return 0;
									}
								}

								/*
								 * write the data to file --
								 * the file pointer is always pointing to the
								 * right position at this point */
								bu_ne = swap16((uint16_t) buff_update);

								if (write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
									write(fd, buff_w, buff_update) < 0){
									perror("error in writing type string (char *)file.\n");
									return 0;
								}


								/*
								 * if eof is bigger than 0 means we updated the string
								 * we need to save the off_t of the new written data
								 * at the start of the original.
								 * */
								if (eof > 0)
								{
									/*go at the beginning of the str record*/
									if (find_record_position(fd, bg_pos) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}

									/*update new string position*/
									uint32_t eof_ne = swap32((uint32_t)eof);
									if (write(fd, &eof_ne, sizeof(eof_ne)) == STATUS_ERROR)
									{
										perror("write file: ");
										printf(" %s:%d", F, L - 3);
										return 0;
									}

									/*set file pointer to the end of the 1st string rec*/
									/*this step is crucial to avoid losing data        */

									if (find_record_position(fd, go_back_to) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}
								}
								else if (str_loc > 0)
								{
									/*
									 * Make sure that in all cases
									 * we go back to the end of the 1st record
									 * */
									if (find_record_position(fd, go_back_to) == STATUS_ERROR)
									{
										__er_file_pointer(F, L - 2);
										return 0;
									}
								}
							}
							__n_buff_update = 0;
							eof = 0;
							str_loc = 0;
							step++;
							if(!(step < rec->fields[i].data.v.size)) exit = 1;
						}

						if (exit){
							if (padding_value > 0) {
								int i;
								for(i = 0; i < padding_value; i++){
									if(get_string_size(fd,NULL) == (size_t) -1){
										__er_file_pointer(F, L - 1);
										return 0;
									}
								}
							}
							/*write the epty update offset*/
							uint64_t empty_offset = 0;
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1){
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}

					if (padding_value > 0){
						int i;
						for(i = 0; i < padding_value; i++){
							if(get_string_size(fd,NULL) == (size_t) -1){
								__er_file_pointer(F, L - 1);
								return 0;
							}
						}
					}

					uint64_t update_off_ne = 0;
					off_t go_back_to_string_array = get_file_offset(fd);

					if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1){
						perror("failed read update off_t int array.\n");
						return 0;
					}

					if (go_back_to_first_rec == 0)
						go_back_to_first_rec = go_back_to + sizeof(update_off_ne);

					update_pos = (off_t)swap64(update_off_ne);
					if (update_pos == 0){
						/*go to EOF*/
						if ((update_pos = go_to_EOF(fd)) == -1){
							__er_file_pointer(F, L - 1);
							return 0;
						}
						/* write the size of the array */
						int size_left = rec->fields[i].data.v.size - step;
						uint32_t size_left_ne = swap32((uint32_t)size_left);
						if (write(fd, &size_left_ne, sizeof(size_left_ne)) == -1){
							perror("error in writing remaining size int array.\n");
							return 0;
						}

						uint32_t padding_ne = swap32(0);
						if (write(fd, &padding_ne, sizeof(padding_ne)) == -1){
							perror("error in writing size array to file.\n");
							return 0;
						}

						int j;
						for (j = 0; j < size_left; j++){
							if (step < rec->fields[i].data.v.size){
								if (!rec->fields[i].data.v.elements.s[step])
									continue;

								/*new string writing process*/

								str_loc = 0;
								lt = strlen(rec->fields[i].data.v.elements.s[step]);
								buff_update = lt * 2;

								/* adding 1 for '\0'*/
								lt++, buff_update++;
								char buff_w[buff_update];
								memset(buff_w,0,buff_update);

								strncpy(buff_w, rec->fields[i].data.v.elements.s[step], lt - 1);

								uint16_t bu_ne = swap16((uint16_t)buff_update);
								uint32_t str_loc_ne = swap32((uint32_t)str_loc);

								if (write(fd, &str_loc_ne, sizeof(str_loc_ne)) < 0 ||
									write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
									write(fd, buff_w, buff_update) < 0){
									perror("write file failed: ");
									printf(" %s:%d", F, L - 2);
									return 0;
								}


								step++;
							}
						}

						/*write the empty update offset*/
						uint64_t empty_offset = 0;
						if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						if (find_record_position(fd, go_back_to_string_array) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}

						update_off_ne = (uint64_t)swap64((uint64_t)update_pos);
						if (write(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
						{
							fprintf(stderr, "can't write update position int array, %s:%d.\n",
									F, L - 1);
							return 0;
						}

						break;
					}

					if (find_record_position(fd, update_pos) == -1){
						__er_file_pointer(F, L - 1);
						return 0;
					}

				} while (update_pos > 0);

				if (rec->fields[i].data.v.size < sz)
				{

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/*write the size of the array */
					uint32_t size_ne = swap32((uint32_t)rec->fields[i].data.v.size);
					if (write(fd, &size_ne, sizeof(size_ne)) == -1)
					{
						perror("error in writing size array to file.\n");
						return 0;
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					pd_he += (sz - rec->fields[i].data.v.size);

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/* write the padding to apply after the  array */
					pad_ne = swap32((uint32_t)pd_he);
					if (write(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int k;
					for (k = step; k < rec->fields[i].data.v.size; k++)
					{
						if (!rec->fields[i].data.v.elements.s[k])
							continue;

						/*string update process*/
						if ((bg_pos = get_file_offset(fd)) == STATUS_ERROR)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						/*read pos of new str if any*/
						uint32_t str_loc_ne = 0;
						if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == STATUS_ERROR)
						{
							perror("can't read string location: ");
							printf(" %s:%d", F, L - 3);
							return 0;
						}
						str_loc = (off_t)swap32(str_loc_ne);

						/*store record  beginning pos*/
						if ((af_str_loc_pos = get_file_offset(fd)) == STATUS_ERROR)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						uint16_t bu_ne = 0;
						if (read(fd, &bu_ne, sizeof(bu_ne)) < 0){
							perror("can't read safety buffer before writing string.\n");
							printf("%s:%d", F, L - 3);
							return 0;
						}

						buff_update = (off_t)swap16(bu_ne);

						/*save the end offset of the first string record */
						if ((go_back_to = get_file_offset(fd)) == STATUS_ERROR)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						/*add the buffer size to this file off_t*/
						/*so we can reposition right after the 1st string after the writing */
						go_back_to += buff_update;
						if (str_loc > 0)
						{
							/*set the file pointer to str_loc*/
							if (find_record_position(fd, str_loc) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}
							/*
							 * in the case of a regular buffer update we have
							 *  to save the off_t to get back to it later
							 * */
							if ((move_to = get_file_offset(fd)) == -1)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}

							uint16_t bu_ne = 0;
							if(read(fd, &bu_ne, sizeof(bu_ne)) < 0)	{
								perror("read file.\n");
								printf("%s:%d", F, L - 3);
								return 0;
							}

							buff_update = (off_t)swap16(bu_ne);
						}

						new_lt = strlen(rec->fields[i].data.v.elements.s[k]) + 1; /*get new str length*/

						if (new_lt > buff_update)
						{
							/*
							 * if the new length is bigger then the buffer,
							 * set the file pointer to the end of the file
							 * to write the new data
							 * */
							if ((eof = go_to_EOF(fd)) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}

							/*expand the buff_update only for the bytes needed*/
							__n_buff_update = buff_update;
							__n_buff_update += (new_lt - buff_update);
							buff_update = __n_buff_update;
						}
						char buff_w[buff_update];
						memset(buff_w,0,buff_update);

						strncpy(buff_w, rec->fields[i].data.v.elements.s[k], new_lt - 1);
						/*
						 * if we did not move to another position
						 * set the file pointer back to the begginning of the string record
						 * to overwrite the data accordingly
						 * */
						if (str_loc == 0 && (__n_buff_update == 0))
						{
							if (find_record_position(fd, af_str_loc_pos) == -1)
							{
								__er_file_pointer(F, L - 3);
								return 0;
							}
						}
						else if (str_loc > 0 && (__n_buff_update == 0))
						{
							if (find_record_position(fd, move_to) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 3);
								return 0;
							}
						}

						/*
						 * write the data to file --
						 * the file pointer is always pointing to the
						 * right position at this point */
						bu_ne = swap16((uint16_t)buff_update);
						if(write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
							write(fd, buff_w, buff_update) < 0){
							perror("error in writing type string (char *)file.\n");
							return 0;
						}

						/*
						 * if eof is bigger than 0 means we updated the string
						 * we need to save the off_t of the new written data
						 * at the start of the original.
						 * */
						if (eof > 0)
						{
							/*go at the beginning of the str record*/
							if (find_record_position(fd, bg_pos) == STATUS_ERROR){
								__er_file_pointer(F, L - 2);
								return 0;
							}

							/*update new string position*/
							uint32_t eof_ne = swap32((uint32_t)eof);
							if (write(fd, &eof_ne, sizeof(eof_ne)) == STATUS_ERROR)
							{
								perror("write file: ");
								printf(" %s:%d", F, L - 3);
								return 0;
							}

							/*set file pointer to the end of the 1st string rec*/
							/*this step is crucial to avoid losing data        */

							if (find_record_position(fd, go_back_to) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}
						}
						else if (str_loc > 0)
						{
							/*
							 * Make sure that in all cases
							 * we go back to the end of the 1st record
							 * */
							if (find_record_position(fd, go_back_to) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}
						}
					}

					/*
					 * move the file pointer after the array
					 * as much as the pad
					 * */
					int i;
					for(i = 0; i < pd_he; i++){
						if(get_string_size(fd,NULL) == (size_t) -1){
							__er_file_pointer(F, L - 1);
							return 0;
						}
					}

					uint64_t update_arr_ne = 0;
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}
				else if (rec->fields[i].data.v.size == sz)
				{
					/*
					 * the sizes are the same
					 * we simply write the array.
					 * */
					if (step > 0)
					{
						if (move_in_file_bytes(fd, -sizeof(sz)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						int size_left = rec->fields[i].data.v.size - step;
						uint32_t sz_ne = swap32((uint32_t)size_left);
						if (write(fd, &sz_ne, sizeof(sz_ne)) == -1)
						{
							fprintf(stderr, "write failed %s:%d.\n", F, L - 1);
							return 0;
						}
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					int j;
					for ( j = 0; k < rec->fields[i].data.v.size; j++)
					{
						if (step < rec->fields[i].data.v.size)
						{
							if (!rec->fields[i].data.v.elements.s[step])
								continue;

							/*string update process*/
							if ((bg_pos = get_file_offset(fd)) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}

							/*read pos of new str if any*/
							uint32_t str_loc_ne = 0;
							if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == STATUS_ERROR)
							{
								perror("can't read string location: ");
								printf(" %s:%d", F, L - 3);
								return 0;
							}
							str_loc = (off_t)swap32(str_loc_ne);

							/*store record  beginning pos*/
							if ((af_str_loc_pos = get_file_offset(fd)) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}

							uint16_t bu_ne = 0;
							if (read(fd, &bu_ne, sizeof(bu_ne)) < 0){
								perror("can't read safety buffer before writing string.\n");
								printf("%s:%d", F, L - 3);
								return 0;
							}

							buff_update = (off_t)swap16(bu_ne);

							/*save the end offset of the first string record */
							if ((go_back_to = get_file_offset(fd)) == STATUS_ERROR)
							{
								__er_file_pointer(F, L - 2);
								return 0;
							}

							/*add the buffer size to this file off_t*/
							/*so we can reposition right after the 1st string after the writing */
							go_back_to += buff_update;
							if (str_loc > 0)
							{
								/*set the file pointer to str_loc*/
								if (find_record_position(fd, str_loc) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}
								/*
								 * in the case of a regular buffer update we have
								 *  to save the off_t to get back to it later
								 * */
								if ((move_to = get_file_offset(fd)) == -1)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								uint16_t bu_ne = 0;
								if (read(fd, &bu_ne, sizeof(bu_ne)) < 0){
									perror("read file.\n");
									printf("%s:%d", F, L - 3);
									return 0;
								}

								buff_update = (off_t)swap16(bu_ne);
							}

							new_lt = strlen(rec->fields[i].data.v.elements.s[step]) + 1; /*get new str length*/

							if (new_lt > buff_update)
							{
								/*
								 * if the new length is bigger then the buffer,
								 * set the file pointer to the end of the file
								 * to write the new data
								 * */
								if ((eof = go_to_EOF(fd)) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								/*expand the buff_update only for the bytes needed*/
								__n_buff_update = buff_update;
								__n_buff_update += (new_lt - buff_update);
								buff_update = __n_buff_update;							
							}
							
							char buff_w[buff_update];
							memset(buff_w,0,buff_update);

							strncpy(buff_w, rec->fields[i].data.v.elements.s[step], new_lt - 1);
							/*
							 * if we did not move to another position
							 * set the file pointer back to the begginning of the string record
							 * to overwrite the data accordingly
							 * */
							if (str_loc == 0 && (__n_buff_update == 0))
							{
								if (find_record_position(fd, af_str_loc_pos) == -1)
								{
									__er_file_pointer(F, L - 3);
									return 0;
								}
							}
							else if (str_loc > 0 && (__n_buff_update == 0))
							{
								if (find_record_position(fd, move_to) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 3);
									return 0;
								}
							}

							/*
							 * write the data to file --
							 * the file pointer is always pointing to the
							 * right position at this point */
							bu_ne = swap16((uint16_t)buff_update);

							if(write(fd, &bu_ne, sizeof(bu_ne)) < 0 ||
								write(fd, buff_w, buff_update) < 0){
								perror("error in writing type string (char *)file.\n");
								return 0;
							}


							/*
							 * if eof is bigger than 0 means we updated the string
							 * we need to save the off_t of the new written data
							 * at the start of the original.
							 * */
							if (eof > 0)
							{
								/*go at the beginning of the str record*/
								if (find_record_position(fd, bg_pos) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}

								/*update new string position*/
								uint32_t eof_ne = swap32((uint32_t)eof);
								if (write(fd, &eof_ne, sizeof(eof_ne)) == STATUS_ERROR)
								{
									perror("write file: ");
									printf(" %s:%d", F, L - 3);
									return 0;
								}

								/*set file pointer to the end of the 1st string rec*/
								/*this step is crucial to avoid losing data        */

								if (find_record_position(fd, go_back_to) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}
							}
							else if (str_loc > 0)
							{
								/*
								 * Make sure that in all cases
								 * we go back to the end of the 1st record
								 * */
								if (find_record_position(fd, go_back_to) == STATUS_ERROR)
								{
									__er_file_pointer(F, L - 2);
									return 0;
								}
							}

							/*zeroing the variables for th new cycle*/
							__n_buff_update = 0;
							eof = 0;
							step++;
						}
					}

					/*
					 * move the file pointer
					 * as much as the padding value
					 * if it si bigger than 0
					 * */
					if (pd_he > 0)
					{
						int i;
						for(i = 0; i < pd_he; i++){
							if(get_string_size(fd,NULL) == (size_t) -1){
								__er_file_pointer(F, L - 2);
								return 0;
							}
						}
					}

					uint64_t update_arr_ne = 0;
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				if (go_back_to_first_rec > 0)
				{
					if (find_record_position(fd, go_back_to_first_rec) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}

			break;
		}
		case TYPE_ARRAY_BYTE:
		{
			if (!update)
			{
				/*write the size of the array */
				uint32_t size_ne = swap32((uint32_t)rec->fields[i].data.v.size);
				if (write(fd, &size_ne, sizeof(size_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				uint32_t padding_ne = 0;
				if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				int k;
				for ( k = 0; k < rec->fields[i].data.v.size; k++)
				{
					uint8_t num_ne = rec->fields[i].data.v.elements.b[k];
					if (write(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				uint64_t upd_ne = 0;
				if (write(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}
			}
			else
			{
				/* update branch*/
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int step = 0;
				int sz = 0;
				int k = 0;
				int padding_value = 0;
				do
				{
					/* check the size */
					uint32_t sz_ne = 0;
					if (read(fd, &sz_ne, sizeof(sz_ne)) == -1)
					{
						fprintf(stderr, "can't read int array size.\n");
						return 0;
					}

					sz = (int)swap32(sz_ne);
					if (rec->fields[i].data.v.size < sz ||
						rec->fields[i].data.v.size == sz)
						break;

					/*read the padding data*/
					uint32_t pd_ne = 0;
					if (read(fd, &pd_ne, sizeof(pd_ne)) == -1)
					{
						fprintf(stderr, "can't read padding array.\n");
						return 0;
					}

					if(pd_ne != 0)
						padding_value = (int)swap32(pd_ne);

					if (step >= sz)
					{
						int array_last = 0;
						int exit = 0;
						if ((array_last = is_array_last_block(fd,NULL, sz, sizeof(unsigned char),0)) == -1)
						{
							fprintf(stderr, "can't verify array last block %s:%d.\n", F, L - 1);
							return 0;
						}

						if (rec->fields[i].data.v.size < (sz + step) && array_last)
						{
							int pad_value = sz - (rec->fields[i].data.v.size - step);
							padding_value += pad_value;

							sz = rec->fields[i].data.v.size - step;
							exit = 1;

							if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
							{
								__er_file_pointer(F, L - 1);
								return 0;
							}

							/* write the updated size of the array */
							uint32_t new_sz = swap32((uint32_t)sz);
							if (write(fd, &new_sz, sizeof(new_sz)) == -1)
							{
								perror("error in writing remaining size int array.\n");
								return 0;
							}

							/* write the updated padding value */
							uint32_t new_pd = swap32((uint32_t)padding_value);
							if (write(fd, &new_pd, sizeof(new_pd)) == -1)
							{
								perror("error in writing new pading value int array.\n");
								return 0;
							}
						}
						else if (rec->fields[i].data.v.size == (sz + step) && array_last)
						{
							exit = 1;
						}

						while (sz)
						{
							if (step < rec->fields[i].data.v.size)
							{
								uint8_t num_ne = rec->fields[i].data.v.elements.b[step];
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
							sz--;
						}

						if (exit)
						{
							/*write the epty update offset*/
							uint64_t empty_offset = 0;
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}
					else
					{

						int exit = 0;
						for (k = 0; k < sz; k++)
						{
							if (step > 0 && k == 0)
							{
								if ((step + sz) > rec->fields[i].data.v.size)
								{
									int pad = sz - (rec->fields[i].data.v.size - step);
									padding_value += pad;

									sz = rec->fields[i].data.v.size - step;
									exit = 1;

									if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
									{
										__er_file_pointer(F, L - 1);
										return 0;
									}

									/* write the updated size of the array */
									uint32_t new_sz = swap32((uint32_t)sz);
									if (write(fd, &new_sz, sizeof(new_sz)) == -1)
									{
										perror("error in writing remaining size int array.\n");
										return 0;
									}

									/*write padding */
									uint32_t new_pd = swap32((uint32_t)padding_value);
									if (write(fd, &new_pd, sizeof(new_pd)) == -1)
									{
										perror("error in writing new padd int array.\n");
										return 0;
									}
								}
							}

							if (step < rec->fields[i].data.v.size)
							{
								if (!rec->fields[i].data.v.elements.b[step])
									continue;

								uint8_t num_ne = rec->fields[i].data.v.elements.b[step];
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
								if(!(step < rec->fields[i].data.v.size)) exit = 1;
							}
						}

						if (exit)
						{
							if (padding_value > 0)
							{
								if (move_in_file_bytes(fd, padding_value * sizeof(unsigned char)) == -1)
								{
									__er_file_pointer(F, L - 1);
									return 0;
								}
							}
							/*write the epty update offset*/
							uint64_t empty_offset = 0;
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}

					if (padding_value > 0)
					{
						if (move_in_file_bytes(fd, padding_value * sizeof(unsigned char)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
					}

					uint64_t update_off_ne = 0;
					off_t go_back_to = get_file_offset(fd);

					if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
					{
						perror("failed read update off_t int array.\n");
						return 0;
					}

					if (go_back_to_first_rec == 0)
						go_back_to_first_rec = go_back_to + sizeof(update_off_ne);

					update_pos = (off_t)swap64(update_off_ne);
					if (update_pos == 0)
					{
						/*go to EOF*/
						if ((update_pos = go_to_EOF(fd)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
						/* write the size of the array */
						int size_left = rec->fields[i].data.v.size - step;
						uint32_t size_left_ne = swap32((uint32_t)size_left);
						if (write(fd, &size_left_ne, sizeof(size_left_ne)) == -1)
						{
							perror("error in writing remaining size int array.\n");
							return 0;
						}

						uint32_t padding_ne = swap32(0);
						if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						int j;
						for (j = 0; j < size_left; j++)
						{
							if (step < rec->fields[i].data.v.size)
							{
								uint8_t num_ne = rec->fields[i].data.v.elements.b[step];
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
						}

						/*write the empty update offset*/
						uint64_t empty_offset = 0;
						if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						if (find_record_position(fd, go_back_to) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}

						update_off_ne = (uint64_t)swap64((uint64_t)update_pos);
						if (write(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
						{
							fprintf(stderr, "can't write update position int array, %s:%d.\n",
									F, L - 1);
							return 0;
						}

						break;
					}

					if (find_record_position(fd, update_pos) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

				} while (update_pos > 0);

				if (rec->fields[i].data.v.size < sz)
				{

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/*write the size of the array */
					uint32_t size_ne = swap32((uint32_t)rec->fields[i].data.v.size);
					if (write(fd, &size_ne, sizeof(size_ne)) == -1)
					{
						perror("error in writing size array to file.\n");
						return 0;
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					pd_he += (sz - rec->fields[i].data.v.size);

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/* write the padding to apply after the  array */
					pad_ne = swap32((uint32_t)pd_he);
					if (write(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int j;
					for (j = step; k < rec->fields[i].data.v.size; j++)
					{
						uint8_t num_ne = rec->fields[i].data.v.elements.b[j];
						if (write(fd, &num_ne, sizeof(num_ne)) == -1)
						{
							perror("failed write int array to file");
							return 0;
						}
					}

					/*
					 * move the file pointer after the array
					 * as much as the pad
					 * */
					if (move_in_file_bytes(fd, pd_he * sizeof(unsigned char)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					uint64_t update_arr_ne = 0;
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}
				else if (rec->fields[i].data.v.size == sz)
				{
					/*
					 * the sizes are the same
					 * we simply write the array.
					 * */
					if (step > 0)
					{
						if (move_in_file_bytes(fd, -sizeof(sz)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						int size_left = rec->fields[i].data.v.size - step;
						uint32_t sz_ne = swap32((uint32_t)size_left);
						if (write(fd, &sz_ne, sizeof(sz_ne)) == -1)
						{
							fprintf(stderr, "write failed %s:%d.\n", F, L - 1);
							return 0;
						}
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					int j;
					for (j = 0; k < rec->fields[i].data.v.size; j++)
					{
						if (step < rec->fields[i].data.v.size)
						{
							uint8_t num_ne = rec->fields[i].data.v.elements.b[step];
							if (write(fd, &num_ne, sizeof(num_ne)) == -1)
							{
								perror("failed write int array to file");
								return 0;
							}
							step++;
						}
					}

					/*
					 * move the file pointer
					 * as much as the padding value
					 * if it si bigger than 0
					 * */
					if (pd_he > 0)
					{
						if (move_in_file_bytes(fd, pd_he * sizeof(unsigned char)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}
					}

					uint64_t update_arr_ne = 0;
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				if (go_back_to_first_rec > 0)
				{
					if (find_record_position(fd, go_back_to_first_rec) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}
			break;
		}
		case TYPE_ARRAY_DOUBLE:
		{
			if (!update)
			{
				/*write the size of the array */
				uint32_t size_ne = swap32((uint32_t)rec->fields[i].data.v.size);
				if (write(fd, &size_ne, sizeof(size_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				uint32_t padding_ne = 0;
				if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				int k;
				for (k = 0; k < rec->fields[i].data.v.size; k++)
				{
					uint64_t num_ne = htond(rec->fields[i].data.v.elements.d[k]);
					if (write(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				uint64_t upd_ne = 0;
				if (write(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}
			}
			else
			{
				/* update branch*/
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int step = 0;
				int sz = 0;
				int k = 0;
				int padding_value = 0;
				do
				{
					/* check the size */
					uint32_t sz_ne = 0;
					if (read(fd, &sz_ne, sizeof(sz_ne)) == -1)
					{
						fprintf(stderr, "can't read int array size.\n");
						return 0;
					}

					sz = (int)swap32(sz_ne);
					if (rec->fields[i].data.v.size < sz ||
						rec->fields[i].data.v.size == sz)
						break;

					/*read the padding data*/
					uint32_t pd_ne = 0;
					if (read(fd, &pd_ne, sizeof(pd_ne)) == -1)
					{
						fprintf(stderr, "can't read padding array.\n");
						return 0;
					}
					
					if(pd_ne != 0)
						padding_value = (int)swap32(pd_ne);

					if (step >= sz)
					{
						int array_last = 0;
						int exit = 0;
						if ((array_last = is_array_last_block(fd,NULL, sz, sizeof(double),0)) == -1)
						{
							fprintf(stderr, "can't verify array last block %s:%d.\n", F, L - 1);
							return 0;
						}

						if (rec->fields[i].data.v.size < (sz + step) && array_last)
						{
							int pad_value = sz - (rec->fields[i].data.v.size - step);
							padding_value += pad_value;

							sz = rec->fields[i].data.v.size - step;
							exit = 1;

							if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
							{
								__er_file_pointer(F, L - 1);
								return 0;
							}

							/* write the updated size of the array */
							uint32_t new_sz = swap32((uint32_t)sz);
							if (write(fd, &new_sz, sizeof(new_sz)) == -1)
							{
								perror("error in writing remaining size int array.\n");
								return 0;
							}

							/* write the updated padding value */
							uint32_t new_pd = swap32((uint32_t)padding_value);
							if (write(fd, &new_pd, sizeof(new_pd)) == -1)
							{
								perror("error in writing new pading value int array.\n");
								return 0;
							}
						}
						else if (rec->fields[i].data.v.size == (sz + step) && array_last)
						{
							exit = 1;
						}

						while (sz)
						{
							if (step < rec->fields[i].data.v.size)
							{
								uint64_t num_ne = htond(rec->fields[i].data.v.elements.d[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
							sz--;
						}

						if (exit)
						{
							/*write the epty update offset*/
							uint64_t empty_offset = 0;
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}
					else
					{

						int exit = 0;
						for (k = 0; k < sz; k++)
						{
							if (step > 0 && k == 0)
							{
								if ((step + sz) > rec->fields[i].data.v.size)
								{
									int pad = sz - (rec->fields[i].data.v.size - step);
									padding_value += pad;

									sz = rec->fields[i].data.v.size - step;
									exit = 1;

									if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
									{
										__er_file_pointer(F, L - 1);
										return 0;
									}

									/* write the updated size of the array */
									uint32_t new_sz = swap32((uint32_t)sz);
									if (write(fd, &new_sz, sizeof(new_sz)) == -1)
									{
										perror("error in writing remaining size int array.\n");
										return 0;
									}

									/*write padding */
									uint32_t new_pd = swap32((uint32_t)padding_value);
									if (write(fd, &new_pd, sizeof(new_pd)) == -1)
									{
										perror("error in writing new padd int array.\n");
										return 0;
									}
								}
							}

							if (step < rec->fields[i].data.v.size)
							{
								uint64_t num_ne = htond(rec->fields[i].data.v.elements.d[k]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
								if(!(step < rec->fields[i].data.v.size)) exit = 1;
							}
						}

						if (exit)
						{
							if (padding_value > 0)
							{
								if (move_in_file_bytes(fd, padding_value * sizeof(double)) == -1)
								{
									__er_file_pointer(F, L - 1);
									return 0;
								}
							}
							/*write the epty update offset*/
							uint64_t empty_offset = 0;
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}

					if (padding_value > 0)
					{
						if (move_in_file_bytes(fd, padding_value * sizeof(double)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
					}

					uint64_t update_off_ne = 0;
					off_t go_back_to = get_file_offset(fd);

					if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
					{
						perror("failed read update off_t int array.\n");
						return 0;
					}

					if (go_back_to_first_rec == 0)
						go_back_to_first_rec = go_back_to + sizeof(update_off_ne);

					update_pos = (off_t)swap64(update_off_ne);
					if (update_pos == 0)
					{
						/*go to EOF*/
						if ((update_pos = go_to_EOF(fd)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
						/* write the size of the array */
						int size_left = rec->fields[i].data.v.size - step;
						uint32_t size_left_ne = swap32((uint32_t)size_left);
						if (write(fd, &size_left_ne, sizeof(size_left_ne)) == -1)
						{
							perror("error in writing remaining size int array.\n");
							return 0;
						}

						uint32_t padding_ne = swap32(0);
						if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						int j;
						for (j = 0; j < size_left; j++)
						{
							if (step < rec->fields[i].data.v.size)
							{
								uint64_t num_ne = htond(rec->fields[i].data.v.elements.d[step]);
								if (write(fd, &num_ne, sizeof(num_ne)) == -1)
								{
									perror("failed write int array to file");
									return 0;
								}
								step++;
							}
						}

						/*write the empty update offset*/
						uint64_t empty_offset = 0;
						if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						if (find_record_position(fd, go_back_to) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}

						update_off_ne = (uint64_t)swap64((uint64_t)update_pos);
						if (write(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
						{
							fprintf(stderr, "can't write update position int array, %s:%d.\n",
									F, L - 1);
							return 0;
						}

						break;
					}

					if (find_record_position(fd, update_pos) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

				} while (update_pos > 0);

				if (rec->fields[i].data.v.size < sz)
				{

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/*write the size of the array */
					uint32_t size_ne = swap32((uint32_t)rec->fields[i].data.v.size);
					if (write(fd, &size_ne, sizeof(size_ne)) == -1)
					{
						perror("error in writing size array to file.\n");
						return 0;
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					pd_he += (sz - rec->fields[i].data.v.size);

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/* write the padding to apply after the  array */
					pad_ne = swap32((uint32_t)pd_he);
					if (write(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}


					int j;
					for (j = step; j < rec->fields[i].data.v.size; j++)
					{
						uint64_t num_ne = htond(rec->fields[i].data.v.elements.d[j]);
						if (write(fd, &num_ne, sizeof(num_ne)) == -1)
						{
							perror("failed write int array to file");
							return 0;
						}
					}

					/*
					 * move the file pointer after the array
					 * as much as the pad
					 * */
					if (move_in_file_bytes(fd, pd_he * sizeof(double)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					uint64_t update_arr_ne = swap64(0);
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}
				else if (rec->fields[i].data.v.size == sz)
				{
					/*
					 * the sizes are the same
					 * we simply write the array.
					 * */
					if (step > 0)
					{
						if (move_in_file_bytes(fd, -sizeof(sz)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						int size_left = rec->fields[i].data.v.size - step;
						uint32_t sz_ne = swap32((uint32_t)size_left);
						if (write(fd, &sz_ne, sizeof(sz_ne)) == -1)
						{
							fprintf(stderr, "write failed %s:%d.\n", F, L - 1);
							return 0;
						}
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					int j;
					for ( j = 0; j < rec->fields[i].data.v.size; j++)
					{
						if (step < rec->fields[i].data.v.size)
						{
							uint64_t num_ne = htond(rec->fields[i].data.v.elements.d[j]);
							if (write(fd, &num_ne, sizeof(num_ne)) == -1)
							{
								perror("failed write int array to file");
								return 0;
							}
							step++;
						}
					}

					/*
					 * move the file pointer
					 * as much as the padding value
					 * if it si bigger than 0
					 * */
					if (pd_he > 0)
					{
						if (move_in_file_bytes(fd, pd_he * sizeof(double)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}
					}

					uint64_t update_arr_ne = 0;
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				if (go_back_to_first_rec > 0)
				{
					if (find_record_position(fd, go_back_to_first_rec) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}
			break;
		}
		case TYPE_FILE:
		{
			if (!update){	

				/*write the size of the LIST */
				uint32_t size_ne = swap32(rec->fields[i].data.file.count);
				if (write(fd, &size_ne, sizeof(size_ne)) == -1)	{
					perror("error in writing size array to file.\n");
					return 0;
				}

				uint32_t padding_ne = 0;
				if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}

				uint32_t i;
				for(i = 0; i< rec->fields[i].data.file.count; i++){
					if(write_file(fd,&rec->fields[i].data.file.recs[i],0,0) == 0){
						perror("failed write record array to file");
						return 0;
					}
				}
				

				uint64_t upd_ne = 0;
				if (write(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("error in writing size array to file.\n");
					return 0;
				}
			}
			else
			{
				size_t len = strlen(rec->fields[i].field_name);
				char *sfx = ".sch";
				int sfxl = (int)strlen(sfx);
				int totl = sfxl + (int)len + 1;
				char sch_file[totl];
				memset(sch_file,0,totl);
				strncpy(sch_file,rec->fields[i].field_name,len);
				strncat(sch_file,sfx,sfxl);
				/* open the file */
				int fd_schema = open_file(sch_file,0);
				if(file_error_handler(1,fd_schema) != 0) return 0;			

				struct Schema sch;
				memset(&sch,0,sizeof(struct Schema));
				struct Header_d hd = {0,0,&sch};	

				if (!read_header(fd_schema, &hd)) {
					close(fd_schema);
					free_record(rec, rec->fields_num);
					return -1;
				}

				close(fd_schema);
				/* update branch*/
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				uint32_t step = 0;
				uint32_t sz = 0;
				uint32_t k = 0;
				int padding_value = 0;
				do
				{
					/* check the size */
					uint32_t sz_ne = 0;
					if (read(fd, &sz_ne, sizeof(sz_ne)) == -1){
						fprintf(stderr, "can't read int array size.\n");
						return 0;
					}

					sz = swap32(sz_ne);
					if (rec->fields[i].data.file.count < sz || rec->fields[i].data.file.count == sz)
						break;

					/*read the padding data*/
					uint32_t pd_ne = 0;
					if (read(fd, &pd_ne, sizeof(pd_ne)) == -1){
						fprintf(stderr, "can't read padding array.\n");
						return 0;
					}

					padding_value = (int)swap32(pd_ne);

					if (step >= sz)
					{
						int array_last = 0;
						int exit = 0;
						
						/*check if the array of type file is in the last block*/
						off_t reset_pointer_here = 0;
						if((reset_pointer_here = get_file_offset(fd)) == -1){
							__er_file_pointer(F, L - 1);
							return 0;
						}

						uint32_t y;
						for(y = 0; y < sz;y++){
							struct Record_f dummy;
							memset(&dummy,0,sizeof(struct Record_f));
							if(read_file(fd, rec->fields[i].field_name, &dummy, *hd.sch_d) == -1){
								fprintf(stderr,"cannot read type file %s:%d.\n",F,L-1);
								return -1;
							}

							free_record(&dummy,dummy.fields_num);

							if(move_in_file_bytes(fd,sizeof(off_t)) == -1){
								__er_file_pointer(F, L - 1);
								return 0;
							}
						}

						uint64_t update_arr = 0;
						if (read(fd, &update_arr, sizeof(update_arr)) == -1){
							perror("failed read update off_t int array.\n");
							return 0;
						}

						if(find_record_position(fd,reset_pointer_here) == -1){
							__er_file_pointer(F, L - 1);
							return 0;
						}

						off_t up_he = (off_t) swap64(update_arr);
						if(up_he == 0) array_last = 1;

						if (rec->fields[i].data.file.count < (sz + step) && array_last)
						{
							int pad_value = sz - (rec->fields[i].data.file.count - step);
							padding_value += pad_value;

							sz = rec->fields[i].data.v.size - step;
							exit = 1;

							if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
							{
								__er_file_pointer(F, L - 1);
								return 0;
							}

							/* write the updated size of the array */
							uint32_t new_sz = swap32((uint32_t)sz);
							if (write(fd, &new_sz, sizeof(new_sz)) == -1)
							{
								perror("error in writing remaining size int array.\n");
								return 0;
							}

							/* write the updated padding value */
							uint32_t new_pd = swap32((uint32_t)padding_value);
							if (write(fd, &new_pd, sizeof(new_pd)) == -1)
							{
								perror("error in writing new pading value int array.\n");
								return 0;
							}
						}
						else if (rec->fields[i].data.file.count == (sz + step) && array_last)
						{
							exit = 1;
						}

						while (sz)
						{
							if (step < rec->fields[i].data.file.count){

								if(write_file(fd,&rec->fields[i].data.file.recs[step],0,0) == 0){
									perror("failed write record array to file");
									return 0;
								}
									
								step++;
							}
							sz--;
						}

						if (exit){
							/*write the epty update offset*/
							uint64_t empty_offset = 0;
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}
					else
					{

						int exit = 0;
						for (k = 0; k < sz; k++)
						{
							if (step > 0 && k == 0)
							{
								if ((step + sz) > rec->fields[i].data.file.count)
								{
									int pad = sz - (rec->fields[i].data.file.count - step);
									padding_value += pad;

									sz = rec->fields[i].data.file.count - step;
									exit = 1;

									if (move_in_file_bytes(fd, 2 * (-sizeof(uint32_t))) == -1)
									{
										__er_file_pointer(F, L - 1);
										return 0;
									}

									/* write the updated size of the array */
									uint32_t new_sz = swap32((uint32_t)sz);
									if (write(fd, &new_sz, sizeof(new_sz)) == -1)
									{
										perror("error in writing remaining size int array.\n");
										return 0;
									}

									/*write padding */
									uint32_t new_pd = swap32((uint32_t)padding_value);
									if (write(fd, &new_pd, sizeof(new_pd)) == -1)
									{
										perror("error in writing new padd int array.\n");
										return 0;
									}
								}
							}

							if (step < rec->fields[i].data.file.count){
								if(write_file(fd,&rec->fields[i].data.file.recs[step],0,0) == 0){
									perror("failed write record array to file");
									return 0;
								}
								step++;
								if(!(step < rec->fields[i].data.file.count)) exit = 1;
							}
						}
						
						if (exit)
						{

							if (padding_value > 0)
							{
								int y;
								for(y = 0; y < padding_value;y++){
									struct Record_f dummy;
									memset(&dummy,0,sizeof(struct Record_f));
									if(read_file(fd, rec->fields[i].field_name,
												&dummy, *hd.sch_d) == -1){
										fprintf(stderr,"cannot read type file %s:%d.\n"
												,F,L-1);
										return -1;
									}

									free_record(&dummy,dummy.fields_num);

									if(move_in_file_bytes(fd,sizeof(off_t)) == -1){
										__er_file_pointer(F, L - 1);
										return 0;
									}
								}
							}
							/*write the epty update offset*/
							uint64_t empty_offset = 0;
							if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
							{
								perror("error in writing size array to file.\n");
								return 0;
							}
							break;
						}
					}

					if (padding_value > 0)
					{
						int y;
						for(y = 0; y < padding_value;y++){
							struct Record_f dummy ={0};
							if(read_file(fd, rec->fields[i].field_name,
										&dummy, *hd.sch_d) == -1){
								fprintf(stderr,"cannot read type file %s:%d.\n"
										,F,L-1);
								return -1;
							}

							free_record(&dummy,dummy.fields_num);

							if(move_in_file_bytes(fd,sizeof(off_t)) == -1){
								__er_file_pointer(F, L - 1);
								return 0;
							}
						}

					}

					uint64_t update_off_ne = 0;
					off_t go_back_to = get_file_offset(fd);

					if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
					{
						perror("failed read update off_t int array.\n");
						return 0;
					}

					if (go_back_to_first_rec == 0)
						go_back_to_first_rec = go_back_to + sizeof(update_off_ne);

					update_pos = (off_t)swap64(update_off_ne);
					if (update_pos == 0)
					{
						/*go to EOF*/
						if ((update_pos = go_to_EOF(fd)) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}
						/* write the size of the array */
						int size_left = rec->fields[i].data.file.count - step;
						uint32_t size_left_ne = swap32((uint32_t)size_left);
						if (write(fd, &size_left_ne, sizeof(size_left_ne)) == -1) {
							perror("error in writing remaining size int array.\n");
							return 0;
						}

						uint32_t padding_ne = 0;
						if (write(fd, &padding_ne, sizeof(padding_ne)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						int j;
						for (j = 0; j < size_left; j++){
							if (step < rec->fields[i].data.file.count){
								if(write_file(fd,&rec->fields[i].data.file.recs[step],0,0) == 0){
									perror("failed write record array to file");
									return 0;
								}
								step++;
							}
						}

						/*write the empty update offset*/
						uint64_t empty_offset = 0;
						if (write(fd, &empty_offset, sizeof(empty_offset)) == -1)
						{
							perror("error in writing size array to file.\n");
							return 0;
						}

						if (find_record_position(fd, go_back_to) == -1)
						{
							__er_file_pointer(F, L - 1);
							return 0;
						}

						update_off_ne = (uint64_t)swap64((uint64_t)update_pos);
						if (write(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
						{
							fprintf(stderr, "can't write update position int array, %s:%d.\n",
									F, L - 1);
							return 0;
						}

						break;
					}

					if (find_record_position(fd, update_pos) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

				} while (update_pos > 0);

				if (rec->fields[i].data.file.count < sz)
				{

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/*write the size of the array */
					uint32_t size_ne = swap32(rec->fields[i].data.file.count);
					if (write(fd, &size_ne, sizeof(size_ne)) == -1){
						perror("error in writing size array to file.\n");
						return 0;
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1) {
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					pd_he += (sz - rec->fields[i].data.file.count);

					if (move_in_file_bytes(fd, -sizeof(uint32_t)) == -1)
					{
						__er_file_pointer(F, L - 1);
						return 0;
					}

					/* write the padding to apply after the  array */
					pad_ne = swap32((uint32_t)pd_he);
					if (write(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}


					uint32_t j;
					for (j = step; j < rec->fields[i].data.file.count; j++){

						if(write_file(fd,&rec->fields[i].data.file.recs[j],0,0) == 0){
							perror("failed write record array to file");
							return 0;
						}
					}

					/*
					 * move the file pointer after the array
					 * as much as the pad
					 * */
					int y;
					for(y = 0; y < pd_he;y++){
						struct Record_f dummy;
						memset(&dummy,0,sizeof(struct Record_f));
						if(read_file(fd, rec->fields[i].field_name,
									&dummy, *hd.sch_d) == -1){
							fprintf(stderr,"cannot read type file %s:%d.\n"
									,F,L-1);
							return -1;
						}

						free_record(&dummy,dummy.fields_num);

						if(move_in_file_bytes(fd,sizeof(off_t)) == -1){
							__er_file_pointer(F, L - 1);
							return 0;
						}
					}

					uint64_t update_arr_ne = 0;
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}
				else if (rec->fields[i].data.file.count == sz)
				{
					/*
					 * the sizes are the same
					 * we simply write the array.
					 * */
					if (step > 0)
					{
						if (move_in_file_bytes(fd, -sizeof(sz)) == -1)
						{
							__er_file_pointer(F, L - 2);
							return 0;
						}

						int size_left = rec->fields[i].data.file.count - step;
						uint32_t sz_ne = swap32((uint32_t)size_left);
						if (write(fd, &sz_ne, sizeof(sz_ne)) == -1)
						{
							fprintf(stderr, "write failed %s:%d.\n", F, L - 1);
							return 0;
						}
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					if (read(fd, &pad_ne, sizeof(pad_ne)) == -1)
					{
						perror("error in writing padding array to file.\n");
						return 0;
					}

					int pd_he = (int)swap32(pad_ne);
					uint32_t j;
					for (j = 0; j < rec->fields[i].data.file.count; j++)
					{
						if (step < rec->fields[i].data.file.count){

							if(write_file(fd,&rec->fields[i].data.file.recs[j],0,0) == 0){
								perror("failed write record array to file");
								return 0;
							}
							step++;
						}
					}

					/*
					 * move the file pointer
					 * as much as the padding value
					 * if it si bigger than 0
					 * */
					if (pd_he > 0)
					{
						int y;
						for(y = 0; y < padding_value;y++){
							struct Record_f dummy ={0};
							if(read_file(fd, rec->fields[i].field_name,
										&dummy, *hd.sch_d) == -1){
								fprintf(stderr,"cannot read type file %s:%d.\n"
										,F,L-1);
								return -1;
							}

							free_record(&dummy,dummy.fields_num);

							if(move_in_file_bytes(fd,sizeof(off_t)) == -1){
								__er_file_pointer(F, L - 1);
								return 0;
							}
						}

					}

					uint64_t update_arr_ne = 0;
					if (write(fd, &update_arr_ne, sizeof(update_arr_ne)) == -1)
					{
						perror("failed write int array to file");
						return 0;
					}
				}

				if (go_back_to_first_rec > 0)
				{
					if (find_record_position(fd, go_back_to_first_rec) == -1)
					{
						__er_file_pointer(F, L - 2);
						return 0;
					}
				}
			}

			break;
		}
	
		default:
			break;
		}
	}

	uint64_t uot_ne = 0;
	if(update_off_t > 0)
		uot_ne = swap64((uint64_t)update_off_t);

	if (write(fd, &uot_ne, sizeof(uot_ne)) == STATUS_ERROR)
	{
		perror("writing off_t for future update");
		printf(" %s:%d", F, L - 3);
		return 0;
	}

	return 1; /*write to file succssed!*/
}


#if defined(__linux__)
off_t get_update_offset(int fd)
#elif defined(_WIN32)
off_t get_update_offset(HANDLE file_handle)
#endif
{
	uint64_t urec_ne = 0;
#if defined(__linux__)
	if (read(fd, &urec_ne, sizeof(urec_ne)) == STATUS_ERROR)
#elif defined(_WIN32)
	if (!ReadFile(file_handle,&urec_ne,sizeof(urec_ne),&written,NULL))
#endif
	{
		perror("could not read the update record position (file.c l 424).\n");
		return -1;
	}

	return (off_t)swap64(urec_ne);
}

/*
 * read_file:
 *  reads a record from a file,
 *  the caller must inistialized the struct Record_f
 * */
/*TODO WRITE READ RAM RECORD */
int read_file(int fd, char *file_name, struct Record_f *rec, struct Schema sch)
{
	create_record(file_name, sch,rec);
	/*read the count of the field written*/
	uint8_t cvf_ne = 0;
	if (read(fd, &cvf_ne, sizeof(cvf_ne)) < 0) {
		perror("could not read fields number:");
		printf(" %s:%d.\n", __FILE__, __LINE__ - 1);
		return -1;
	}

	uint8_t i_ne = 0;
	uint8_t i;
	for( i = 0; i < cvf_ne; i++){
		/*position in the field_set array*/
		if (read(fd, &i_ne, sizeof(i_ne)) < 0) {
			perror("could not write fields number");
			return 0;
		}

		rec->field_set[i_ne] = 1;
	}

	

	off_t str_loc = 0;
	size_t buff_update = 0; /* to get the real size of the string*/
	off_t move_to = 0;


	for (i = 0; i < (uint8_t) rec->fields_num; i++) {
		if(rec->field_set[i] == 0) continue;

		switch (rec->fields[i].type) {
		case TYPE_INT:
		{
			uint32_t i_ne = 0;
			if (read(fd, &i_ne, sizeof(uint32_t)) < 0) {
				perror("could not read type int file.c 491.\n");
				free_record(rec, rec->fields_num);
				return -1;
			}
			rec->fields[i].data.i = (int)swap32(i_ne);
			break;
		}
		case TYPE_LONG: 
		{
			uint64_t l_ne = 0;
			if (read(fd, &l_ne, sizeof(l_ne)) < 0){
				perror("could not read type long, file.c 498.\n");
				free_record(rec, rec->fields_num);
				return -1;
			}

			rec->fields[i].data.l = (long)swap64(l_ne);
			break;
		}
		case TYPE_FLOAT:
		{
			uint32_t f_ne = 0;
			if (read(fd, &f_ne, sizeof(uint32_t)) < 0) {
				perror("could not read type float, file.c l 505.\n");
				free_record(rec, rec->fields_num);
				return -1;
			}

			rec->fields[i].data.f = ntohf(f_ne);
			break;
		}
		case TYPE_PACK:
		{
			uint32_t p_ne = 0;
			if (read(fd, &p_ne, sizeof(uint32_t)) < 0) {
				perror("could not read type float, file.c l 505.\n");
				free_record(rec, rec->fields_num);
				return -1;
			}

			rec->fields[i].data.p = swap32(p_ne);
			break;
		}
		case TYPE_DATE:
		{
			uint32_t p_ne = 0;
			if (read(fd, &p_ne, sizeof(uint32_t)) < 0) {
				perror("could not read type float, file.c l 505.\n");
				free_record(rec, rec->fields_num);
				return -1;
			}

			rec->fields[i].data.date = swap32(p_ne);
			break;
		}
		case TYPE_STRING:
		{
			/*read pos of new str if any*/
			uint32_t str_loc_ne = 0;
			if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == -1)
			{
				perror("can't read string location: ");
				printf(" %s:%d", F, L - 3);
				free_record(rec, rec->fields_num);
				return -1;
			}
			str_loc = (off_t)swap32(str_loc_ne);

			uint16_t bu_up_ne = 0;
			if (read(fd, &bu_up_ne, sizeof(bu_up_ne)) < 0){
				perror("read from file failed: ");
				printf("%s:%d.\n", F, L - 2);
				free_record(rec, rec->fields_num);
				return -1;
			}

			buff_update = (size_t)swap16(bu_up_ne);

			if (str_loc > 0)
			{
				/*save the offset past the buff_w*/
				move_to = get_file_offset(fd) + buff_update;
				/*move to (other)string location*/
				if (find_record_position(fd, str_loc) == -1)
				{
					__er_file_pointer(F, L - 2);
					free_record(rec, rec->fields_num);
					return -1;
				}

				uint16_t bu_up_ne = 0;
				if (read(fd, &bu_up_ne, sizeof(bu_up_ne)) < 0){
					perror("read file: ");
					printf("%s:%d", F, L - 2);
					free_record(rec, rec->fields_num);
					return -1;
				}

				buff_update = (off_t)swap16(bu_up_ne);
			}

			rec->fields[i].data.s = (char*)ask_mem(buff_update*sizeof(char));
			if (!rec->fields[i].data.s){
				fprintf(stderr,"ask_mem failed: %s:%d.\n", F, L - 3);
				free_record(rec, rec->fields_num);
				return -1;
			}

			/*read the actual string*/
			if (read(fd, rec->fields[i].data.s, buff_update) < 0){
				perror("could not read buffer string, file.c l 539.\n");
				free_record(rec, rec->fields_num);
				return -1;
			}

			/*set file pointer back at the end of the original str record*/
			if (str_loc > 0)
			{
				if (find_record_position(fd, move_to) == -1){
					__er_file_pointer(F, L - 2);
					free_record(rec, rec->fields_num);
					return -1;
				}
			}

			break;
		}
		case TYPE_BYTE:
		{
			if (read(fd, &rec->fields[i].data.b, sizeof(unsigned char)) == -1){
				perror("could not read type byte: ");
				printf(" %s:%d.\n", F, L - 2);
				free_record(rec, rec->fields_num);
				return -1;
			}

			break;
		}
		case TYPE_DOUBLE:
		{
			uint64_t d_ne = 0;
			if (read(fd, &d_ne, sizeof(d_ne)) < 0)
			{
				perror("could not read type double:");
				printf(" %s:%d.\n", F, L - 2);
				free_record(rec, rec->fields_num);
				return -1;
			}

			rec->fields[i].data.d = ntohd(d_ne);
			break;
		}
		case TYPE_ARRAY_INT:
		{
			if (!rec->fields[i].data.v.elements.i){
				rec->fields[i].data.v.insert = insert_element;
				rec->fields[i].data.v.destroy = free_dynamic_array;
			}

			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				if (read(fd, &size_array, sizeof(size_array)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int sz = (int)swap32(size_array);
				uint32_t padding = 0;
				if (read(fd, &padding, sizeof(padding)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int padd = (int)swap32(padding);

				int j;
				for ( j = 0; j < sz; j++)
				{
					uint32_t num_ne = 0;
					if (read(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("can't read int array from file.\n");
						free_record(rec, rec->fields_num);
						return -1;
					}
					int num = (int)swap32(num_ne);
					rec->fields[i].data.v.insert((void *)&num,
						&rec->fields[i].data.v,
						rec->fields[i].type);
				}

				if (padd > 0)
				{
					if (move_in_file_bytes(fd, padd * sizeof(int)) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return -1;
					}
				}

				uint64_t upd_ne = 0;
				if (read(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("can't read int array from file.\n");
					free_record(rec, rec->fields_num);
					return -1;
				}

				if (go_back_here == 0)
				{
					go_back_here = get_file_offset(fd);
				}

				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0)
				{
					if (find_record_position(fd, array_upd) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return -1;
					}
				}
			} while (array_upd > 0);

			if (find_record_position(fd, go_back_here) == -1)
			{
				__er_file_pointer(F, L - 1);
				free_record(rec, rec->fields_num);
				return -1;
			}
			break;
		}
		case TYPE_ARRAY_LONG:
		{
			if (!rec->fields[i].data.v.elements.l)
			{
				rec->fields[i].data.v.insert = insert_element;
				rec->fields[i].data.v.destroy = free_dynamic_array;
			}

			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				if (read(fd, &size_array, sizeof(size_array)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int sz = (int)swap32(size_array);
				uint32_t padding = 0;
				if (read(fd, &padding, sizeof(padding)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int padd = (int)swap32(padding);

				int j;
				for (j = 0; j < sz; j++)
				{
					uint64_t num_ne = 0;
					if (read(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("can't read int array from file.\n");
						free_record(rec, rec->fields_num);
						return -1;
					}
					long num = (long)swap64(num_ne);
					rec->fields[i].data.v.insert((void *)&num,
												 &rec->fields[i].data.v,
												 rec->fields[i].type);
				}

				if (padd > 0)
				{
					if (move_in_file_bytes(fd, padd * sizeof(long)) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return -1;
					}
				}

				uint64_t upd_ne = 0;
				if (read(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("can't read int array from file.\n");
					free_record(rec, rec->fields_num);
					return -1;
				}

				if (go_back_here == 0)
				{
					go_back_here = get_file_offset(fd);
				}
				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0)
				{
					if (find_record_position(fd, array_upd) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return -1;
					}
				}
			} while (array_upd > 0);

			if (find_record_position(fd, go_back_here) == -1)
			{
				__er_file_pointer(F, L - 1);
				free_record(rec, rec->fields_num);
				return -1;
			}
			break;
		}
		case TYPE_ARRAY_STRING:
		{
			if (!rec->fields[i].data.v.elements.s)
			{
				rec->fields[i].data.v.insert = insert_element;
				rec->fields[i].data.v.destroy = free_dynamic_array;
			}

			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				if (read(fd, &size_array, sizeof(size_array)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int sz = (int)swap32(size_array);
				uint32_t padding = 0;
				if (read(fd, &padding, sizeof(padding)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int padd = (int)swap32(padding);

				int j;
				for (j = 0; j < sz; j++)
				{

					/*read pos of new str if any*/
					uint32_t str_loc_ne = 0;
					if (read(fd, &str_loc_ne, sizeof(str_loc_ne)) == -1)
					{
						perror("can't read string location: ");
						printf(" %s:%d", F, L - 3);
						return -1;
					}
					str_loc = (size_t)swap32(str_loc_ne);

					uint16_t bu_up_ne = 0;
					if (read(fd, &bu_up_ne, sizeof(bu_up_ne)) < 0)
					{
						perror("read from file failed: ");
						printf("%s:%d.\n", F, L - 2);
						free_record(rec, rec->fields_num);
						return -1;
					}

					buff_update = (size_t)swap16(bu_up_ne);

					if (str_loc > 0)
					{
						/*save the offset past the buff_w*/
						move_to = get_file_offset(fd) + buff_update;
						/*move to (other)string location*/
						if (find_record_position(fd, str_loc) == -1)
						{
							__er_file_pointer(F, L - 2);
							free_record(rec, rec->fields_num);
							return -1;
						}

						uint16_t bu_up_ne = 0;
						if (read(fd, &bu_up_ne, sizeof(bu_up_ne)) < 0)
						{
							perror("read file: ");
							printf("%s:%d", F, L - 2);
							free_record(rec, rec->fields_num);
							return -1;
						}

						buff_update = (size_t)swap16(bu_up_ne);
					}

					char *all_buf = (char*)ask_mem(buff_update*sizeof(char));
					if (!all_buf){
						printf("ask_mem() failed, %s:%d.\n",F,L-2);
						free_record(rec, rec->fields_num);
						return -1;
					}

					/*read the actual string*/
					if (read(fd, all_buf, buff_update) < 0){
						perror("could not read buffer string, file.c l 539.\n");
						free_record(rec, rec->fields_num);
						return -1;
					}

					rec->fields[i].data.v.insert((void *)all_buf,
							 &rec->fields[i].data.v,
							 rec->fields[i].type);
					cancel_memory(NULL,all_buf,buff_update);

					/*set file pointer back at the end of the original str record*/
					if (str_loc > 0)
					{
						if (find_record_position(fd, move_to) == -1)
						{
							__er_file_pointer(F, L - 2);
							free_record(rec, rec->fields_num);
							return -1;
						}
					}
				}

				if (padd > 0)
				{
					int x;
					for( x = 0; x < padd; x++){
						if(get_string_size(fd,NULL) == (size_t)-1){
							__er_file_pointer(F, L - 1);
							free_record(rec, rec->fields_num);
							return -1;
						}

					}
				}

				uint64_t upd_ne = 0;
				if (read(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("can't read int array from file.\n");
					free_record(rec, rec->fields_num);
					return -1;
				}

				if (go_back_here == 0)
				{
					go_back_here = get_file_offset(fd);
				}
				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0)
				{
					if (find_record_position(fd, array_upd) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return -1;
					}
				}
			} while (array_upd > 0);

			if (find_record_position(fd, go_back_here) == -1)
			{
				__er_file_pointer(F, L - 1);
				free_record(rec, rec->fields_num);
				return -1;
			}
			break;
		}
		case TYPE_ARRAY_FLOAT:
		{
			if (!rec->fields[i].data.v.elements.f)
			{
				rec->fields[i].data.v.insert = insert_element;
				rec->fields[i].data.v.destroy = free_dynamic_array;
			}

			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				if (read(fd, &size_array, sizeof(size_array)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int sz = (int)swap32(size_array);
				uint32_t padding = 0;
				if (read(fd, &padding, sizeof(padding)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int padd = (int)swap32(padding);

				int j;
				for (j = 0; j < sz; j++)
				{
					uint32_t num_ne = 0;
					if (read(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("can't read int array from file.\n");
						free_record(rec, rec->fields_num);
						return -1;
					}

					float num = ntohf(num_ne);
					rec->fields[i].data.v.insert((void *)&num,
												 &rec->fields[i].data.v,
												 rec->fields[i].type);
				}

				if (padd > 0)
				{
					if (move_in_file_bytes(fd, padd * sizeof(float)) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return -1;
					}
				}

				uint64_t upd_ne = 0;
				if (read(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("can't read int array from file.\n");
					free_record(rec, rec->fields_num);
					return -1;
				}

				if (go_back_here == 0)
				{
					go_back_here = get_file_offset(fd);
				}

				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0)
				{
					if (find_record_position(fd, array_upd) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return -1;
					}
				}
			} while (array_upd > 0);

			if (find_record_position(fd, go_back_here) == -1)
			{
				__er_file_pointer(F, L - 1);
				free_record(rec, rec->fields_num);
				return -1;
			}
			break;
		}
		case TYPE_ARRAY_DOUBLE:
		{
			if (!rec->fields[i].data.v.elements.d)
			{
				rec->fields[i].data.v.insert = insert_element;
				rec->fields[i].data.v.destroy = free_dynamic_array;
			}

			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				if (read(fd, &size_array, sizeof(size_array)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int sz = (int)swap32(size_array);
				uint32_t padding = 0;
				if (read(fd, &padding, sizeof(padding)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int padd = (int)swap32(padding);

				int j;
				for (j = 0; j < sz; j++)
				{
					uint64_t num_ne = 0;
					if (read(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("can't read int array from file.\n");
						free_record(rec, rec->fields_num);
					return -1;
					}
					double num = ntohd(num_ne);
					rec->fields[i].data.v.insert((void *)&num,
												 &rec->fields[i].data.v,
												 rec->fields[i].type);
				}

				if (padd > 0)
				{
					if (move_in_file_bytes(fd, padd * sizeof(double)) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
					return -1;
					}
				}

				uint64_t upd_ne = 0;
				if (read(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("can't read int array from file.\n");
					free_record(rec, rec->fields_num);
					return -1;
				}

				if (go_back_here == 0)
				{
					go_back_here = get_file_offset(fd);
				}
				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0)
				{
					if (find_record_position(fd, array_upd) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
					return -1;
					}
				}
			} while (array_upd > 0);

			if (find_record_position(fd, go_back_here) == -1)
			{
				__er_file_pointer(F, L - 1);
				free_record(rec, rec->fields_num);
					return -1;
			}
			break;
		}
		case TYPE_ARRAY_BYTE:
		{
			if (!rec->fields[i].data.v.elements.b)
			{
				rec->fields[i].data.v.insert = insert_element;
				rec->fields[i].data.v.destroy = free_dynamic_array;
			}

			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				if (read(fd, &size_array, sizeof(size_array)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int sz = (int)swap32(size_array);
				uint32_t padding = 0;
				if (read(fd, &padding, sizeof(padding)) == -1)
				{
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int padd = (int)swap32(padding);

				int j;
				for (j = 0; j < sz; j++)
				{
					uint8_t num_ne = 0;
					if (read(fd, &num_ne, sizeof(num_ne)) == -1)
					{
						perror("can't read int array from file.\n");
						free_record(rec, rec->fields_num);
					return -1;
					}

					rec->fields[i].data.v.insert((void *)&num_ne,
									 &rec->fields[i].data.v,
									 rec->fields[i].type);
				}

				if (padd > 0)
				{
					if (move_in_file_bytes(fd, padd * sizeof(unsigned char)) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
					return -1;
					}
				}

				uint64_t upd_ne = 0;
				if (read(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("can't read int array from file.\n");
					free_record(rec, rec->fields_num);
					return -1;
				}

				if (go_back_here == 0)
				{
					go_back_here = get_file_offset(fd);
				}
				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0)
				{
					if (find_record_position(fd, array_upd) == -1)
					{
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
					return -1;
					}
				}
			} while (array_upd > 0);

			if (find_record_position(fd, go_back_here) == -1)
			{
				__er_file_pointer(F, L - 1);
				free_record(rec, rec->fields_num);
				return -1;
			}
			break;
		}
		case TYPE_FILE:
		{
			size_t len = strlen(rec->fields[i].field_name);
			char *sfx = ".sch";
			int sfxl = (int)strlen(sfx);
			int totl = sfxl + (int)len + 1;
			char sch_file[totl];
			memset(sch_file,0,totl);
			strncpy(sch_file,rec->fields[i].field_name,len);
			strncat(sch_file,sfx,sfxl);

			/* open the file*/
			int fd_schema = open_file(sch_file,0);
			if(file_error_handler(1,fd_schema) != 0) return 0;			

			struct Schema sch;
			memset(&sch,0,sizeof(struct Schema));
			struct Header_d hd = {0,0,&sch};	

			if (!read_header(fd_schema, &hd)) {
				close(fd_schema);
				free_record(rec, rec->fields_num);
				return -1;
			}

			close(fd_schema);



			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				if (read(fd, &size_array, sizeof(size_array)) == -1){
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				uint32_t sz = swap32(size_array);
				uint32_t padding = 0;
				if (read(fd, &padding, sizeof(padding)) == -1){
					perror("error readig array.");
					free_record(rec, rec->fields_num);
					return -1;
				}

				int padd = (int)swap32(padding);
				uint32_t j;
				for(j = 0; j < sz; j++){
					if (!rec->fields[i].data.file.recs){
						rec->fields[i].data.file.recs = (struct Record_f*)ask_mem(sizeof(struct Record_f));
						rec->fields[i].data.file.count = sz;
						if(!rec->fields[i].data.file.recs){
							fprintf(stderr,"ask_mem() failed %s:%d.\n",F,L-3);
							free_record(rec, rec->fields_num);
							return -1;
						}
						
						if(read_file(fd, rec->fields[i].field_name, 
									&rec->fields[i].data.file.recs[j],
									*hd.sch_d) == -1){
							fprintf(stderr,"cannot read type file %s:%d.\n",F,L-1);
							free_record(rec, rec->fields_num);
							return -1;
						}
					}else{

						uint32_t new_size = rec->fields[i].data.file.count + 1;	
						struct Record_f* new_rec = (struct Record_f*)reask_mem(rec->fields[i].data.file.recs,
								rec->fields[i].data.file.count * sizeof(struct Record_f),
								new_size * sizeof(struct Record_f));

						if(!new_rec){
							fprintf(stderr,"reask_mem() failed, %s:%d.\n",__FILE__,__LINE__-4);
							free_record(rec, rec->fields_num);
							return -1;
						}
						rec->fields[i].data.file.recs = new_rec;
						rec->fields[i].data.file.count = new_size;

						if(read_file(fd, rec->fields[i].field_name,
									&rec->fields[i].data.file.recs[new_size -1]
									,*hd.sch_d) == -1){
							fprintf(stderr,"cannot read type file %s:%d.\n",F,L-1);
							free_record(rec, rec->fields_num);
							return -1;
						}
					}

					/*TODO refactor this code file.c l 5930*/

					/*check if the record has updates */
					off_t rests_pos_here = get_file_offset(fd);
					off_t update_rec_pos = 0; 
					while ((update_rec_pos = get_update_offset(fd)) > 0) {
						uint32_t new_size = rec->fields[i].data.file.count + 1;

						rec->fields[i].data.file.recs= (struct Record_f*)reask_mem(
								rec->fields[i].data.file.recs,	
								rec->fields[i].data.file.count * sizeof(struct Record_f),
								new_size * sizeof(struct Record_f));

						if (!rec->fields[i].data.file.recs) {
							fprintf(stderr,"reask_mem() failed, %s:%d.\n",__FILE__,__LINE__-6);
							free_record(rec, rec->fields_num);
							return -1;
						}

						rec->fields[i].data.file.count = new_size;
						if (find_record_position(fd, update_rec_pos) == -1) {
							__er_file_pointer(F, L - 1);
							free_record(rec, rec->fields_num);
							return -1;
						}

						if(read_file(fd, 
								rec->fields[i].field_name,
								&rec->fields[i].data.file.recs[new_size -1],
								*hd.sch_d) == -1) {
							fprintf(stderr,"cannot read record of embedded file,%s:%d.\n",F,L-1);
							free_record(rec, rec->fields_num);
							return -1;
						}
					}

					if(update_rec_pos == -1 || rests_pos_here== -1){
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return -1;
					}

					if (find_record_position(fd, rests_pos_here) == -1) {
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return -1;
					}

					if (move_in_file_bytes(fd, sizeof(off_t)) == -1) {
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return -1;
					}
					
				}

				if (padd > 0)
				{
					int y;
					for(y = 0; y < padd;y++){
						struct Record_f dummy;
						memset(&dummy,0,sizeof(struct Record_f));
						if(read_file(fd, rec->fields[i].field_name,
									&dummy, *hd.sch_d) == -1){
							fprintf(stderr,"cannot read type file %s:%d.\n"
									,F,L-1);
							return -1;
						}

						free_record(&dummy,dummy.fields_num);

						if(move_in_file_bytes(fd,sizeof(off_t)) == -1){
							__er_file_pointer(F, L - 1);
							return 0;
						}
					}
				}

				uint64_t upd_ne = 0;
				if (read(fd, &upd_ne, sizeof(upd_ne)) == -1)
				{
					perror("can't read int array from file.\n");
					free_record(rec, rec->fields_num);
					return -1;
				}

				if (go_back_here == 0) go_back_here = get_file_offset(fd);

				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0){
					if (find_record_position(fd, array_upd) == -1){
						__er_file_pointer(F, L - 1);
						free_record(rec, rec->fields_num);
						return -1;
					}
				}
			} while (array_upd > 0);

			if (find_record_position(fd, go_back_here) == -1)
			{
				__er_file_pointer(F, L - 1);
				free_record(rec, rec->fields_num);
				return -1;
			}
			break;
		}
		default:
			break;
		}
	}

	return 0;
}
int file_error_handler(int count, ...)
{
	va_list args;
	va_start(args, count);
#if defined(__linux__)
	int fds[count];
#elif defined(_WIN32)
	HANDLE handles[count];
#elif
	int i = 0, j = 0;

	int err = 0;
	for (i = 0; i < count; i++) {
#if defined(__linux__)
		int fd = va_arg(args, int);
		if (fd == STATUS_ERROR) 
#elif defined(_WIN32)
		HANDLE file_handle = va_args(args,HANDLE);
		if (file_handle == INVALID_HANDLE_VALUE)
#endif
		{
			j++;
			continue;
		}
#if defined(__linux__)
		if (fd == ENOENT) err++;
		fds[i] = fd;
#elif defined(_WIN32)
		if (file_handle == ERROR_FILE_NOT_FOUND) err++;
		handles[i] = file_handle;
#endif
	}

	if(j != 0 ){
		int x;
		for(x = 0 ;x < 0; x++){
#if denifed(__linux__)
			if(fds[x] != -1	) close(fds[x]);
#elif defined(_WIN32)
			if(handles[x] !=  INVALID_HANDLE_VALUE || handles[x] != ERROR_FILE_NOT_FOUND)
				CloseHandle(handles[x]);
#endif
	   } 
	}
#if denifed(__linux__)
	if(err > 0) return ENOENT;	
#elif defined(_WIN32)
	if(err > 0) return ERROR_FILE_NOT_FOUND;	
#endif
	return j;
}
#endif


int add_index(int index_nr, char *file_name, int bucket)
{
	if (index_nr == 0)
		return -1;

	size_t l = strlen(file_name) + strlen(".inx") + 1;
	char buff[l];
	memset(buff, 0, l);

	if (copy_to_string(buff, l, "%s%s", file_name, ".inx") < 0) {
		fprintf(stderr,
				"copy_to_string() failed. %s:%d.\n",
				F, L - 3);
		return -1;
	}
#if defined(__linux__)
	int fd = open_file(buff, 0);
	if (file_error_handler(1, fd) > 0) 
#elif defined(_WIN32)
	HANDLE file_handle = open_file(buff,0)
#endif
	{
		fprintf(stderr,"can't open %s, %s:%d.\n", buff, F, L - 3);
		return -1;
	}

	HashTable *ht = NULL;
	int ht_i = 0;
	if (!read_all_index_file(fd, &ht, &ht_i))
	{
		fprintf(stderr,"read_all_index_file(),failed %s:%d.\n",
				F, L - 4);
		return -1;
	}

	HashTable *ht_new = (HashTable*)reask_mem(ht,ht_i * sizeof *ht,(ht_i + index_nr) * sizeof(HashTable));
	if (!ht_new){
		fprintf(stderr,"(%s): reask_mem() failed, %s:%d.\n",prog,F, L - 2);
		free_ht_array(ht, ht_i);
		return -1;
	}

	ht = ht_new;
	int total_indexes = ht_i + index_nr;
	close_file(1, fd);

	int j;
	for (j = ht_i; j < total_indexes; ++j)
	{
		HashTable dummy;
		memset(&dummy,0,sizeof(HashTable)); 
		dummy.write = write_ht;
		dummy.size = bucket;

		ht[j] = dummy;
	}
	/*reopen the file with O_TRUNCATE flag to overwrite the old content*/
	fd = open_file(buff, 1);
	if (file_error_handler(1, fd) > 0) {
		fprintf(stderr,	"can't open %s, %s:%d.\n", buff, F, L - 3);
		free_ht_array(ht, total_indexes);
		return -1;
	}

	if (!write_index_file_head(fd, total_indexes)) {
		fprintf(stderr, "write_index_file_head() failed, %s:%d.",
				F, L - 3);
		free_ht_array(ht, total_indexes);
		return -1;
	}

	int i;
	for (i = 0; i < total_indexes; ++i) {
		/*
		 * create an Hashtable in the
		 * reallocated hastable array
		 * */
		if (ht[i].data_map[0] == NULL) {
			if (!write_index_body(fd, i, &ht[i])) {
				fprintf(stderr,"write_index_body() failed %s:%d.\n",F, L - 3);
				free_ht_array(ht, total_indexes);
				return -1;
			}

			destroy_hasht(&ht[i]);
			continue;
		}

		if (!write_index_body(fd, i, &ht[i])) {
			fprintf(stderr,	"write_index_body() failed %s:%d.\n",F, L - 3);
			free_ht_array(ht, total_indexes);
			return -1;
		}

		destroy_hasht(&ht[i]);
	}

	cancel_memory(NULL,ht,total_indexes * sizeof *ht);
	close_file(1, fd);
	return 0;
}


static size_t get_disk_size_record(struct Record_f *rec)
{

	size_t size = 0;
	size += (sizeof(uint8_t));
	int i;
	for(i = 0; i < rec->fields_num; i++){
		if(rec->field_set[i] == 0) continue;

		size += sizeof(uint8_t);
		switch(rec->fields[i].type){
		case -1:
			break;
		case TYPE_INT:
			size += sizeof(uint32_t);
			break;
		case TYPE_DATE:
			size += sizeof(uint32_t);
			break;
		case TYPE_LONG:
			size += sizeof(uint64_t);
			break;
		case TYPE_BYTE:
			size += sizeof(uint8_t);
			break;
		case TYPE_STRING:
			size += (sizeof(uint32_t) + sizeof(uint16_t));
			size += ((strlen(rec->fields[i].data.s) * 2) + 1);
			break;
		case TYPE_FLOAT:
			size += sizeof(uint32_t);
			break;
		case TYPE_DOUBLE:
			size += sizeof(uint64_t);
			break;
		case TYPE_ARRAY_INT:
			size += (sizeof(uint32_t) * 2);
			size += (sizeof(uint32_t) * rec->fields[i].data.v.size);
			size += (sizeof(uint64_t));
			break;
		case TYPE_ARRAY_LONG:
			size += (sizeof(uint32_t) * 2);
			size += (sizeof(uint64_t) * rec->fields[i].data.v.size);
			size += (sizeof(uint64_t));
			break;
		case TYPE_ARRAY_BYTE:
			size += (sizeof(uint32_t) * 2);
			size += (sizeof(uint8_t) * rec->fields[i].data.v.size);
			size += (sizeof(uint64_t));
			break;
		case TYPE_ARRAY_FLOAT:
			size += (sizeof(uint32_t) * 2);
			size += (sizeof(uint32_t) * rec->fields[i].data.v.size);
			size += (sizeof(uint64_t));
			break;
		case TYPE_ARRAY_DOUBLE:
			size += (sizeof(uint32_t) * 2);
			size += (sizeof(uint64_t) * rec->fields[i].data.v.size);
			size += (sizeof(uint64_t));
			break;
		case TYPE_ARRAY_STRING:
			size += (sizeof(uint32_t) * 2);
			size += ((sizeof(uint32_t) + sizeof(uint16_t)) * rec->fields[i].data.v.size);
			int j;
			for(j = 0; j < rec->fields[i].data.v.size; j++){
				size += ((strlen(rec->fields[i].data.s) * 2) + 1);
			}
			size += (sizeof(uint64_t));
			break;
		case TYPE_FILE:
			size += (sizeof(uint32_t) * 2);
			uint32_t k;
			for(k = 0; k < rec->fields[i].data.file.count; k++){
				size += get_disk_size_record(&rec->fields[i].data.file.recs[k]);
			}
			size += (sizeof(uint64_t));
			break;
		default:
			return -1;
		}
	}
	
	size += (sizeof(uint64_t));
	return size;
}

static int init_ram_file(struct Ram_file *ram, size_t size)
{
	if(size <= 0){
		ram->mem = (uint8_t*)ask_mem(STD_RAM_FILE*sizeof(uint8_t)); 
		if(!ram->mem){
			fprintf(stderr,"ask_mem() failed, %s:%d.\n",F,L-2);
			return -1;
		}
		ram->size = 0;
		ram->offset = 0;
		ram->capacity = STD_RAM_FILE;
		return 0;
	}

	ram->mem = (uint8_t*)ask_mem(size*sizeof(uint8_t));
	if(!ram->mem){
		fprintf(stderr,"ask_mem() failed, %s:%d.\n",F,L-2);
		return -1;
	}
	ram->size = 0;
	ram->offset = 0;
	ram->capacity = size;
	return 0;
}

void clear_ram_file(struct Ram_file *ram)
{
	memset(ram->mem,0,ram->capacity);
	ram->size = 0;
}

void close_ram_file(struct Ram_file *ram)
{
	cancel_memory(NULL,ram->mem,ram->capacity);
	ram->size = 0;
	ram->capacity = 0;
}

size_t get_offset_ram_file(struct Ram_file *ram)
{
	return ram->size;
}

#if defined(__linux__)
int get_all_record(int fd, struct Ram_file *ram)
#elif defined(_WIN32)
int get_all_record(HANDLE file_handle, struct Ram_file *ram)
#endif
{

#if defined(__linux__)
	off_t eof = go_to_EOF(fd);
	if(begin_in_file(fd) == -1) return -1;
#elif defined(_WIN32)
	off_t eof = go_to_EOF(file_handle);
	if(begin_in_file(file_handle) == -1) return -1;
#endif

	if(init_ram_file(ram,(size_t)eof) == -1) return -1;	

#if defined(__linux__)
	if(read(fd,ram->mem,ram->capacity) == -1) return -1;
#elif defined(_WIN32)
	DWORD written = 0;
	if(!ReadFile(file_handle,ram->mem,ram->capacity,&written,NULL)) return -1;
#endif

	ram->size = (size_t)eof;
	ram->offset = 0;
	return 0;
}

off_t read_ram_file(char* file_name, struct Ram_file *ram, struct Record_f *rec, struct Schema sch)
{
	create_record(file_name, sch,rec);

	uint8_t fields_on_file = 0;
	memcpy(&fields_on_file,&ram->mem[ram->offset],sizeof(uint8_t));
	move_ram_file_ptr(ram,sizeof(uint8_t));

	uint8_t indexes[fields_on_file];
	memset(indexes,-1,fields_on_file);
	memcpy(indexes,&ram->mem[ram->offset],sizeof(uint8_t)*fields_on_file);		
	move_ram_file_ptr(ram,sizeof(uint8_t)*2);

	uint8_t i;
	for(i = 0; i < fields_on_file; i++){
		rec->field_set[indexes[i]] = 1;
		switch(rec->fields[indexes[i]].type){
		case TYPE_INT:
		{
			uint32_t n = 0;
			memcpy(&n,&ram->mem[ram->offset],sizeof(uint32_t));
			move_ram_file_ptr(ram,sizeof(uint32_t));
			rec->fields[indexes[i]].data.i = swap32(n);
			break;
		}
		case TYPE_LONG:
		{
			uint64_t n = 0;
			memcpy(&n,&ram->mem[ram->offset],sizeof(uint64_t));
			move_ram_file_ptr(ram,sizeof(uint64_t));
			rec->fields[indexes[i]].data.l = swap64(n);
			break;
		}
		case TYPE_BYTE:
		{
			uint8_t n = 0;
			memcpy(&n,&ram->mem[ram->offset],sizeof(uint8_t));
			move_ram_file_ptr(ram,sizeof(uint8_t));
			rec->fields[indexes[i]].data.b = n;
			break;
		}
		case TYPE_STRING:
		{
			uint32_t str_loc_ne = 0;
			memcpy(&str_loc_ne,&ram->mem[ram->offset],sizeof(uint32_t));
			move_ram_file_ptr(ram,sizeof(uint32_t));
			off_t str_loc = (off_t)swap32(str_loc_ne);


			uint16_t buf_up_ne = 0;
			memcpy(&buf_up_ne,&ram->mem[ram->offset],sizeof(uint16_t));
			move_ram_file_ptr(ram,sizeof(uint16_t));
			size_t buf_up = swap16(buf_up_ne);

			off_t move_back_to = 0;
			if(str_loc > 0){
				move_back_to = ram->offset + buf_up;	
				/* move to the new location*/
				ram->offset = str_loc;

				buf_up_ne = 0;
				memcpy(&buf_up_ne,&ram->mem[ram->offset],sizeof(uint16_t));
				move_ram_file_ptr(ram,sizeof(uint16_t));
				buf_up = swap16(buf_up_ne);
			}

			rec->fields[indexes[i]].data.s = (char*)ask_mem(buf_up*sizeof(char));
			if(!rec->fields[indexes[i]].data.s){
				fprintf(stderr,"ask_mem() failed, %s:%d.\n",F,L-2);
				return -1;
			}

			memcpy(rec->fields[indexes[i]].data.s,&ram->mem[ram->offset],buf_up);
			move_ram_file_ptr(ram,buf_up);
			if(str_loc > 0) ram->offset = move_back_to;

			break;
		}
		case TYPE_FLOAT:
		{
			uint32_t n = 0;
			memcpy(&n,&ram->mem[ram->offset],sizeof(uint32_t));
			move_ram_file_ptr(ram,sizeof(uint32_t));
			rec->fields[indexes[i]].data.f = ntohf(n);
			break;
		}
		case TYPE_PACK:
		{
			uint32_t n = 0;
			memcpy(&n,&ram->mem[ram->offset],sizeof(uint32_t));
			move_ram_file_ptr(ram,sizeof(uint32_t));
			rec->fields[indexes[i]].data.p = swap32(n);
			break;
		}
		case TYPE_DOUBLE:
		{
			uint64_t n = 0;
			memcpy(&n,&ram->mem[ram->offset],sizeof(uint64_t));
			move_ram_file_ptr(ram,sizeof(uint64_t));
			rec->fields[indexes[i]].data.d = ntohd(n);
			break;
		}
		case TYPE_ARRAY_INT:
		{
			if (!rec->fields[indexes[i]].data.v.elements.i){
				rec->fields[indexes[i]].data.v.insert = insert_element;
				rec->fields[indexes[i]].data.v.destroy = free_dynamic_array;
			}

			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				memcpy(&size_array,&ram->mem[ram->offset],sizeof(uint32_t));
				move_ram_file_ptr(ram,sizeof(uint32_t));
				int sz = (int)swap32(size_array);


				uint32_t padding = 0;
				memcpy(&padding,&ram->mem[ram->offset],sizeof(uint32_t));
				move_ram_file_ptr(ram,sizeof(uint32_t));
				int padd = (int)swap32(padding);

				int j;
				for (j = 0; j < sz; j++){
					uint32_t n = 0;
					memcpy(&n,&ram->mem[ram->offset],sizeof(uint32_t));
					move_ram_file_ptr(ram,sizeof(uint32_t));
					int num = (int)swap32(n);
					rec->fields[indexes[i]].data.v.insert((void *)&num,
						&rec->fields[indexes[i]].data.v,
						rec->fields[indexes[i]].type);
				}

				if (padd > 0) ram->offset += (sizeof(uint32_t) * padd);

				uint64_t upd_ne = 0;
				memcpy(&upd_ne,&ram->mem[ram->offset],sizeof(uint64_t));
				move_ram_file_ptr(ram,sizeof(uint64_t));

				if (go_back_here == 0) go_back_here = ram->offset;

				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0) ram->offset = array_upd;

			} while (array_upd > 0);

			ram->offset = go_back_here;
			break;
		}
		case TYPE_ARRAY_LONG:
		{
			if (!rec->fields[indexes[i]].data.v.elements.l){
				rec->fields[indexes[i]].data.v.insert = insert_element;
				rec->fields[indexes[i]].data.v.destroy = free_dynamic_array;
			}
			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				memcpy(&size_array,&ram->mem[ram->offset],sizeof(uint32_t));
				move_ram_file_ptr(ram,sizeof(uint32_t));
				int sz = (int)swap32(size_array);


				uint32_t padding = 0;
				memcpy(&padding,&ram->mem[ram->offset],sizeof(uint32_t));
				move_ram_file_ptr(ram,sizeof(uint32_t));
				int padd = (int)swap32(padding);

				int j;
				for (j = 0; j < sz; j++){
					uint64_t n = 0;
					memcpy(&n,&ram->mem[ram->offset],sizeof(uint64_t));
					move_ram_file_ptr(ram,sizeof(uint64_t));

					long num = (long)swap64(n);

					rec->fields[indexes[i]].data.v.insert((void *)&num,
						&rec->fields[indexes[i]].data.v,
						rec->fields[indexes[i]].type);
				}

				if (padd > 0) ram->offset += (sizeof(uint64_t) * padd);

				uint64_t upd_ne = 0;
				memcpy(&upd_ne,&ram->mem[ram->offset],sizeof(uint64_t));
				move_ram_file_ptr(ram,sizeof(uint64_t));

				if (go_back_here == 0) go_back_here = ram->offset;

				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0) ram->offset = array_upd;

			} while (array_upd > 0);

			ram->offset = go_back_here;
			break;
		}
		case TYPE_ARRAY_BYTE:
		{
			if (!rec->fields[indexes[i]].data.v.elements.b){
				rec->fields[indexes[i]].data.v.insert = insert_element;
				rec->fields[indexes[i]].data.v.destroy = free_dynamic_array;
			}
			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				memcpy(&size_array,&ram->mem[ram->offset],sizeof(uint32_t));
				move_ram_file_ptr(ram,sizeof(uint32_t));
				int sz = (int)swap32(size_array);


				uint32_t padding = 0;
				memcpy(&padding,&ram->mem[ram->offset],sizeof(uint32_t));
				move_ram_file_ptr(ram,sizeof(uint32_t));
				int padd = (int)swap32(padding);

				int j;
				for (j = 0; j < sz; j++){
					uint8_t n = 0;
					memcpy(&n,&ram->mem[ram->offset],sizeof(uint8_t));
					move_ram_file_ptr(ram,sizeof(uint8_t));
					rec->fields[indexes[i]].data.v.insert((void *)&n,
						&rec->fields[indexes[i]].data.v,
						rec->fields[indexes[i]].type);
				}

				if (padd > 0) ram->offset += (sizeof(uint8_t) * padd);

				uint64_t upd_ne = 0;
				memcpy(&upd_ne,&ram->mem[ram->offset],sizeof(uint64_t));
				move_ram_file_ptr(ram,sizeof(uint64_t));

				if (go_back_here == 0) go_back_here = ram->offset;

				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0) ram->offset = array_upd;

			} while (array_upd > 0);

			ram->offset = go_back_here;
			break;
		}
		case TYPE_ARRAY_FLOAT:
		{
			if (!rec->fields[indexes[i]].data.v.elements.f){
				rec->fields[indexes[i]].data.v.insert = insert_element;
				rec->fields[indexes[i]].data.v.destroy = free_dynamic_array;
			}
			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				memcpy(&size_array,&ram->mem[ram->offset],sizeof(uint32_t));
				move_ram_file_ptr(ram,sizeof(uint32_t));
				int sz = (int)swap32(size_array);


				uint32_t padding = 0;
				memcpy(&padding,&ram->mem[ram->offset],sizeof(uint32_t));
				move_ram_file_ptr(ram,sizeof(uint32_t));
				int padd = (int)swap32(padding);

				int j;
				for (j = 0; j < sz; j++){
					uint32_t n = 0;
					memcpy(&n,&ram->mem[ram->offset],sizeof(uint32_t));
					move_ram_file_ptr(ram,sizeof(uint32_t));
					float num = (float)ntohf(n);
					rec->fields[indexes[i]].data.v.insert((void *)&num,
						&rec->fields[indexes[i]].data.v,
						rec->fields[indexes[i]].type);
				}

				if (padd > 0) ram->offset += (sizeof(uint32_t) * padd);

				uint64_t upd_ne = 0;
				memcpy(&upd_ne,&ram->mem[ram->offset],sizeof(uint64_t));
				move_ram_file_ptr(ram,sizeof(uint64_t));

				if (go_back_here == 0) go_back_here = ram->offset;

				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0) ram->offset = array_upd;

			} while (array_upd > 0);

			ram->offset = go_back_here;
			break;
		}

		case TYPE_ARRAY_DOUBLE:
		{
			if (!rec->fields[indexes[i]].data.v.elements.d){
				rec->fields[indexes[i]].data.v.insert = insert_element;
				rec->fields[indexes[i]].data.v.destroy = free_dynamic_array;
			}
			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				memcpy(&size_array,&ram->mem[ram->offset],sizeof(uint32_t));
				move_ram_file_ptr(ram,sizeof(uint32_t));
				int sz = (int)swap32(size_array);


				uint32_t padding = 0;
				memcpy(&padding,&ram->mem[ram->offset],sizeof(uint32_t));
				move_ram_file_ptr(ram,sizeof(uint32_t));
				int padd = (int)swap32(padding);

				int j;
				for (j = 0; j < sz; j++){
					uint64_t n = 0;
					memcpy(&n,&ram->mem[ram->offset],sizeof(uint64_t));
					move_ram_file_ptr(ram,sizeof(uint64_t));

					double num = (double)ntohd(n);

					rec->fields[indexes[i]].data.v.insert((void *)&num,
						&rec->fields[indexes[i]].data.v,
						rec->fields[indexes[i]].type);
				}

				if (padd > 0) ram->offset += (sizeof(uint64_t) * padd);

				uint64_t upd_ne = 0;
				memcpy(&upd_ne,&ram->mem[ram->offset],sizeof(uint64_t));
				move_ram_file_ptr(ram,sizeof(uint64_t));

				if (go_back_here == 0) go_back_here = ram->offset;

				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0) ram->offset = array_upd;

			} while(array_upd > 0);

			ram->offset = go_back_here;
			break;
		}
		case TYPE_ARRAY_STRING:
		{
			if (!rec->fields[indexes[i]].data.v.elements.s){
				rec->fields[indexes[i]].data.v.insert = insert_element;
				rec->fields[indexes[i]].data.v.destroy = free_dynamic_array;
			}
			off_t array_upd = 0;
			off_t go_back_here = 0;
			do
			{
				uint32_t size_array = 0;
				memcpy(&size_array,&ram->mem[ram->offset],sizeof(uint32_t));
				move_ram_file_ptr(ram,sizeof(uint32_t));
				int sz = (int)swap32(size_array);


				uint32_t padding = 0;
				memcpy(&padding,&ram->mem[ram->offset],sizeof(uint32_t));
				move_ram_file_ptr(ram,sizeof(uint32_t));
				int padd = (int)swap32(padding);

				int j;
				for (j = 0; j < sz; j++){
					uint32_t str_loc_ne = 0;
					memcpy(&str_loc_ne,&ram->mem[ram->offset],sizeof(uint32_t));
					move_ram_file_ptr(ram,sizeof(uint32_t));
					off_t str_loc = (off_t)	swap32(str_loc_ne);


					uint16_t buf_up_ne = 0;
					memcpy(&buf_up_ne,&ram->mem[ram->offset],sizeof(uint16_t));
					move_ram_file_ptr(ram,sizeof(uint16_t));
					size_t buf_up = (size_t)swap16(str_loc_ne);

					off_t move_back_to = 0;
					if(str_loc > 0){
						move_back_to = ram->offset + buf_up;	
						/* move to the new location*/
						ram->offset = str_loc;

						buf_up_ne = 0;
						memcpy(&buf_up_ne,&ram->mem[ram->offset],sizeof(uint16_t));
						move_ram_file_ptr(ram,sizeof(uint16_t));
						buf_up = (size_t)swap16(str_loc_ne);
					}

					char *string = (char*)ask_mem(buf_up*sizeof(char));
					if(!string){
						fprintf(stderr,"ask_mem() failed, %s:%d.\n",F,L-2);
						return -1;
					}

					memcpy(string,&ram->mem[ram->offset],buf_up);
					move_ram_file_ptr(ram, buf_up);
					if(str_loc > 0) ram->offset = move_back_to;

					rec->fields[indexes[i]].data.v.insert((void *)string,
						&rec->fields[indexes[i]].data.v,
						rec->fields[indexes[i]].type);

					cancel_memory(NULL,string,buf_up);
				}

				if (padd > 0) {
					int x;
					for( x = 0; x < padd; x++){
						move_ram_file_ptr(ram,sizeof(uint32_t));
						uint16_t buf_upn = 0;
						memcpy(&buf_upn,&ram->mem[ram->offset],sizeof(uint16_t));
						move_ram_file_ptr(ram,sizeof(uint16_t));
						size_t buf_up = swap16(buf_upn);
						move_ram_file_ptr(ram,buf_up);
					}
				}

				uint64_t upd_ne = 0;
				memcpy(&upd_ne,&ram->mem[ram->offset],sizeof(uint64_t));
				move_ram_file_ptr(ram,sizeof(uint64_t));

				if (go_back_here == 0) go_back_here = ram->offset;

				array_upd = (off_t)swap64(upd_ne);
				if (array_upd > 0) ram->offset = array_upd;

			} while (array_upd > 0);

			ram->offset = go_back_here;
			break;
		}
		case TYPE_FILE:
			/*coming soon*/
			break;
		}
	}

	return 0;
}

static void move_ram_file_ptr(struct Ram_file *ram,size_t size)
{
	if(ram->size == ram->offset){
		ram->size += size;
		ram->offset = ram->size;
	}else{
		ram->offset += size;
	}

}
/*
 *
 * if you pass init_ram_size as 0, in case the struct Ram_file memory is NULL
 * it will allocate the STD_RAM_FILE size to it (3 Mib)
 * */
int write_ram_record(struct Ram_file *ram, struct Record_f *rec, int update, size_t init_ram_size, off_t offset)
{
	if(ram->capacity == 0)
		if(init_ram_file(ram, init_ram_size) == -1) return -1;

	size_t rec_disk_size = get_disk_size_record(rec);

	/* if this is true it means we are at EOF
	 * this block check if we have enough size in the ram file*/
	if(ram->offset == ram->size){
		if(rec_disk_size > (ram->capacity - ram->size)){
			size_t new_size = ram->capacity * 2;
			uint8_t *n_buff = (uint8_t*)reask_mem(ram->mem,ram->capacity, new_size * sizeof(uint8_t));
			if(!n_buff){
				fprintf(stderr,"reask_mem() failed, %s:%d.\n",__FILE__,__LINE__-2);
				return -1;
			}

			ram->capacity = new_size;
			ram->mem = n_buff;
			ram->offset = ram->size;
		}
	}
	
	uint8_t cnt = 0;
	uint8_t i;
	for(i = 0; i < rec->fields_num; i++){
		if(rec->field_set[i] == 0) continue;
		cnt++;
	}

	if(cnt == 0){
		fprintf(stderr,"no active fields in the record %s:%d\n",__FILE__,__LINE__);
		return -1;
	}

	if(ram->size == ram->capacity && ram->offset != ram->size)
		memcpy(&ram->mem[ram->offset],&cnt,sizeof(uint8_t));
	else
		memcpy(&ram->mem[ram->size],&cnt,sizeof(uint8_t));

	move_ram_file_ptr(ram,sizeof(uint8_t));


	for(i = 0; i < rec->fields_num; i++){
		if(rec->field_set[i] == 0) continue;
		if(ram->size == ram->capacity && ram->offset != ram->size)
			memcpy(&ram->mem[ram->offset],&i,sizeof(uint8_t));
		else
			memcpy(&ram->mem[ram->size],&i,sizeof(uint8_t));

		move_ram_file_ptr(ram,sizeof(uint8_t));
	}

	for(i = 0; i < rec->fields_num; i++){
		if (rec->field_set[i] == 0)continue;

		switch(rec->fields[i].type){
		case TYPE_INT:
		{
			uint32_t value = swap32((uint32_t)rec->fields[i].data.i);
			if(ram->size == ram->capacity && ram->offset != ram->size)
				memcpy(&ram->mem[ram->offset],&value,sizeof(uint32_t));
			else
				memcpy(&ram->mem[ram->size],&value,sizeof(uint32_t));

			move_ram_file_ptr(ram,sizeof(uint32_t));
			break;
		}
		case TYPE_LONG:
		{
			uint64_t value = swap64((uint64_t)rec->fields[i].data.l);
			if(ram->size == ram->capacity && ram->offset != ram->size)
				memcpy(&ram->mem[ram->offset],&value,sizeof(uint64_t));
			else
				memcpy(&ram->mem[ram->size],&value,sizeof(uint64_t));

			move_ram_file_ptr(ram,sizeof(uint64_t));
			break;
		}
		case TYPE_BYTE:
			if(ram->size == ram->capacity && ram->offset != ram->size)
				memcpy(&ram->mem[ram->offset],&rec->fields[i].data.b,sizeof(uint8_t));
			else
				memcpy(&ram->mem[ram->size],&rec->fields[i].data.b,sizeof(uint8_t));

			move_ram_file_ptr(ram,sizeof(uint8_t));
			break;
		case TYPE_DATE:
		{
			uint32_t value = swap32(rec->fields[i].data.date);
			if(ram->size == ram->capacity && ram->offset != ram->size)
				memcpy(&ram->mem[ram->offset],&value,sizeof(uint32_t));
			else
				memcpy(&ram->mem[ram->size],&value,sizeof(uint32_t));
			
			move_ram_file_ptr(ram,sizeof(uint32_t));
			break;
		}
		case TYPE_FLOAT:
		{
			uint32_t value = htonf(rec->fields[i].data.f);
			if(ram->size == ram->capacity && ram->offset != ram->size)
				memcpy(&ram->mem[ram->offset],&value,sizeof(uint32_t));
			else
				memcpy(&ram->mem[ram->size],&value,sizeof(uint32_t));

			move_ram_file_ptr(ram,sizeof(uint32_t));
			break;
		}
		case TYPE_PACK:
		{
			uint32_t value = swap32((uint32_t)rec->fields[i].data.p);
			if(ram->size == ram->capacity && ram->offset != ram->size)
				memcpy(&ram->mem[ram->offset],&value,sizeof(uint32_t));
			else
				memcpy(&ram->mem[ram->size],&value,sizeof(uint32_t));

			move_ram_file_ptr(ram,sizeof(uint32_t));
			break;
		}
		case TYPE_DOUBLE:
		{
			uint64_t value = htond(rec->fields[i].data.d);
			if(ram->size == ram->capacity && ram->offset != ram->size)
				memcpy(&ram->mem[ram->offset],&value,sizeof(uint64_t));
			else
				memcpy(&ram->mem[ram->size],&value,sizeof(uint64_t));
			
			move_ram_file_ptr(ram,sizeof(uint64_t));
			break;
		}
		case TYPE_STRING:
		{
			if(!update){
				uint16_t l = (uint16_t)strlen(rec->fields[i].data.s);
				uint16_t buf_up_ne = swap16((l*2)+1);	
				uint32_t str_loc = 0;	

				if(ram->size == ram->capacity && ram->offset != ram->size)
					memcpy(&ram->mem[ram->offset],&str_loc,sizeof(uint32_t));
				else
					memcpy(&ram->mem[ram->size],&str_loc,sizeof(uint32_t));
				
				move_ram_file_ptr(ram,sizeof(uint32_t));

				if(ram->size == ram->capacity && ram->offset != ram->size)
					memcpy(&ram->mem[ram->offset],&buf_up_ne,sizeof(uint16_t));
				else
					memcpy(&ram->mem[ram->size],&buf_up_ne,sizeof(uint16_t));

				move_ram_file_ptr(ram,sizeof(uint16_t));
				
				char buff[(l * 2) + 1];
				memset(buff,0,(l * 2) +1);
				strncpy(buff,rec->fields[i].data.s,l);
				if(ram->size == ram->capacity && ram->offset != ram->size)
					memcpy(&ram->mem[ram->offset],buff,(l * 2) + 1);
				else
					memcpy(&ram->mem[ram->size],buff,(l * 2) + 1);

				move_ram_file_ptr(ram,(l * 2) +1);
				break;
			}else{
				uint64_t move_to = 0;
				uint32_t eof = 0;
				uint16_t bu_ne = 0;
				uint16_t new_lt = 0;
				/*save the starting offset for the string record*/
				uint64_t bg_pos = ram->offset;
				uint32_t str_loc_ne= 0;

				/*read the other str_loc if any*/
				if(ram->size == ram->capacity && ram->offset != ram->size)
					memcpy(&str_loc_ne,&ram->mem[ram->offset],sizeof(uint32_t));
				else
					memcpy(&str_loc_ne,&ram->mem[ram->size],sizeof(uint32_t));


				move_ram_file_ptr(ram,sizeof(uint32_t));
				uint32_t str_loc = swap32(str_loc_ne);

				/* save pos where the data starts*/
				uint64_t af_str_loc_pos = 0;
				if(ram->size == ram->capacity && ram->offset != ram->size)
					af_str_loc_pos = ram->offset;
				else
					af_str_loc_pos = ram->size;

				uint16_t buff_update_ne = 0;

				if(ram->size == ram->capacity && ram->offset != ram->size)
					memcpy(&buff_update_ne,&ram->mem[ram->offset],sizeof(uint16_t));
				else
					memcpy(&buff_update_ne,&ram->mem[ram->size],sizeof(uint16_t));

				move_ram_file_ptr(ram,sizeof(uint16_t));

				uint16_t buff_update = swap16(buff_update_ne);
				
				uint64_t pos_after_first_str_record = 0; 
				if(ram->size == ram->capacity && ram->offset != ram->size)
					pos_after_first_str_record = ram->offset + buff_update; 
				else
					pos_after_first_str_record = ram->size + buff_update; 

				if (str_loc > 0){
					/*set the file pointer to str_loc*/
					ram->offset = str_loc;

					/*
					 * in the case of a regular buffer update we have
					 *  to save the off_t to get back to it later
					 * */
					move_to = ram->offset; 

					uint16_t bu_ne = 0;
					memcpy(&bu_ne,&ram->mem[ram->offset],sizeof(uint16_t));
					ram->offset +=  sizeof(uint16_t);	

					buff_update = (off_t)swap16(bu_ne);
				}

				new_lt = strlen(rec->fields[i].data.s) + 1; /*get new str length*/

				if (new_lt > buff_update) {
					/*expand the buff_update only for the bytes needed*/
					buff_update += (new_lt - buff_update);

					/*
					 * if the new length is bigger then the buffer,
					 * set the file pointer to EOF to write the new data
					 * */
					eof = ram->size;
					ram->offset = eof;
					if(eof == ram->capacity){
						errno = 0;
						uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem,ram->capacity,
								(ram->capacity + buff_update) * sizeof(uint8_t));
						if(!n_mem){
							fprintf(stderr,"reask_mem failed with '%s', %s:%d.\n",strerror(errno),__FILE__,__LINE__-1);
							return -1;
						}
						ram->mem = n_mem;
						ram->capacity += buff_update; 

					}else if((eof + buff_update) > ram->capacity){
						errno = 0;
						uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem,ram->capacity,
								(ram->capacity + buff_update) * sizeof(uint8_t));
						if(!n_mem){
							fprintf(stderr,"reask_mem failed with '%s', %s:%d.\n",strerror(errno),__FILE__,__LINE__-1);
							return -1;
						}
						ram->mem = n_mem;
						ram->capacity += ((eof + buff_update) - ram->capacity); 
					}
				}

				char buff_w[buff_update];
				memset(buff_w,0,buff_update);

				strncpy(buff_w, rec->fields[i].data.s, new_lt - 1);
				/*
				 * if we did not move to another position
				 * set the file pointer back to the begginning of the string record
				 * to overwrite the data accordingly
				 * */
				if (str_loc == 0 && ((new_lt - buff_update) <= 0) && eof == 0){
					ram->offset = af_str_loc_pos;
				} else if (str_loc > 0 && ((new_lt - buff_update) <= 0) && eof == 0){
					ram->offset = move_to;
				}

				/*
				 * write the data to file --
				 * the file pointer is always pointing to the
				 * right position at this point */
				bu_ne = swap16((uint16_t)buff_update);

				if(eof == 0)
					memcpy(&ram->mem[ram->offset],&bu_ne,sizeof(uint16_t));
				else 
					memcpy(&ram->mem[ram->size],&bu_ne,sizeof(uint16_t));
					

				move_ram_file_ptr(ram,sizeof(uint16_t));
				memcpy(&ram->mem[ram->offset],buff_w,buff_update);
				move_ram_file_ptr(ram,buff_update);

				/*
				 * if eof is bigger than 0 means we updated the string
				 * we need to save the off_t of the new written data
				 * at the start of the original.
				 * */
				if (eof > 0){
					/*go at the beginning of the str record*/
					ram->offset = bg_pos;

					/*update new string position*/
					uint32_t eof_ne = swap32((uint32_t)eof);
					memcpy(&ram->mem[ram->offset],&eof_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					/*set file pointer to the end of the 1st string rec*/
					/*this step is crucial to avoid losing data        */

					ram->offset = pos_after_first_str_record;
				}else if (str_loc > 0){
					/*
					 * Make sure that in all cases
					 * we go back to the end of the 1st record
					 * */
					ram->offset = pos_after_first_str_record;
				}
			}
			break;	
		}
		case TYPE_ARRAY_INT:
		{
			if(!update){
				uint32_t sz = swap32(rec->fields[i].data.v.size);
				memcpy(&ram->mem[ram->size],&sz, sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				uint32_t place_holder = 0;
				memcpy(&ram->mem[ram->size],&place_holder, sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				uint32_t arr[rec->fields[i].data.v.size];
				memset(arr,0,sizeof(uint32_t) * rec->fields[i].data.v.size);

				int j;
				for(j = 0; j < rec->fields[i].data.v.size; j++){
					arr[j] = swap32(rec->fields[i].data.v.elements.i[j]);
				} 

				memcpy(&ram->mem[ram->size],arr,sizeof(uint32_t) * rec->fields[i].data.v.size);
				ram->size += (sizeof(uint32_t) * rec->fields[i].data.v.size);
				ram->offset += (sizeof(uint32_t) * rec->fields[i].data.v.size);

				uint64_t upd = 0;	
				memcpy(&ram->mem[ram->size],&upd,sizeof(uint64_t));
				ram->size += (sizeof(uint64_t));
				ram->offset += sizeof(uint64_t);
			}else{
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int sz = 0;
				int k = 0;
				int step = 0;
				int padding = 0;

				do{
					/*check size of the array on file*/
					uint32_t sz_ne = 0; 
					memcpy(&sz_ne,&ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					sz = (int)swap32(sz_ne);

					if(rec->fields[i].data.v.size < sz || rec->fields[i].data.v.size == sz) break;

					/* 
					 * the program reach this branch only if the size of 
					 * the new array in the record is bigger than what we already have on file
					 * so we need to extend the array on file.
					 * */

					/*read padding value*/
					uint32_t pd_ne = 0;
					memcpy(&pd_ne, &ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					padding = (int)swap32(pd_ne);

					/*
					 * this is never true at the first iteration
					 * */
					if(step >= sz){

						int exit = 0;
						int array_last = 0;
						if((array_last = is_array_last_block(-1,ram,sz,sizeof(int),TYPE_INT)) == -1){
							fprintf(stderr,"(%s): can't verify array last block %s:%d",prog,__FILE__,__LINE__-1);
							return -1;
						}
						
						if(rec->fields[i].data.v.size < (sz + step) && array_last){
							padding += (sz - (rec->fields[i].data.v.size - step));

							sz = rec->fields[i].data.v.size - step;
							exit = 1;
							ram->offset -= (2*sizeof(uint32_t));
							/*write the update size of the array*/
							uint32_t new_sz_ne = swap32((uint32_t)sz);
							memcpy(&ram->mem[ram->offset],&new_sz_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);

							/*write the padding*/
							uint32_t pd_ne = swap32((uint32_t)padding);
							memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);

						}else if(rec->fields[i].data.v.size == (sz + step) && array_last){
							exit = 1;
						}

						while(sz){
							if(step < rec->fields[i].data.v.size){
								uint32_t num_ne = swap32(rec->fields[i].data.v.elements.i[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint32_t));
								ram->offset += sizeof(uint32_t);
								step++;
							}
							sz--;
						}

						if(exit){
							uint64_t up_pos_ne = 0;
							memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;	
						}
					} else {

						/* 
						 * in the first iteration in the 
						 * do-while loop this will
						 * overwrite the array on file
						 * */

						int exit = 0;
						for(k = 0;k < sz; k++){
							if(step > 0 && k == 0 ){
								if((step + sz) > rec->fields[i].data.v.size){
									int array_last = 0;
									if((array_last = is_array_last_block(-1,ram,sz,sizeof(int),TYPE_INT)) == -1){
										fprintf(stderr,"(%s): can't verify array last block %s:%d",prog,__FILE__,__LINE__-1);
										return -1;
									}

									if(rec->fields[i].data.v.size < (sz + step) && array_last){
										padding += (sz - (rec->fields[i].data.v.size - step));

										sz = rec->fields[i].data.v.size - step;
										exit = 1;
										ram->offset -= (2*sizeof(uint32_t));
										/*write the update size of the array*/
										uint32_t new_sz_ne = swap32((uint32_t)sz);
										memcpy(&ram->mem[ram->offset],&new_sz_ne,sizeof(uint32_t));
										ram->offset += sizeof(uint32_t);

										/*write the padding*/
										uint32_t pd_ne = swap32((uint32_t)padding);
										memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
										ram->offset += sizeof(uint32_t);
									}
								}
							}

							if(step < rec->fields[i].data.v.size){
								if(!rec->fields[i].data.v.elements.i[step]) continue;

								uint32_t num_ne = swap32(rec->fields[i].data.v.elements.i[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint32_t));
								ram->offset += sizeof(uint32_t);

								step++;
								if(!(step < rec->fields[i].data.v.size)) exit = 0;
							}
						}

						if(exit){
							if(padding > 0) ram->offset += (padding * sizeof(int));

							uint64_t up_pos_ne = 0;
							memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;/*from the main do-while loop*/
						}
					}

					if(padding > 0) ram->offset += (sizeof(int) * padding);

					uint64_t update_off_ne = 0;
					off_t go_back_to = ram->offset;

					memcpy(&update_off_ne,&ram->mem[ram->offset],sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);

					if(go_back_to_first_rec == 0) go_back_to_first_rec = ram->offset;

					update_pos = (off_t) swap64(update_off_ne);
					/*
					 * if the update_pos is == 0 it means 
					 * we need to move at the end of the file and write the remaining 
					 * element of the array
					 *
					 * */
					if(update_pos == 0){
						/*go to EOF*/	
						update_pos = ram->size;
						ram->offset = ram->size;

						uint32_t size_left = rec->fields[i].data.v.size - step;
						/*
						 * compute the size that we need to write
						 * remeber that each array record is:
						 * 	- uint32_t sz;
						 * 	- uint32_t padding;
						 * 	- sizeof(each element) * sz;
						 * 	- uint64_t update_pos;
						 *
						 * 	we account for 2 uint64_t due to the whole record structure
						 * */
						uint64_t remaining_write_size = ( (2 * sizeof(uint32_t)) + (size_left * sizeof(int)) + sizeof(uint64_t));
						if(ram->size == ram->capacity || ((ram->size + remaining_write_size) > ram->capacity)){
							/*you have to expand the capacity*/
							uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem,
									ram->capacity * sizeof(uint8_t), 
								ram->capacity + (remaining_write_size + 1) * sizeof(uint8_t));
							if(!n_mem){
								fprintf(stderr,"(%s): reask_mem failed %s:%d.\n",prog,__FILE__,__LINE__-2);
								return -1;
							}
							ram->mem = n_mem;
							ram->capacity += (remaining_write_size + 1);
							memset(&ram->mem[ram->offset],0,remaining_write_size +1);
						}


						uint32_t sz_left_ne = swap32(size_left);
						memcpy(&ram->mem[ram->offset],&sz_left_ne,sizeof(uint32_t));
						ram->size += sizeof(uint32_t);
						ram->offset = ram->size;

						uint32_t pd_ne = 0;
						memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
						ram->size += sizeof(uint32_t);
						ram->offset = ram->size;

						uint32_t j;
						for(j = 0; j < size_left; j++){
							if(step < rec->fields[i].data.v.size){
								if(!rec->fields[i].data.v.elements.i[step]) continue;

								uint32_t num_ne = swap32(rec->fields[i].data.v.elements.i[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint32_t));
								ram->size += sizeof(uint32_t);
								ram->offset = ram->size;

								step++;
							}
						}
						uint64_t up_pos_ne = 0;
						memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
						ram->size += sizeof(uint64_t);
						ram->offset = ram->size;

						/*we need to write this rec position in the old rec*/
						ram->offset = go_back_to;
						update_off_ne = swap64((uint64_t)update_pos);
						memcpy(&ram->mem[ram->offset],&update_off_ne,sizeof(uint64_t));
						ram->offset += sizeof(uint64_t);

						
						break;
					}

					ram->offset = update_pos;

				}while(update_pos > 0);

				if(rec->fields[i].data.v.size < sz){
					ram->offset -= sizeof(uint32_t);

					padding = sz - rec->fields[i].data.v.size;
					uint32_t p_ne = swap32((uint32_t)padding);
					uint32_t sz_ne = swap32(rec->fields[i].data.v.size);
					memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					/*write the padding*/
					memcpy(&ram->mem[ram->offset],&p_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					int j;
					for(j = step; j < rec->fields[i].data.v.size; j++){
						if(step < rec->fields[i].data.v.size){
							if(!rec->fields[i].data.v.elements.i[j]) continue;

							uint32_t num_ne = swap32(rec->fields[i].data.v.elements.i[j]);
							memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);
							step++;
						}
					}
					ram->offset += (padding * sizeof(int));
					uint64_t up_pos_ne = 0;

					memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);


				}else if(rec->fields[i].data.v.size == sz){

					if(step > 0){

						ram->offset -= sizeof(uint32_t);

						int size_left = rec->fields[i].data.v.size - step;

						uint32_t sz_ne = swap32((uint32_t)size_left);
						memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(uint32_t));
						ram->offset += sizeof(uint32_t);
					}

						/* read padding value*/
						uint32_t pd_ne = 0;
						memcpy(&pd_ne, &ram->mem[ram->offset],sizeof(uint32_t));
						ram->offset += sizeof(uint32_t);
						padding = (int)swap32(pd_ne);
						int j;		
						for(j = 0; j < rec->fields[i].data.v.size; j++){
							if(step < rec->fields[i].data.v.size){

								uint32_t num_ne = swap32(rec->fields[i].data.v.elements.i[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint32_t));
								ram->offset += sizeof(uint32_t);
								step++;
							}
						}

						if(padding > 0)	ram->offset += (padding * sizeof(uint32_t));
						/*ram->offset += sizeof(uint32_t);*/

						uint64_t up_pos_ne = 0;
						memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
						ram->offset += sizeof(uint64_t);
				}

				if(go_back_to_first_rec > 0) ram->offset = go_back_to_first_rec;
			}
			break;
		}
		case TYPE_ARRAY_LONG:
		{
			if(!update){
				uint32_t sz = swap32(rec->fields[i].data.v.size);
				memcpy(&ram->mem[ram->size],&sz, sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				uint32_t pad = 0;
				memcpy(&ram->mem[ram->size],&pad, sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				uint64_t arr[rec->fields[i].data.v.size];
				memset(arr,0,sizeof(uint64_t) * rec->fields[i].data.v.size);
				int j;
				for(j = 0; j < rec->fields[i].data.v.size; j++){
					arr[j] = swap64(rec->fields[i].data.v.elements.l[j]);
				} 

				memcpy(&ram->mem[ram->size],arr,sizeof(uint64_t) * rec->fields[i].data.v.size);
				ram->size += (sizeof(uint64_t) * rec->fields[i].data.v.size);
				ram->offset += (sizeof(uint64_t) * rec->fields[i].data.v.size);

				uint64_t upd = 0;	
				memcpy(&ram->mem[ram->size],&upd,sizeof(uint64_t));
				ram->size += (sizeof(uint64_t));
				ram->offset += sizeof(uint64_t);
				break;
			}else{
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int sz = 0;
				int k = 0;
				int step = 0;
				int padding = 0;

				do{
					/*check size of the array on file*/
					uint32_t sz_ne = 0; 
					memcpy(&sz_ne,&ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					sz = (int)swap32(sz_ne);

					if(rec->fields[i].data.v.size < sz || rec->fields[i].data.v.size == sz) break;

					/* 
					 * the program reach this branch only if the size of 
					 * the new array in the record is bigger than what we already have on file
					 * so we need to extend the array on file.
					 * */

					/*read padding value*/
					uint32_t pd_ne = 0;
					memcpy(&pd_ne, &ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					padding = (int)swap32(pd_ne);

					/*
					 * this is never true at the first iteration
					 * */
					if(step >= sz){

						int exit = 0;
						int array_last = 0;
						if((array_last = is_array_last_block(-1,ram,sz,sizeof(long),TYPE_INT)) == -1){
							fprintf(stderr,"(%s): can't verify array last block %s:%d",prog,__FILE__,__LINE__-1);
							return -1;
						}
						
						if(rec->fields[i].data.v.size < (sz + step) && array_last){
							padding += (sz - (rec->fields[i].data.v.size - step));

							sz = rec->fields[i].data.v.size - step;
							exit = 1;
							ram->offset -= (2*sizeof(uint32_t));
							/*write the update size of the array*/
							uint32_t new_sz_ne = swap32((uint32_t)sz);
							memcpy(&ram->mem[ram->offset],&new_sz_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);

							/*write the padding*/
							uint32_t pd_ne = swap32((uint32_t)padding);
							memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);

						}else if(rec->fields[i].data.v.size == (sz + step) && array_last){
							exit = 1;
						}

						while(sz){
							if(step < rec->fields[i].data.v.size){
								uint64_t num_ne = swap64(rec->fields[i].data.v.elements.l[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint64_t));
								ram->offset += sizeof(uint64_t);
								step++;
							}
							sz--;
						}

						if(exit){
							uint64_t up_pos_ne = 0;
							memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;	
						}
					} else {

						/* 
						 * in the first iteration in the 
						 * do-while loop this will
						 * overwrite the array on file
						 * */

						int exit = 0;
						for(k=0;k < sz; k++){
							if(step > 0 && k == 0 ){
								if((step + sz) > rec->fields[i].data.v.size){
									int array_last = 0;
									if((array_last = is_array_last_block(-1,ram,sz,sizeof(long),TYPE_INT)) == -1){
										fprintf(stderr,"(%s): can't verify array last block %s:%d",prog,__FILE__,__LINE__-1);
										return -1;
									}

									if(rec->fields[i].data.v.size < (sz + step) && array_last){
										padding += (sz - (rec->fields[i].data.v.size - step));

										sz = rec->fields[i].data.v.size - step;
										exit = 1;
										ram->offset -= (2*sizeof(uint32_t));
										/*write the update size of the array*/
										uint32_t new_sz_ne = swap32((uint32_t)sz);
										memcpy(&ram->mem[ram->offset],&new_sz_ne,sizeof(uint32_t));
										ram->offset += sizeof(uint32_t);

										/*write the padding*/
										uint32_t pd_ne = swap32((uint32_t)padding);
										memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
										ram->offset += sizeof(uint32_t);
									}
								}
							}

							if(step < rec->fields[i].data.v.size){
								if(!rec->fields[i].data.v.elements.l[step]) continue;

								uint64_t num_ne = swap64(rec->fields[i].data.v.elements.l[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint64_t));
								ram->offset += sizeof(uint64_t);
								step++;

								if(!(step < rec->fields[i].data.v.size)) exit = 0;
							}
						}

						if(exit){
							if(padding > 0) ram->offset += (padding * sizeof(long));

							uint64_t up_pos_ne = 0;
							memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;/*from the main do-while loop*/
						}
					}

					if(padding > 0) ram->offset += (sizeof(long) * padding);

					uint64_t update_off_ne = 0;
					off_t go_back_to = ram->offset;

					memcpy(&update_off_ne,&ram->mem[ram->offset],sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);

					if(go_back_to_first_rec == 0) go_back_to_first_rec = ram->offset;

					update_pos = (off_t) swap64(update_off_ne);
					/*
					 * if the update_pos is == 0 it means 
					 * we need to move at the end of the file and write the remaining 
					 * element of the array
					 *
					 * */
					if(update_pos == 0){
						/*go to EOF*/	
						update_pos = ram->size;
						ram->offset = ram->size;

						uint32_t size_left = rec->fields[i].data.v.size - step;
						/*
						 * compute the size that we need to write
						 * remeber that each array record is:
						 * 	- uint32_t sz;
						 * 	- uint32_t padding;
						 * 	- sizeof(each element) * sz;
						 * 	- uint64_t update_pos;
						 *
						 * 	we account for 2 uint64_t due to the whole record structure
						 * */
						uint64_t remaining_write_size = ( (2 * sizeof(uint32_t)) + (size_left * sizeof(long)) + sizeof(uint64_t));
						if(ram->size == ram->capacity || ((ram->size + remaining_write_size) > ram->capacity)){
							/*you have to expand the capacity*/
							uint8_t *n_mem = (uint8_t*) reask_mem(ram->mem, 
										ram->capacity * sizeof(uint8_t),
									ram->capacity + (remaining_write_size + 1) * sizeof(uint8_t));
							if(!n_mem){
								fprintf(stderr,"(%s): reask_mem failed %s:%d.\n",prog,__FILE__,__LINE__-2);
								return -1;
							}
							ram->mem = n_mem;
							ram->capacity += (remaining_write_size + 1);
							memset(&ram->mem[ram->offset],0,remaining_write_size +1);
						}


						uint32_t sz_left_ne = swap32(size_left);
						memcpy(&ram->mem[ram->offset],&sz_left_ne,sizeof(uint32_t));
						ram->size += sizeof(uint32_t);
						ram->offset = ram->size;

						uint32_t pd_ne = 0;
						memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
						ram->size += sizeof(uint32_t);
						ram->offset = ram->size;

						uint32_t j;
						for(j = 0; j < size_left; j++){
							if(step < rec->fields[i].data.v.size){
								uint64_t num_ne = swap64(rec->fields[i].data.v.elements.l[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint64_t));
								ram->size += sizeof(uint64_t);
								ram->offset = ram->size;

								step++;
							}
						}
						uint64_t up_pos_ne = 0;
						memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
						ram->size += sizeof(uint64_t);
						ram->offset = ram->size;

						/*we need to write this rec position in the old rec*/
						ram->offset = go_back_to;
						update_off_ne = swap64((uint64_t)update_pos);
						memcpy(&ram->mem[ram->offset],&update_off_ne,sizeof(uint64_t));
						ram->offset += sizeof(uint64_t);
						
						break;
					}

					ram->offset = update_pos;

				}while(update_pos > 0);

				if(rec->fields[i].data.v.size < sz){
					ram->offset -= sizeof(uint32_t);

					padding = sz - rec->fields[i].data.v.size;
					uint32_t p_ne = swap32((uint32_t)padding);
					uint32_t sz_ne = swap32(rec->fields[i].data.v.size);
					memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					/*write the padding*/
					memcpy(&ram->mem[ram->offset],&p_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					int j;
					for(j = step; j <rec->fields[i].data.v.size; j++){
						if(step < rec->fields[i].data.v.size){

							uint64_t num_ne = swap64(rec->fields[i].data.v.elements.l[j]);
							memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							step++;
						}
					}
					ram->offset += (padding * sizeof(long));

					uint64_t up_pos_ne = 0;
					memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);
				}else if(rec->fields[i].data.v.size == sz){

					if(step > 0){

						ram->offset -= sizeof(uint32_t);
						int size_left = rec->fields[i].data.v.size - step;

						uint32_t sz_ne = swap32((uint32_t)size_left);
						memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(uint32_t));
						ram->offset += sizeof(uint32_t);
					}
						/* read padding value*/
						uint32_t pd_ne = 0;
						memcpy(&pd_ne, &ram->mem[ram->offset],sizeof(uint32_t));
						ram->offset += sizeof(uint32_t);
						padding = (int)swap32(pd_ne);
						
						int j;
						for(j = 0; j < rec->fields[i].data.v.size; j++){
							if(step < rec->fields[i].data.v.size){
								uint64_t num_ne = swap64(rec->fields[i].data.v.elements.l[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint64_t));
								ram->offset += sizeof(uint64_t);
								step++;
							}
						}

						if(padding > 0)	ram->offset += (padding * sizeof(long));

						uint64_t up_pos_ne = 0;
						memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
						ram->offset += sizeof(uint64_t);
				}

				if(go_back_to_first_rec > 0) ram->offset = go_back_to_first_rec;
			}
			break;

		}
		case TYPE_ARRAY_BYTE:
		{
			if(!update){
				uint32_t sz = swap32(rec->fields[i].data.v.size);
				memcpy(&ram->mem[ram->size],&sz, sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				uint32_t place_holder = 0;
				memcpy(&ram->mem[ram->size],&place_holder, sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				uint8_t arr[rec->fields[i].data.v.size];
				memset(arr,0,sizeof(uint8_t) * rec->fields[i].data.v.size);

				int j;
				for(j = 0; j < rec->fields[i].data.v.size; j++){
					arr[j] = rec->fields[i].data.v.elements.b[j];
				} 

				memcpy(&ram->mem[ram->size],arr,sizeof(uint8_t) * rec->fields[i].data.v.size);
				ram->size += (sizeof(uint8_t) * rec->fields[i].data.v.size);
				ram->offset += (sizeof(uint8_t) * rec->fields[i].data.v.size);

				uint64_t upd = 0;	
				memcpy(&ram->mem[ram->size],&upd,sizeof(uint64_t));
				ram->size += (sizeof(uint64_t));
				ram->offset += sizeof(uint64_t);
			}else{
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int sz = 0;
				int k = 0;
				int step = 0;
				int padding = 0;

				do{
					/*check size of the array on file*/
					uint32_t sz_ne = 0; 
					memcpy(&sz_ne,&ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					sz = (int)swap32(sz_ne);

					if(rec->fields[i].data.v.size < sz || rec->fields[i].data.v.size == sz) break;

					/* 
					 * the program reach this branch only if the size of 
					 * the new array in the record is bigger than what we already have on file
					 * so we need to extend the array on file.
					 * */

					/*read padding value*/
					uint32_t pd_ne = 0;
					memcpy(&pd_ne, &ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					padding = (int)swap32(pd_ne);

					/*
					 * this is never true at the first iteration
					 * */
					if(step >= sz){

						int exit = 0;
						int array_last = 0;
						if((array_last = is_array_last_block(-1,ram,sz,sizeof(unsigned char),0)) == -1){
							fprintf(stderr,"(%s): can't verify array last block %s:%d",prog,__FILE__,__LINE__-1);
							return -1;
						}
						
						if(rec->fields[i].data.v.size < (sz + step) && array_last){
							padding += (sz - (rec->fields[i].data.v.size - step));

							sz = rec->fields[i].data.v.size - step;
							exit = 1;
							ram->offset -= (2*sizeof(uint32_t));
							/*write the update size of the array*/
							uint32_t new_sz_ne = swap32((uint32_t)sz);
							memcpy(&ram->mem[ram->offset],&new_sz_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);

							/*write the padding*/
							uint32_t pd_ne = swap32((uint32_t)padding);
							memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);

						}else if(rec->fields[i].data.v.size == (sz + step) && array_last){
							exit = 1;
						}

						while(sz){
							if(step < rec->fields[i].data.v.size){
								uint8_t num_ne = rec->fields[i].data.v.elements.b[step];
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint8_t));
								ram->offset += sizeof(uint8_t);
								step++;
							}
							sz--;
						}

						if(exit){
							uint64_t up_pos_ne = 0;
							memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;	
						}
					} else {

						/* 
						 * in the first iteration in the 
						 * do-while loop this will
						 * overwrite the array on file
						 * */

						int exit = 0;
						for(k = 0;k < sz; k++){
							if(step > 0 && k == 0 ){
								if((step + sz) > rec->fields[i].data.v.size){
									int array_last = 0;
									if((array_last = is_array_last_block(-1,ram,sz,
													sizeof(unsigned char),
													0)) == -1){
										fprintf(stderr,"(%s): can't verify array last block %s:%d",prog,__FILE__,__LINE__-1);
										return -1;
									}

									if(rec->fields[i].data.v.size < (sz + step) && array_last){
										padding += (sz - (rec->fields[i].data.v.size - step));

										sz = rec->fields[i].data.v.size - step;
										exit = 1;
										ram->offset -= (2*sizeof(uint32_t));
										/*write the update size of the array*/
										uint32_t new_sz_ne = swap32((uint32_t)sz);
										memcpy(&ram->mem[ram->offset],&new_sz_ne,sizeof(uint32_t));
										ram->offset += sizeof(uint32_t);

										/*write the padding*/
										uint32_t pd_ne = swap32((uint32_t)padding);
										memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
										ram->offset += sizeof(uint32_t);
									}
								}
							}

							if(step < rec->fields[i].data.v.size){
								uint8_t num_ne = rec->fields[i].data.v.elements.b[step];
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint8_t));
								ram->offset += sizeof(uint8_t);

								step++;
								if(!(step < rec->fields[i].data.v.size)) exit = 0;
							}
						}

						if(exit){
							if(padding > 0) ram->offset += (padding * sizeof(uint8_t));

							uint64_t up_pos_ne = 0;
							memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;/*from the main do-while loop*/
						}
					}

					if(padding > 0) ram->offset += (sizeof(uint8_t) * padding);

					uint64_t update_off_ne = 0;
					off_t go_back_to = ram->offset;

					memcpy(&update_off_ne,&ram->mem[ram->offset],sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);

					if(go_back_to_first_rec == 0) go_back_to_first_rec = ram->offset;

					update_pos = (off_t) swap64(update_off_ne);
					/*
					 * if the update_pos is == 0 it means 
					 * we need to move at the end of the file and write the remaining 
					 * element of the array
					 *
					 * */
					if(update_pos == 0){
						/*go to EOF*/	
						update_pos = ram->size;
						ram->offset = ram->size;

						uint32_t size_left = rec->fields[i].data.v.size - step;
						/*
						 * compute the size that we need to write
						 * remeber that each array record is:
						 * 	- uint32_t sz;
						 * 	- uint32_t padding;
						 * 	- sizeof(each element) * sz;
						 * 	- uint64_t update_pos;
						 *
						 * 	we account for 2 uint64_t due to the whole record structure
						 * */
						uint64_t remaining_write_size = ( (2 * sizeof(uint32_t)) + (size_left * sizeof(uint8_t)) + sizeof(uint64_t));
						if(ram->size == ram->capacity || ((ram->size + remaining_write_size) > ram->capacity)){
							/*you have to expand the capacity*/
							uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem,
									ram->capacity * sizeof(uint8_t), 
								ram->capacity + (remaining_write_size + 1) * sizeof(uint8_t));
							if(!n_mem){
								fprintf(stderr,"(%s): reask_mem failed %s:%d.\n",prog,__FILE__,__LINE__-2);
								return -1;
							}
							ram->mem = n_mem;
							ram->capacity += (remaining_write_size + 1);
							memset(&ram->mem[ram->offset],0,remaining_write_size +1);
						}


						uint32_t sz_left_ne = swap32(size_left);
						memcpy(&ram->mem[ram->offset],&sz_left_ne,sizeof(uint32_t));
						ram->size += sizeof(uint32_t);
						ram->offset = ram->size;

						uint32_t pd_ne = 0;
						memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
						ram->size += sizeof(uint32_t);
						ram->offset = ram->size;

						uint32_t j;
						for(j = 0; j < size_left; j++){
							if(step < rec->fields[i].data.v.size){

								uint8_t num_ne = rec->fields[i].data.v.elements.b[step];
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint8_t));
								ram->size += sizeof(uint8_t);
								ram->offset = ram->size;

								step++;
							}
						}
						uint64_t up_pos_ne = 0;
						memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
						ram->size += sizeof(uint64_t);
						ram->offset = ram->size;

						/*we need to write this rec position in the old rec*/
						ram->offset = go_back_to;
						update_off_ne = swap64((uint64_t)update_pos);
						memcpy(&ram->mem[ram->offset],&update_off_ne,sizeof(uint64_t));
						ram->offset += sizeof(uint64_t);
						break;
					}

					ram->offset = update_pos;

				}while(update_pos > 0);

				if(rec->fields[i].data.v.size < sz){
					ram->offset -= sizeof(uint32_t);

					padding = sz - rec->fields[i].data.v.size;
					uint32_t p_ne = swap32((uint32_t)padding);
					uint32_t sz_ne = swap32(rec->fields[i].data.v.size);
					memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					/*write the padding*/
					memcpy(&ram->mem[ram->offset],&p_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					int j;
					for(j = step; j < rec->fields[i].data.v.size; j++){
						if(step < rec->fields[i].data.v.size){

							uint8_t num_ne = rec->fields[i].data.v.elements.b[j];
							memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint8_t));
							ram->offset += sizeof(uint8_t);
							step++;
						}
					}
					ram->offset += (padding * sizeof(uint8_t));
					uint64_t up_pos_ne = 0;

					memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);


				}else if(rec->fields[i].data.v.size == sz){

					if(step > 0){

						ram->offset -= sizeof(uint32_t);

						int size_left = rec->fields[i].data.v.size - step;

						uint32_t sz_ne = swap32((uint32_t)size_left);
						memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(uint32_t));
						ram->offset += sizeof(uint32_t);
					}

						/* read padding value*/
						uint32_t pd_ne = 0;
						memcpy(&pd_ne, &ram->mem[ram->offset],sizeof(uint32_t));
						ram->offset += sizeof(uint32_t);
						padding = (int)swap32(pd_ne);
						int j;		
						for(j = 0; j < rec->fields[i].data.v.size; j++){
							if(step < rec->fields[i].data.v.size){

								uint8_t num_ne = rec->fields[i].data.v.elements.b[step];
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint8_t));
								ram->offset += sizeof(uint8_t);
								step++;
							}
						}

						if(padding > 0)	ram->offset += (padding * sizeof(uint8_t));
						/*ram->offset += sizeof(uint32_t);*/

						uint64_t up_pos_ne = 0;
						memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
						ram->offset += sizeof(uint64_t);
				}

				if(go_back_to_first_rec > 0) ram->offset = go_back_to_first_rec;
			}
			break;
		}
		case TYPE_ARRAY_FLOAT:
		{
			if(!update){
				uint32_t sz = swap32(rec->fields[i].data.v.size);
				memcpy(&ram->mem[ram->size],&sz, sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				uint32_t pad = 0;
				memcpy(&ram->mem[ram->size],&pad, sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				uint32_t arr[rec->fields[i].data.v.size];
				memset(arr,0,sizeof(uint32_t) * rec->fields[i].data.v.size);

				int j;
				for(j = 0; j < rec->fields[i].data.v.size; j++){
					arr[j] = htonf(rec->fields[i].data.v.elements.f[j]);
				} 

				memcpy(&ram->mem[ram->size],arr,sizeof(uint32_t) * rec->fields[i].data.v.size);
				ram->size += (sizeof(uint32_t) * rec->fields[i].data.v.size);
				ram->offset += (sizeof(uint32_t) * rec->fields[i].data.v.size);

				uint64_t upd = 0;	
				memcpy(&ram->mem[ram->size],&upd,sizeof(uint64_t));
				ram->size += (sizeof(uint64_t));
				ram->offset += sizeof(uint64_t);
				break;
		}else{
			off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int sz = 0;
				int k = 0;
				int step = 0;
				int padding = 0;

				do{
					/*check size of the array on file*/
					uint32_t sz_ne = 0; 
					memcpy(&sz_ne,&ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					sz = (int)swap32(sz_ne);

					if(rec->fields[i].data.v.size < sz || rec->fields[i].data.v.size == sz) break;

					/* 
					 * the program reach this branch only if the size of 
					 * the new array in the record is bigger than what we already have on file
					 * so we need to extend the array on file.
					 * */

					/*read padding value*/
					uint32_t pd_ne = 0;
					memcpy(&pd_ne, &ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					padding = (int)swap32(pd_ne);

					/*
					 * this is never true at the first iteration
					 * */
					if(step >= sz){

						int exit = 0;
						int array_last = 0;
						if((array_last = is_array_last_block(-1,ram,sz,sizeof(float),TYPE_FLOAT)) == -1){
							fprintf(stderr,"(%s): can't verify array last block %s:%d",prog,__FILE__,__LINE__-1);
							return -1;
						}
						
						if(rec->fields[i].data.v.size < (sz + step) && array_last){
							padding += (sz - (rec->fields[i].data.v.size - step));

							sz = rec->fields[i].data.v.size - step;
							exit = 1;
							ram->offset -= (2*sizeof(uint32_t));
							/*write the update size of the array*/
							uint32_t new_sz_ne = swap32((uint32_t)sz);
							memcpy(&ram->mem[ram->offset],&new_sz_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);

							/*write the padding*/
							uint32_t pd_ne = swap32((uint32_t)padding);
							memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);

						}else if(rec->fields[i].data.v.size == (sz + step) && array_last){
							exit = 1;
						}

						while(sz){
							if(step < rec->fields[i].data.v.size){
								uint32_t num_ne = htonf(rec->fields[i].data.v.elements.f[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint32_t));
								ram->offset += sizeof(uint32_t);
								step++;
							}
							sz--;
						}

						if(exit){
							uint64_t up_pos_ne = 0;
							memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;	
						}
					} else {

						/* 
						 * in the first iteration in the 
						 * do-while loop this will
						 * overwrite the array on file
						 * */

						int exit = 0;
						for(k = 0;k < sz; k++){
							if(step > 0 && k == 0 ){
								if((step + sz) > rec->fields[i].data.v.size){
									int array_last = 0;
									if((array_last = is_array_last_block(-1,ram,sz,sizeof(float),TYPE_FLOAT)) == -1){
										fprintf(stderr,"(%s): can't verify array last block %s:%d",prog,__FILE__,__LINE__-1);
										return -1;
									}

									if(rec->fields[i].data.v.size < (sz + step) && array_last){
										padding += (sz - (rec->fields[i].data.v.size - step));

										sz = rec->fields[i].data.v.size - step;
										exit = 1;
										ram->offset -= (2*sizeof(uint32_t));
										/*write the update size of the array*/
										uint32_t new_sz_ne = swap32((uint32_t)sz);
										memcpy(&ram->mem[ram->offset],&new_sz_ne,sizeof(uint32_t));
										ram->offset += sizeof(uint32_t);

										/*write the padding*/
										uint32_t pd_ne = swap32((uint32_t)padding);
										memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
										ram->offset += sizeof(uint32_t);
									}
								}
							}

							if(step < rec->fields[i].data.v.size){
								uint32_t num_ne = htonf(rec->fields[i].data.v.elements.f[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint32_t));
								ram->offset += sizeof(uint32_t);

								step++;
								if(!(step < rec->fields[i].data.v.size)) exit = 0;
							}
						}

						if(exit){
							if(padding > 0) ram->offset += (padding * sizeof(float));

							uint64_t up_pos_ne = 0;
							memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;/*from the main do-while loop*/
						}
					}

					if(padding > 0) ram->offset += (sizeof(float) * padding);

					uint64_t update_off_ne = 0;
					off_t go_back_to = ram->offset;

					memcpy(&update_off_ne,&ram->mem[ram->offset],sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);

					if(go_back_to_first_rec == 0) go_back_to_first_rec = ram->offset;

					update_pos = (off_t) swap64(update_off_ne);
					/*
					 * if the update_pos is == 0 it means 
					 * we need to move at the end of the file and write the remaining 
					 * element of the array
					 *
					 * */
					if(update_pos == 0){
						/*go to EOF*/	
						update_pos = ram->size;
						ram->offset = ram->size;

						uint32_t size_left = rec->fields[i].data.v.size - step;
						/*
						 * compute the size that we need to write
						 * remeber that each array record is:
						 * 	- uint32_t sz;
						 * 	- uint32_t padding;
						 * 	- sizeof(each element) * sz;
						 * 	- uint64_t update_pos;
						 *
						 * 	we account for 2 uint64_t due to the whole record structure
						 * */
						uint64_t remaining_write_size = ( (2 * sizeof(uint32_t)) + (size_left * sizeof(float)) + sizeof(uint64_t));
						if(ram->size == ram->capacity || ((ram->size + remaining_write_size) > ram->capacity)){
							/*you have to expand the capacity*/
							uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem, 
									ram->capacity * sizeof(uint8_t),
									ram->capacity + (remaining_write_size + 1) * sizeof(uint8_t));
							if(!n_mem){
								fprintf(stderr,"(%s): reask_mem() failed %s:%d.\n",prog,__FILE__,__LINE__-2);
								return -1;
							}
							ram->mem = n_mem;
							ram->capacity += (remaining_write_size + 1);
							memset(&ram->mem[ram->offset],0,remaining_write_size +1);
						}


						uint32_t sz_left_ne = swap32(size_left);
						memcpy(&ram->mem[ram->offset],&sz_left_ne,sizeof(uint32_t));
						ram->size += sizeof(uint32_t);
						ram->offset = ram->size;

						uint32_t pd_ne = 0;
						memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
						ram->size += sizeof(uint32_t);
						ram->offset = ram->size;

						uint32_t j;
						for(j = 0; j < size_left; j++){
							if(step < rec->fields[i].data.v.size){
								uint32_t num_ne = htonf(rec->fields[i].data.v.elements.f[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint32_t));
								ram->size += sizeof(uint32_t);
								ram->offset = ram->size;

								step++;
							}
						}
						uint64_t up_pos_ne = 0;
						memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
						ram->size += sizeof(uint64_t);
						ram->offset = ram->size;

						/*we need to write this rec position in the old rec*/
						ram->offset = go_back_to;
						update_off_ne = swap64((uint64_t)update_pos);
						memcpy(&ram->mem[ram->offset],&update_off_ne,sizeof(uint64_t));
						ram->offset += sizeof(uint64_t);

						
						break;
					}

					ram->offset = update_pos;

				}while(update_pos > 0);

				if(rec->fields[i].data.v.size < sz){
					ram->offset -= sizeof(uint32_t);

					padding = sz - rec->fields[i].data.v.size;
					uint32_t p_ne = swap32((uint32_t)padding);
					uint32_t sz_ne = swap32(rec->fields[i].data.v.size);
					memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					/*write the padding*/
					memcpy(&ram->mem[ram->offset],&p_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					int j;
					for(j = step; j < rec->fields[i].data.v.size; j++){
						if(step < rec->fields[i].data.v.size){
							uint32_t num_ne = htonf(rec->fields[i].data.v.elements.f[j]);
							memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);
							step++;
						}
					}
					ram->offset += (padding * sizeof(float));
					uint64_t up_pos_ne = 0;

					memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);


				}else if(rec->fields[i].data.v.size == sz){

					if(step > 0){

						ram->offset -= sizeof(uint32_t);

						int size_left = rec->fields[i].data.v.size - step;

						uint32_t sz_ne = swap32((uint32_t)size_left);
						memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(uint32_t));
						ram->offset += sizeof(uint32_t);
					}

						/* read padding value*/
						uint32_t pd_ne = 0;
						memcpy(&pd_ne, &ram->mem[ram->offset],sizeof(uint32_t));
						ram->offset += sizeof(uint32_t);
						padding = (int)swap32(pd_ne);
						
						int j;
						for(j = 0; j < rec->fields[i].data.v.size; j++){
							if(step < rec->fields[i].data.v.size){
								uint32_t num_ne = htonf(rec->fields[i].data.v.elements.f[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint32_t));
								ram->offset += sizeof(uint32_t);
								step++;
							}
						}

						if(padding > 0)	ram->offset += (padding * sizeof(float));

						uint64_t up_pos_ne = 0;
						memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
						ram->offset += sizeof(uint64_t);
				}

				if(go_back_to_first_rec > 0) ram->offset = go_back_to_first_rec;
			}
			break;

		}
		case TYPE_ARRAY_DOUBLE:
		{
			if(!update){
				uint32_t sz = swap32(rec->fields[i].data.v.size);
				memcpy(&ram->mem[ram->size],&sz, sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				uint32_t pad = 0;
				memcpy(&ram->mem[ram->size],&pad, sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				uint64_t arr[rec->fields[i].data.v.size];
				memset(arr,0,sizeof(uint64_t) * rec->fields[i].data.v.size);
				int j;
				for(j = 0; j < rec->fields[i].data.v.size; j++){
					arr[j] = htond(rec->fields[i].data.v.elements.d[j]);
				} 

				memcpy(&ram->mem[ram->size],arr,sizeof(uint64_t) * rec->fields[i].data.v.size);
				ram->size += (sizeof(uint64_t) * rec->fields[i].data.v.size);
				ram->offset += (sizeof(uint64_t) * rec->fields[i].data.v.size);

				uint64_t upd = 0;	
				memcpy(&ram->mem[ram->size],&upd,sizeof(uint64_t));
				ram->size += (sizeof(uint64_t));
				ram->offset += sizeof(uint64_t);
			}else{
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int sz = 0;
				int k = 0;
				int step = 0;
				int padding = 0;

				do{
					/*check size of the array on file*/
					uint32_t sz_ne = 0; 
					memcpy(&sz_ne,&ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					sz = (int)swap32(sz_ne);

					if(rec->fields[i].data.v.size < sz || rec->fields[i].data.v.size == sz) break;

					/* 
					 * the program reach this branch only if the size of 
					 * the new array in the record is bigger than what we already have on file
					 * so we need to extend the array on file.
					 * */

					/*read padding value*/
					uint32_t pd_ne = 0;
					memcpy(&pd_ne, &ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					padding = (int)swap32(pd_ne);

					/*
					 * this is never true at the first iteration
					 * */
					if(step >= sz){

						int exit = 0;
						int array_last = 0;
						if((array_last = is_array_last_block(-1,ram,sz,sizeof(double),TYPE_DOUBLE)) == -1){
							fprintf(stderr,"(%s): can't verify array last block %s:%d",prog,__FILE__,__LINE__-1);
							return -1;
						}
						
						if(rec->fields[i].data.v.size < (sz + step) && array_last){
							padding += (sz - (rec->fields[i].data.v.size - step));

							sz = rec->fields[i].data.v.size - step;
							exit = 1;
							ram->offset -= (2*sizeof(uint32_t));
							/*write the update size of the array*/
							uint32_t new_sz_ne = swap32((uint32_t)sz);
							memcpy(&ram->mem[ram->offset],&new_sz_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);

							/*write the padding*/
							uint32_t pd_ne = swap32((uint32_t)padding);
							memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);

						}else if(rec->fields[i].data.v.size == (sz + step) && array_last){
							exit = 1;
						}

						while(sz){
							if(step < rec->fields[i].data.v.size){
								uint64_t num_ne = htond(rec->fields[i].data.v.elements.d[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint64_t));
								ram->offset += sizeof(uint64_t);
								step++;
							}
							sz--;
						}

						if(exit){
							uint64_t up_pos_ne = 0;
							memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;	
						}
					} else {

						/* 
						 * in the first iteration in the 
						 * do-while loop this will
						 * overwrite the array on file
						 * */

						int exit = 0;
						for(k = 0;k < sz; k++){
							if(step > 0 && k == 0 ){
								if((step + sz) > rec->fields[i].data.v.size){
									int array_last = 0;
									if((array_last = is_array_last_block(-1,ram,sz,sizeof(double),0)) == -1){
										fprintf(stderr,"(%s): can't verify array last block %s:%d",prog,__FILE__,__LINE__-1);
										return -1;
									}

									if(rec->fields[i].data.v.size < (sz + step) && array_last){
										padding += (sz - (rec->fields[i].data.v.size - step));

										sz = rec->fields[i].data.v.size - step;
										exit = 1;
										ram->offset -= (2*sizeof(uint32_t));
										/*write the update size of the array*/
										uint32_t new_sz_ne = swap32((uint32_t)sz);
										memcpy(&ram->mem[ram->offset],&new_sz_ne,sizeof(uint32_t));
										ram->offset += sizeof(uint32_t);

										/*write the padding*/
										uint32_t pd_ne = swap32((uint32_t)padding);
										memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
										ram->offset += sizeof(uint32_t);
									}
								}
							}

							if(step < rec->fields[i].data.v.size){
								uint64_t num_ne = htond(rec->fields[i].data.v.elements.d[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint64_t));
								ram->offset += sizeof(uint64_t);
								step++;

								if(!(step < rec->fields[i].data.v.size)) exit = 0;
							}
						}

						if(exit){
							if(padding > 0) ram->offset += (padding * sizeof(double));

							uint64_t up_pos_ne = 0;
							memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;/*from the main do-while loop*/
						}
					}

					if(padding > 0) ram->offset += (sizeof(double) * padding);

					uint64_t update_off_ne = 0;
					off_t go_back_to = ram->offset;

					memcpy(&update_off_ne,&ram->mem[ram->offset],sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);

					if(go_back_to_first_rec == 0) go_back_to_first_rec = ram->offset;

					update_pos = (off_t) swap64(update_off_ne);
					/*
					 * if the update_pos is == 0 it means 
					 * we need to move at the end of the file and write the remaining 
					 * element of the array
					 *
					 * */
					if(update_pos == 0){
						/*go to EOF*/	
						update_pos = ram->size;
						ram->offset = ram->size;

						uint32_t size_left = rec->fields[i].data.v.size - step;
						/*
						 * compute the size that we need to write
						 * remeber that each array record is:
						 * 	- uint32_t sz;
						 * 	- uint32_t padding;
						 * 	- sizeof(each element) * sz;
						 * 	- uint64_t update_pos;
						 *
						 * */
						uint64_t remaining_write_size = ( (2 * sizeof(uint32_t)) + (size_left * sizeof(double)) + sizeof(uint64_t));
						if(ram->size == ram->capacity || ((ram->size + remaining_write_size) > ram->capacity)){
							/*you have to expand the capacity*/
							uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem, 
									ram->capacity * sizeof(uint8_t),
									ram->capacity + (remaining_write_size + 1) * sizeof(uint8_t));
							if(!n_mem){
								fprintf(stderr,"(%s): reask_mem() failed %s:%d.\n",prog,__FILE__,__LINE__-2);
								return -1;
							}

							ram->mem = n_mem;
							ram->capacity += (remaining_write_size + 1);
							memset(&ram->mem[ram->offset],0,remaining_write_size +1);
						}


						uint32_t sz_left_ne = swap32(size_left);
						memcpy(&ram->mem[ram->offset],&sz_left_ne,sizeof(uint32_t));
						ram->size += sizeof(uint32_t);
						ram->offset = ram->size;

						uint32_t pd_ne = 0;
						memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
						ram->size += sizeof(uint32_t);
						ram->offset = ram->size;

						uint32_t j;
						for(j = 0; j < size_left; j++){
							if(step < rec->fields[i].data.v.size){
								uint64_t num_ne = htond(rec->fields[i].data.v.elements.d[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint64_t));
								ram->size += sizeof(uint64_t);
								ram->offset = ram->size;

								step++;
							}
						}
						uint64_t up_pos_ne = 0;
						memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
						ram->size += sizeof(uint64_t);
						ram->offset = ram->size;

						/*we need to write this rec position in the old rec*/
						ram->offset = go_back_to;
						update_off_ne = swap64((uint64_t)update_pos);
						memcpy(&ram->mem[ram->offset],&update_off_ne,sizeof(uint64_t));
						ram->offset += sizeof(uint64_t);
						
						break;
					}

					ram->offset = update_pos;

				}while(update_pos > 0);

				if(rec->fields[i].data.v.size < sz){
					ram->offset -= sizeof(uint32_t);

					padding = sz - rec->fields[i].data.v.size;
					uint32_t p_ne = swap32((uint32_t)padding);
					uint32_t sz_ne = swap32(rec->fields[i].data.v.size);
					memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					/*write the padding*/
					memcpy(&ram->mem[ram->offset],&p_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					int j;
					for(j = step; j <rec->fields[i].data.v.size; j++){
						if(step < rec->fields[i].data.v.size){

							uint64_t num_ne = htond(rec->fields[i].data.v.elements.d[j]);
							memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							step++;
						}
					}
					ram->offset += (padding * sizeof(long));

					uint64_t up_pos_ne = 0;
					memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);
				}else if(rec->fields[i].data.v.size == sz){

					if(step > 0){

						ram->offset -= sizeof(uint32_t);
						int size_left = rec->fields[i].data.v.size - step;

						uint32_t sz_ne = swap32((uint32_t)size_left);
						memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(uint32_t));
						ram->offset += sizeof(uint32_t);
					}
						/* read padding value*/
						uint32_t pd_ne = 0;
						memcpy(&pd_ne, &ram->mem[ram->offset],sizeof(uint32_t));
						ram->offset += sizeof(uint32_t);
						padding = (int)swap32(pd_ne);
						
						int j;
						for(j = 0; j< rec->fields[i].data.v.size; j++){
							if(step < rec->fields[i].data.v.size){
								uint64_t num_ne = htond(rec->fields[i].data.v.elements.d[step]);
								memcpy(&ram->mem[ram->offset],&num_ne,sizeof(uint64_t));
								ram->offset += sizeof(uint64_t);
								step++;
							}
						}

						if(padding > 0)	ram->offset += (padding * sizeof(double));

						uint64_t up_pos_ne = 0;
						memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
						ram->offset += sizeof(uint64_t);
				}

				if(go_back_to_first_rec > 0) ram->offset = go_back_to_first_rec;
			}

			break;
		}
		case TYPE_ARRAY_STRING:
		{
			if(!update){
				uint32_t sz = swap32(rec->fields[i].data.v.size);
				memcpy(&ram->mem[ram->size],&sz, sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);
				uint32_t pad = 0;
				memcpy(&ram->mem[ram->size],&pad, sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				int j;
				for(j = 0; j < rec->fields[i].data.v.size; j++){

					uint16_t l = (uint16_t)strlen(rec->fields[i].data.v.elements.s[j]);
					uint16_t buf_up_ne = swap16((l*2)+1);	
					uint32_t str_loc = 0;	

					memcpy(&ram->mem[ram->size],&str_loc, sizeof(uint32_t));
					ram->size += sizeof(uint32_t);
					ram->offset += sizeof(uint32_t);

					memcpy(&ram->mem[ram->size],&buf_up_ne, sizeof(uint16_t));
					ram->size += sizeof(uint16_t);
					ram->offset += sizeof(uint16_t);
					char buff[(l * 2) + 1];
					memset(buff,0,(l * 2) +1);
					strncpy(buff,rec->fields[i].data.v.elements.s[j],l);
					memcpy(&ram->mem[ram->size],buff,(l * 2) + 1);
					ram->size += (( l * 2) + 1);
					ram->offset += (( l * 2) + 1);

				}
				uint64_t upd = 0;	
				memcpy(&ram->mem[ram->size],&upd,sizeof(uint64_t));
				ram->size += sizeof(uint64_t);
				ram->offset += sizeof(uint64_t);
				break;
			}else{
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				int sz = 0;
				int k = 0;
				int step = 0;
				int padding = 0;

				do{
					/*check size of the array on file*/
					uint32_t sz_ne = 0; 
					memcpy(&sz_ne,&ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					sz = (int)swap32(sz_ne);

					if(rec->fields[i].data.v.size < sz || rec->fields[i].data.v.size == sz) break;

					/* 
					 * the program reach this branch only if the size of 
					 * the new array in the record is bigger than what we already have on file
					 * so we need to extend the array on file.
					 * */

					/*read padding value*/
					uint32_t pd_ne = 0;
					memcpy(&pd_ne, &ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					padding = (int)swap32(pd_ne);

					/*
					 * this is never true at the first iteration
					 * */
					if(step >= sz){

						int exit = 0;
						int array_last = 0;
						if((array_last = is_array_last_block(-1,ram,sz,0,TYPE_ARRAY_STRING)) == -1){
							fprintf(stderr,"(%s): can't verify array last block %s:%d",prog,__FILE__,__LINE__-1);
							return -1;
						}
						
						if(rec->fields[i].data.v.size < (sz + step) && array_last){
							padding += (sz - (rec->fields[i].data.v.size - step));

							sz = rec->fields[i].data.v.size - step;
							exit = 1;
							ram->offset -= (2*sizeof(uint32_t));
							/*write the update size of the array*/
							uint32_t new_sz_ne = swap32((uint32_t)sz);
							memcpy(&ram->mem[ram->offset],&new_sz_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);

							/*write the padding*/
							uint32_t pd_ne = swap32((uint32_t)padding);
							memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
							ram->offset += sizeof(uint32_t);

						}else if(rec->fields[i].data.v.size == (sz + step) && array_last){
							exit = 1;
						}

						while(sz){
							if(step < rec->fields[i].data.v.size){
								/*write the string*/
								uint64_t move_to = 0;
								uint32_t eof = 0;
								uint16_t bu_ne = 0;
								uint16_t new_lt = 0;
								/*save the starting offset for the string record*/
								uint64_t bg_pos = ram->size;
								uint32_t str_loc_ne= 0;

								/*read the other str_loc if any*/
								if(ram->size == ram->capacity && ram->offset != ram->size)
									memcpy(&str_loc_ne,&ram->mem[ram->offset],sizeof(uint32_t));
								else
									memcpy(&str_loc_ne,&ram->mem[ram->size],sizeof(uint32_t));


								ram->offset += sizeof(uint32_t);
								uint32_t str_loc = swap32(str_loc_ne);

								/* save pos where the data starts*/
								uint64_t af_str_loc_pos = ram->size;
								if(ram->size == ram->capacity && ram->offset != ram->size)
									af_str_loc_pos = ram->offset;
								else
									af_str_loc_pos = ram->size;

								uint16_t buff_update_ne = 0;

								if(ram->size == ram->capacity && ram->offset != ram->size)
									memcpy(&buff_update_ne,&ram->mem[ram->offset],sizeof(uint16_t));
								else
									memcpy(&buff_update_ne,&ram->mem[ram->size],sizeof(uint16_t));

								ram->offset += sizeof(uint16_t);

								uint16_t buff_update = swap16(buff_update_ne);
								uint64_t pos_after_first_str_record = ram->offset + buff_update; 
								if(ram->size == ram->capacity && ram->offset != ram->size)
									pos_after_first_str_record = ram->offset + buff_update; 
								else
									pos_after_first_str_record = ram->size + buff_update; 

								if (str_loc > 0){
									/*set the file pointer to str_loc*/
									ram->offset = str_loc;

									/*
									 * in the case of a regular buffer update we have
									 *  to save the off_t to get back to it later
									 * */
									move_to = ram->offset; 

									uint16_t bu_ne = 0;
									memcpy(&bu_ne,&ram->mem[ram->offset],sizeof(uint16_t));
									ram->offset +=  sizeof(uint16_t);	

									buff_update = (off_t)swap16(bu_ne);
								}

								new_lt = strlen(rec->fields[i].data.s) + 1; /*get new str length*/

								if (new_lt > buff_update) {
									/*expand the buff_update only for the bytes needed*/
									buff_update += (new_lt - buff_update);

									/*
									 * if the new length is bigger then the buffer,
									 * set the file pointer to EOF to write the new data
									 * */
									eof = ram->size;
									ram->offset = eof;
									if(eof == ram->capacity){
										errno = 0;
										uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem,ram->capacity,
												(ram->capacity + buff_update) * sizeof(uint8_t));
										if(!n_mem){
											fprintf(stderr,"reask_mem failed with '%s', %s:%d.\n",strerror(errno),__FILE__,__LINE__-1);
											return -1;
										}
										ram->mem = n_mem;
										ram->capacity += buff_update; 

									}else if((eof + buff_update) > ram->capacity){
										errno = 0;
										uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem,ram->capacity,
												(ram->capacity + buff_update) * sizeof(uint8_t));
										if(!n_mem){
											fprintf(stderr,"reask_mem failed with '%s', %s:%d.\n",strerror(errno),__FILE__,__LINE__-1);
											return -1;
										}
										ram->mem = n_mem;
										ram->capacity += ((eof + buff_update) - ram->capacity); 
									}
								}

								char buff_w[buff_update];
								memset(buff_w,0,buff_update);

								strncpy(buff_w, rec->fields[i].data.s, new_lt - 1);
								/*
								 * if we did not move to another position
								 * set the file pointer back to the begginning of the string record
								 * to overwrite the data accordingly
								 * */
								if (str_loc == 0 && ((new_lt - buff_update) < 0)){
									ram->offset = af_str_loc_pos;
								} else if (str_loc > 0 && ((new_lt - buff_update) < 0)){
									ram->offset = move_to;
								}

								/*
								 * write the data to file --
								 * the file pointer is always pointing to the
								 * right position at this point */
								bu_ne = swap16((uint16_t)buff_update);

								if(eof == 0)
									memcpy(&ram->mem[ram->offset],&bu_ne,sizeof(uint16_t));
								else 
									memcpy(&ram->mem[ram->size],&bu_ne,sizeof(uint16_t));


								ram->offset += sizeof(uint16_t);
								memcpy(&ram->mem[ram->offset],buff_w,buff_update);
								ram->offset += sizeof(buff_w);

								/*
								 * if eof is bigger than 0 means we updated the string
								 * we need to save the off_t of the new written data
								 * at the start of the original.
								 * */
								if (eof > 0){
									/*go at the beginning of the str record*/
									ram->offset = bg_pos;

									/*update new string position*/
									uint32_t eof_ne = swap32((uint32_t)eof);
									memcpy(&ram->mem[ram->offset],&eof_ne,sizeof(uint32_t));
									ram->offset += sizeof(uint32_t);

									/*set file pointer to the end of the 1st string rec*/
									/*this step is crucial to avoid losing data        */

									ram->offset = pos_after_first_str_record;
								}else if (str_loc > 0){
									/*
									 * Make sure that in all cases
									 * we go back to the end of the 1st record
									 * */
									ram->offset = pos_after_first_str_record;
								}
								step++;
							}
							sz--;
						}

						if(exit){
							uint64_t up_pos_ne = 0;
							memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;	
						}
					} else {

						/* 
						 * in the first iteration in the 
						 * do-while loop this will
						 * overwrite the array on file
						 * */

						int exit = 0;
						for(k = 0;k < sz; k++){
							if(step > 0 && k == 0 ){
								if((step + sz) > rec->fields[i].data.v.size){
									int array_last = 0;
									if((array_last = is_array_last_block(-1,ram,sz,0,TYPE_ARRAY_STRING)) == -1){
										fprintf(stderr,"(%s): can't verify array last block %s:%d",prog,__FILE__,__LINE__-1);
										return -1;
									}

									if(rec->fields[i].data.v.size < (sz + step) && array_last){
										padding += (sz - (rec->fields[i].data.v.size - step));

										sz = rec->fields[i].data.v.size - step;
										exit = 1;
										ram->offset -= (2*sizeof(uint32_t));
										/*write the update size of the array*/
										uint32_t new_sz_ne = swap32((uint32_t)sz);
										memcpy(&ram->mem[ram->offset],&new_sz_ne,sizeof(uint32_t));
										ram->offset += sizeof(uint32_t);

										/*write the padding*/
										uint32_t pd_ne = swap32((uint32_t)padding);
										memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
										ram->offset += sizeof(uint32_t);
									}
								}
							}

							if(step < rec->fields[i].data.v.size){
								/*write string*/
								uint64_t move_to = 0;
								uint32_t eof = 0;
								uint16_t bu_ne = 0;
								uint16_t new_lt = 0;
								/*save the starting offset for the string record*/
								uint64_t bg_pos = ram->size;
								uint32_t str_loc_ne= 0;

								/*read the other str_loc if any*/
								if(ram->size == ram->capacity && ram->offset != ram->size)
									memcpy(&str_loc_ne,&ram->mem[ram->offset],sizeof(uint32_t));
								else
									memcpy(&str_loc_ne,&ram->mem[ram->size],sizeof(uint32_t));


								ram->offset += sizeof(uint32_t);
								uint32_t str_loc = swap32(str_loc_ne);

								/* save pos where the data starts*/
								uint64_t af_str_loc_pos = ram->size;
								if(ram->size == ram->capacity && ram->offset != ram->size)
									af_str_loc_pos = ram->offset;
								else
									af_str_loc_pos = ram->size;

								uint16_t buff_update_ne = 0;

								if(ram->size == ram->capacity && ram->offset != ram->size)
									memcpy(&buff_update_ne,&ram->mem[ram->offset],sizeof(uint16_t));
								else
									memcpy(&buff_update_ne,&ram->mem[ram->size],sizeof(uint16_t));

								ram->offset += sizeof(uint16_t);

								uint16_t buff_update = swap16(buff_update_ne);
								uint64_t pos_after_first_str_record = ram->offset + buff_update; 
								if(ram->size == ram->capacity && ram->offset != ram->size)
									pos_after_first_str_record = ram->offset + buff_update; 
								else
									pos_after_first_str_record = ram->size + buff_update; 

								if (str_loc > 0){
									/*set the file pointer to str_loc*/
									ram->offset = str_loc;

									/*
									 * in the case of a regular buffer update we have
									 *  to save the off_t to get back to it later
									 * */
									move_to = ram->offset; 

									uint16_t bu_ne = 0;
									memcpy(&bu_ne,&ram->mem[ram->offset],sizeof(uint16_t));
									ram->offset +=  sizeof(uint16_t);	

									buff_update = (off_t)swap16(bu_ne);
								}

								new_lt = strlen(rec->fields[i].data.v.elements.s[k]) + 1; /*get new str length*/

								if (new_lt > buff_update) {
									/*expand the buff_update only for the bytes needed*/
									buff_update += (new_lt - buff_update);

									/*
									 * if the new length is bigger then the buffer,
									 * set the file pointer to EOF to write the new data
									 * */
									eof = ram->size;
									ram->offset = eof;
									if(eof == ram->capacity){
										errno = 0;
										uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem,ram->capacity,
												(ram->capacity + buff_update) * sizeof(uint8_t));
										if(!n_mem){
											fprintf(stderr,"reask_mem failed with '%s', %s:%d.\n",strerror(errno),__FILE__,__LINE__-1);
											return -1;
										}
										ram->mem = n_mem;
										ram->capacity += buff_update; 

									}else if((eof + buff_update) > ram->capacity){
										errno = 0;
										uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem,ram->capacity,
												(ram->capacity + buff_update) * sizeof(uint8_t));
										if(!n_mem){
											fprintf(stderr,"reask_mem failed with '%s', %s:%d.\n",strerror(errno),__FILE__,__LINE__-1);
											return -1;
										}
										ram->mem = n_mem;
										ram->capacity += ((eof + buff_update) - ram->capacity); 
									}
								}

								char buff_w[buff_update];
								memset(buff_w,0,buff_update);

								strncpy(buff_w, rec->fields[i].data.v.elements.s[k], new_lt - 1);
								/*
								 * if we did not move to another position
								 * set the file pointer back to the begginning of the string record
								 * to overwrite the data accordingly
								 * */
								if (str_loc == 0 && ((new_lt - buff_update) < 0)){
									ram->offset = af_str_loc_pos;
								} else if (str_loc > 0 && ((new_lt - buff_update) < 0)){
									ram->offset = move_to;
								}

								/*
								 * write the data to file --
								 * the file pointer is always pointing to the
								 * right position at this point */
								bu_ne = swap16((uint16_t)buff_update);

								if(eof == 0)
									memcpy(&ram->mem[ram->offset],&bu_ne,sizeof(uint16_t));
								else 
									memcpy(&ram->mem[ram->size],&bu_ne,sizeof(uint16_t));


								ram->offset += sizeof(uint16_t);
								memcpy(&ram->mem[ram->offset],buff_w,buff_update);
								ram->offset += sizeof(buff_w);

								/*
								 * if eof is bigger than 0 means we updated the string
								 * we need to save the off_t of the new written data
								 * at the start of the original.
								 * */
								if (eof > 0){
									/*go at the beginning of the str record*/
									ram->offset = bg_pos;

									/*update new string position*/
									uint32_t eof_ne = swap32((uint32_t)eof);
									memcpy(&ram->mem[ram->offset],&eof_ne,sizeof(uint32_t));
									ram->offset += sizeof(uint32_t);

									/*set file pointer to the end of the 1st string rec*/
									/*this step is crucial to avoid losing data        */

									ram->offset = pos_after_first_str_record;
								}else if (str_loc > 0){
									/*
									 * Make sure that in all cases
									 * we go back to the end of the 1st record
									 * */
									ram->offset = pos_after_first_str_record;
								}

								step++;
								if(!(step < rec->fields[i].data.v.size)) exit = 0;
							}
						}

						if(exit){
							if (padding > 0) {
								int i;
								for(i = 0; i < padding; i++){
									if(get_string_size(-1,ram) == (size_t) -1){
										__er_file_pointer(F, L - 1);
										return 0;
									}
								}
							}


							uint64_t up_pos_ne = 0;
							memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;/*from the main do-while loop*/
						}
					}

					if (padding > 0) {
						int i;
						for(i = 0; i < padding; i++){
							if(get_string_size(-1,ram) == (size_t) -1){
								__er_file_pointer(F, L - 1);
								return 0;
							}
						}
					}

					uint64_t update_off_ne = 0;
					off_t go_back_to = ram->offset;

					memcpy(&update_off_ne,&ram->mem[ram->offset],sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);

					if(go_back_to_first_rec == 0) go_back_to_first_rec = ram->offset;

					update_pos = (off_t) swap64(update_off_ne);
					/*
					 * if the update_pos is == 0 it means 
					 * we need to move at the end of the file and write the remaining 
					 * element of the array
					 *
					 * */
					if(update_pos == 0){
						/*go to EOF*/	
						update_pos = ram->size;
						ram->offset = ram->size;

						uint32_t size_left = rec->fields[i].data.v.size - step;
						/*
						 * compute the size that we need to write
						 * remeber that each array record is:
						 * 	- uint32_t sz;
						 * 	- uint32_t padding;
						 * 	- sizeof(each element) * sz;
						 * 	- uint64_t update_pos;
						 *
						 * 	we account for 1 uint64_t due to the whole record structure
						 * */

						size_t each_str_size = 0;
						int x;
						for(x = step; x <  rec->fields[i].data.v.size; x++){
							each_str_size += (sizeof(uint32_t) + sizeof(uint16_t));
							each_str_size+= ((strlen(rec->fields[i].data.v.elements.s[x]) * 2) + 1);

						}

						uint64_t remaining_write_size = ((2 * sizeof(uint32_t)) + 
										each_str_size 		+ 
										sizeof(uint64_t));

						if(ram->size == ram->capacity || ((ram->size + remaining_write_size) > ram->capacity)){
							/*you have to expand the capacity*/
							uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem, 
									ram->capacity * sizeof(uint8_t),
									ram->capacity + (remaining_write_size + 1) * sizeof(uint8_t));
							if(!n_mem){
								fprintf(stderr,"(%s): reask_mem() failed %s:%d.\n",prog,__FILE__,__LINE__-2);
								return -1;
							}

							ram->mem = n_mem;
							ram->capacity += (remaining_write_size + 1);
							memset(&ram->mem[ram->offset],0,remaining_write_size +1);
						}


						uint32_t sz_left_ne = swap32(size_left);
						memcpy(&ram->mem[ram->offset],&sz_left_ne,sizeof(uint32_t));
						ram->size += sizeof(uint32_t);
						ram->offset = ram->size;

						uint32_t pd_ne = 0;
						memcpy(&ram->mem[ram->offset],&pd_ne,sizeof(uint32_t));
						ram->size += sizeof(uint32_t);
						ram->offset = ram->size;

						uint32_t j;
						for(j = 0; j < size_left; j++){
							if(step < rec->fields[i].data.v.size){
								/* write the string*/
								uint16_t l = (uint16_t)strlen(rec->fields[i].data.v.elements.s[j]);
								uint16_t buf_up_ne = swap16((l*2)+1);	
								uint32_t str_loc = 0;	

								memcpy(&ram->mem[ram->size],&str_loc, sizeof(uint32_t));
								ram->offset += sizeof(uint32_t);

								memcpy(&ram->mem[ram->size],&buf_up_ne, sizeof(uint16_t));
								ram->offset += sizeof(uint16_t);
								char buff[(l * 2) + 1];
								memset(buff,0,(l * 2) +1);
								strncpy(buff,rec->fields[i].data.v.elements.s[j],l);
								memcpy(&ram->mem[ram->size],buff,(l * 2) + 1);
								ram->offset += (( l * 2) + 1);


								step++;
							}
						}
						uint64_t up_pos_ne = 0;
						memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
						ram->size += sizeof(uint64_t);
						ram->offset = ram->size;

						/*we need to write this rec position in the old rec*/
						ram->offset = go_back_to;
						update_off_ne = swap64((uint64_t)update_pos);
						memcpy(&ram->mem[ram->offset],&update_off_ne,sizeof(uint64_t));
						ram->offset += sizeof(uint64_t);

						break;
					}

					ram->offset = update_pos;

				}while(update_pos > 0);

				if(rec->fields[i].data.v.size < sz){
					ram->offset -= sizeof(uint32_t);

					padding = sz - rec->fields[i].data.v.size;
					uint32_t p_ne = swap32((uint32_t)padding);
					uint32_t sz_ne = swap32(rec->fields[i].data.v.size);
					memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					/*write the padding*/
					memcpy(&ram->mem[ram->offset],&p_ne,sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					int j;
					for(j = step; j <rec->fields[i].data.v.size; j++){
						if(step < rec->fields[i].data.v.size){

							/* write string*/
							uint64_t move_to = 0;
							uint32_t eof = 0;
							uint16_t bu_ne = 0;
							uint16_t new_lt = 0;
							/*save the starting offset for the string record*/
							uint64_t bg_pos = ram->size;
							uint32_t str_loc_ne= 0;

							/*read the other str_loc if any*/
							if(ram->size == ram->capacity && ram->offset != ram->size)
								memcpy(&str_loc_ne,&ram->mem[ram->offset],sizeof(uint32_t));
							else
								memcpy(&str_loc_ne,&ram->mem[ram->size],sizeof(uint32_t));


							ram->offset += sizeof(uint32_t);
							uint32_t str_loc = swap32(str_loc_ne);

							/* save pos where the data starts*/
							uint64_t af_str_loc_pos = ram->size;
							if(ram->size == ram->capacity && ram->offset != ram->size)
								af_str_loc_pos = ram->offset;
							else
								af_str_loc_pos = ram->size;

							uint16_t buff_update_ne = 0;

							if(ram->size == ram->capacity && ram->offset != ram->size)
								memcpy(&buff_update_ne,&ram->mem[ram->offset],sizeof(uint16_t));
							else
								memcpy(&buff_update_ne,&ram->mem[ram->size],sizeof(uint16_t));

							ram->offset += sizeof(uint16_t);

							uint16_t buff_update = swap16(buff_update_ne);
							uint64_t pos_after_first_str_record = ram->offset + buff_update; 
							if(ram->size == ram->capacity && ram->offset != ram->size)
								pos_after_first_str_record = ram->offset + buff_update; 
							else
								pos_after_first_str_record = ram->size + buff_update; 

							if (str_loc > 0){
								/*set the file pointer to str_loc*/
								ram->offset = str_loc;

								/*
								 * in the case of a regular buffer update we have
								 *  to save the off_t to get back to it later
								 * */
								move_to = ram->offset; 

								uint16_t bu_ne = 0;
								memcpy(&bu_ne,&ram->mem[ram->offset],sizeof(uint16_t));
								ram->offset +=  sizeof(uint16_t);	

								buff_update = (off_t)swap16(bu_ne);
							}

							new_lt = strlen(rec->fields[i].data.v.elements.s[j]) + 1; /*get new str length*/

							if (new_lt > buff_update) {
								/*expand the buff_update only for the bytes needed*/
								buff_update += (new_lt - buff_update);

								/*
								 * if the new length is bigger then the buffer,
								 * set the file pointer to EOF to write the new data
								 * */
								eof = ram->size;
								ram->offset = eof;
								if(eof == ram->capacity){
									errno = 0;
									uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem,ram->capacity,
											(ram->capacity + buff_update) * sizeof(uint8_t));
									if(!n_mem){
										fprintf(stderr,"reask_mem failed with '%s', %s:%d.\n",strerror(errno),__FILE__,__LINE__-1);
										return -1;
									}
									ram->mem = n_mem;
									ram->capacity += buff_update; 

								}else if((eof + buff_update) > ram->capacity){
									errno = 0;
									uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem,ram->capacity,
											(ram->capacity + buff_update) * sizeof(uint8_t));
									if(!n_mem){
										fprintf(stderr,"reask_mem failed with '%s', %s:%d.\n",strerror(errno),__FILE__,__LINE__-1);
										return -1;
									}
									ram->mem = n_mem;
									ram->capacity += ((eof + buff_update) - ram->capacity); 
								}
							}

							char buff_w[buff_update];
							memset(buff_w,0,buff_update);

							strncpy(buff_w, rec->fields[i].data.v.elements.s[j], new_lt - 1);
							/*
							 * if we did not move to another position
							 * set the file pointer back to the begginning of the string record
							 * to overwrite the data accordingly
							 * */
							if (str_loc == 0 && ((new_lt - buff_update) < 0)){
								ram->offset = af_str_loc_pos;
							} else if (str_loc > 0 && ((new_lt - buff_update) < 0)){
								ram->offset = move_to;
							}

							/*
							 * write the data to file --
							 * the file pointer is always pointing to the
							 * right position at this point */
							bu_ne = swap16((uint16_t)buff_update);

							if(eof == 0)
								memcpy(&ram->mem[ram->offset],&bu_ne,sizeof(uint16_t));
							else 
								memcpy(&ram->mem[ram->size],&bu_ne,sizeof(uint16_t));


							ram->offset += sizeof(uint16_t);
							memcpy(&ram->mem[ram->offset],buff_w,buff_update);
							ram->offset += sizeof(buff_w);

							/*
							 * if eof is bigger than 0 means we updated the string
							 * we need to save the off_t of the new written data
							 * at the start of the original.
							 * */
							if (eof > 0){
								/*go at the beginning of the str record*/
								ram->offset = bg_pos;

								/*update new string position*/
								uint32_t eof_ne = swap32((uint32_t)eof);
								memcpy(&ram->mem[ram->offset],&eof_ne,sizeof(uint32_t));
								ram->offset += sizeof(uint32_t);

								/*set file pointer to the end of the 1st string rec*/
								/*this step is crucial to avoid losing data        */

								ram->offset = pos_after_first_str_record;
							}else if (str_loc > 0){
								/*
								 * Make sure that in all cases
								 * we go back to the end of the 1st record
								 * */
								ram->offset = pos_after_first_str_record;
							}

							step++;
						}
					}

					if (padding > 0) {
						int i;
						for(i = 0; i < padding; i++){
							if(get_string_size(-1,ram) == (size_t) -1){
								__er_file_pointer(F, L - 1);
								return 0;
							}
						}
					}

					uint64_t up_pos_ne = 0;
					memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);
				}else if(rec->fields[i].data.v.size == sz){

					if(step > 0){

						ram->offset -= sizeof(uint32_t);
						int size_left = rec->fields[i].data.v.size - step;

						uint32_t sz_ne = swap32((uint32_t)size_left);
						memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(uint32_t));
						ram->offset += sizeof(uint32_t);
					}
					/* read padding value*/
					uint32_t pd_ne = 0;
					memcpy(&pd_ne, &ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);
					padding = (int)swap32(pd_ne);

					int j;
					for(j = 0; j< rec->fields[i].data.v.size; j++){
						if(step < rec->fields[i].data.v.size){
							/* write string*/
							uint64_t move_to = 0;
							uint32_t eof = 0;
							uint16_t bu_ne = 0;
							uint16_t new_lt = 0;
							/*save the starting offset for the string record*/
							uint64_t bg_pos = ram->size;
							uint32_t str_loc_ne= 0;

							/*read the other str_loc if any*/
							if(ram->size == ram->capacity && ram->offset != ram->size)
								memcpy(&str_loc_ne,&ram->mem[ram->offset],sizeof(uint32_t));
							else
								memcpy(&str_loc_ne,&ram->mem[ram->size],sizeof(uint32_t));


							ram->offset += sizeof(uint32_t);
							uint32_t str_loc = swap32(str_loc_ne);

							/* save pos where the data starts*/
							uint64_t af_str_loc_pos = ram->size;
							if(ram->size == ram->capacity && ram->offset != ram->size)
								af_str_loc_pos = ram->offset;
							else
								af_str_loc_pos = ram->size;

							uint16_t buff_update_ne = 0;

							if(ram->size == ram->capacity && ram->offset != ram->size)
								memcpy(&buff_update_ne,&ram->mem[ram->offset],sizeof(uint16_t));
							else
								memcpy(&buff_update_ne,&ram->mem[ram->size],sizeof(uint16_t));

							ram->offset += sizeof(uint16_t);

							uint16_t buff_update = swap16(buff_update_ne);
							uint64_t pos_after_first_str_record = ram->offset + buff_update; 
							if(ram->size == ram->capacity && ram->offset != ram->size)
								pos_after_first_str_record = ram->offset + buff_update; 
							else
								pos_after_first_str_record = ram->size + buff_update; 

							if (str_loc > 0){
								/*set the file pointer to str_loc*/
								ram->offset = str_loc;

								/*
								 * in the case of a regular buffer update we have
								 *  to save the off_t to get back to it later
								 * */
								move_to = ram->offset; 

								uint16_t bu_ne = 0;
								memcpy(&bu_ne,&ram->mem[ram->offset],sizeof(uint16_t));
								ram->offset +=  sizeof(uint16_t);	

								buff_update = (off_t)swap16(bu_ne);
							}

							new_lt = strlen(rec->fields[i].data.v.elements.s[j]) + 1; /*get new str length*/

							if (new_lt > buff_update) {
								/*expand the buff_update only for the bytes needed*/
								buff_update += (new_lt - buff_update);

								/*
								 * if the new length is bigger then the buffer,
								 * set the file pointer to EOF to write the new data
								 * */
								eof = ram->size;
								ram->offset = eof;
								if(eof == ram->capacity){
									errno = 0;
									uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem,ram->capacity,
											(ram->capacity + buff_update) * sizeof(uint8_t));
									if(!n_mem){
										fprintf(stderr,"reask_mem failed with '%s', %s:%d.\n",strerror(errno),__FILE__,__LINE__-1);
										return -1;
									}
									ram->mem = n_mem;
									ram->capacity += buff_update; 

								}else if((eof + buff_update) > ram->capacity){
									errno = 0;
									uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem,ram->capacity,
											(ram->capacity + buff_update) * sizeof(uint8_t));
									if(!n_mem){
										fprintf(stderr,"reask_mem failed with '%s', %s:%d.\n",strerror(errno),__FILE__,__LINE__-1);
										return -1;
									}
									ram->mem = n_mem;
									ram->capacity += ((eof + buff_update) - ram->capacity); 
								}
							}

							char buff_w[buff_update];
							memset(buff_w,0,buff_update);

							strncpy(buff_w, rec->fields[i].data.v.elements.s[j], new_lt - 1);
							/*
							 * if we did not move to another position
							 * set the file pointer back to the begginning of the string record
							 * to overwrite the data accordingly
							 * */
							if (str_loc == 0 && ((new_lt - buff_update) < 0)){
								ram->offset = af_str_loc_pos;
							} else if (str_loc > 0 && ((new_lt - buff_update) < 0)){
								ram->offset = move_to;
							}

							/*
							 * write the data to file --
							 * the file pointer is always pointing to the
							 * right position at this point */
							bu_ne = swap16((uint16_t)buff_update);

							if(eof == 0)
								memcpy(&ram->mem[ram->offset],&bu_ne,sizeof(uint16_t));
							else 
								memcpy(&ram->mem[ram->size],&bu_ne,sizeof(uint16_t));


							ram->offset += sizeof(uint16_t);
							memcpy(&ram->mem[ram->offset],buff_w,buff_update);
							ram->offset += sizeof(buff_w);

							/*
							 * if eof is bigger than 0 means we updated the string
							 * we need to save the off_t of the new written data
							 * at the start of the original.
							 * */
							if (eof > 0){
								/*go at the beginning of the str record*/
								ram->offset = bg_pos;

								/*update new string position*/
								uint32_t eof_ne = swap32((uint32_t)eof);
								memcpy(&ram->mem[ram->offset],&eof_ne,sizeof(uint32_t));
								ram->offset += sizeof(uint32_t);

								/*set file pointer to the end of the 1st string rec*/
								/*this step is crucial to avoid losing data        */

								ram->offset = pos_after_first_str_record;
							}else if (str_loc > 0){
								/*
								 * Make sure that in all cases
								 * we go back to the end of the 1st record
								 * */
								ram->offset = pos_after_first_str_record;
							}

							step++;
						}
					}

					if(padding > 0)	ram->offset += (padding * sizeof(long));

					uint64_t up_pos_ne = 0;
					memcpy(&ram->mem[ram->offset],&up_pos_ne,sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);
				}

				if(go_back_to_first_rec > 0) ram->offset = go_back_to_first_rec;
			}

			break;
		}
		case TYPE_FILE:
		{
			if(!update){
				uint32_t size = swap32(rec->fields[i].data.file.count);
				memcpy(&ram->mem[ram->size],&size,sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				/*pad*/
				uint32_t pad = 0;
				memcpy(&ram->mem[ram->size],&pad,sizeof(uint32_t));
				ram->size += sizeof(uint32_t);
				ram->offset += sizeof(uint32_t);

				uint32_t j;
				for(j = 0; j < rec->fields[i].data.file.count; j++){
					if(write_ram_record(ram,&rec->fields[i].data.file.recs[j],update,init_ram_size,offset) == -1){
						fprintf(stderr,"cannot write record to ram. %s:%d.\n", __FILE__,__LINE__ - 1);
						return -1;
					}
				}

				uint64_t upd = 0;	
				memcpy(&ram->mem[ram->size],&upd,sizeof(uint64_t));
				ram->size += sizeof(uint64_t);
				ram->offset += sizeof(uint64_t);
			}else{
				/*open the schema file*/
					
				/*create file name*/
				size_t file_name_length = strlen(rec->fields[i].field_name) + strlen(".sch");
				char file_name[file_name_length + 1];
				memset(file_name,0,file_name_length + 1);
				strncpy(file_name,rec->fields[i].field_name,strlen(rec->fields[i].field_name));
				strncat(file_name,".sch",strlen(".sch")+1);

				int fd_schema = open(file_name,0);
				if (fd_schema == -1){
					fprintf(stderr,"schema file not found, %s:%d\n",__FILE__,__LINE__-2);
					return -1;
				}

				struct Schema sch;
				memset(&sch,0,sizeof(struct Schema));
				struct Header_d hd = {0,0,&sch};
				while((is_locked(1,fd_schema)) == LOCKED);
				if(!read_header(fd_schema,&hd)){
					fprintf(stderr,"cannot read schema from file %s:%d",__FILE__,__LINE__-1);
					return -1;
				}
				close(fd_schema);

				/* update branch*/
				off_t update_pos = 0;
				off_t go_back_to_first_rec = 0;
				uint32_t step = 0;
				uint32_t sz = 0;
				uint32_t k = 0;
				int padding_value = 0;
				do
				{
					/* check the size */
					uint32_t sz_ne = 0;
					memcpy(&sz_ne, &ram->mem[ram->offset],sizeof(sz_ne));
					ram->offset += sizeof(sz_ne);

					sz = swap32(sz_ne);
					if (rec->fields[i].data.file.count < sz || rec->fields[i].data.file.count == sz)
						break;

					/*read the padding data*/
					uint32_t pd_ne = 0;
					memcpy(&pd_ne,&ram->mem[ram->offset], sizeof(pd_ne));
					ram->offset += sizeof(uint32_t);

					padding_value = (int)swap32(pd_ne);

					if (step >= sz)
					{
						int array_last = 0;
						int exit = 0;
						
						/*check if the array of type file is in the last block*/
						off_t reset_pointer_here = ram->offset;

						uint32_t y;
						for(y = 0; y < sz;y++){
							struct Record_f dummy;
							memset(&dummy,0,sizeof(struct Record_f));
							off_t offset = 0;
							if(read_ram_file(rec->file_name,
											ram,
											&dummy,
											*hd.sch_d) == -1){
								fprintf(stderr,"cannot read ram file, %s:%d.\n",__FILE__,__LINE__-1);
								return -1;
							}


							free_record(&dummy,dummy.fields_num);

							ram->offset += offset + sizeof(off_t);
						}

						uint64_t update_arr = 0;
						memcpy(&update_arr,&ram->mem[ram->offset],sizeof(update_arr));

						ram->offset = reset_pointer_here;

						off_t up_he = (off_t) swap64(update_arr);
						if(up_he == 0) array_last = 1;

						if (rec->fields[i].data.file.count < (sz + step) && array_last)
						{
							int pad_value = sz - (rec->fields[i].data.file.count - step);
							padding_value += pad_value;

							sz = rec->fields[i].data.v.size - step;
							exit = 1;

							ram->offset += ( 2 * (-sizeof(uint32_t)));

							/* write the updated size of the array */
							uint32_t new_sz = swap32((uint32_t)sz);
							memcpy(&ram->mem[ram->offset], &new_sz, sizeof(new_sz));
							ram->offset += sizeof(uint32_t);

							uint32_t new_pd = swap32((uint32_t)padding_value);
							memcpy(&ram->mem[ram->offset], &new_pd, sizeof(new_sz));
							ram->offset += sizeof(uint32_t);
						}
						else if (rec->fields[i].data.file.count == (sz + step) && array_last)
						{
							exit = 1;
						}

						while (sz)
						{
							if (step < rec->fields[i].data.file.count){

								if(write_ram_record(ram,
											&rec->fields[i].data.file.recs[step],
											update,0,0) == -1){
									fprintf(stderr,"write_ram_record failed, %s:%d.\n",__FILE__, __LINE__ - 1);
									return -1;
								}
								step++;
							}
							sz--;
						}

						if (exit){
							/*write the epty update offset*/
							uint64_t empty_offset = 0;
							memcpy(&ram->mem[ram->offset],&empty_offset,sizeof(uint64_t));
							break;
						}
					}
					else
					{

						int exit = 0;
						for (k = 0; k < sz; k++)
						{
							if (step > 0 && k == 0)
							{
								if ((step + sz) > rec->fields[i].data.file.count)
								{
									int pad = sz - (rec->fields[i].data.file.count - step);
									padding_value += pad;

									sz = rec->fields[i].data.file.count - step;
									exit = 1;

									ram->offset += (2 *(-sizeof(uint32_t)));

									/* write the updated size of the array */
									uint32_t new_sz = swap32((uint32_t)sz);
									memcpy(&ram->mem[ram->offset],&new_sz,sizeof(uint32_t));
									ram->offset += sizeof(uint32_t);

									/*write padding */
									uint32_t new_pd = swap32((uint32_t)padding_value);
									memcpy(&ram->mem[ram->offset],&new_pd,sizeof(uint32_t));
									ram->offset += sizeof(uint32_t);
								}
							}

							if (step < rec->fields[i].data.file.count){
								if(write_ram_record(ram,
											&rec->fields[i].data.file.recs[step],
											update,0,0) == -1){
									fprintf(stderr,"write_ram_record failed, %s:%d.\n",__FILE__, __LINE__ - 1);
									return -1;
								}
								step++;
								if(!(step < rec->fields[i].data.file.count)) exit = 1;
							}
						}
						
						if (exit)
						{

							if (padding_value > 0)
							{
								int y;
								for(y = 0; y < padding_value;y++){
									struct Record_f dummy;
									memset(&dummy,0,sizeof(struct Record_f));
									memcpy(&dummy,&ram->mem[ram->offset] ,sizeof(struct Record_f));

									size_t dummy_rec_size = get_disk_size_record(&dummy);
									ram->offset += dummy_rec_size;

									free_record(&dummy,dummy.fields_num);

									ram->offset += sizeof(off_t);
								}
							}

							/*write the empty update offset*/
							uint64_t empty_offset = 0;
							memcpy(&ram->mem[ram->offset],&empty_offset,sizeof(uint64_t));
							ram->offset += sizeof(uint64_t);
							break;
						}
					}

					if (padding_value > 0)
					{
						int y;
						for(y = 0; y < padding_value;y++){
							struct Record_f dummy;
							memset(&dummy,0,sizeof(struct Record_f));
							memcpy(&dummy,&ram->mem[ram->offset] ,sizeof(struct Record_f));

							size_t dummy_rec_size = get_disk_size_record(&dummy);
							ram->offset += dummy_rec_size;

							free_record(&dummy,dummy.fields_num);

							ram->offset += sizeof(off_t);
						}

					}

					off_t go_back_to = ram->offset;
					uint64_t update_off_ne = 0;
					memcpy(&update_off_ne,&ram->mem[ram->offset],sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);

					if (go_back_to_first_rec == 0)
						go_back_to_first_rec = go_back_to + sizeof(update_off_ne);

					update_pos = (off_t)swap64(update_off_ne);
					if (update_pos == 0)
					{
						/*go to EOF*/
						update_pos = ram->size;
						ram->offset = ram->size;
						size_t each_rec_size = 0;
						uint32_t x;
						for(x = step; x <  rec->fields[i].data.file.count; x++){
							each_rec_size += get_disk_size_record(&rec->fields[i].data.file.recs[x]);
						}

						uint64_t remaining_write_size = ((2 * sizeof(uint32_t)) + 
										each_rec_size 		+ 
										(2*sizeof(uint64_t)));

						if(ram->size == ram->capacity || ((ram->size + remaining_write_size) > ram->capacity)){
							/*you have to expand the capacity*/
							uint8_t *n_mem = (uint8_t*)reask_mem(ram->mem, 
									ram->capacity * sizeof(uint8_t),
									ram->capacity + (remaining_write_size + 1) * sizeof(uint8_t));
							if(!n_mem){
								fprintf(stderr,"(%s): reask_mem() failed %s:%d.\n",prog,__FILE__,__LINE__-2);
								return -1;
							}

							ram->mem = n_mem;
							ram->capacity += (remaining_write_size + 1);
							memset(&ram->mem[ram->offset],0,remaining_write_size +1);
						}




						/* write the size of the array */
						int size_left = rec->fields[i].data.file.count - step;
						uint32_t size_left_ne = swap32((uint32_t)size_left);
						memcpy(&ram->mem[ram->offset],&size_left_ne,sizeof(uint32_t));
						move_ram_file_ptr(ram,sizeof(uint32_t));

						uint32_t padding_ne = 0;
						memcpy(&ram->mem[ram->offset],&padding_ne,sizeof(uint32_t));
						move_ram_file_ptr(ram,sizeof(uint32_t));

						int j;
						for (j = 0; j < size_left; j++){
							if (step < rec->fields[i].data.file.count){
								/* i think here i need to change
								 * the update field*/
								if(write_ram_record(ram,
											&rec->fields[i].data.file.recs[step],
											0,0,0) == -1){
									fprintf(stderr,"write_ram_record failed, %s:%d.\n",__FILE__, __LINE__ - 1);
									return -1;
								}
								step++;
							}
						}

						/*write the empty update offset*/
						uint64_t empty_offset = 0;
						memcpy(&ram->mem[ram->offset],&empty_offset,sizeof(uint64_t));
						move_ram_file_ptr(ram,sizeof(uint64_t));

						ram->offset = go_back_to;

						update_off_ne = swap64((uint64_t)update_pos);
						memcpy(&ram->mem[ram->offset],&update_off_ne,sizeof(uint64_t));
						ram->offset += sizeof(uint64_t);


						break;
					}

					/*if upfaste_pos is not zero we move to that position*/
					ram->offset = update_pos;

				} while (update_pos > 0);

				if (rec->fields[i].data.file.count < sz)
				{

					ram->offset -= sizeof(uint32_t);

					/*write the size of the array */
					uint32_t size_ne = swap32(rec->fields[i].data.file.count);
					memcpy(&ram->mem[ram->offset],&size_ne,sizeof(uint32_t));


					/*read and check the padding, */
					uint32_t pad_ne = 0;
					memcpy(&pad_ne,&ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					int pd_he = (int)swap32(pad_ne);
					pd_he += (sz - rec->fields[i].data.file.count);

					ram->offset -= sizeof(uint32_t);

					/* write the padding to apply after the  array */
					pad_ne = swap32((uint32_t)pd_he);
					memcpy(&ram->mem[ram->offset],&pad_ne,sizeof(uint32_t));

					uint32_t j;
					for (j = step; j < rec->fields[i].data.file.count; j++){
						/*write ram record*/
						if(write_ram_record(ram,
									&rec->fields[i].data.file.recs[step],
									update,0,0) == -1){
							fprintf(stderr,"write_ram_record failed, %s:%d.\n",__FILE__, __LINE__ - 1);
							return -1;
						}
					}

					/*
					 * move the file pointer after the array
					 * as much as the pad
					 * */
					int y;
					for(y = 0; y < pd_he;y++){
						struct Record_f dummy;
						memset(&dummy,0,sizeof(struct Record_f));
						memcpy(&dummy,&ram->mem[ram->offset] ,sizeof(struct Record_f));

						size_t dummy_rec_size = get_disk_size_record(&dummy);
						ram->offset += dummy_rec_size;

						free_record(&dummy,dummy.fields_num);

						ram->offset += sizeof(off_t);
					}

					uint64_t update_arr_ne = 0;
					memcpy(&ram->mem[ram->offset],&update_arr_ne, sizeof(update_arr_ne));
				}
				else if (rec->fields[i].data.file.count == sz)
				{
					/*
					 * the sizes are the same
					 * we simply write the array.
					 * */
					if (step > 0){
						ram->offset -= sizeof(sz);

						int size_left = rec->fields[i].data.file.count - step;
						uint32_t sz_ne = swap32((uint32_t)size_left);
						memcpy(&ram->mem[ram->offset],&sz_ne,sizeof(sz_ne));
						ram->offset += sizeof(sz_ne);
					}

					/*read and check the padding, */
					uint32_t pad_ne = 0;
					memcpy(&pad_ne,&ram->mem[ram->offset],sizeof(uint32_t));
					ram->offset += sizeof(uint32_t);

					int pd_he = (int)swap32(pad_ne);
					uint32_t j;
					for (j = 0; j < rec->fields[i].data.file.count; j++)
					{
						if (step < rec->fields[i].data.file.count){

							/*write ram memory*/
							if(write_ram_record(ram,
										&rec->fields[i].data.file.recs[step],
										update,0,0) == -1){
								fprintf(stderr,"write_ram_record failed, %s:%d.\n",__FILE__, __LINE__ - 1);
								return -1;
							}
							step++;
						}
					}

					/*
					 * move the file pointer
					 * as much as the padding value
					 * if it si bigger than 0
					 * */
					if (pd_he > 0)
					{
						int y;
						for(y = 0; y < padding_value;y++){
							struct Record_f dummy;
							memset(&dummy,0,sizeof(struct Record_f));
							memcpy(&dummy,&ram->mem[ram->offset] ,sizeof(struct Record_f));

							size_t dummy_rec_size = get_disk_size_record(&dummy);
							ram->offset += dummy_rec_size;

							free_record(&dummy,dummy.fields_num);

							ram->offset += sizeof(off_t);
						}

					}

					uint64_t update_arr_ne = 0;
					memcpy(&ram->mem[ram->offset],&update_arr_ne,sizeof(uint64_t));
					ram->offset += sizeof(uint64_t);
				}

				if (go_back_to_first_rec > 0) ram->offset = go_back_to_first_rec;
			}

			break;
		}
		default:
		break;
		}

	} 

	uint64_t upd_rec = 0;
	if(offset) upd_rec = swap64(offset); 

	memcpy(&ram->mem[ram->offset],&upd_rec,sizeof(uint64_t));
	move_ram_file_ptr(ram,sizeof(uint64_t));

	return 0;
}


static int is_array_last_block(int fd, struct Ram_file *ram, int element_nr, size_t bytes_each_element, int type)
{
	if(fd != -1){
		off_t go_back_to = 0;
		if ((go_back_to = get_file_offset(fd)) == -1)
		{
			__er_file_pointer(F, L - 1);
			return -1;
		}

		if(type == TYPE_ARRAY_STRING){
			int i;
			for(i = 0; i < element_nr; i++){
				if(get_string_size(fd,NULL) == (size_t)-1){
					__er_file_pointer(F, L - 1);
					return -1;
				} 
			}
		}else {
			if(move_in_file_bytes(fd, element_nr * bytes_each_element) == -1)
			{
				__er_file_pointer(F, L - 1);
				return -1;
			}
		}	
		uint64_t update_off_ne = 0;
		if (read(fd, &update_off_ne, sizeof(update_off_ne)) == -1)
		{
			perror("failed read update off_t int array.\n");
			return 0;
		}

		if (find_record_position(fd, go_back_to) == -1)
		{
			__er_file_pointer(F, L - 1);
			return -1;
		}

		off_t update_pos = (off_t)swap64(update_off_ne);

		return update_pos == 0;
	}

	if(ram){
		if(ram->mem){
			off_t go_back_to = ram->offset;

			if(type == TYPE_ARRAY_STRING){
				int i;
				for(i = 0; i < element_nr; i++){
					if(get_string_size(-1,ram) == (size_t)-1){
						__er_file_pointer(F, L - 1);
						return -1;
					} 
				}
			}else {
				ram->offset += (element_nr * bytes_each_element);
			}	

			uint64_t update_off_ne = 0;
			memcpy(&update_off_ne,&ram->mem[ram->offset],sizeof(uint64_t));
			ram->offset += sizeof(uint64_t);

			ram->offset = go_back_to;

			off_t update_pos = (off_t)swap64(update_off_ne);

			return update_pos == 0;


		}
	}

	return -1;
}

#if defined(__linux__)
int buffered_write(int *fd, struct Record_f *rec, int update, off_t rec_ram_file_pos, off_t offset)
#elif defined(_WIN32)
int buffered_write(HANDLE *file_handle, struct Record_f *rec, int update, off_t rec_ram_file_pos, off_t offset)
#endif
{
	struct Ram_file ram;
	memset(&ram,0,sizeof(struct Ram_file));
	if(!update){
		size_t rec_disk_size = get_disk_size_record(rec);
		if(init_ram_file(&ram,rec_disk_size) == -1){
			fprintf(stderr,"init_ram_file failed, %s:%d.\n",__FILE__, __LINE__ - 1);
			return -1;
		}
	}else{
		if(get_all_record(*fd,&ram) == -1){
			fprintf(stderr,"cannot read all records %s:%d\n",__FILE__,__LINE__-1);	
			return -1;
		}

		move_ram_file_ptr(&ram,rec_ram_file_pos);
	}

	if(write_ram_record(&ram,rec,update,0,offset) == -1){
		fprintf(stderr,"write_ram_record failed, %s:%d.\n",__FILE__, __LINE__ - 1);
		close_ram_file(&ram);
		return -1;
	}

	/*build the file name*/
	size_t l = strlen(rec->file_name) + strlen(".dat");
	char buf[l+1];
	memset(buf,0,l+1);
	if(update){
		strncpy(buf,rec->file_name,strlen(rec->file_name));
		strncat(buf,".dat",strlen(".dat")+1);		

#if define(__linux__)
		close(*fd);
		*fd = open_file(buf,1);	/* open the file back with O_TRUNC*/
		if(file_error_handler(1,*fd) != 0) 
#elif defined(_WIN32)
		CloseHandle(*file_handle);
		*file_handle = open_file(buf,1)
		if(file_error_handler(1,*file_handle) != 0) 
#endif
		{
			close_ram_file(&ram);
			return -1;
		}
	}
#if defined(__linux__)
	if(write(*fd,ram.mem,ram.size) == -1)
#elif defined(_WIN32)
	DWORD written = 0;
	if(!WriteFile(file_handle,ram.mem,ram.size,&written,NULL))
#endif
	{
		fprintf(stderr,"write to file failed, %s:%d.\n",__FILE__, __LINE__ - 1);
		close_ram_file(&ram);
		return -1;
	}

	if(update){
#if define(__linux__)
		close(*fd);
		*fd = open_file(buf,0); /*open the file in nirmal mode*/
		if(file_error_handler(1,*fd) != 0) 
#elif defined(_WIN32)
		CloseHandle(*file_handle);
		*file_handle = open_file(buf,0)
		if(file_error_handler(1,*file_handle) != 0) 
#endif
		{
			close_ram_file(&ram);
			return -1;
		}
	}
	close_ram_file(&ram);
	return 0;
}

#if defined(__linux__)
static size_t get_string_size(int fd, struct Ram_file *ram)
#elif defined(_WIN32)
static size_t get_string_size(struct Ram_file *ram)
#endif
{

#if defined(__linux__)
	if(fd != -1 && ram) {
		fprintf(stderr,"(%s): wrong usage of %s(), you can pass either fd or Ram_file, both is not allowed.\n",prog,__func__);
		return -1;
	}

	if(fd != -1){
		if(move_in_file_bytes(fd,sizeof(uint32_t)) == -1) return -1;

		uint16_t bu_ne = 0;
		if(read(fd,&bu_ne,sizeof(bu_ne)) == -1) return -1;

		size_t buffer_update = (size_t)swap16(bu_ne);

		if(move_in_file_bytes(fd,buffer_update) == -1) return -1;

		return 0;
	}
#endif

	if(ram){
		if(ram->mem){
			ram->offset += sizeof(uint32_t);			

			uint16_t bu_ne = 0;
			memcpy(&bu_ne,&ram->mem[ram->offset],sizeof(uint16_t));
			ram->offset += sizeof(uint16_t);			

			size_t buff_up = (size_t)swap16(bu_ne); 
			ram->offset += buff_up;

			return 0;
		}	
	}

	return -1;
}

#if defined(_WIN32) 
#include <windows.h>
#include "file.h"
#include "types.h"
#include "str_op.h"
#include "common.h"
#include "endian.h"
#include "debug.h"

HANDLE open_file(char *fileName, uint32_t use_trunc)
{
	DWORD creation = 0;	
	DWORD access = 0;
	HANDLE h_file;

	if(!use_trunc){
		access = GENERIC_WRITE | GENERIC_READ 
		creation = OPEN_EXISTING;

	}else{
		access = GENERIC_WRITE | GENERIC_READ 
		creation = TRUNCATE_EXISTING;
	}

	h_file = createFileA(fileName,access,0,NULL,creation,FILE_ATTRIBUTE_NORMAL,NULL);
	if(h_file == INVALID_HANDLE_VALUE)
		return GetLastError();

	return h_file;
}

HANDLE create_file(char *file_name)
{

	DWORD err = getFileAttributesA(file_name);
	if(err !=  INVALID_FILE_ATTRIBUTE){
		fprintf(stderr,"file %s already exist");
		return INVALID_HANDLE_VALUE;
	}

	HANDLE h_file = createFileA(file_name,GENERIC_READ | GENERIC_WRITE, 
								FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
								NULL,
								CREATE_NEW,
								FILE_ATTRIBUTE_NORMAL,
								NULL);
	if(h_file == INVALID_HANDLE_VALUE) 
		return GetLastError();

	return h_file;

}

void close_file(int count, ...)
{
	int fails = 0;

	va_list args;
	va_start(args,count);

	int i;
	for(i = 0; i < count; i++){
		HANDLE h = va_args(args,HANDLE);
		if(h != INVALID_HANDLE_VALUE){
			if(!CloseHandle(h))
				fails++;
		}
	}
		
	va_end(args);
	if(fails) fprintf(stderr,"error closing the file: %ld",GetLastError());
}

int delete_file(int count,...)
{
	int err = 0;
	va_args args;
	va_start(args,count);
	int i;
	for(i = 0; i < count; i++){
		char *file_name = va_args(args, char*);
		if(file_name){
			if(!DeleteFileA(file_name))
				err++;
		}
	}

	if(err){
		fprintf(stderr,"failed to delete one or more file, error code: %ld\n",GetLastError());
		return -1;
	}

	return 0;

}

static off_t seek_file_win(HANDLE file_handle,long long offset, DWORD file_position)
{
	LARGE_INTEGER li;
	li.QuadPart = offset;
	LARGE_INTEGER new_file_ptr;
	if(!SetFilePointerEx(file_handle,li,&new_file_ptr,file_position)){
		fprintf(stderr,"seek failed %s:%d\n",__FILE__,__LINE__-1);
		return -1;
	}

	return (off_t) new_file_ptr.QuadPart;
}

off_t begin_in_file(HANDLE file_handle)
{
	return seek_file_win(file_handle,0,FILE_BEGIN);

}

off_t get_file_offset(HANDLE file_handle)
{
	return seek_file_win(file_handle,0,FILE_CURRENT);
}

off_t find_record_position(HANDLE file_handle, long long offset)
{
	return seek_file_win(file_handle,offset,FILE_BEGIN);

}

off_t go_to_EOF(HANDLE file_handle)
{
	return seek_file_win(file_handle,0,FILE_END);
}

off_t move_in_file_bytes(HANDLE file_handle, off_t offset)
{
	return seek_file_win(file_handle,offset,FILE_CURRENT);
}

DWORD get_file_size(HANDLE file_handle)
{
	return GetFileSize(file_handle, NULL)
}
#endif
