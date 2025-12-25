#include <string.h>

#include "export_db_lua.h"
#include "date.h"
#include "common.h"
#include "memory.h"
#include "file.h"
#include "key.h"

#define LOWER_STR(s) for(char *p = &s[0]; p && ((int)*p >= 65 || (int)*p <= 90) ;*p = (int)*p + 22,p++)
#define UPPER_STR(s) for(char *p = &s[0]; p && ((int)*p >= 97 || (int)*p <= 122) ;(int)*p -= 22,p++)


static int l_get_record(lua_State *L);
static int l_get_all_records(lua_State *L);
static int l_write_record(lua_State *L);
static int l_create_record(lua_State *L);
static int l_string_data_to_add_template(lua_State *L);
static int l_get_numeric_key(lua_State *L);
static int l_save_key_at_index(lua_State *L);

/* functions that will be callable from Lua scripts*/
static const luaL_Reg db_funcs[] = {
	{"get_record",l_get_record}, 			/* get_record(file_name,key) */
	{"get_all_records",l_get_all_records},	/* get_all_records(file_name) */
	{"write_record",l_write_record},		/* write_record(file_name,data) -- some optional args -- */
	{"create_record",l_create_record},		/* create_record(file_name,data) */
	{"string_data_to_add_template",
		l_string_data_to_add_template},		/* string_data_to_add_template(file_name) */
	{"get_numeric_key",l_get_numeric_key},	/* get_numeric_key(file_name,mode) -- some optional args --  */
	{"save_key_at_index",
		l_save_key_at_index},               /* save_key_at_index(file_name,key,index,offset)*/
	{NULL,NULL}
};

int luaopen_db(lua_State *L){
	luaL_newlib(L,db_funcs);
	return 1;
}

/* 
 * from lua:
 * .get_record(file_name,key)
 * */
static int l_get_record(lua_State *L)
{
	char *file_name = (char*)luaL_checkstring(L,1);
	luaL_argcheck(L, file_name != NULL, 1,"file_name expected");
	int key_type = 0;
	int n = 0;
	void *k = NULL;
	int type = lua_type(L,2);
	if(type == LUA_TNUMBER){
		key_type = UINT; 
		n = (int)luaL_checkinteger(L,2);
		if( n < 0) goto err_key;
		k = (void*)&n;
	}else if(type == LUA_TSTRING){
		key_type = STR; 
		k = (void*)luaL_checkstring(L,2);
	}else{
		goto err_key;
	}


	struct Record_f rec;
	memset(&rec,0,sizeof(struct Record_f));

	struct Schema sch;
	memset(&sch,0,sizeof(struct Schema));
	struct Header_d hd = {0,0,&sch};
		
	int fds[3];
	memset(fds,-1,3*sizeof(int));
	char file_names[3][MAX_FILE_PATH_LENGTH] = {0};

	int m_al= 0;
	if(is_memory_allocated() == NULL){
		if(create_arena(EIGTH_Kib) == -1) goto err_memory;
		m_al = 1;
	}

	if(open_files(file_name,fds,file_names,-1) == -1) goto err_open_file;
	if(is_db_file(&hd,fds) == -1) goto err_not_db_file;
	int result = -1;
	if((result = get_record(-1,file_name,&rec,k,key_type,hd,fds)) == -1) goto err_get_record_failed;
	if(result == KEY_NOT_FOUND) goto err_rec_not_found;
	if(port_record(L,&rec)) goto err_exp_data_to_lua;
	if(m_al && close_arena() == -1) goto err_close_memory;
	close_file(3,fds[0],fds[1],fds[2]);

	return 1;

err_key:
		lua_pushnil(L);
		lua_pushstring(L,"only string or unsigned integer are allowed as key.");
		return 2;

err_open_file:
		lua_pushnil(L);
		lua_pushstring(L,"could not open the file.");
		if(m_al) close_arena();
		return 2;

err_get_record_failed:
		lua_pushnil(L);
		lua_pushstring(L,"get_record failed.");
		close_file(3,fds[0],fds[1],fds[2]);
		if(m_al) close_arena();
		return 2;

err_not_db_file:
		lua_pushnil(L);
		lua_pushstring(L,"not a db file.");
		close_file(3,fds[0],fds[1],fds[2]);
		if(m_al) close_arena();
		return 2;
err_rec_not_found:
		lua_pushnil(L);
		lua_pushstring(L,"record not found.");
		close_file(3,fds[0],fds[1],fds[2]);
		if(m_al) close_arena();
		return 2;
err_memory:
		lua_pushnil(L);
		lua_pushstring(L,"cannot allocate memory.");
		return 2;
err_close_memory:
		lua_pushnil(L);
		lua_pushstring(L,"cannot allocate memory.");
		close_file(3,fds[0],fds[1],fds[2]);
		return 2;

err_exp_data_to_lua:
		lua_pushnil(L);
		lua_pushstring(L,"cannot export record data.");
		close_file(3,fds[0],fds[1],fds[2]);
		return 2;
}


static int l_get_all_records(lua_State *L)
{
	char *file_name = (char*)luaL_checkstring(L,1);
	luaL_argcheck(L, file_name != NULL, 1,"file_name expected");


	struct Record_f **recs = NULL;
	struct Schema sch;
	memset(&sch,0,sizeof(struct Schema));
	struct Header_d hd = {0,0,&sch};
		
	int fds[4];/*last file descriptor used for size record array*/
	memset(fds,-1,4*sizeof(int));
	char file_names[3][MAX_FILE_PATH_LENGTH] = {0};

	int m_al = 0;
	if(is_memory_allocated() == NULL){
		if(create_arena(EIGTH_Kib) == -1) goto err_memory;
		m_al = 1;
	}

	if(open_files(file_name,fds,file_names,-1) == -1) goto err_open_file;
	if(is_db_file(&hd,fds) == -1) goto err_not_db_file;
	if(get_all_records(file_name,fds,&recs,hd) == -1) goto err_get_record_failed;
	close_file(3,fds[0],fds[1],fds[2]);

	int i = 0;
	lua_newtable(L);
	for(i = 0;i < fds[3]; i++){
		if(!recs[i])break;
		port_record(L,recs[i]);
		lua_rawseti(L, -2, i + 1);
	}
	
	if(m_al && close_arena() == -1) goto err_memory;

	return 1;

err_open_file:
		lua_pushnil(L);
		lua_pushstring(L,"could not open the file.");
		if(m_al) close_arena();
		return 2;

err_get_record_failed:
		lua_pushnil(L);
		lua_pushstring(L,"get_record failed.");
		close_file(3,fds[0],fds[1],fds[2]);
		if(m_al) close_arena();
		return 2;

err_not_db_file:
		lua_pushnil(L);
		lua_pushstring(L,"not a db file.");
		close_file(3,fds[0],fds[1],fds[2]);
		if(m_al) close_arena();
		return 2;
err_memory:
		lua_pushnil(L);
		lua_pushstring(L,"cannot allocate memory.");
		return 2;
}
/*
 * calling from lua write_record(file_name,data_to_add)
 * --@param file_name
 * --@param data_to_add 
 *
 * both param are mandatory and they must be present.
 * if no other @params are passed, the funtion will compute a key for the record
 * that you want to write.
 *
 *
 * --@param key #OPTIONAL# but must be the 3th param, if present.
 *  this will be the key, or a key mode.
 *
 * exmaple (from lua):
 * 		write_record(file_name,data_to_add) 
 * 		--@@ will write a record in the file (file_name) with automatic numeric key.
 *
 * 		write_record(file_name,data_to_add,"hey")
 * 		--@@ will write a record in the file (file_name) with "hey" as a key.
 *
 * 		write_record(file_name,data_to_add,23)
 * 		--@@ will write a record in the file (file_name) with 23 as a key.
 * 		
 * 		write_record(file_name,data_to_add,"base",100)
 * 		--@@ will write a record in the file (file_name) with 100 + nr of records in the file, as a key.
 * */
static int l_write_record(lua_State *L)
{
	char *file_name = (char*)luaL_checkstring(L,1);
	luaL_argcheck(L, file_name != NULL, 1,"file_name expected");

	char *data_to_add = (char*)luaL_checkstring(L,2);
	luaL_argcheck(L, data_to_add != NULL, 2,"data expected!");
	
	int m_al = 0;

	int fds[3];
	memset(fds,-1,3*sizeof(int));

	struct Record_f rec = {0};
	struct Schema sch;
	memset(&sch,0,sizeof(struct Schema));
	struct Header_d hd = {0,0,&sch};

	char file_names[3][MAX_FILE_PATH_LENGTH] = {0};


	if(is_memory_allocated() == NULL){
		if(create_arena(EIGTH_Kib) == -1) goto err_memory;
		m_al = 1;
	}

	int lock = 1;
	if(open_files(file_name,fds,file_names,-1) == -1) goto err_open_file;
	if(is_db_file(&hd,fds) == -1) goto err_not_db_file;

	int type = lua_type(L,3);
	void* k = NULL;
	int key_type = -1;
	long long n = 0;

	if(type == LUA_TNIL || type == -1) {	/*we have to generate a key*/
		if(type == -1){
			key_type = UINT;
			if ((n = generate_numeric_key(fds,REG,-1)) == -1) goto err_key_gen;
			if((unsigned short)n < USHRT_MAX){
				k = (void*)(uint16_t*)&n;
			} else{
				k = (void*)(uint32_t*)&n;
			}
			lua_pushinteger(L,n);
		}
	}else if(type == LUA_TNUMBER){
		key_type = UINT; 
		n = (long long)luaL_checkinteger(L,3);
		if( n < 0) goto err_key;
		k = (void*)&n;
		lua_pushinteger(L,n);
	}else if(type == LUA_TSTRING){
		char *param = (char*)luaL_checkstring(L,3);
		LOWER_STR(param);
		if(strlen(param) == strlen("base") &&
			strncmp("base",param,strlen("base")) == 0){

			int base = luaL_checkinteger(L,4); 
			if ((n = generate_numeric_key(fds,BASE,base)) == -1) goto err_key_gen;
			k = (void*)&n;
			lua_pushinteger(L,n);

		}else{
			key_type = STR; 
			k = (void*)param;
			lua_pushstring(L,param);
		}
	}else{
		goto err_key;
	}

	if(check_data(file_name,data_to_add,fds,file_names,&rec,&hd,&lock,-1) == -1) goto err_invalid_data;
	if(write_record(fds,(void*)k,key_type,&rec,0,file_names,&lock,-1) == -1) goto err_write_rec;
	port_record(L,&rec);

	if(m_al) close_arena();
	close_file(3,fds[0],fds[1],fds[2]);

	return 2;

err_open_file:
	lua_pushnil(L);
	lua_pushstring(L,"could not open the file.");
	if(m_al) close_arena();
	return 2;
err_memory:
	lua_pushnil(L);
	lua_pushstring(L,"cannot allocate memory.");
	return 2;
err_not_db_file:
	lua_pushnil(L);
	lua_pushstring(L,"not a db file.");
	close_file(3,fds[0],fds[1],fds[2]);
	if(m_al) close_arena();
	return 2;
err_key:
	lua_pushnil(L);
	lua_pushstring(L,"error detecting key.");
	close_file(3,fds[0],fds[1],fds[2]);
	if(m_al) close_arena();
	return 2;
err_key_gen:
	lua_pushnil(L);
	lua_pushstring(L,"key generation failed");
	close_file(3,fds[0],fds[1],fds[2]);
	if(m_al) close_arena();
	return 2;
err_write_rec:
	lua_pushnil(L);
	lua_pushstring(L,"write record failed");
	close_file(3,fds[0],fds[1],fds[2]);
	if(m_al) close_arena();
	return 3;
err_invalid_data:
	lua_pushnil(L);
	lua_pushstring(L,"data not valid.");
	close_file(3,fds[0],fds[1],fds[2]);
	if(m_al) close_arena();
	return 3;

}

/*
 * this function return a record (table) to Lua,
 * without writing to the file.
 * */
static int l_create_record(lua_State *L)
{
	char *file_name = (char*)luaL_checkstring(L,1);
	luaL_argcheck(L, file_name != NULL, 1,"file_name expected");

	char *data_to_add = (char*)luaL_checkstring(L,2);
	luaL_argcheck(L, data_to_add != NULL, 2,"data expected!");
	
	int m_al = 0;

	int fds[3];
	memset(fds,-1,3*sizeof(int));

	struct Record_f rec = {0};
	struct Schema sch;
	memset(&sch,0,sizeof(struct Schema));
	struct Header_d hd = {0,0,&sch};

	char file_names[3][MAX_FILE_PATH_LENGTH] = {0};

	if(is_memory_allocated() == NULL){
		if(create_arena(EIGTH_Kib) == -1) goto err_memory;
		m_al = 1;
	}

	int lock = 1;
	if(open_files(file_name,fds,file_names,-1) == -1) goto err_open_file;
	if(is_db_file(&hd,fds) == -1) goto err_not_db_file;
	if(check_data(file_name,data_to_add,fds,file_names,&rec,&hd,&lock,-1) == -1) goto err_invalid_data;

	port_record(L,&rec);
	if(m_al) close_arena();
	close_file(3,fds[0],fds[1],fds[2]);

	return 1;

err_memory:
	lua_pushnil(L);
	lua_pushstring(L,"cannot allocate memory.");
	return 2;
err_open_file:
	lua_pushnil(L);
	lua_pushstring(L,"could not open the file.");
	if(m_al) close_arena();
	return 2;
err_not_db_file:
	lua_pushnil(L);
	lua_pushstring(L,"not a db file.");
	close_file(3,fds[0],fds[1],fds[2]);
	if(m_al) close_arena();
	return 2;
err_invalid_data:
	lua_pushnil(L);
	lua_pushstring(L,"data not valid.");
	close_file(3,fds[0],fds[1],fds[2]);
	if(m_al) close_arena();
	return 2;
}

static int l_string_data_to_add_template(lua_State *L)
{
	char *file_name = (char*)luaL_checkstring(L,1);
	luaL_argcheck(L, file_name != NULL, 1,"file_name expected");

	int m_al = 0;

	int fds[3];
	memset(fds,-1,3*sizeof(int));

	struct Schema sch;
	memset(&sch,0,sizeof(struct Schema));
	struct Header_d hd = {0,0,&sch};

	char file_names[3][MAX_FILE_PATH_LENGTH] = {0};

	if(is_memory_allocated() == NULL){
		if(create_arena(EIGTH_Kib) == -1) goto err_memory;
		m_al = 1;
	}

	if(open_files(file_name,fds,file_names,ONLY_SCHEMA) == -1) goto err_open_file;
	if(is_db_file(&hd,fds) == -1) goto err_not_db_file;
	

	int i, sum = 0; 
	for(i = 0; i < hd.sch_d->fields_num; i++){
		sum += strlen(hd.sch_d->fields_name[i]);
		switch(hd.sch_d->types[i]){
			case TYPE_FLOAT:
			case TYPE_DOUBLE:
				sum += 4;
				break;
			case TYPE_FILE:
				sum += 6;
				break;
			case TYPE_INT:
			case TYPE_BYTE:
			case TYPE_LONG:
			case TYPE_STRING:
			case TYPE_DATE:
			default:
				sum += 2;
				break;
		}
	}
	
	int colon_nr = (i * 2 ) - 1;
	char *st = (char*)ask_mem(colon_nr+sum+1);
	if(!st) goto err_ask_mem;

	memset(st,0,colon_nr+sum+1);


	int bwritten = 0;
	for(i = 0; i < hd.sch_d->fields_num; i++){
		size_t sz = strlen(hd.sch_d->fields_name[i]);
		memcpy(&st[bwritten],hd.sch_d->fields_name[i],sz);
		bwritten += sz;
		memcpy(&st[bwritten],":",1);
		bwritten += 1;
		switch(hd.sch_d->types[i]){
		case TYPE_INT:
		case TYPE_LONG:
		case TYPE_BYTE:
			memcpy(&st[bwritten],"%d",2);
			bwritten += 2;
			break;
		case TYPE_DOUBLE:
		case TYPE_FLOAT:
			memcpy(&st[bwritten],"%.2d",4);
			bwritten += 4;
			break;
		case TYPE_FILE:
			memcpy(&st[bwritten],"[w|%s]",6);
			bwritten += 6;
			break;
		case TYPE_STRING:
		default:
			memcpy(&st[bwritten],"%s",2);
			bwritten += 2;
			break;
		}
		if((hd.sch_d->fields_num - i) > 1) {
			memcpy(&st[bwritten],":",1);
			bwritten += 1;
		}
	}

	lua_pushstring(L,st);
	close_file(1,fds[2]);
	if(m_al)
		close_arena();
	else
		cancel_memory(NULL,st,colon_nr+colon_nr+1);

	return 1;

err_memory:
	lua_pushnil(L);
	lua_pushstring(L,"cannot allocate memory.");
	return 2;
err_open_file:
	lua_pushnil(L);
	lua_pushstring(L,"could not open the file.");
	if(m_al) close_arena();
	return 2;
err_not_db_file:
	lua_pushnil(L);
	lua_pushstring(L,"not a db file.");
	close_file(1,fds[2]);
	if(m_al) close_arena();
	return 2;
err_ask_mem:
	lua_pushnil(L);
	lua_pushstring(L,"ask_mem() failed");
	close_file(1,fds[2]);
	if(m_al) close_arena();
	return 2;
}



static int l_get_numeric_key(lua_State *L)
{		
	char *file_name = (char*)luaL_checkstring(L,1);
	luaL_argcheck(L, file_name != NULL, 1,"file_name expected");

	int mode = (int)luaL_checkinteger(L,2);
	luaL_argcheck(L, mode >= 0, 2,"mode must be bigger than 0");
	long long n = 0;

	int m_al = 0;
	if(is_memory_allocated() == NULL){
		if(create_arena(EIGTH_Kib) == -1) goto err_memory;
		m_al = 1;
	}

	int fds[3];
	memset(fds,-1,3*sizeof(int));

	struct Schema sch;
	memset(&sch,0,sizeof(struct Schema));
	struct Header_d hd = {0,0,&sch};

	char file_names[3][MAX_FILE_PATH_LENGTH] = {0};
	if(open_files(file_name,fds,file_names,-1) == -1) goto err_open_file;
	if(is_db_file(&hd,fds) == -1) goto err_not_db_file;

	switch(mode){
	case REG:
	{
		if((n = generate_numeric_key(fds,REG,-1)) == -1) goto err_key_gen;
		lua_pushinteger(L,n);
		break;
	}
	case BASE:
	{
		int base = (int)luaL_checkinteger(L,3);
		if((n = generate_numeric_key(fds,BASE,base)) == -1) goto err_key_gen;
		lua_pushinteger(L,n);
		break;
	}
	case INCREM:
	{
		if((n = generate_numeric_key(fds,INCREM,-1)) == -1) goto err_key_gen;
		lua_pushinteger(L,n);
		break;
	}
	default:
		/*error*/
		lua_pushnil(L);
		lua_pushstring(L,"key mode unknown");
		return 2;
	}

	close_file(1,fds[0]);
	if(m_al) close_arena();
	return 1;

err_memory:
	lua_pushnil(L);
	lua_pushstring(L,"cannot allocate memory.");
	return 2;
err_open_file:
	lua_pushnil(L);
	lua_pushstring(L,"could not open the file.");
	if(m_al) close_arena();
	return 2;
err_not_db_file:
	lua_pushnil(L);
	lua_pushstring(L,"not a db file.");
	close_file(1,fds[0]);
	if(m_al) close_arena();
	return 2;
err_key_gen:
	lua_pushnil(L);
	lua_pushstring(L,"error generating  key.");
	close_file(1,fds[0]);
	if(m_al) close_arena();
	return 2;
}


static int l_save_key_at_index(lua_State *L)
{
	char *file_name = (char*)luaL_checkstring(L,1);	
	luaL_argcheck(L, file_name != NULL, 1,"file_name expected");

	void *key = NULL;
	int key_type = 0;
	unsigned int n = 0; /*to store key as a  number before casting*/
	int type = lua_type(L,2);
	switch(type){
	case -1:
	case LUA_TNIL:
		lua_pushnil(L);
		lua_pushstring(L,"key must be present.");
		return 1;
	case LUA_TSTRING:
		key_type = STR;	
		key = (void*)luaL_checkstring(L,2);
		break;
	case LUA_TNUMBER:
		if(!lua_isinteger(L,2)){
			lua_pushnil(L);
			lua_pushstring(L,"key must be present.");
			return 1;
		}
		key_type = UINT;

		n = (unsigned int)luaL_checkinteger(L,2);
		key = (void*)&n;
		break;
	default:
		lua_pushnil(L);
		lua_pushstring(L,"key must be present.");
		return 1;
	}

	int index = (int)luaL_checkinteger(L,3);
	luaL_argcheck(L, index >= 0, 3,"index cannot be negative");

	long long record_offset = (int)luaL_checkinteger(L,3);
	luaL_argcheck(L, record_offset >= 0, 4,"offset cannot be negative");

	int fds[3];
	memset(fds,-1,sizeof(int)*3);
	char file_names[3][MAX_FILE_PATH_LENGTH] = {0};

	int m_al = 0;
	if(is_memory_allocated() == NULL){
		if(create_arena(EIGTH_Kib) == -1) goto err_memory;
		m_al = 1;
	}

	if(open_files(file_name,fds,file_names,ONLY_INDEX) == -1) goto err_open_file;


	HashTable *ht = NULL;
	int tbl_ix = 0;
	/* load all indexes in memory */
	if (!read_all_index_file(fds[0], &ht, &tbl_ix)) goto err_load_index;

	if(set_tbl(&ht[index],key,record_offset,key_type) == -1) goto err_set_index;

	if(write_index(fds,tbl_ix,ht,file_names[0]) == -1) goto err_write_index;


	if(m_al)
		close_arena();
	else
		cancel_memory(NULL,ht,sizeof(HashTable));
	
	/*if indexing succseed return the index number*/
	lua_pushinteger(L,index);
	return 1;

err_memory:
	lua_pushnil(L);
	lua_pushstring(L,"cannot allocate memory.");
	return 2;
err_open_file:
	lua_pushnil(L);
	lua_pushstring(L,"could not open the file.");
	if(m_al) close_arena();
	return 2;
err_load_index:
	lua_pushnil(L);
	lua_pushstring(L,"could not load index file.");
	if(m_al) close_arena();
	return 2;
err_set_index:
	lua_pushnil(L);
	lua_pushstring(L,"could not set index.");
	if(m_al) close_arena();
	return 2;
err_write_index:
	lua_pushnil(L);
	lua_pushstring(L,"could not write index file.");
	if(m_al) close_arena();
	return 2;
}

/* external functions*/
int port_record(lua_State *L, struct Record_f* r){
	lua_newtable(L);
	lua_pushstring(L,r->file_name);
	lua_setfield(L,-2,"file_name");

	lua_pushinteger(L,r->offset);
	lua_setfield(L,-2,"file_offset");
	lua_pushinteger(L,r->fields_num);
	lua_setfield(L,-2,"fields_number");

	lua_newtable(L);
	int i;
	for(i = 0; i < r->fields_num; i++){
		if(r->field_set[i] == 0) continue;

		switch(r->fields[i].type){
			case TYPE_INT:
				lua_pushinteger(L,r->fields[i].data.i);
				lua_setfield(L,-2,r->fields[i].field_name);
				break;
			case TYPE_LONG:
				lua_pushinteger(L,r->fields[i].data.l);
				lua_setfield(L,-2,r->fields[i].field_name);
				break;
			case TYPE_BYTE:
				lua_pushinteger(L,r->fields[i].data.b);
				lua_setfield(L,-2,r->fields[i].field_name);
				break;
			case TYPE_STRING:
				lua_pushstring(L,r->fields[i].data.s);
				lua_setfield(L,-2,r->fields[i].field_name);
				break;
			case TYPE_DATE:
			{
					char date[11];
					memset(date,0,11);
					if(convert_number_to_date(date,r->fields[i].data.date) == -1) return -1;
					lua_pushstring(L,date);
					lua_setfield(L,-2,r->fields[i].field_name);
					break;
			}
			case TYPE_FLOAT:
				lua_pushnumber(L,r->fields[i].data.f);
				lua_setfield(L,-2,r->fields[i].field_name);
				break;
			case TYPE_DOUBLE:
				lua_pushnumber(L,r->fields[i].data.f);
				lua_setfield(L,-2,r->fields[i].field_name);
				break;
			case TYPE_FILE:
			{
					lua_newtable(L);
					struct Record_f *t;
					int j;
					for(j = 1, t = r->fields[i].data.file.recs; t != NULL; t = t->next){
						if(port_record(L,t) == -1) return -1;
						lua_rawseti(L, -2, j);
						j++;
					}

					lua_setfield(L,-2,r->fields[i].field_name);
					break;
			}
			default:
				/*TODO*/
				break;
		}
	}
	lua_setfield(L,-2,"fields");

	lua_pushinteger(L,r->count);
	lua_setfield(L,-2,"count");

	if(r->next) {
		lua_newtable(L);
		struct Record_f *t;
		for(i = 1,t = r->next; t != NULL; t = t->next){
			port_record(L,t);
			lua_rawseti(L, -2, i);
			i++;
		}
		lua_setfield(L,-2,"next");
		return 0;
	}
	return 0;
}

/*assume the record is on top of the stack*/
int port_table_to_record(lua_State *L, struct Record_f *rec)
{
	if(lua_getfield(L,-1,"offset") != LUA_TNUMBER) return -1;
	int is_num;
	rec->offset = (file_offset) lua_tonumberx(L,-1,&is_num);
	if(!is_num){
		/*TODO: error*/
		return -1;
	}
	lua_pop(L,1);

	if(lua_getfield(L,-1,"fields") != LUA_TTABLE){
		/*TODO: error*/
		return -1;
	}
		
	int i;
	for(i = 0; i < rec->fields_num; i++){
		if(!rec->field_set[i]) continue;
	
		switch(rec->fields[i].type){
		case TYPE_INT:
		{
			is_num = 0;
			if(lua_getfield(L,-1,rec->fields[i].field_name) != LUA_TNUMBER) return -1;
			rec->fields[i].data.i = (int) lua_tonumberx(L,-1,&is_num); 
			if(!is_num){
				lua_settop(L,0);
				return -1;
			}
			lua_pop(L,1);
			break;
		}
		case TYPE_LONG:
		{
			is_num = 0;
			if(lua_getfield(L,-1,rec->fields[i].field_name) != LUA_TNUMBER) return -1;
			rec->fields[i].data.l = (long) lua_tonumberx(L,-1,&is_num); 
			if(!is_num){
				lua_settop(L,0);
				return -1;
			}
			lua_pop(L,1);
			break;
		}
		case TYPE_BYTE:
		{
			is_num = 0;
			if(lua_getfield(L,-1,rec->fields[i].field_name) != LUA_TNUMBER) return -1;
			rec->fields[i].data.b = (unsigned char) lua_tonumberx(L,-1,&is_num); 
			if(!is_num){
				lua_settop(L,0);
				return -1;
			}
			lua_pop(L,1);
			break;
		}
		case TYPE_FLOAT:
		{
			is_num = 0;
			if(lua_getfield(L,-1,rec->fields[i].field_name) != LUA_TNUMBER) return -1;
			rec->fields[i].data.f = (float) lua_tonumberx(L,-1,&is_num); 
			if(!is_num){
				lua_settop(L,0);
				return -1;
			}
			lua_pop(L,1);
			break;
		}
		case TYPE_DOUBLE:
		{
			is_num = 0;
			if(lua_getfield(L,-1,rec->fields[i].field_name) != LUA_TNUMBER) return -1;
			rec->fields[i].data.b = (double) lua_tonumberx(L,-1,&is_num); 
			if(!is_num){
				lua_settop(L,0);
				return -1;
			}
			lua_pop(L,1);
			break;
		}
		case TYPE_STRING:
		{
			
			if(lua_getfield(L,-1,rec->fields[i].field_name) != LUA_TSTRING) return -1;
			char *s = (char*)lua_tostring(L,-1);
			if(!s){
				lua_settop(L,0);
				return -1;
			}
			size_t sz = strlen(s);
			rec->fields[i].data.s = (char *)ask_mem(sz+1);
			memset(rec->fields[i].data.s,0,sz+1);
			memcpy(rec->fields[i].data.s,s,sz);
			lua_pop(L,1);
		}
		case TYPE_DATE:
		{
			if(lua_getfield(L,-1,rec->fields[i].field_name) != LUA_TSTRING) return -1;
			char *s = (char*)lua_tostring(L,-1);
			if(!s){
				lua_settop(L,0);
				return -1;
			}
			if((rec->fields[i].data.date = convert_date_to_number(s)) == 0){
				lua_settop(L,0);
				return -1;
			}
			lua_pop(L,1);
		}
		case TYPE_FILE:
		{
			if(port_table_to_record(L,rec->fields[i].data.file.recs) == -1){
				lua_settop(L,0);
				return -1;
			}
		}
		/*TYPE ARRAYS*/
		default:
		}
	}

	return 0;
}
