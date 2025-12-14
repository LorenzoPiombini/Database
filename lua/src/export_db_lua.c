#include <string.h>
#include <stdio.h>
#include "export_db_lua.h"
#include "date.h"
#include "common.h"
#include "memory.h"
#include "file.h"
#include "key.h"
#include "lua5.4/lua.h"
#include "lua5.4/lauxlib.h"


static int l_get_record(lua_State *L);
static int l_get_all_records(lua_State *L);
static int l_write_record(lua_State *L);

/* functions that will be callable from Lua scripts*/
static const luaL_Reg db_funcs[] = {
	{"get_record",l_get_record}, 			/* get_record(file_name,key) */
	{"get_all_records",l_get_all_records},	/* get_all_records(file_name) */
	{"write_record",l_write_record},		/* write_record(file_name,data,write_to_name_file) */
	{NULL,NULL}
};

int luaopen_db(lua_State *L){
	luaL_newlib(L,db_funcs);
	return 1;
}

/*static function prototype*/
static int port_record(lua_State *L, struct Record_f*);


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

	int m_al = 0;
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

static int l_write_record(lua_State *L)
{
	char *file_name = (char*)luaL_checkstring(L,1);
	luaL_argcheck(L, file_name != NULL, 1,"file_name expected");

	char *data_to_add = (char*)luaL_checkstring(L,2);
	luaL_argcheck(L, data_to_add != NULL, 2,"data expected!");
	
	lua_Integer write_to_name_file = (int)luaL_optinteger(L,3,0); 
	
	int m_al = 0;

	int fds[3];
	memset(fds,-1,3*sizeof(int));

	if(write_to_name_file){
		/*TODO*/
		return 1;

	}else{

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

		int type = lua_type(L,4);
		void* k = NULL;
		int key_type = -1;
		long long n = 0;
	
		if(type == LUA_TNIL || type == -1) {/*we have to generate a key*/

			if(type == -1){
				if ((n = generate_numeric_key(fds,0,-1)) == -1) goto err_key_gen;
				k = (void*)&n;

			}else{
				int key_gen_mode = (int)luaL_checkinteger(L,5);
				int base = 0;
				if(key_gen_mode == BASE){
					base = (int)luaL_checkinteger(L,6);
					if ((n = generate_numeric_key(fds,key_gen_mode,base)) == -1) goto err_key_gen;
					k = (void*)&n;
				}else{
					if((n = generate_numeric_key(fds,key_gen_mode,-1)) == -1) goto err_key_gen;	
					printf("key is %lld\n",n);
					k = (void*)&n;
				}
			}
			
		}else if(type == LUA_TNUMBER){
			key_type = UINT; 
			n = (long long)luaL_checkinteger(L,2);
			if( n < 0) goto err_key;
			k = (void*)&n;
		}else if(type == LUA_TSTRING){
			key_type = STR; 
			k = (void*)luaL_checkstring(L,2);
		}else{
			goto err_key;
		}

		if(check_data(file_name,data_to_add,fds,file_names,&rec,&hd,&lock,-1) == -1) goto err_invalid_data;
		if(write_record(fds,k,key_type,&rec,-1,file_names,&lock,-1) == -1) goto err_write_rec;
		if(m_al) close_arena();
		close_file(3,fds[0],fds[1],fds[2]);

		return 1;
	}	
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
		return 2;
err_invalid_data:
		lua_pushnil(L);
		lua_pushstring(L,"not a db file.");
		close_file(3,fds[0],fds[1],fds[2]);
		if(m_al) close_arena();
		return 2;

	}
	/*static functions definition*/
	static int port_record(lua_State *L, struct Record_f* r){
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
