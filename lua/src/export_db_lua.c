#include <string.h>
#include <stdio.h>
#include "export_db_lua.h"
#include "date.h"
#include "common.h"
#include "memory.h"
#include "file.h"
#include "lua5.4/lua.h"
#include "lua5.4/lauxlib.h"


static int l_get_record(lua_State *L);
static int l_get_all_records(lua_State *L);

static const luaL_Reg db_funcs[] = {
	{"get_record",l_get_record},
	{"get_all_records",l_get_all_records},
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
	char file_names[3][1024] = {0};

	if(is_memory_allocated() == NULL)
		if(create_arena(EIGTH_Kib) == -1) goto err_memory;

	if(open_files(file_name,fds,file_names,-1) == -1) goto err_open_file;
	if(is_db_file(&hd,fds) == -1) goto err_not_db_file;
	int result = -1;
	if((result = get_record(-1,file_name,&rec,k,key_type,hd,fds)) == -1) goto err_get_record_failed;
	if(result == KEY_NOT_FOUND) goto err_rec_not_found;
	if(port_record(L,&rec)) goto err_exp_data_to_lua;
	if(close_arena() == -1) goto err_memory;
	close_file(3,fds[0],fds[1],fds[2]);

	return 1;

err_key:
		lua_pushnil(L);
		lua_pushstring(L,"only string or unsigned integer are allowed as key.");
		return 2;

err_open_file:
		lua_pushnil(L);
		lua_pushstring(L,"could not open the file.");
		return 2;

err_get_record_failed:
		lua_pushnil(L);
		lua_pushstring(L,"get_record failed.");
		return 2;

err_not_db_file:
		lua_pushnil(L);
		lua_pushstring(L,"not a db file.");
		return 2;
err_rec_not_found:
		lua_pushnil(L);
		lua_pushstring(L,"record not found.");
		return 2;
err_memory:
		lua_pushnil(L);
		lua_pushstring(L,"cannot allocate memory.");
		return 2;
err_exp_data_to_lua:
		lua_pushnil(L);
		lua_pushstring(L,"cannot export record data.");
		return 2;
}


static int l_get_all_records(lua_State *L)
{
	char *file_name = (char*)luaL_checkstring(L,1);
	luaL_argcheck(L, file_name != NULL, 1,"file_name expected");

	struct Record_f rec;
	memset(&rec,0,sizeof(struct Record_f));

	struct Schema sch;
	memset(&sch,0,sizeof(struct Schema));
	struct Header_d hd = {0,0,&sch};
		
	int fds[3];
	memset(fds,-1,3*sizeof(int));
	char file_names[3][1024] = {0};

	if(is_memory_allocated() == NULL)
		if(create_arena(EIGTH_Kib) == -1) goto err_memory;

	if(open_files(file_name,fds,file_names,-1) == -1) goto err_open_file;
	if(is_db_file(&hd,fds) == -1) goto err_not_db_file;
	int result = -1;
	if((result = get_all_records(file_name,fds,&rec,hd)) == -1) goto err_get_record_failed;
	close_file(3,fds[0],fds[1],fds[2]);

	struct Record_f* t = &rec;
	while(t){
		if(port_record(L,t)) goto err_exp_data_to_lua;
		t = t->next;
	}
	if(close_arena() == -1) goto err_memory;

	return 1;

err_open_file:
		lua_pushnil(L);
		lua_pushstring(L,"could not open the file.");
		close_arena();
		return 2;

err_get_record_failed:
		lua_pushnil(L);
		lua_pushstring(L,"get_record failed.");
		close_file(3,fds[0],fds[1],fds[2]);
		close_arena();
		return 2;

err_not_db_file:
		lua_pushnil(L);
		lua_pushstring(L,"not a db file.");
		close_file(3,fds[0],fds[1],fds[2]);
		close_arena();
		return 2;
err_memory:
		lua_pushnil(L);
		lua_pushstring(L,"cannot allocate memory.");
		return 2;
err_exp_data_to_lua:
		lua_pushnil(L);
		lua_pushstring(L,"cannot export record data.");
		close_arena();
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
		lua_pushinteger(L,r->field_set[i]);
		lua_rawseti(L, -2, i + 1);
	}
	lua_setfield(L,-2,"active_fields");

	lua_newtable(L);	
	for(i = 0; i < r->fields_num; i++){
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
	return 0;
}
