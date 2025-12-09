#include <string.h>
#include <stdio.h>
#include "export_db_lua.h"
#include "date.h"
#include "lua5.1/lua.h"
#include "lua5.1/lauxlib.h"


/*static function prototype*/
static int port_record(struct Record_f*);


/* 
 * from lua:
 * .get_record(file_name,key)
 * */
static int l_get_record(lua_State *L)
{
	char *file_name = lua_checkstring(L,1);
	int key_type = 0;
	void *k = NULL;
	int type = lua_type(L,2);
	if(type == LUA_TNUMBER){
		key_type = UINT; 
		int n = lua_checkinteger(L,2);
		if( n < 0) goto err_key;
		k = &n;
	}else if(type == LUA_TSTRING){
		key_type = str; 
		k = lua_checkstring(L,2);
	}else{
		goto err_key;
	}


	struct Record_f rec;
	memset(&rec,0,sizeof(struct Record_f));

	struct Header_d hd;
	memset(&hd,0,sizeof(struct Header_d));

	struct Schema sch;
	memset(&sch,0,sizeof(struct Schema));

		
	int fds[3];
	memset(fds,-1,3*sizeof(int));

	if(open_files(file_name,fds,-1) == -1) goto err_open_file;
	if(is_db_file(hd,fds) == -1) goto err_not_db_file;
	int result = -1;
	if((result = get_record(-1,file_name,&rec,k,key_type,hd,fds)) == -1) goto err_get_record_failed;
	if(result == KEY_NOT_FOUND) goto err_rec_not_found;

err_key:
		lua_pushnil(L);
		lua_pushstring(L,"only string or unsigned integer are allowed as key.";
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
}


/*static functions definition*/


static int port_record(struct Record_f *r){
	if(!L) return -1;

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
		lua_pushinteger(L,*r->fields_set)
		lua_setfield(L,-2);
	}
	lua_setfield(L,-2,"active_fields");

	lua_newtable(L);	
	for(i = 0; i < r->fields_num; i++){
		switch(r->fields[i].type){
		case TYPE_INT:
			lua_pushinteger(L,r->fields[i].data.i);
			lua_setfield(L,-2,r->fields[i].fields_name);
			break;
		case TYPE_LONG:
			lua_pushinteger(L,r->fields[i].data.l);
			lua_setfield(L,-2,r->fields[i].fields_name);
			break;
		case TYPE_BYTE:
			lua_pushinteger(L,r->fields[i].data.b);
			lua_setfield(L,-2,r->fields[i].fields_name);
			break;
		case TYPE_DATE:
		{
			char date[11];
			memset(date,0,11);
			if(convert_number_to_date(date,r->fields[i].data.date) == -1) return -1;
			lua_pushstring(L,date);
			lua_setfield(L,-2,r->fields[i].fields_name);
			break;
		}
		case TYPE_FLOAT:
			lua_pushnumber(L,r->fields[i].data.f);
			lua_setfield(L,-2,r->fields[i].fields_name);
			break;
		case TYPE_DOUBLE:
			lua_pushnumber(L,r->fields[i].data.f);
			lua_setfield(L,-2,r->fields[i].fields_name);
			break;
		case TYPE_FILE:
		{
			struct Record_f *t;
			for(t = r->fields[i].data.file.recs; t != NULL; t = t->next){
				if(port_record(t) == -1) return -1;
			}

			lua_setfield(L,-2,r->fields[i].fields_name);
			break;
		}
		default:
			/*TODO*/
		}
	}
}
