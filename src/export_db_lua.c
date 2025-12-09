#include <string.h>
#include "export_db_lua.h"
#include "date.h"


int port_record(struct Record_f *r){
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
			int j;
			for(j = 0; j < r->fields[i].data.file.count; j++){
				/*TODO*/
			}

		}
		default:
			/*TODO*/
		}
				

	}
}
