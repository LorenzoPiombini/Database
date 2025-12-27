#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include <record.h>
#include "export_db_lua.h"
#include "lua_start.h"

lua_State *L = NULL;
static time_t sec = 0; 

static int load(lua_State *L, char *file_config);

int init_lua()
{
	L = luaL_newstate();
	luaL_openlibs(L);
	if(load(L,"db_config.lua") == -1) return -1;
	check_config_file();
	return 0;
}

void close_lua()
{
	lua_close(L);
}

void check_config_file()
{
	struct stat file_data;
	if(stat("db_config.lua",&file_data) == -1) {
		return;
	}
	
	if(sec == 0){
		sec = file_data.st_mtim.tv_sec;
		return;
	}
	if(file_data.st_mtim.tv_sec > sec){
		clear_lua_stack();
		if(load(L,"db_config.lua") == -1) return;
		sec = file_data.st_mtim.tv_sec;
	}

}
void clear_lua_stack()
{
	lua_settop(L,0);
}

int execute_lua_function(char *func_name, char *func_sig, ...)
{
	va_list vl;
	int narg, nres;

	va_start(vl,func_sig);
	lua_getglobal(L,func_name);

	for(narg = 0; *func_sig != '\0'; narg++,func_sig++){
	
		luaL_checkstack(L,1,"too many arguments");

		switch(*func_sig){
		case 'r': /* Record_f */	
			if(port_record(L,va_arg(vl,struct Record_f*)) == -1){
				return -1;
			}
			break;
		case 'd': /* double */	
			lua_pushnumber(L,va_arg(vl,double));
			break;
		case 'i': /* integer*/	
			lua_pushinteger(L,va_arg(vl,int));
			break;
		case 'l': /* long integer*/	
			lua_pushinteger(L,va_arg(vl,long));
			break;
		case 's': /* string*/	
			char *s = va_arg(vl,char*);
			lua_pushstring(L,s);
			break;
		case '>': /*end of input*/
			func_sig++;
			goto fcall;
		default:
			return -1;
		}
	}

fcall:
	nres = strlen(func_sig);/*number of Lua function results*/

	/*execute the lua function*/
	if(lua_pcall(L,narg,nres,0) != 0){
		fprintf(stderr,"%s\n",lua_tostring(L,-1));
		lua_pop(L,1);
		return -1;
	}

	if(nres > 0){
		/*get the results */ 
		nres = -nres;
		while(*func_sig){
			switch(*func_sig){
			case 'r':
			{
				/*record*/
				if(port_table_to_record(L,*va_arg(vl,struct Record_f**)) == -1){
					clear_lua_stack();
					return -1;
				}
				break;
			}
			case 'd':
			{
				int is_num;
				double d = lua_tonumberx(L,nres,&is_num);
				if(!is_num){
					clear_lua_stack();
					return -1;
				}
				*va_arg(vl, double *) = d;
				break;
			}
			case 'i':
			{
				int is_num;
				long long l = (long long)lua_tonumberx(L,nres,&is_num);
				if(!is_num || l < 0){
					clear_lua_stack();
					return -1;
				}
				*va_arg(vl, long long*) = l;
				break;
			}
			case 's':
			{
				char *s = (char*)lua_tostring(L,nres);
				if(!s){
					clear_lua_stack();
					return -1;
				}
				*va_arg(vl,char **) = s;
				break;
			}
			default:
				clear_lua_stack();
				return -1;
			}
			nres++;
			func_sig++;
		}
	}
	va_end(vl);
	return 0;
}

static int load(lua_State *L, char *file_config)
{
	if(luaL_loadfile(L,file_config) || lua_pcall(L,0,0,0)){
		return -1;
	}
	return 0;
}
