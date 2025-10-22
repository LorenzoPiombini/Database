#include <signal.h>
#include <unistd.h>
#include "handlesig.h"
#include "monitor.h"
#include "network.h"
#include "memory.h"


#include "freestand.h"

static char prog[] = "net_interface";
int hdl_sock = -1;
pid_t worker = 0;
static void handler(int signo);

int handle_sig()
{
	/*set up signal handler*/
	struct sigaction act;
	struct sigaction act_for_child;
	set_memory(&act,0,sizeof (struct sigaction));
	set_memory(&act_for_child,0,sizeof (struct sigaction));

	act.sa_handler = &handler;
	act_for_child.sa_handler = SIG_IGN;
	act_for_child.sa_flags = SA_NOCLDWAIT;
	if(/*sigaction(SIGSEGV, &act, NULL) == -1 ||*/
			sigaction(SIGINT,&act,NULL) == -1 || 
			sigaction(SIGPIPE,&act,NULL) == -1  ||
			sigaction(SIGCHLD,&act_for_child,NULL) == -1 ){ /*this is to avoid zombies*/
		display_to_stdout("(%s): cannot handle the signal.\n",prog);
		return -1;
	}
	return 0;
}

static void handler(int signo)
{
	switch(signo){
	/*case SIGSEGV:*/
	case SIGINT:
	case SIGPIPE:
		stop_monitor();	
		stop_listening(hdl_sock);
		close_prog_memory();
		if(signo == SIGINT)
			display_to_stdout("\b\b(%s):cleaning on interrupt, recived %s.\n",prog,"SIGINT");
		else if(signo== SIGPIPE)
			display_to_stdout("(%s):cleaning on interrupt, recived %s.\n",prog,"SIGPIPE");
		else 
			display_to_stdout("(%s):cleaning on interrupt, recived %s.\n",prog,"SIGSEGV");
		break;
	default:
		break;
	}
	kill(worker,SIGINT);
	sys_exit(-1);
}
