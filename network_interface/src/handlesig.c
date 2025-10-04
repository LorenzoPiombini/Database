#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "handlesig.h"
#include "monitor.h"
#include "network.h"
#include "memory.h"

static char prog[] = "net_interface";
int hdl_sock = -1;
static void handler(int signo);

int handle_sig()
{
	/*set up signal handler*/
	struct sigaction act;
	struct sigaction act_for_child;
	memset(&act,0,sizeof (struct sigaction));
	memset(&act_for_child,0,sizeof (struct sigaction));

	act.sa_handler = &handler;
	act_for_child.sa_handler = SIG_IGN;
	act_for_child.sa_flags = SA_NOCLDWAIT;
	if(/*sigaction(SIGSEGV, &act, NULL) == -1 ||*/
			sigaction(SIGINT,&act,NULL) == -1 || 
			sigaction(SIGPIPE,&act,NULL) == -1  ||
			sigaction(SIGCHLD,&act_for_child,NULL) == -1 ){ /*this is to avoid zombies*/
		fprintf(stderr,"(%s): cannot handle the signal.\n",prog);
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
		close_shared_memory();
		close_prog_memory();
		if(signo == SIGINT)
			fprintf(stderr,"\b\b(%s):cleaning on interrupt, recived %s.\n",prog,"SIGINT");
		else if(signo== SIGPIPE)
			fprintf(stderr,"(%s):cleaning on interrupt, recived %s.\n",prog,"SIGPIPE");
		else 
			fprintf(stderr,"(%s):cleaning on interrupt, recived %s.\n",prog,"SIGSEGV");
		break;
	default:
		break;
	}
	exit(-1);
}
