#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <sys/time.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/ioctl.h> 
#include <string.h>
#include <unistd.h>

int usage(char *whoami);
int doconnect(int *sd,struct timeval *timeout,struct sockaddr_in *sa_us);
int dowrite(int sd,char *message,int length);
int doread(int sd, char *buf, int length, struct timeval *timeout);

extern char *optarg;
extern int optind;

char *whoami;

#define agentinfo "geturl-tiny/0.3 (Linux x86_64)"

/* expects us to get text back, will only serve up the first BUFSIZ bytes = 8192, that's
   plenty for what we want, which is tiny api call results */
char * geturl(char *hostname, int port, char *url) {
  int sd;
  struct sockaddr_in sa_us;
  struct timeval timeout;
  int result;
  struct hostent *hostinfo=NULL;
  char *message=NULL;
  static char buf[BUFSIZ];

  if ((hostinfo=gethostbyname(hostname)) == NULL ) {
    fprintf(stderr,"%s: host lookup failed\n",whoami);
    return(NULL);
  }

  /* set up socket and connect */
  sa_us.sin_family=AF_INET;
  memcpy(&sa_us.sin_addr,hostinfo->h_addr_list[0],hostinfo->h_length);
  sa_us.sin_port=htons(port);
  timeout.tv_sec=30;
  timeout.tv_usec=0;
  doconnect(&sd,&timeout,&sa_us);

  /* set up message and send it */
  if ((message=malloc(strlen(url)+25)) == NULL) {
    fprintf(stderr,"%s: out of memory\n",whoami);
    return(NULL);
  }
  sprintf(message,"GET %s HTTP/1.0\n",url);
  dowrite(sd,message,strlen(message));
  free(message);
  sprintf(buf,"Host: %s\n",hostname);
  dowrite(sd,buf,strlen(buf));
  sprintf(buf,"User-Agent: %s\n\n",agentinfo);
  dowrite(sd,buf,strlen(buf));
  /* read reply */
  errno=0;
  buf[0]='\0';
  result=doread(sd,buf,sizeof(buf),&timeout);
  if (result == -1) {
    fprintf(stderr,"%s: read error\n",whoami);
    close(sd);
    return(NULL);
  }
  close(sd);
  return(buf);
}

/* fixme need to check content length and only retrieve that amount */
int doread(int sd, char *buf, int length, struct timeval *timeout)
{
  int result;
  fd_set fds;
  int count = 0;

  FD_ZERO(&fds);
  FD_SET(sd,&fds);

  result = -1;
  while (count < length) {
    result = select(FD_SETSIZE,&fds,NULL,NULL,timeout);
    if (result <= 0) {
      perror("read error of some sort (0)");
      
    }
    else {
      result=recv(sd,buf+count,length-count,0);
      if (result == -1) {
	perror("read error of some sort (1)");
	if (errno==EWOULDBLOCK) {
	  FD_ZERO(&fds);
	  FD_SET(sd,&fds);
	  if (select(FD_SETSIZE,&fds,NULL,NULL,timeout) != 1) {
	    fprintf(stderr,"%s: timeout %d secs trying to read\n",
		    whoami,(int)timeout->tv_sec);
	    if (select(FD_SETSIZE,&fds,NULL,NULL,timeout) != 1) {
	      fprintf(stderr,"%s: -2- timeout %d secs trying to read\n",
		      whoami,(int)timeout->tv_sec);
	    }
	    return(-1);
	  }
	  else result=recv(sd,buf+count,length-count,0);
	}
	else {
	  fprintf(stderr,"%s: can't read from socket\n",whoami); 
	  perror(whoami); 
	  return(-1);
	}
      }
      else if (result == 0) {
	break;
      }
      else {
	count += result;
	buf[count] = '\0';
      }
    }
  }
  return(result);
}

int dowrite(int sd,char *message,int length)
{
  int result;

  while (1) {

    result=send(sd,message,(unsigned int) length,0);
    if (result == -1) {
      perror("some error, let's see it");
      if (errno!=EAGAIN) { 
	fprintf(stderr,"%s: write to server failed\n",whoami);
	perror(whoami);
	exit(1);
      }
    }
    else break;
  }
  return(result);     
}

int doconnect(int *sd,struct timeval *timeout,struct sockaddr_in *sa_us)
{
  int val;
  fd_set fds;
  
  if ((*sd = socket(AF_INET,SOCK_STREAM,0)) == -1) {
    fprintf(stderr, "%s: could not get socket\n",whoami);
    perror(whoami);
    exit(1);
  }
  /*
  val=1;
  if (ioctl(*sd, FIONBIO, &val) == -1) {
    fprintf(stderr,"%s: could not make connection \
         non-blocking\n",whoami);
    perror(whoami);
    exit(1);
  }
  */
  if (connect(*sd,(struct sockaddr *) sa_us,sizeof(*sa_us)) == -1) {
    if (errno != EINPROGRESS) {
      fprintf(stderr,"%s: could not connect\n", whoami);
      perror(whoami);
      exit(1);
    }
    else {
      FD_ZERO(&fds);
      FD_SET(*sd,&fds);
      if (select(FD_SETSIZE,NULL,&fds,NULL,timeout) != 1) {
	fprintf(stderr,"%s: timeout %d secs trying to connect\n",
		whoami,(int)timeout->tv_sec);
	exit(1);
      }
      else if ((connect(*sd,(struct sockaddr *) sa_us,sizeof(*sa_us))== -1)
	       && ( errno != EISCONN)) { 
	/* shouldn't in theory but.. */
        fprintf(stderr, "%s: connect failed\n",whoami);
        perror(whoami);
        exit(1);
      }
    }
  }
  errno=0;
  return(0);
}

