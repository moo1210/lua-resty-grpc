// LD_PRELOAD spy: log TCP setsockopt calls made by the nginx worker.
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h>

static int (*real_setsockopt)(int, int, int, const void *, socklen_t);

int setsockopt(int fd, int level, int optname, const void *optval, socklen_t optlen) {
  if (!real_setsockopt) real_setsockopt = dlsym(RTLD_NEXT, "setsockopt");
  if (level == IPPROTO_TCP) {
    const char *path = getenv("SS_SPY_LOG");
    FILE *f = fopen(path ? path : "/tmp/grpc-bench/logs/setsockopt.log", "a");
    if (f) {
      fprintf(f, "pid=%d fd=%d opt=%d val=%d\n", (int) getpid(), fd, optname,
              (optval && optlen >= sizeof(int)) ? *(const int *) optval : -1);
      fclose(f);
    }
  }
  return real_setsockopt(fd, level, optname, optval, optlen);
}
