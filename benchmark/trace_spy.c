// LD_PRELOAD tracer: log send/recv/writev/read + connect timeline for nginx worker.
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <time.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>

static ssize_t (*real_send)(int, const void *, size_t, int);
static ssize_t (*real_recv)(int, void *, size_t, int);
static ssize_t (*real_writev)(int, const struct iovec *, int);
static ssize_t (*real_read)(int, void *, size_t);
static ssize_t (*real_write)(int, const void *, size_t);
static int (*real_connect)(int, const struct sockaddr *, socklen_t);

static void logline(const char *op, int fd, long len, long ret) {
  const char *path = getenv("SS_SPY_LOG");
  FILE *f = fopen(path ? path : "/tmp/grpc-bench/logs/trace.log", "a");
  if (!f) return;
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  fprintf(f, "%ld.%06ld pid=%d %s fd=%d len=%ld ret=%ld\n",
          (long) ts.tv_sec, ts.tv_nsec / 1000, (int) getpid(), op, fd, len, ret);
  fclose(f);
}

ssize_t send(int fd, const void *buf, size_t len, int flags) {
  if (!real_send) real_send = dlsym(RTLD_NEXT, "send");
  ssize_t r = real_send(fd, buf, len, flags);
  logline("send", fd, (long) len, (long) r);
  return r;
}

ssize_t recv(int fd, void *buf, size_t len, int flags) {
  if (!real_recv) real_recv = dlsym(RTLD_NEXT, "recv");
  ssize_t r = real_recv(fd, buf, len, flags);
  logline("recv", fd, (long) len, (long) r);
  return r;
}

ssize_t writev(int fd, const struct iovec *iov, int cnt) {
  if (!real_writev) real_writev = dlsym(RTLD_NEXT, "writev");
  ssize_t r = real_writev(fd, iov, cnt);
  logline("writev", fd, (long) cnt, (long) r);
  return r;
}

ssize_t read(int fd, void *buf, size_t len) {
  if (!real_read) real_read = dlsym(RTLD_NEXT, "read");
  ssize_t r = real_read(fd, buf, len);
  logline("read", fd, (long) len, (long) r);
  return r;
}

ssize_t write(int fd, const void *buf, size_t len) {
  if (!real_write) real_write = dlsym(RTLD_NEXT, "write");
  ssize_t r = real_write(fd, buf, len);
  logline("write", fd, (long) len, (long) r);
  return r;
}

int connect(int fd, const struct sockaddr *addr, socklen_t alen) {
  if (!real_connect) real_connect = dlsym(RTLD_NEXT, "connect");
  int port = 0;
  if (addr && addr->sa_family == AF_INET) {
    port = ntohs(((const struct sockaddr_in *) addr)->sin_port);
  }
  logline(port == 9000 ? "connect-grpc" : "connect", fd, port, 0);
  return real_connect(fd, addr, alen);
}
