#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#include "threadpool.h"

void my_task(tpool_t pool, void *arg) {
  char str[256];
  time_t t;
  long i = (long) arg;
  time(&t); strftime(str, sizeof(str), "%T", localtime(&t));
  printf("%s: Task %ld started\n", str, i);
  usleep(3000000);
  time(&t); strftime(str, sizeof(str), "%T", localtime(&t));
  printf("%s: Task %ld ended\n", str, i);
}

int main(int argc, char *argv[]) {

  tpool_t pool;
  int num_threads, num_tasks;

  if (argc != 3) {
    fprintf(stderr, "Usage: %s NUM_THREADS NUM_TASKS\n", argv[0]);
    return -1;
  }
  
  num_threads = strtol(argv[1], NULL, 10);
  num_tasks = strtol(argv[2], NULL, 10);
  
  pool = tpool_create(num_threads);

  for (long i = 0; i < num_tasks; i++)
    tpool_schedule_task(pool, my_task, (void *) i);

  tpool_join(pool);
  
  return 0;
}
