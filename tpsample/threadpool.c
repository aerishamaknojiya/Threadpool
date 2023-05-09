#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>

#include "threadpool.h"
typedef struct task *task_t;

struct task
{
  void (*function)(tpool_t, void *);
  void *arg;
  struct task *next;
};
struct tpool
{
  pthread_t *threads;
  int numThreads;
  int join;
  int run;
  int stop;
  task_t head;
  task_t tail;
  int qsize;
  pthread_mutex_t lock;
  pthread_cond_t isempty;
  pthread_cond_t isnotempty;
};

/* Function executed by each pool worker thread. This function is
 * responsible for running individual tasks. The function continues
 * running as long as either the pool is not yet joined, or there are
 * unstarted tasks to run. If there are no tasks to run, and the pool
 * has not yet been joined, the worker thread must be blocked.
 *
 * Parameter: param: The pool associated to the thread.
 * Returns: nothing.
 */
void *run_tasks(void *param)
{
  tpool_t pool = param;
  task_t currentTask;
  while (pool->run)
  {
    pthread_mutex_lock(&(pool->lock));
    printf("entered 1\n");

    while (pool->qsize == 0)
    {
      printf("entered 2\n");

      if (pool->stop == 1)
      {
        printf("entered 3\n");

        pthread_mutex_unlock(&(pool->lock));
        pthread_exit(NULL);
      }
      pthread_mutex_unlock(&(pool->lock));
      pthread_cond_wait(&(pool->isnotempty), &(pool->lock));
    }
    currentTask = pool->head;
    pool->qsize--;
    if (pool->qsize == 0)
    {
      pool->head = NULL;
      pool->tail = NULL;
    }
    else
    {
      pool->head = currentTask->next;
    }
    if (pool->qsize == 0 && pool->join != 0)
    {
      pthread_cond_signal(&(pool->isempty));
    }
    if (pool->stop == 1)
    {

      pool->run = 0;
    }
    pthread_mutex_unlock(&(pool->lock));
    currentTask->function(pool, currentTask->arg);
    free(currentTask);

    /* TO BE COMPLETED BY THE STUDENT */
  }
}
/* Creates (allocates) and initializes a new thread pool. Also creates
 * `num_threads` worker threads associated to the pool, so that
 * `num_threads` tasks can run in parallel at any given time.
 *
 * Parameter: num_threads: Number of worker threads to be created.
 * Returns: a pointer to the new thread pool object.
 */
tpool_t tpool_create(unsigned int num_threads)
{

  tpool_t pool = malloc(sizeof(struct tpool));
  if (pool == NULL)
  {
    perror("malloc() failed for pool");
  }
  pool->run = 1;
  pool->stop = 0;
  pool->numThreads = num_threads;
  pool->join = 0;
  pool->qsize = 0;
  pool->head = NULL;
  pool->tail = NULL;
  pthread_mutex_init(&(pool->lock), NULL);
  pthread_cond_init(&(pool->isempty), NULL);
  pthread_cond_init(&(pool->isnotempty), NULL);
  if (num_threads <= 0)
  {
    perror("invalid thread number!\n");
  }
  int i;
  pool->threads = malloc(num_threads * sizeof(pthread_t));
  if (pool->threads == NULL)
  {
    perror("malloc() failed for thread pool");
  }
  for (i = 0; i < num_threads; i++)
  {
    if (pthread_create(&(pool->threads[i]), NULL, run_tasks, pool) != 0)
    {
      perror("thread creation failed\n");
    }
  }
  return pool;
}

/* Schedules a new task, to be executed by one of the worker threads
 * associated to the pool. The task is represented by function `fun`,
 * which receives the pool and a generic pointer as parameters. If any
 * of the worker threads is available, `fun` is started immediately by
 * one of the worker threads. If all of the worker threads are busy,
 * `fun` is scheduled to be executed when a worker thread becomes
 * available. Tasks are retrieved by individual worker threads in the
 * order in which they are scheduled, though due to the nature of
 * concurrency they may not start exactly in the same order. This
 * function returns immediately, and does not wait for `fun` to
 * complete.
 *
 * Parameters: pool: the pool that is expected to run the task.
 *             fun: the function that should be executed.
 *             arg: the argument to be passed to fun.
 */
void tpool_schedule_task(tpool_t pool, void (*fun)(tpool_t, void *), void *arg)
{
  task_t currentTask = malloc(sizeof(struct task));
  if (currentTask == NULL)
  {
    perror("malloc failed for schedule task\n");
  }
  currentTask->function = fun;
  currentTask->arg = arg;
  currentTask->next = NULL;
  pthread_mutex_lock(&(pool->lock));

  if (pool->qsize == 0)
  {
    pool->head = currentTask;
    pool->tail = currentTask;
    pthread_cond_signal(&(pool->isnotempty));
  }
  else
  {
    pool->tail->next = currentTask;
    pool->tail = currentTask;
  }
  pool->qsize++;
  if (pool->join)
  {
    free(currentTask);
  }
  pthread_mutex_unlock(&(pool->lock));
}

/* Blocks until the thread pool has no more scheduled tasks; then,
 * joins all worker threads, and frees the pool and all related
 * resources. Once this function returns, no additional tasks can be
 * scheduled, and the thread pool resources are destroyed/freed.
 *
 * Parameters: pool: the pool to be joined.
 */
void tpool_join(tpool_t pool)
{
  int i;
  pthread_mutex_lock(&(pool->lock));
  pool->join = 1;
  while (pool->qsize != 0)
  {

    pthread_cond_wait(&(pool->isempty), &(pool->lock));
  }
  pool->stop = 1;
  pthread_cond_broadcast(&(pool->isnotempty));
  pthread_mutex_unlock(&(pool->lock));

  for (i = 0; i < pool->numThreads; i++)
  {

    pthread_join(pool->threads[i], NULL);
  }
  free(pool->threads);
  pthread_mutex_destroy(&(pool->lock));
  pthread_cond_destroy(&(pool->isempty));
  pthread_cond_destroy(&(pool->isnotempty));
  free(pool);
}