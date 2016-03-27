
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"
#include "tools/work_queue.h"

/*The worker node contains two, six-core Xeon e5-2620 v3 processors 
(2.4 GHz, 15MB L3 cache, hyper-threading, AVX2 instruction support)
thus the total working context number is 2 * 6 * 2 = 24*/
#define NUM_WORKER_THREADS 48
static void* workerThread(void* args);
static void* projectIdeaWorkerThread(void* args);

WorkQueue<Request_msg> workQueue;
WorkQueue<Request_msg> projectIdeaQueue;

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

struct thread_primes_args
{
  Request_msg request;
  Response_msg response;
};

static void* thread_primes(void* args)
{
  thread_primes_args* arg = (thread_primes_args*)args;
  execute_work(arg->request, arg->response);
  return NULL;
}

// Implements logic required by compareprimes command via multiple
// calls to execute_work.  This function fills in the appropriate
// response.
static void execute_compareprimes(const Request_msg& req, Response_msg& resp) {

    int params[4];
    int counts[4];

    // grab the four arguments defining the two ranges
    params[0] = atoi(req.get_arg("n1").c_str());
    params[1] = atoi(req.get_arg("n2").c_str());
    params[2] = atoi(req.get_arg("n3").c_str());
    params[3] = atoi(req.get_arg("n4").c_str());

    //run 4 independent work in parallel
    thread_primes_args args[4];
    pthread_t tid[3];
    for (int i=0; i<3; i++) {
      create_computeprimes_req(args[i].request, params[i]);
      pthread_create(&tid[i], NULL, thread_primes, (void*)&args[i]);
    }
    create_computeprimes_req(args[3].request, params[3]);
    thread_primes((void*)&args[3]);

    //reap all the 3 threads
    for(int i=0; i<3; i++)
    {
      pthread_join(tid[i], NULL);
    }

    //convert response to counts
    for(int i=0; i<4; i++)
    {
      counts[i] = atoi(args[i].response.get_response().c_str());
    }

    //generate response
    if (counts[1]-counts[0] > counts[3]-counts[2])
      resp.set_response("There are more primes in first range.");
    else
      resp.set_response("There are more primes in second range.");
}


void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";

    // create 47 worker threads
    pthread_t tid;
    for (int i = 0; i < NUM_WORKER_THREADS - 1; i++) {
        pthread_create(&tid, NULL, workerThread, NULL);
    }
    // create 1 worker thread to handle only the project idea messages
    pthread_create(&tid, NULL, projectIdeaWorkerThread, NULL);

}

void worker_handle_request(const Request_msg& req) {
  //put work into queue
  if (req.get_arg("cmd") == "projectidea") {
     projectIdeaQueue.put_work(req);
  }
  else {
     workQueue.put_work(req);
  }
}

static void* workerThread(void* args)
{
  while(true)
  {
    Request_msg req = workQueue.get_work();

    // Make the tag of the reponse match the tag of the request.  This
    // is a way for your master to match worker responses to requests.
    Response_msg resp(req.get_tag());

    // Output debugging help to the logs (in a single worker node
    // configuration, this would be in the log logs/worker.INFO)
    DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

    double startTime = CycleTimer::currentSeconds();

    if (req.get_arg("cmd").compare("compareprimes") == 0) {

      // The compareprimes command needs to be special cased since it is
      // built on four calls to execute_execute work.  All other
      // requests from the client are one-to-one with calls to
      // execute_work.
      execute_compareprimes(req, resp);

    } else {

      // actually perform the work.  The response string is filled in by
      // 'execute_work'
      execute_work(req, resp);
    }

    double dt = CycleTimer::currentSeconds() - startTime;
    DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ")\n";

    // send a response string to the master
    worker_send_response(resp);
  }
  return NULL;
}


static void* projectIdeaWorkerThread(void* args)
{
    while (true) {
        Request_msg req;
        req = projectIdeaQueue.get_work();

        // Make the tag of the reponse match the tag of the request.  This
        // is a way for your master to match worker responses to requests.
        Response_msg resp(req.get_tag());

        // Output debugging help to the logs (in a single worker node
        // configuration, this would be in the log logs/worker.INFO)
        DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

        double startTime = CycleTimer::currentSeconds();

        // actually perform the work.  The response string is filled in by
        // 'execute_work'
        execute_work(req, resp);

        double dt = CycleTimer::currentSeconds() - startTime;
        DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag() << ")\n";

        // send a response string to the master
        worker_send_response(resp);
    }
    return NULL;
}
