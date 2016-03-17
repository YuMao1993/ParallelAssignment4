#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>

#include "server/messages.h"
#include "server/master.h"
#include <queue>
#include <map>

/**
 * Request_info records the request's info
 */
typedef struct _Request_info{
  Client_handle client_handle;
  Request_msg client_req;

  _Request_info(Client_handle client_handle, Request_msg client_req):
  client_handle(client_handle),
  client_req(client_req){}

} Request_info;

/**
 * the state of worker
 */
typedef struct _Worker_state{
  Worker_handle worker_handle;
  bool is_busy;
} Worker_state;

/**
 * the state of the master
 */
static struct Master_state {

  //tag counter
  int next_tag;

  //is the server ready to go?
  bool server_ready;
  
  //max number of workers configured
  int max_num_workers;

  //the number of all pending requests:
  //pending requests =request being processed + requests in the queue
  int num_pending_client_requests;

  //workers
  std::vector<Worker_state> my_workers;

  //request queue
  std::queue<Request_info> requests_queue;

  //worker client map
  std::map<Worker_handle, Client_handle> workerClientMap;

} mstate;



void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 5;

  mstate.next_tag = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_pending_client_requests = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // fire off new workers
  int initNumWorker = max_workers;
  for(int i = 0; i<initNumWorker; i++)
  {
    int tag = random();
    Request_msg req(tag);
    req.set_arg("name", "my worker " + i);
    request_new_worker_node(req);   
  }

}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.
  Worker_state state;
  state.worker_handle = worker_handle;
  state.is_busy = false;
  mstate.my_workers.push_back(state);

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.

  DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;

  Client_handle client = mstate.workerClientMap[worker_handle];
  send_client_response(client, resp);
  mstate.num_pending_client_requests --;

  //find the worker
  for(unsigned int i=0; i< mstate.my_workers.size(); i++)
  {
    if(mstate.my_workers[i].worker_handle == worker_handle)
    {
      //mark the worker as idle
      mstate.my_workers[i].is_busy = false;
      
      //if the request queue is not null
      //execute the next request.
      if(!mstate.requests_queue.empty())
      {
        Worker_handle worker_handle = mstate.my_workers[i].worker_handle;
        Request_info req = mstate.requests_queue.front();
        
        DLOG(INFO) << "deque:" << req.client_req.get_request_string()<< std::endl;
        
        mstate.requests_queue.pop();
        int tag = mstate.next_tag++;
        Request_msg worker_req(tag, req.client_req);
        mstate.workerClientMap[worker_handle] = req.client_handle;
        mstate.my_workers[i].is_busy = true; //mark the worker as busy
        send_request_to_worker(worker_handle, req.client_req);
      }
      
      break;
    }
  }

  return;

}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;

  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }

  mstate.num_pending_client_requests++;

  //assign the work to the worker that is not busy
  bool is_assigned = false;
  for(unsigned int i=0; i<mstate.my_workers.size(); i++)
  {
    if(!mstate.my_workers[i].is_busy)
    {
      Worker_handle worker_handle = mstate.my_workers[i].worker_handle;
      int tag = mstate.next_tag++;
      Request_msg worker_req(tag, client_req);
      mstate.workerClientMap[worker_handle] = client_handle;
      mstate.my_workers[i].is_busy = true; //mark the worker as busy
      send_request_to_worker(worker_handle, worker_req);
      is_assigned = true;
      break;
    }
  }

  //if all workers are busy, push the request to queue
  if(!is_assigned)
  {
    DLOG(INFO) << "enque:" << client_req.get_tag() << ":" << client_req.get_request_string() << std::endl;
    mstate.requests_queue.push(Request_info(client_handle, client_req));
  }

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.
  return;

}

void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.

}

