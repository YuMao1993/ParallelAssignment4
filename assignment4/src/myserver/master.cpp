#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>

#include "server/messages.h"
#include "server/master.h"
#include <queue>
#include <map>

#define RESERVED_CONTEXT 1
#define MAX_EXEC_CONTEXT 24
#define ELASTICx

#define RESERVE_CONTEXT_FOR_PI(N) \
if(true)\
{ \
  if(mstate.my_workers[i].num_running_task == MAX_EXEC_CONTEXT - N - RESERVED_CONTEXT + 1) \
    continue;\
}


#define INIT_NUM_WORKER 1
#define THRESHOLD 1

/**
 * Enum type of work
 */
 enum Work_type {
  WISDOM,
  PROJECTIDEA,
  TELLMENOW,
  COUNTPRIMES,
  COMPAREPRIMES,
  NUM_OF_WORKTYPE
 };

/**
 * Request_info records the request's info
 */
typedef struct _Request_info {
  Client_handle client_handle;
  Request_msg client_req;

  _Request_info(Client_handle client_handle, Request_msg client_req):
  client_handle(client_handle),
  client_req(client_req){}

} Request_info;

typedef int WorkType;

/**
 * the state of worker
 */
typedef struct _Worker_state {
  Worker_handle worker_handle;
  int num_running_task;
  int num_work_type[5];
} Worker_state;

/**
 * Cache Manager
 */
static struct Cache_manager {
  std::map<std::string, std::string>cacheMap;
} cache_manager;

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

  //current number of workers
  int current_num_wokers;

  //the number of all pending requests:
  //pending requests =request being processed + requests in the queue
  int num_pending_client_requests;

  //workers
  std::vector<Worker_state> my_workers;

  //request queue
  std::queue<Request_info> requests_queue;

  //vip request requests_queue
  std::queue<Request_info> requests_queue_vip;

  //tag client map
  std::map<int, Client_handle> tagClientMap;

  //tag type map
  std::map<int, int> tagTypeMap;

  //tag request_string map
  std::map<int, std::string> tagReqStringMap;

  //update mstate
  inline void handle_work_done(const Response_msg& resp, unsigned int worker_idx)
  {
    int tag = resp.get_tag();
    int work_type = tagTypeMap[tag];
    my_workers[worker_idx].num_work_type[work_type]--;
    if (work_type == COMPAREPRIMES)
    {
      my_workers[worker_idx].num_running_task -= 4;
    }
    else
    {
      my_workers[worker_idx].num_running_task--;
    }
  }

} mstate;

static inline void send_and_update(Client_handle client_handle, 
            Worker_handle worker_handle, const Request_msg& client_req,
            unsigned int worker_idx, WorkType work_type)
{
  int tag = mstate.next_tag++;
  worker_handle = mstate.my_workers[worker_idx].worker_handle;
  Request_msg worker_req(tag, client_req);
  mstate.tagClientMap[tag] = client_handle;
  mstate.tagTypeMap[tag] = work_type;
  mstate.tagReqStringMap[tag] = client_req.get_request_string();
  
  send_request_to_worker(worker_handle, worker_req);
  mstate.my_workers[worker_idx].num_work_type[work_type]++;

  if (work_type == COMPAREPRIMES)
  {
    mstate.my_workers[worker_idx].num_running_task += 4;
  }
  else
  {
    mstate.my_workers[worker_idx].num_running_task++;
  }


}

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 5;

  mstate.next_tag = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_pending_client_requests = 0;
  mstate.current_num_wokers = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // fire up new workers
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
  state.num_running_task = 0;
  mstate.current_num_wokers++;

  // init num_work_type
  for (int i = 0; i < NUM_OF_WORKTYPE; i ++)
  {
    state.num_work_type[i] = 0;
  }
  
  mstate.my_workers.push_back(state);

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) 
{

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.

  DLOG(INFO) << ">>>Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;

  Client_handle client = mstate.tagClientMap[resp.get_tag()];
  send_client_response(client, resp);
  DLOG(INFO) << "<<Master send response back to client: " << client << std::endl;

  mstate.num_pending_client_requests --;
  DLOG(INFO) << "pending_client_requests: " <<  mstate.num_pending_client_requests << std::endl;

  //store to cache
  int tag = resp.get_tag(); 
  std::string req_string = mstate.tagReqStringMap[tag];
  cache_manager.cacheMap[req_string] = resp.get_response();


  //find the worker
  for(unsigned int i=0; i< mstate.my_workers.size(); i++)
  {
    if(mstate.my_workers[i].worker_handle == worker_handle)
    {
      // update mstate
      mstate.handle_work_done(resp, i);
      break;
    }
  }
  // re-dispatching vip queue 
  for (unsigned int i = 0; i < mstate.requests_queue_vip.size(); i++)
  {
    Request_info req = mstate.requests_queue_vip.front();
    DLOG(INFO) << "deque(vip):" << req.client_req.get_request_string()<< std::endl;
    mstate.requests_queue_vip.pop();
    handle_client_request(req.client_handle, req.client_req);
  }
  // re-dispatching queue
  for (unsigned int i = 0; i < mstate.requests_queue.size(); i++)
  {
    Request_info req = mstate.requests_queue.front(); 
    DLOG(INFO) << "deque:" << req.client_req.get_request_string()<< std::endl;
    mstate.requests_queue.pop();
    handle_client_request(req.client_handle, req.client_req);
  }

  return;

}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) 
{
  
  std::string request_string = client_req.get_request_string();
  std::string request_arg = client_req.get_arg("cmd");
  DLOG(INFO) << ">>Received request: " << request_string << std::endl;

  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.
  if (request_arg == "lastrequest") 
  {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }


  // cache early response
  if (cache_manager.cacheMap.find(request_string) != cache_manager.cacheMap.end())
  {
    std::string response_string = cache_manager.cacheMap[request_string];
    Response_msg resp;

    resp.set_response(response_string);
    send_client_response(client_handle, resp);
    // hit and return
    return;
  }

  bool is_assigned = false;
  mstate.num_pending_client_requests++;

  // Assign to worker base on its work_status
  // assign to low workload node 
  if (request_arg == "418wisdom")
  {
    for(unsigned int i = 0; i < mstate.my_workers.size(); i++)
    {
      RESERVE_CONTEXT_FOR_PI(1);
      if(mstate.my_workers[i].num_running_task <= MAX_EXEC_CONTEXT)
      {

        send_and_update(client_handle, mstate.my_workers[i].worker_handle,
                        client_req, i, WISDOM);
        is_assigned = true;
        break;
      }
    }
  }
  // assign to idle node 
  if (request_arg == "projectidea")
  {
    for(unsigned int i=0; i<mstate.my_workers.size(); i++)
    {
      // RESERVE_CONTEXT_FOR_PI(1);
      if(mstate.my_workers[i].num_running_task <= MAX_EXEC_CONTEXT &&
         mstate.my_workers[i].num_work_type[PROJECTIDEA] < 1)
      {
        send_and_update(client_handle, mstate.my_workers[i].worker_handle,
                        client_req, i, PROJECTIDEA);
        is_assigned = true;
        break;
      }
    }
    // if(!is_assigned)
    // {
    //   //try the reserved context
    //   if(mstate.my_workers[0].num_running_task <= MAX_EXEC_CONTEXT && 
    //      mstate.my_workers[0].num_work_type[PROJECTIDEA] < 2)
    //   {
    //     send_and_update(client_handle, mstate.my_workers[0].worker_handle,
    //                     client_req, 0, PROJECTIDEA);   
    //     is_assigned = true;    
    //   }
    // }
  }
  // find any possible spot
  if (request_arg == "tellmenow")
  {
    for(unsigned int i=0; i<mstate.my_workers.size(); i++)
    {
      if(mstate.my_workers[i].num_running_task < MAX_EXEC_CONTEXT)
      {
        send_and_update(client_handle, mstate.my_workers[i].worker_handle,
                        client_req, i, TELLMENOW);
        is_assigned = true;
        break;
      }
    }
  }
  // find node that has low workload
  if (request_arg == "countprimes")
  {
    for(unsigned int i=0; i<mstate.my_workers.size(); i++)
    {
      RESERVE_CONTEXT_FOR_PI(1);
      if(mstate.my_workers[i].num_running_task < MAX_EXEC_CONTEXT)
      {
        send_and_update(client_handle, mstate.my_workers[i].worker_handle,
                        client_req, i, COUNTPRIMES);
        is_assigned = true;
        break;
      }
    }
  }
  // find node that has more than 4 context
  if (request_arg == "compareprimes")
  {
    for(unsigned int i=0; i<mstate.my_workers.size(); i++)
    {
      RESERVE_CONTEXT_FOR_PI(4);
      ///only execute if remianing context > 4
      if(MAX_EXEC_CONTEXT - mstate.my_workers[i].num_running_task < 4) 
        continue;
      
      send_and_update(client_handle, mstate.my_workers[i].worker_handle,
                        client_req, i, COMPAREPRIMES);
      is_assigned = true;
      break;
    }
  }


  //if all workers are busy, push the request to queue
  if(!is_assigned)
  {
    if (request_arg == "tellmenow" || request_arg == "projectidea")
    {
      DLOG(INFO) << "enque vip:" << client_req.get_tag() << ":" << client_req.get_request_string() << std::endl;
      mstate.requests_queue_vip.push(Request_info(client_handle, client_req));
    }
    else
    {
      DLOG(INFO) << "enque:" << client_req.get_tag() << ":" << client_req.get_request_string() << std::endl;
      mstate.requests_queue.push(Request_info(client_handle, client_req));
    }  
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
  unsigned int queue_size = mstate.requests_queue.size() + mstate.requests_queue_vip.size();
  // profile worker usage
  // printf("current queue size: %u\n", queue_size);
  // printf("current num_worker: %d\n", mstate.current_num_wokers);
  // printf("max num_worker: %d\n", mstate.max_num_workers);
  // for (unsigned int i = 0; i < mstate.my_workers.size(); i++)
  // {
  //   DLOG(INFO) << "====HANDLE PROBE:====" << std::endl;
  //   printf("====HANDLE PROBE:====\n");
  //   for (int j = 0; j < NUM_OF_WORKTYPE; j++)
  //   {
  //     DLOG(INFO) << j << ": " << mstate.my_workers[i].num_work_type[j] << std::endl;
  //     printf("worktype: %d\n", mstate.my_workers[i].num_work_type[j]);
  //   }
  //   printf("\n");
  //   DLOG(INFO) << std::endl;
     
  // }
  // DLOG(INFO) << "request_queue size: " << mstate.requests_queue.size() << std::endl;
  // DLOG(INFO) << "request_queue vip size: " << mstate.requests_queue_vip.size() << std::endl;
  #ifdef ELASTIC
  if (!queue_size && mstate.my_workers.size() > 1)
  {
    for (unsigned int i = 0; i < mstate.my_workers.size(); i++)
    {
      if (!mstate.my_workers[i].num_running_task)
      {
        kill_worker_node(mstate.my_workers[i].worker_handle);
        mstate.my_workers.erase(mstate.my_workers.begin()+i);
        mstate.current_num_wokers--;
        // printf("decrease worker node\n");
        // printf("current num_worker: %d\n", mstate.current_num_wokers);
        // printf("max num_worker: %d\n", mstate.max_num_workers);
      }
    }
  }
  else if ((queue_size > THRESHOLD) &&
           mstate.current_num_wokers < mstate.max_num_workers)
  {
    // printf("increase worker node\n");
    // printf("current num_worker: %d\n", mstate.current_num_wokers);
    // printf("max num_worker: %d\n", mstate.max_num_workers);
    int workerid = random();
    Request_msg req(workerid);
    req.set_arg("name", "my worker new");
    request_new_worker_node(req);
  }
  #endif
}

