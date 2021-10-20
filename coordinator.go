package mr 

import "os"
import "sync"
import "log"
import "net/http"
import "net/rpc"
import "net"
import "time"

type Task_Sta int 

const (
	idle Task_Sta = 0
	in_progress Task_Sta = 1
	completed Task_Sta = 2
) // the state of all the tasks

type Coordinator struct {
	// Your definitions here.
   files []string
   nReduce int
   mux sync.RWMutex
   tasks_sta []Task_Sta
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Assign_Task(args *Args, reply *Reply) error {
    c.mux.RLock()
	first_idle_map_task := -1
	for i := 0; i < len(c.files); i++ {
	  if c.tasks_sta[i]==idle {
		first_idle_map_task = i
		break 
	  }
	} 
	c.mux.RUnlock()
    if first_idle_map_task!=-1 {
       reply.Task_type = Map
	   reply.Task_id = first_idle_map_task
	   reply.File_name = c.files[first_idle_map_task]
	   reply.NReduce = c.nReduce 
	   c.mux.Lock()
	   c.tasks_sta[first_idle_map_task] = in_progress
	   c.mux.Unlock()
	   go func(task_idx int, des_c *Coordinator) {
            time.Sleep(10*time.Second)
			c.mux.Lock()
			if des_c.tasks_sta[task_idx]!=completed {
               des_c.tasks_sta[task_idx] = idle
			}
			c.mux.Unlock()
	   }(first_idle_map_task, c)
	} else {
        flag := true
		c.mux.RLock()
        for i := 0; i < len(c.files); i++ {
           if c.tasks_sta[i]==in_progress {
			   flag = false
			   break
		   }
		}
		c.mux.RUnlock()
		if flag==false {
           reply.Task_type = Wait
		} else {
			first_idle_reduce_task := -1
			c.mux.RLock()
		    for i := len(c.files); i < len(c.files)+c.nReduce; i++ {
                if c.tasks_sta[i]==idle {
				   first_idle_reduce_task = i
				   break
				}
			}
			c.mux.RUnlock()
			if first_idle_reduce_task!=-1 {
				reply.Task_type = Reduce
				reply.Task_id = first_idle_reduce_task-len(c.files)
				reply.MMap = len(c.files)
				c.mux.Lock()
				c.tasks_sta[first_idle_reduce_task] = in_progress
				c.mux.Unlock()
				go func(task_idx int, des_c *Coordinator) {
                    time.Sleep(10*time.Second)
					des_c.mux.Lock()
					if des_c.tasks_sta[task_idx]!=completed {
						des_c.tasks_sta[task_idx] = idle
					}
					des_c.mux.Unlock()
				}(first_idle_reduce_task, c) 
			} else {
				flag := true
				c.mux.RLock()
				for i := len(c.files); i < len(c.files)+c.nReduce; i++ {
					if c.tasks_sta[i]==in_progress {
						flag = false
						break
					}
				}
				c.mux.RUnlock()
				if flag==true {
					reply.Task_type = Exit
				} else {
					reply.Task_type = Wait
				}
			}
		}
	}
	return nil
}

func (c *Coordinator) Work_Done(args1 *Args1, reply1 *Reply1) error {
	c.mux.Lock()
	c.tasks_sta[args1.Task_idx] = completed
	c.mux.Unlock()
    return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true
	// Your code here.
	c.mux.RLock()
    for _, cur_task_sta := range c.tasks_sta {
		if cur_task_sta!=2 {
			ret = false
			break 
		}
	}
	c.mux.RUnlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{}
    c.files = files
	c.nReduce = nReduce
    c.tasks_sta = make([]Task_Sta, len(files)+nReduce)
	for i, _ := range c.tasks_sta {
		c.tasks_sta[i] = idle
	}
	c.server()
	return &c
}
