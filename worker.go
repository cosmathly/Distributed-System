package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
func (a ByKey) Len() int { 
	return len(a) 
}
func (a ByKey) Swap(i, j int) { 
	a[i], a[j] = a[j], a[i] 
}
func (a ByKey) Less(i, j int) bool { 
	return a[i].Key < a[j].Key 
}
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
    
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	var ret bool
    for {
		args := Args{}
		reply := Reply{}
		ret = call("Coordinator.Assign_Task", &args, &reply)
		if ret==false {
			os.Exit(0)
		}
		switch reply.Task_type {
		case Map:
	    file, err := os.Open(reply.File_name)
		if err!=nil {
			log.Fatalf("cannot open %v", reply.File_name)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			file.Close()
			log.Fatalf("cannot read %v", reply.File_name)
		}
		file.Close()	
		kvs := mapf(reply.File_name, string(content))
		intermediate_files := make([]*os.File, reply.NReduce)
		var cur_err error
		for i := 0; i < reply.NReduce; i++ {
			intermediate_files[i], cur_err = ioutil.TempFile(".", "tmp*")
			if cur_err!=nil {
				log.Fatal(cur_err)
			}
		}
		file_enc := make([]*json.Encoder, reply.NReduce)
		for i := 0; i < reply.NReduce; i++ {
			file_enc[i] = json.NewEncoder(intermediate_files[i])
		}
		for _, kv := range kvs {
           cur_idx := ihash(kv.Key)%reply.NReduce
           err := file_enc[cur_idx].Encode(&kv)
		   if err!=nil {
			   log.Fatal(err)
		   }
		}
		prefix := "mr-"+strconv.Itoa(reply.Task_id)+"-"
		for i := 0; i < reply.NReduce; i++ {
            err := os.Rename(intermediate_files[i].Name(), prefix+strconv.Itoa(i))
			if err!=nil {
				log.Fatal(err)
			}
		}
		args1 := Args1{reply.Task_id}
		reply1 := Reply1{}
		call("Coordinator.Work_Done", &args1, &reply1)
		case Reduce:
		prefix := "mr-"
		suffix := "-"+strconv.Itoa(reply.Task_id)
		kvs := []KeyValue{}
		for i := 0; i < reply.MMap; i++ {
			file, err := os.Open(prefix+strconv.Itoa(i)+suffix)
			if err!=nil {
			   log.Fatal(err)
			}
            dec := json.NewDecoder(file)
            for {
			   var kv KeyValue
			   if err := dec.Decode(&kv); err!=nil {
				   break
			   }
			   kvs = append(kvs, kv)
			}
		}
		tmp_file, err := ioutil.TempFile(".", "tmp*")
		if err!=nil {
			log.Fatal(err)
		}
        sort.Sort(ByKey(kvs))
		for i := 0; i < len(kvs); {
			list_values := []string{}
			j := i
			for ; j < len(kvs); j++ {
				if kvs[j].Key==kvs[i].Key {
					list_values = append(list_values, kvs[j].Value)
				} else {
					break
				}
			}
            ret := reducef(kvs[i].Key, list_values)
			fmt.Fprintf(tmp_file, "%v %v\n", kvs[i].Key, ret)
			i = j
		}
		cur_err := os.Rename(tmp_file.Name(), "mr-out-"+strconv.Itoa(reply.Task_id))
		if cur_err!=nil {
			log.Fatal(cur_err)
		}
		args1 := Args1{reply.Task_id+reply.MMap}
		reply1 := Reply1{}
		call("Coordinator.Work_Done", &args1, &reply1)
		case Wait:
		time.Sleep(100*time.Millisecond)
		case Exit:
		os.Exit(0)
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
