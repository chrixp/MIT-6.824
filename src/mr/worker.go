package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "encoding/gob"
import "encoding/json"
import "os"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff) % 10
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		job := askCoordinatorForJob()

		switch typeAssertedJob := (*job).(type) {
			case MapJob:
				executeMapFunction(mapf, typeAssertedJob)
			default:
				fmt.Println("NO TYPE FOUND")
		}
		time.Sleep(time.Second * 10)
	}
}

const MAP_OUTPUT_FOLDER = "intermediates"

func executeMapFunction(mapf func(string, string) []KeyValue, job MapJob) {
	fileName := job.FileName
	fileContent := readFileAsString(fileName)
	mapResult := mapf(fileName, fileContent)

	// intermediateFileName := fmt.SPrintF("%s/%v", MAP_OUTPUT_FOLDER, fileName)
	// file, err := os.Open(intermediateFileName)
	// if err != nil {
	// 	return
	// }

	if(len(mapResult) > 0) {
		// Put intermediate results into reduce buckets
		reduceBuckets := make(map[int][]KeyValue)
		for _, kv := range mapResult {
			bucketNumber := ihash(kv.Key)
			reduceBuckets[bucketNumber] = append(reduceBuckets[bucketNumber], kv)
		}

		// Write each bucket to its file
		for bucketNumber, bucketContent := range reduceBuckets {
			successfulWrites := writeEncodedBucketContentToFile(bucketNumber, &bucketContent, &job)
			fmt.Println(successfulWrites)
		}
	}
}

func writeEncodedBucketContentToFile(bucketNumber int, bucketContent *[]KeyValue, job *MapJob) (successfulWrites *[]string) {
	successfulWrites = &[]string{}
	fmt.Printf("Bucket number %v \n", bucketNumber)
	bucketFileName := fmt.Sprintf("%s/%v-%v", MAP_OUTPUT_FOLDER, job.ID, bucketNumber)
	fmt.Printf("Writing content to bucket %v ...\n", bucketFileName)
	bucketFile, err := os.Create(bucketFileName)

	if err != nil {
		fmt.Println("Error has happened")
		fmt.Println(err)
		return 
	}

	
	enc := json.NewEncoder(bucketFile)
	for _, kv := range *bucketContent {
		enc.Encode(&kv)
	}

	bucketFile.Close()
	return
}

func askCoordinatorForJob() *Job {
	var job Job
	call("Coordinator.GiveAvailableJob", "", &job)
	return &job
}
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	gob.Register(MapJob{})
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
