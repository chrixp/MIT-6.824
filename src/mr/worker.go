package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "encoding/gob"
import "encoding/json"
import "os"
import "sort"
import "path"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
			case ReduceJob:
				executeReduceFunction(reducef, typeAssertedJob)
			default:
				time.Sleep( 2 * time.Second)
				return
		}
	}
}

const MAP_OUTPUT_FOLDER = "intermediates"

func executeMapFunction(mapf func(string, string) []KeyValue, job MapJob) {
	// fmt.Println("Executing map function")
	fileName := job.FilePath
	fileContent := readFileAsString(fileName)
	if(fileContent == "") {
		fmt.Println("File content is empty")
		return
	}
	mapResult := mapf(fileName, fileContent)

	if(len(mapResult) > 0) {
		// Put intermediate results into reduce buckets
		reduceBuckets := make(map[int][]KeyValue)
		for _, kv := range mapResult {
			bucketNumber := ihash(kv.Key)
			reduceBuckets[bucketNumber] = append(reduceBuckets[bucketNumber], kv)
		}

		// Write each bucket to its file
		reduceBucketFileNames := make(map[int]string)
		for bucketNumber, bucketContent := range reduceBuckets {
			reduceFileName, writeErr := writeEncodedBucketContentToFile(bucketNumber, &bucketContent, &job)
			if(writeErr != nil) {
				fmt.Println(writeErr)
				return 
			} else {
				reduceBucketFileNames[bucketNumber] = reduceFileName
			}

		}
		var temp string
		// fmt.Printf("Finished Map Job. %v Reduce jobs found. Signaling to coordinator...\n", len(reduceBucketFileNames))
		call("Coordinator.SignalCompletionOfMapJob", MapJobResult{
			ReduceJobs: reduceBucketFileNames,
			ID: job.ID,
		}, &temp)
	}
}

func executeReduceFunction(reducef func(string, []string) string, job ReduceJob) {
	kva := []KeyValue{}
	for _,fileName := range(job.FileNames) {
		intermediateKva := readEncodedBucketContentFromFile(fileName)
		if(len(*intermediateKva) == 0) {
			fmt.Println("Empty intermediate kva. Terminating program")
		}
		kva = append(kva, (*intermediateKva)...)
	}


	oname := fmt.Sprintf("mr-out-%v", job.BucketNumber)
	ofile, _ := os.Create(oname)

	sort.Sort(ByKey(kva))

	i := 0
	for i < len(kva) {
		j := i + 1
		// Finding the range from i to j, where every element between i and j have the same key
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	var temp string
	call("Coordinator.SignalCompletionOfReduceJob", job.ID, &temp)

}

func readEncodedBucketContentFromFile(fileName string) (kva *[]KeyValue) {
	kva = &[]KeyValue{}
	file, err := os.Open(fileName)
    if err != nil {
		fmt.Println(err)
        return 
    }
    defer file.Close()

	dec := json.NewDecoder(file)
	for {
	  var kv KeyValue
	  if err := dec.Decode(&kv); err != nil {
		break
	  }
	  *kva = append(*kva, kv)
	}
	return
}

func writeEncodedBucketContentToFile(bucketNumber int, bucketContent *[]KeyValue, job *MapJob) (string, error) {
	bucketFileName := path.Join(MAP_OUTPUT_FOLDER, fmt.Sprintf("%v-%v", job.FileName, bucketNumber))
	bucketFile, err := os.Create(bucketFileName)

	if err != nil {
		fmt.Println("Error has happened")
		return "", err
	}

	
	enc := json.NewEncoder(bucketFile)
	for _, kv := range *bucketContent {
		enc.Encode(&kv)
	}

	bucketFile.Close()
	return bucketFileName, nil
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
	gob.Register(MapJob{})
	gob.Register(ReduceJob{})
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
