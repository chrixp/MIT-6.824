package mr

import (
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"fmt"
	"bufio"
	"sync"
	"encoding/gob"
	"path"
	
)



type Coordinator struct {
	// Your definitions here.
	files []string
	pendingMapJobs PendingMapJobs
	pendingReduceJobs PendingReduceJobs
	taskNumber int
}

func (c *Coordinator) GiveAvailableJob(payload string, job *Job) error {
	if(!c.pendingMapJobs.isEmpty()) {
		(*job) = c.pendingMapJobs.getJob()
		// fmt.Printf("A worker picks up a MapJob with path %v\n", (*job).(MapJob).FilePath)
	} else if (!c.pendingReduceJobs.isEmpty()) {
		(*job) = c.pendingReduceJobs.getJob()
		// fmt.Printf("A worker picks up a ReduceJob for bucket %v\n", (*job).(ReduceJob).BucketNumber)
	}
	return nil
}

func (c *Coordinator) SignalCompletionOfMapJob(payload map[int]string, res *string) error {
	for bucketNumber, bucketFileName := range payload {
		c.pendingReduceJobs.addJob(bucketFileName, bucketNumber)
	}
	// fmt.Printf("Received %v Reduce Jobs\n", len(payload))
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
	fmt.Println("Sock is %d", sockname)
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


	// Your code here.
	return c.pendingReduceJobs.isEmpty() && c.pendingMapJobs.isEmpty()
}



func readFileAsLines(path string) ([]string, error) {
    file, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    var lines []string
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        lines = append(lines, scanner.Text())
    }
    return lines, scanner.Err()
}


const NUMBER_OF_CHUNKS int = 5
const CHUNK_PATH string = "chunks"

func (coordinator *Coordinator) splitFileIntoChunks(fileName string, wg *sync.WaitGroup) *[NUMBER_OF_CHUNKS]string {
	defer wg.Done()

	chunkFileNames := [NUMBER_OF_CHUNKS]string{}
	lines, _ := readFileAsLines(fileName)
	linesPerChunk := len(lines) / NUMBER_OF_CHUNKS
	lineIndex := 0

	for i := 0; i <= NUMBER_OF_CHUNKS; i++ {
		if(lineIndex < len(lines)) {
			chunkFileName := fmt.Sprintf("%v-%v", fileName, i)
			chunkFilePath :=  path.Join(CHUNK_PATH, chunkFileName)
			chunkFile, err := os.Create(chunkFilePath)
			if(err != nil) {
				fmt.Println(err)
			}
			for lineIndex < len(lines) && lineIndex < (i + 1) * linesPerChunk {
				fmt.Fprintf(chunkFile, "%v\n", lines[lineIndex])
				lineIndex += 1
			}
			coordinator.pendingMapJobs.addJob(chunkFileName, chunkFilePath)
		}
		
	}

	return &chunkFileNames
}



//	defer wg.Done()
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// os.RemoveAll(CHUNK_PATH) // Remove all current split chunks
	// os.MkdirAll(CHUNK_PATH, 0777)
	gob.Register(MapJob{})
	gob.Register(ReduceJob{})
	c := Coordinator{ 
		files: files,
		pendingReduceJobs: InitPendingReduceJobs(nReduce),
	}

	var splitChunkWg sync.WaitGroup

	for _, file := range files {
		splitChunkWg.Add(1)
		go c.splitFileIntoChunks(file, &splitChunkWg)
	}

	splitChunkWg.Wait()

	c.server()
	return &c
}
