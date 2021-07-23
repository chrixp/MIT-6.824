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
	
)



type Coordinator struct {
	// Your definitions here.
	files []string
	pendingMapJobs PendingMapJobs
	taskNumber int
}

func (coordinator *Coordinator) addFile(fileName string) {
	coordinator.pendingMapJobs.addJob(fileName)
}

func (c *Coordinator) GiveAvailableJob(payload string, job *Job) error {
	if(!c.pendingMapJobs.isEmpty()) {
		(*job) = c.pendingMapJobs.getJob()
		fmt.Printf("A worker picks up a MapJob with id %v\n", (*job).(MapJob).ID)
	}
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
	ret := false

	// Your code here.


	return ret
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
			chunkFileName :=  fmt.Sprintf("%v/%v-%v", CHUNK_PATH, fileName, i)
			chunkFile, err := os.Create(chunkFileName)
			if(err != nil) {
				fmt.Println(err)
			}
			for lineIndex < len(lines) && lineIndex < (i + 1) * linesPerChunk {
				fmt.Fprintf(chunkFile, "%v\n", lines[lineIndex])
				lineIndex += 1
			}
			coordinator.addFile(chunkFileName)
		}
		
	}
	
	fmt.Printf("Splitting %s in to %v chunks\n", fileName, len(chunkFileNames))

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
	c := Coordinator{ files: files }

	var splitChunkWg sync.WaitGroup

	for _, file := range files {
		splitChunkWg.Add(1)
		go c.splitFileIntoChunks(file, &splitChunkWg)
	}

	splitChunkWg.Wait()

	// Your code here.
	for _,file := range c.files {
		fmt.Printf("File waiting to be picked up: %d \n", file)
	}


	c.server()
	return &c
}
