package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"sync"
	"io/ioutil"
	// "fmt"
)

type JobType int

const (
	ReduceJobType JobType = iota
	MapJobType
)

type Job interface {
	GetType() JobType
}

// Map Job Definition
type MapJob struct {
	FileName string
	FilePath string
}


func (mapJob MapJob) GetType() JobType {
	return MapJobType
}


// Pending Map Jobs
type PendingMapJobs struct {
	Jobs []MapJob
	mu sync.Mutex
}

func (pendingMapJobs *PendingMapJobs) addJob(fileName string, filePath string) {
	pendingMapJobs.mu.Lock()
	pendingMapJobs.Jobs = append(pendingMapJobs.Jobs, MapJob{
		FileName: fileName,
		FilePath: filePath,
	})
	pendingMapJobs.mu.Unlock()
}

func (pendingMapJobs *PendingMapJobs) getJob() (ret MapJob) {
	pendingMapJobs.mu.Lock()
	defer pendingMapJobs.mu.Unlock()
	if(len(pendingMapJobs.Jobs) > 0) {
		jobCount := len(pendingMapJobs.Jobs)
		ret = pendingMapJobs.Jobs[jobCount - 1]
		pendingMapJobs.Jobs = pendingMapJobs.Jobs[:jobCount -1]

	}
	return
}

func (pendingMapJobs *PendingMapJobs) isEmpty() bool {
	pendingMapJobs.mu.Lock()
	defer pendingMapJobs.mu.Unlock()
	return len(pendingMapJobs.Jobs) == 0
}

// Reduce Job Definition
type ReduceJob struct {
	FileNames []string
	BucketNumber int
}

func (reduceJob ReduceJob) GetType() JobType {
	return ReduceJobType
}

// Pending Reduce Jobs
type PendingReduceJobs struct {
	Jobs []ReduceJob
	mu sync.Mutex
}

func (pendingReduceJobs *PendingReduceJobs) addJob(fileName string, bucketNumber int) {
	pendingReduceJobs.mu.Lock()
	pendingReduceJobs.Jobs[bucketNumber].FileNames = append(pendingReduceJobs.Jobs[bucketNumber].FileNames, fileName)
	pendingReduceJobs.mu.Unlock()
}

func (pendingReduceJobs *PendingReduceJobs) getJob() (ret ReduceJob) {
	pendingReduceJobs.mu.Lock()
	defer pendingReduceJobs.mu.Unlock()
	for bucketNumber,_ := range pendingReduceJobs.Jobs {
		if(len(pendingReduceJobs.Jobs[bucketNumber].FileNames) > 0) {
			ret = pendingReduceJobs.Jobs[bucketNumber]
			pendingReduceJobs.Jobs[bucketNumber].FileNames = nil
			return
		}
	}

	return 
}

func (pendingReduceJobs *PendingReduceJobs) isEmpty() bool {
	pendingReduceJobs.mu.Lock()
	defer pendingReduceJobs.mu.Unlock()
	for bucketNumber,_ := range pendingReduceJobs.Jobs {
		if(len(pendingReduceJobs.Jobs[bucketNumber].FileNames) > 0) {
			return false
		}
	}

	return true
}

func InitPendingReduceJobs(nReduce int) PendingReduceJobs {
	twoDReduceJobs := make([]ReduceJob, nReduce)
	for i:=0; i < nReduce; i++ {
		twoDReduceJobs[i] = ReduceJob{ BucketNumber: i }
	}
	return PendingReduceJobs {
		Jobs: twoDReduceJobs,
	}
}


// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func readFileAsString(fileName string) string {
	file, err := os.Open(fileName)
	if err != nil {
		return ""
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return ""
	}
	file.Close()
	return string(content)
}
