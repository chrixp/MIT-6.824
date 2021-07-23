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
	"github.com/twinj/uuid"
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
	ID uuid.UUID
	FileName string
}

func createMapJob(fileName string) (job MapJob) {
	job = MapJob{
		FileName: fileName,
		ID: uuid.NewV4(),
	}
	return
}

func (mapJob MapJob) GetType() JobType {
	return MapJobType
}

// Reduce Job Definition
type ReduceJob struct {
	FileName string
	BucketNumber int
}

func (reduceJob ReduceJob) GetType() JobType {
	return ReduceJobType
}


// Pending Map Jobs
type PendingMapJobs struct {
	Jobs []MapJob
	mu sync.Mutex
}

func (pendingMapJobs *PendingMapJobs) addJob(fileName string) {
	pendingMapJobs.mu.Lock()
	newJob := createMapJob(fileName)
	pendingMapJobs.Jobs = append(pendingMapJobs.Jobs, newJob)
	pendingMapJobs.mu.Unlock()
}

func (pendingMapJobs *PendingMapJobs) getJob() (ret MapJob) {
	pendingMapJobs.mu.Lock()
	jobCount := len(pendingMapJobs.Jobs)
	ret = pendingMapJobs.Jobs[jobCount - 1]
	pendingMapJobs.Jobs = pendingMapJobs.Jobs[:jobCount -1]
	pendingMapJobs.mu.Unlock()
	return
}

func (pendingMapJobs *PendingMapJobs) isEmpty() bool {
	return len(pendingMapJobs.Jobs) == 0
}

// Pending Reduce Jobs
type PendingReduceJobs struct {
	Jobs [][]ReduceJobs
	mu sync.Mutex
}

func (pendingReduceJobs *PendingReduceJobs) addJob(fileNames []string, bucketNumber int) {
	pendingReduceJobs.mu.Lock()
	for _, fileName := range fileNames {
		
	}
	pendingReduceJobs.mu.Unlock()
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
