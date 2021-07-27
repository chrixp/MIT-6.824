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
	"time"
	"fmt"
)

type JobType int

const (
	ReduceJobType JobType = iota
	MapJobType 
)

type Job interface {
	GetType() JobType
	GetUuid() uuid.UUID
}

// Map Job Definition
type MapJob struct {
	FileName string
	FilePath string
	ID uuid.UUID
}


func (mapJob MapJob) GetType() JobType {
	return MapJobType
}

func (mapJob MapJob) GetUuid() uuid.UUID {
	return mapJob.ID
}

func InitMapJob(fileName string, filePath string) MapJob {
	return MapJob{
		FileName: fileName,
		FilePath: filePath,
		ID: uuid.NewV4(),
	}
}

// Map Job Result definition

type MapJobResult struct {
	ReduceJobs map[int]string
	ID uuid.UUID
}

// Pending Map Jobs
type PendingMapJobs struct {
	Jobs []MapJob
	mu sync.Mutex
}

func (pendingMapJobs *PendingMapJobs) size() int {
	pendingMapJobs.mu.Lock()
	defer pendingMapJobs.mu.Unlock()
	return len(pendingMapJobs.Jobs)
}

func (pendingMapJobs *PendingMapJobs) addJob(fileName string, filePath string) {
	pendingMapJobs.mu.Lock()
	pendingMapJobs.Jobs = append(pendingMapJobs.Jobs, InitMapJob(fileName, filePath))
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
	ID uuid.UUID
}

func (reduceJob ReduceJob) GetType() JobType {
	return ReduceJobType
}

func (reduceJob ReduceJob) GetUuid() uuid.UUID {
	return reduceJob.ID
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

func (pendingReduceJobs *PendingReduceJobs) size() int {
	pendingReduceJobs.mu.Lock()
	defer pendingReduceJobs.mu.Unlock()
	i := 0 
	for bucketNumber,_ := range pendingReduceJobs.Jobs {
		if(len(pendingReduceJobs.Jobs[bucketNumber].FileNames) > 0) {
			i += 1
		}
	}
	return i
}

func InitPendingReduceJobs(nReduce int) PendingReduceJobs {
	twoDReduceJobs := make([]ReduceJob, nReduce)
	for i:=0; i < nReduce; i++ {
		twoDReduceJobs[i] = ReduceJob{ 
			BucketNumber: i,
			ID: uuid.NewV4(),
		}
	}
	return PendingReduceJobs {
		Jobs: twoDReduceJobs,
	}
}

// Running Jobs

type RunningJob struct {	
	JobPt Job
	TimeStarted time.Time
}

type RunningJobs struct {
	Jobs []RunningJob
	mu sync.Mutex
}

func (runningJobs *RunningJobs) addJob(newJob Job, timeStarted time.Time) {
	//fmt.Printf("Adding New Job %v\n", newJob.GetUuid())
	runningJobs.mu.Lock()
	runningJobs.Jobs = append(runningJobs.Jobs, RunningJob{ 
		JobPt: newJob,
		TimeStarted: timeStarted,
	})
	runningJobs.mu.Unlock()
}

func (runningJobs *RunningJobs) removeJob(id uuid.UUID) {
	runningJobs.mu.Lock()
	defer runningJobs.mu.Unlock()
	for i,job := range runningJobs.Jobs {
		if (job.JobPt).GetUuid() == id {
			jobsLength := len(runningJobs.Jobs)
			runningJobs.Jobs[i] = runningJobs.Jobs[jobsLength - 1]
			runningJobs.Jobs = runningJobs.Jobs[:jobsLength - 1]
			// fmt.Printf("Remove Job %v\n", id)
			return
		}
	}
	fmt.Println("Can not be found")


}

func (runningJobs *RunningJobs) isEmpty() bool {
	runningJobs.mu.Lock()
	defer runningJobs.mu.Unlock()
	return len(runningJobs.Jobs) == 0
}

func (runningJobs *RunningJobs) size() int {
	runningJobs.mu.Lock()
	defer runningJobs.mu.Unlock()
	fmt.Println(runningJobs.Jobs)
	return len(runningJobs.Jobs)
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
