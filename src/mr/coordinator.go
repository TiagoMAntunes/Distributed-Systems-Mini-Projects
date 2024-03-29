package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nextWorker   int
	lock         *sync.Mutex
	jobs         JobList
	reduceNumber int
	stopLock     sync.Mutex
	stop         bool
}

type JobList struct {
	mapJobs         chan Job
	reduceJobs      chan Job
	statusLock      *sync.RWMutex
	doReduce        bool
	mapLock         *sync.Mutex
	inProgress      map[string]assignedJob // job name -> job
	countLock       *sync.Mutex
	mapCount        int
	reduceCountLock *sync.Mutex
	reduceCount     int
}

type assignedJob struct {
	task     Job
	finished bool
}

// Your code here -- RPC handlers for the worker to call.

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

	c.stopLock.Lock()
	ret := c.stop
	c.stopLock.Unlock()

	return ret
}

func (c *Coordinator) Register(req *RegisterRequest, rep *RegisterReply) error {
	c.lock.Lock()
	id := c.nextWorker
	c.nextWorker++
	c.lock.Unlock()

	rep.ID = id
	rep.ReduceNumber = c.reduceNumber
	return nil
}

func (c *Coordinator) GetWork(req *WorkRequest, rep *WorkReply) error {
	//fmt.Println("Assign work to ", req.ID)
	c.jobs.statusLock.RLock()
	status := c.jobs.doReduce
	c.jobs.statusLock.RUnlock()

	var ch chan Job
	var task Job
	var wait bool
	if status {
		ch = c.jobs.reduceJobs
	} else {
		ch = c.jobs.mapJobs
	}

	// find job
	select {
	case task = <-ch:
		wait = false
	default:
		//fmt.Println("No job found")
		wait = true
	}

	rep.Wait = wait
	rep.Task = task

	// if gotta wait, skip ahead
	if wait {
		return nil
	}

	saveJob := assignedJob{task: task}

	// save info
	c.jobs.mapLock.Lock()
	_, ok := c.jobs.inProgress[task.Filename]
	if !ok {
		c.jobs.inProgress[task.Filename] = saveJob
	}
	c.jobs.mapLock.Unlock()

	// avoid stalls
	go func() {
		time.Sleep(time.Second * 10) // as said in the hints section
		c.jobs.mapLock.Lock()
		job, ok := c.jobs.inProgress[task.Filename]
		c.jobs.mapLock.Unlock()

		if ok && !job.finished {
			// restart the job
			ch <- job.task // put it back in the list to be done
		}
	}()
	return nil
}

func (c *Coordinator) SubmitWork(req *WorkSubmitRequest, rep *WorkSubmitReply) error {
	c.jobs.mapLock.Lock()
	job := c.jobs.inProgress[req.Filename]
	if !job.finished {
		// job not finished before
		job.finished = true
		c.jobs.inProgress[req.Filename] = job // update status
	} else {
		// this worker was late
		c.jobs.mapLock.Unlock()
		return nil
	}
	c.jobs.mapLock.Unlock()

	c.jobs.statusLock.RLock()
	reduceStatus := c.jobs.doReduce
	c.jobs.statusLock.RUnlock()

	if !reduceStatus {
		// update status to initiate reduce jobs
		c.jobs.countLock.Lock()
		c.jobs.mapCount--

		if c.jobs.mapCount == 0 {
			// can start doing reduce functions
			c.jobs.statusLock.Lock()
			c.jobs.doReduce = true
			c.jobs.statusLock.Unlock()
		}

		c.jobs.countLock.Unlock()

	} else {
		c.jobs.reduceCountLock.Lock()
		c.jobs.reduceCount--

		if c.jobs.reduceCount == 0 {
			c.stopLock.Lock()
			c.stop = true
			c.stopLock.Unlock()
		}
		c.jobs.reduceCountLock.Unlock()

	}

	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{reduceNumber: nReduce, stop: false}

	c.lock = new(sync.Mutex)
	c.nextWorker = 0

	maps := make(chan Job, len(files))
	reduces := make(chan Job, nReduce)

	for i := 0; i < len(files); i++ {
		maps <- Job{JobType: MAP, Filename: files[i], ID: i}
	}

	for i := 0; i < nReduce; i++ {
		reduces <- Job{JobType: REDUCE, Filename: fmt.Sprintf("mr-out-%v", i), ID: i, MapCount: len(files)}
	}

	jobs := JobList{mapJobs: maps, reduceJobs: reduces, statusLock: new(sync.RWMutex), inProgress: make(map[string]assignedJob), mapLock: new(sync.Mutex), mapCount: len(files), countLock: new(sync.Mutex), reduceCountLock: new(sync.Mutex), reduceCount: nReduce}
	c.jobs = jobs

	c.server()
	return &c
}
