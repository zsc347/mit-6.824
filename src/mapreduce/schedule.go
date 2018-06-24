package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	// Your code here (Part III, Part IV).
	var done sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wk, ok := <-registerChan
		if ok == false {
			fmt.Println("Worker chanel closed")
			return
		}

		task := DoTaskArgs{}
		task.JobName = jobName
		if phase == mapPhase {
			task.File = mapFiles[i]
		}
		task.Phase = phase
		task.NumOtherPhase = nOther
		task.TaskNumber = i

		done.Add(1)
		go func(i int) {
			ok = call(wk, "Worker.DoTask", task, new(struct{}))
			if ok == false {
				fmt.Printf("Worker %s: execute %s phase index %d job error", wk, phase, i)
			}
			done.Done()
			registerChan <- wk
		}(i)
	}
	done.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
