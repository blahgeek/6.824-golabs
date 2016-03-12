package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	var notherphase int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
		notherphase = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
		notherphase = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO

	// use registerChannel as queue of free workers

	job_finished := make([]bool, ntasks)
	for i := 0; i < ntasks; i += 1 {
		job_finished[i] = false
	}

	for {
		all_finished := true
		for i := 0; i < ntasks; i += 1 {
			if job_finished[i] {
				continue
			}
			all_finished = false
			go func(w string, task int) {
				debug("Calling worker %v for task %v\n", w, task)
				ok := call(w, "Worker.DoTask", &DoTaskArgs{mr.jobName, mr.files[task], phase, task, notherphase}, &struct{}{})
				if ok {
					debug("Worker %v finished task %v\n", w, task)
					job_finished[task] = true
					mr.registerChannel <- w
				} else {
					debug("Worker %v failed task %v\n", w, task)
				}
			}(<-mr.registerChannel, i)
		}
		if all_finished {
			break
		}
	}
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
