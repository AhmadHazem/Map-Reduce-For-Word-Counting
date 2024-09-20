package mapreduce

func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var numOtherPhase int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)     // number of map tasks
		numOtherPhase = mr.nReduce // number of reducers
	case reducePhase:
		ntasks = mr.nReduce           // number of reduce tasks
		numOtherPhase = len(mr.files) // number of map tasks
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)

	for i := 0; i < ntasks; i++ {
		srvAddress := <-mr.registerChannel
		err := call(srvAddress, "Worker.RunTask", RunTaskArgs{mr.jobName, mr.files[i], phase, i, numOtherPhase}, new(struct{}))
		if err {
			mr.Register(&RegisterArgs{srvAddress}, new(struct{}))
			mr.workers = mr.workers[1:]
		} else {
			i--
		}
	}

	debug("Schedule: %v phase done\n", phase)
}
