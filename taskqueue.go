package taskqueue

import (
	"sync"
	"context"
) 

// represent staus of task queue
type Status struct {
	InQueue int
	Completed int
	Running int
}

// represent a single job queue
type taskQueue struct {
	ctx context.Context
	cancel context.CancelFunc
	maxTasks int
	queue chan func()
	wg sync.WaitGroup
	status Status
}

// create new task queue with given context and concurrency
func New(CTX context.Context, maxTasks int) *taskQueue {
	ctx, cancel := context.WithCancel(CTX)
	return &taskQueue{
		ctx: ctx,
		cancel: cancel,
		maxTasks: maxTasks,
		queue: make(chan func(), maxTasks),
		status: Status{},
	}
}

// add a task to task queue
func (tq *taskQueue) Add(task func())	{
	select{
		case <- tq.ctx.Done():
			return
		default:
			run := tq.status.Running
			max := tq.maxTasks
			in := tq.status.InQueue
			if run < max && in == max{
				tq.Start()
			}
			select {
			case tq.queue <- task:
				tq.status.InQueue++
				tq.wg.Add(1)
			default:
				return
			}
	}				
}

// start maxTasks number of tasks in task queue until no task or context cancelled
func (tq *taskQueue) Start() {
	for i := 0; i < tq.maxTasks; i++ {
		go tq.worker()
	}
	tq.Wait()
}

//utilty function of Start
func (tq *taskQueue) worker() {
	for {
		select{
		case <- tq.ctx.Done():
			return
		case task, ok := <- tq.queue:
			if !ok {
				return
			}
			tq.status.InQueue--
			tq.status.Running++
			task()
			tq.wg.Done()
			tq.status.Running--
			tq.status.Completed++
		}
	}
}

// block execution until queue empty
func (tq *taskQueue) Wait() {
	tq.wg.Wait()
}

// return status of task queue
func (tq *taskQueue) Status() Status{
	return tq.status
}

// set maxTasks
func (tq *taskQueue) SetConcurrency(Concurrency int) {
	tq.maxTasks = Concurrency
}