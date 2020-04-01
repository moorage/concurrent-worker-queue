package concurrentworkerqueue

import (
	"sync"
)

//ConcurrentWorkerQueue generically handles a worker queue of max concurrency
type ConcurrentWorkerQueue struct {
	queue chan interface{}
	// cancels chan bool
	// cancel         context.Context
	procFunc       func(interface{}) error
	waitgroup      sync.WaitGroup
	maxConcurrency int
	addedCount     int
	addedCountMut  sync.RWMutex
	succeededCount int
	succedCountMut sync.RWMutex
	errors         []error
	mutErrors      sync.RWMutex
}

// New convenience function for making one!  Calls Run() automatically.
func New(maxConcurrency int, processor func(interface{}) error) *ConcurrentWorkerQueue {
	c := &ConcurrentWorkerQueue{
		maxConcurrency: maxConcurrency,
		queue:          make(chan interface{}, maxConcurrency),
		// cancel:         cancel,
		// cancels:  make(chan bool, maxConcurrency), // fan out a single cancel call
		procFunc: processor,
	}
	c.Run()
	return c
}

// Run starts the workers.  Called automatically by New, since it's non-blocking.
func (c *ConcurrentWorkerQueue) Run() {
	for i := 0; i < c.maxConcurrency; i++ {
		go c.concurrent()
	}
	// go c.fanoutCancel() // listen for cancels
}

// Add item to the queue.  You can
// add while the workers are processing
func (c *ConcurrentWorkerQueue) Add(item interface{}) {
	c.waitgroup.Add(1) // for download
	c.addedCountMut.Lock()
	c.addedCount++
	c.addedCountMut.Unlock()

	c.waitgroup.Add(1) // for adding to queue
	go func(item interface{}) {
		c.waitgroup.Done() // for queuing
		// log.Printf("Waiting to add %+v", item)
		c.queue <- item // blocks on maxConcurrency
		// log.Printf("Added %+v", item)
	}(item) // go routine so we can block until added
}

// concurrent is a worker alongside maxConcurrency other workers
func (c *ConcurrentWorkerQueue) concurrent() {
	for {
		select {
		// case <-c.cancels:
		// 	return
		case item := <-c.queue:
			if item == nil { // FIXME not sure why this can happen; probably closed chan
				// log.Printf("NIL Value Alert %+v", item)
				continue
			}
			// log.Printf("Running Proc %+v", item)
			err := c.procFunc(item)
			// log.Printf("Returned from Proc %+v", item)
			if err != nil {
				c.mutErrors.Lock()
				// fmt.Printf("Ran into error on ConcurrentWorkerQueue for item %+v: %s", item, err)
				c.errors = append(c.errors, err)
				c.mutErrors.Unlock()
			} else {
				c.succedCountMut.Lock()
				c.succeededCount++
				c.succedCountMut.Unlock()
			}
			c.waitgroup.Done()
			// log.Printf("Completed %+v", item)
		}
	}
}

// fanoutCancel listens for a single cancel and cancels all
// func (c *ConcurrentWorkerQueue) fanoutCancel() {
// for {
// 	select {
// 	case <-c.cancel.Done(): // block until cancel or closed
// 		for i := 0; i < c.maxConcurrency; i++ {
// 			c.cancels <- true
// 		}
// 	}
// }
// }

// Errored returns if there were any errrors processing an item
func (c *ConcurrentWorkerQueue) Errored() bool {
	c.mutErrors.RLock()
	errored := len(c.errors) > 0
	c.mutErrors.RUnlock()
	return errored
}

// Errors returns the actual errors returned from processing items
func (c *ConcurrentWorkerQueue) Errors() []error {
	c.mutErrors.RLock()
	defer c.mutErrors.RUnlock()
	return c.errors
}

// Wait for all downloads to complete
func (c *ConcurrentWorkerQueue) Wait() int {
	c.waitgroup.Wait()
	close(c.queue)
	// close(c.cancels)
	c.succedCountMut.RLock()
	defer c.succedCountMut.RUnlock()
	return c.succeededCount
}
