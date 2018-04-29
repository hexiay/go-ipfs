package hamt

import (
	"context"
	//"fmt"
	//"os"

	cid "gx/ipfs/QmcZfnkapfECQGcLZaf9B79NRg7cRa9EnZh4LSbkCzwNvY/go-cid"
	ipld "gx/ipfs/Qme5bWv7wtjUNGsK2BNGVUFPKiuxWrsqrtvYwCLRw8YFES/go-ipld-format"
)

// Batchsize must be at least as large as the largest number of cids
// requested in a single job.  For best perforamce it should likely be
// sligtly larger as jobs are poped from the todo stack in order and a
// job close to the batchSize could forse a very small batch to run.
const batchSize = 320

type fetcher struct {
	ctx   context.Context
	dserv ipld.DAGService

	newJob chan *job
	reqRes chan *Shard
	result chan result

	idle bool

	done chan struct{}

	todoFirst *job            // do this job first since we are waiting for its results
	todo      jobStack        // stack of jobs that still need to be done
	jobs      map[*Shard]*job // map of all jobs in which the results have not been collected yet
}

type result struct {
	vals map[string]*Shard
	errs []error
}

type job struct {
	id   *Shard
	idx  int
	cids []*cid.Cid
	res  result
}

type jobStack struct {
	c []*job
}

func startFetcher(ctx context.Context, dserv ipld.DAGService) *fetcher {
	//fmt.Printf("fetcher: starting...\n")
	f := &fetcher{
		ctx:    ctx,
		dserv:  dserv,
		newJob: make(chan *job),
		reqRes: make(chan *Shard),
		result: make(chan result),
		idle:   true,
		done:   make(chan struct{}),
		jobs:   make(map[*Shard]*job),
	}
	go f.mainLoop()
	return f
}

func (f *fetcher) addJob(hamt *Shard) bool {
	children := hamt.missingChildShards()
	if children == nil {
		return false
	}
	j := &job{id: hamt, cids: children}
	f.newJob <- j
	return true
}

func (f *fetcher) getResult(hamt *Shard) result {
	f.reqRes <- hamt
	res := <-f.result
	return res
}

func (f *fetcher) mainLoop() {
	var want *Shard
	for {
		select {
		case j := <-f.newJob:
			//fmt.Printf("fetcher: new job: %p\n", j.id)
			if len(j.cids) > batchSize {
				panic("job size larger than batchSize")
			}
			f.todo.push(j)
			f.jobs[j.id] = j
			if f.idle {
				f.launch()
			}
		case id := <-f.reqRes:
			//fmt.Printf("fetcher: reqRes: %p\n", id)
			j := f.jobs[id]
			if j.res.vals != nil {
				//fmt.Printf("fetcher: reqRes hit, sending result\n")
				delete(f.jobs, id)
				f.result <- j.res
			} else {
				//fmt.Printf("fetcher: reqRes miss: %p\n", id)
				if j.idx != -1 {
					//fmt.Printf("fetcher: reqRes super miss: %p\n", id)
					// move job to todoFirst so that it will be done on the
					// next batch job
					f.todo.remove(j)
					f.todoFirst = j
				}
				want = id
			}
		case <-f.done:
			f.launch()
			//fmt.Printf("fetcher: batch job done, want = %p\n", want)
			if want != nil {
				j := f.jobs[want]
				if j.res.vals != nil {
					//fmt.Printf("fetcher: sending result\n")
					delete(f.jobs, want)
					f.result <- j.res
					want = nil
				}
			}
		case <-f.ctx.Done():
			//fmt.Printf("fetcher: exiting\n")
			return
		}
	}
}

type batchJob struct {
	cids []*cid.Cid
	jobs []*job
}

func (b *batchJob) add(j *job) {
	b.cids = append(b.cids, j.cids...)
	b.jobs = append(b.jobs, j)
	j.idx = -1
}

func (f *fetcher) launch() {
	bj := batchJob{}

	// always do todoFirst
	if f.todoFirst != nil {
		bj.add(f.todoFirst)
		f.todoFirst = nil
	}

	// pop requets from todo list until we hit the batchSize
	for !f.todo.empty() && len(bj.cids)+len(f.todo.top().cids) <= batchSize {
		j := f.todo.pop()
		bj.add(j)
	}

	if len(bj.cids) == 0 {
		//fmt.Printf("fetcher: entering idle state: no more jobs")
		f.idle = true
		return
	}

	// launch job
	//fmt.Printf("fetcher: starting batch job, size = %d\n", len(bj.cids))
	f.idle = false
	go func() {
		ch := f.dserv.GetMany(f.ctx, bj.cids)
		fetched := result{vals: make(map[string]*Shard)}
		for no := range ch {
			if no.Err != nil {
				fetched.errs = append(fetched.errs, no.Err)
				continue
			}
			hamt, err := NewHamtFromDag(f.dserv, no.Node)
			if err != nil {
				fetched.errs = append(fetched.errs, err)
				continue
			}
			f.addJob(hamt)
			fetched.vals[string(no.Node.Cid().Bytes())] = hamt
		}
		for _, job := range bj.jobs {
			//fmt.Printf("job done: %p\n", job.id)
			job.res = fetched
		}
		f.done <- struct{}{}
	}()
}

func (js *jobStack) empty() bool {
	return len(js.c) == 0
}

func (js *jobStack) top() *job {
	return js.c[len(js.c)-1]
}

func (js *jobStack) push(j *job) {
	j.idx = len(js.c)
	js.c = append(js.c, j)
}

func (js *jobStack) pop() *job {
	j := js.top()
	js.remove(j)
	return j
}

func (js *jobStack) remove(j *job) {
	js.c[j.idx] = nil
	j.idx = -1
	js.popEmpty()
}

func (js *jobStack) popEmpty() {
	for len(js.c) > 0 && js.c[len(js.c)-1] == nil {
		js.c = js.c[:len(js.c)-1]
	}
}
