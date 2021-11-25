package multi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/framey-io/go-tarantool"
)

func BenchmarkRoundRobinLb(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connMulti := dummy2Writable2NonWritableCluster(ctx, cancel)

	b.SetParallelism(100000)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if rand.Intn(2) == 1 {
				if c := connMulti.roundRobinWritable(); c == nil {
					panic("could not find connection")
				}
			} else {
				if c := connMulti.roundRobinNonWritable(); c == nil {
					panic("could not find connection")
				}
			}
		}
	})
}

func dummy2Writable2NonWritableCluster(ctx context.Context, cancel context.CancelFunc) *ConnectionMulti {
	connMulti := &ConnectionMulti{
		opts: OptsMulti{
			CheckTimeout: time.Second,
			Context:      ctx,
			Cancel:       cancel,
		},
		lb: new(lb),
	}
	conns := make(map[string]*loadBalancedConnection)
	conns["1"] = &loadBalancedConnection{
		mx:        new(sync.RWMutex),
		closeMx:   new(sync.RWMutex),
		connectMx: new(sync.Mutex),
		connMulti: connMulti,
		addr:      "1",
		_type:     writable,
		delegate:  &tarantool.Connection{},
	}
	conns["2"] = &loadBalancedConnection{
		mx:        new(sync.RWMutex),
		closeMx:   new(sync.RWMutex),
		connectMx: new(sync.Mutex),
		connMulti: connMulti,
		addr:      "2",
		_type:     writable,
		delegate:  &tarantool.Connection{},
	}
	conns["3"] = &loadBalancedConnection{
		mx:        new(sync.RWMutex),
		closeMx:   new(sync.RWMutex),
		connectMx: new(sync.Mutex),
		connMulti: connMulti,
		addr:      "3",
		_type:     nonWritable,
		delegate:  &tarantool.Connection{},
	}
	conns["4"] = &loadBalancedConnection{
		mx:        new(sync.RWMutex),
		closeMx:   new(sync.RWMutex),
		connectMx: new(sync.Mutex),
		connMulti: connMulti,
		addr:      "4",
		_type:     nonWritable,
		delegate:  &tarantool.Connection{},
	}
	connMulti.writableLbMx = new(sync.Mutex)
	connMulti.nonWritableLbMx = new(sync.Mutex)
	connMulti.synchronizeCollections(conns)
	return connMulti
}

func TestLbDistribution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connMulti := dummy2Writable2NonWritableCluster(ctx, cancel)

	for i := 0; i < 100000; i++ {
		go func() {

			for ctx.Err() == nil {
				connMulti.roundRobinWritable()
			}

		}()

		go func() {

			for ctx.Err() == nil {

				connMulti.roundRobinNonWritable()
			}

		}()
	}

	<-ctx.Done()
	//for _, connection := range connMulti.writableLoadBalancedConnections {
	//	println("writable", connection.addr," ", connection.pickedW)
	//}
	//for _, connection := range connMulti.nonWritableLoadBalancedConnections {
	//	println("non-writable", connection.addr," ", connection.pickedNW)
	//}
}

func TestE2EBlackBox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	auth := BasicAuth{
		User: "admin",
		Pass: "pass",
	}
	db, err := ConnectWithDefaults(ctx, cancel, auth, "localhost:3303", "bogus:2322")
	defer func(db *ConnectionMulti) {
		if cErr := db.Close(); cErr != nil {
			panic(cErr)
		}
		if db.ConnectedNow() {
			panic("still connected")
		}
		if len(db.groupedByAddressConnections) != 0 {
			panic("groupedByAddressConnections is not empty ")
		}
	}(db)
	if err != nil {
		panic(fmt.Sprintf("Could not connect to cluster %v", err))
	}

	for i := 0; i < 100; i++ {
		go func() {
			for ctx != nil && ctx.Err() == nil {
				id := ""  //uuid.New().String()
				id2 := "" //uuid.New().String()
				if db == nil || ctx == nil {
					return
				}
				if r, rErr := db.Insert("TEST_TABLE", []interface{}{id, id2, 3}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || r.Code != tarantool.OkCode {
					if r == nil || r.Code != tarantool.ErrTupleFound {
						panic(errors.New(fmt.Sprintf("Insert failed because: %v --- %v\n", rErr, r)))
					}
				}
				if db == nil || ctx == nil {
					return
				}
				if r, rErr := db.Select("TEST_TABLE", "T_IDX_1", 0, 1, tarantool.IterEq, []interface{}{id2, 3}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || r.Code != tarantool.OkCode {
					panic(errors.New(fmt.Sprintf("Query failed because: %v------%v\n", rErr, r)))
				} else {
					if rErr != nil && !errors.Is(rErr, ctx.Err()) {
						panic(rErr)
					}
					if len(r.Tuples()) != 0 {
						single := r.Tuples()[0]
						if single[0].(string) != id {
							panic(errors.New(fmt.Sprintf("expected:%v actual:%v", id, single[0].(string))))
						}
					} else {
						fmt.Sprintln("Query returned nothing")
					}
				}
				if db == nil || ctx == nil {
					return
				}
				if r, rErr := db.Delete("TEST_TABLE", "T_IDX_1", []interface{}{id2, 3}); (rErr != nil && !errors.Is(rErr, ctx.Err())) || r.Code != tarantool.OkCode {
					if r == nil || r.Code != tarantool.ErrTupleNotFound {
						panic(errors.New(fmt.Sprintf("Delete failed because: %v --- %v\n", rErr, r)))
					}
				}
			}
		}()
	}
	timerSync := time.NewTicker(4 * time.Second)
	defer timerSync.Stop()
	go func() {
		for {
			select {
			case _, open := <-timerSync.C:
				if !open || ctx.Err() != nil {
					return
				}
				new_ := make(map[string]*loadBalancedConnection)
				new_["localhost:3301"] = &loadBalancedConnection{
					mx:        new(sync.RWMutex),
					closeMx:   new(sync.RWMutex),
					connectMx: new(sync.Mutex),
					connMulti: db,
					addr:      "localhost:3301",
					_type:     writable,
				}
				db.sync(new_)
			}
		}
	}()
	timerRefresh := time.NewTicker(2500 * time.Millisecond)
	defer timerRefresh.Stop()
	go func() {
		time.Sleep(time.Second)
		for {
			select {
			case _, open := <-timerRefresh.C:
				if !open || ctx.Err() != nil {
					return
				}
				db.refresh(
					&loadBalancedConnection{addr: "localhost:3303", _type: writable},
					&loadBalancedConnection{addr: "localhost:3301", _type: nonWritable})
			}
		}
	}()
	timerCloseWritable := time.NewTicker(3500 * time.Millisecond)
	defer timerCloseWritable.Stop()
	go func() {
		time.Sleep(time.Second)
		for {
			select {
			case _, open := <-timerCloseWritable.C:
				if !open || ctx.Err() != nil {
					return
				}
				c := db.getConnection(true)
				if c != nil {
					if cErr := c.Close(); cErr != nil {
						log.Println(cErr)
					}
				}
			}
		}
	}()
	timerCloseNonWritable := time.NewTicker(2800 * time.Millisecond)
	defer timerCloseNonWritable.Stop()
	go func() {
		time.Sleep(time.Second)
		for {
			select {
			case _, open := <-timerCloseNonWritable.C:
				if !open || ctx.Err() != nil {
					return
				}
				c := db.getConnection(false)
				if c != nil {
					if cErr := c.Close(); cErr != nil {
						log.Println(cErr)
					}
				}
			}
		}
	}()
	<-ctx.Done()
}
