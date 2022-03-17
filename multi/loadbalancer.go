package multi

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/framey-io/go-tarantool"
)

type clusterMemberType string

const (
	writable    = clusterMemberType("writable")
	nonWritable = clusterMemberType("non-writable")
)

type lb struct {
	groupedByAddressConnections map[string]*loadBalancedConnection

	writableLbMx                             *sync.Mutex
	roundRobinLoadBalancerCurrentWritableIdx uint8
	writableLoadBalancedConnections          []*loadBalancedConnection

	nonWritableLbMx                             *sync.Mutex
	roundRobinLoadBalancerCurrentNonWritableIdx uint8
	nonWritableLoadBalancedConnections          []*loadBalancedConnection
}

type loadBalancedConnection struct {
	closed    bool
	closeMx   *sync.RWMutex
	connectMx *sync.Mutex
	mx        *sync.RWMutex
	connMulti *connectionMulti
	delegate  *tarantool.Connection
	_type     clusterMemberType
	addr      string
}

func (c *loadBalancedConnection) Close() (err error) {
	c.closeMx.Lock()
	defer c.closeMx.Unlock()
	if !c.closed && c.delegate != nil && !c.delegate.ClosedNow() {
		ctx, cancel := context.WithTimeout(context.Background(), c.connMulti.connOpts.Timeout)
		defer cancel()

		for !c.delegate.ClosedNow() && (ctx.Err() != nil || !c.delegate.InUseNow()) {
			if err = c.delegate.Close(); err != nil {
				log.Printf("Closing connection %s failed: %s\n", c.addr, err.Error())
			}
		}
	}
	c.closed = true
	return
}

func (c *loadBalancedConnection) isReady() bool {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.connMulti.opts.Context.Err() == nil && c._type != "" && c.delegate != nil && c.delegate.ConnectedNow()
}

func (c *loadBalancedConnection) Refresh(newC *loadBalancedConnection) {
	if c.connMulti.opts.Context.Err() != nil {
		return
	}
	if newC != nil && newC._type != "" {
		c.mx.Lock()
		c._type = newC._type
		c.mx.Unlock()
	}
	if c.delegate == nil || !c.delegate.ConnectedNow() {
		c.closeMx.RLock()
		defer c.closeMx.RUnlock()
		if c.delegate != nil && c.closed {
			return
		}
		c.connectMx.Lock()
		defer c.connectMx.Unlock()
		if c.delegate == nil || !c.delegate.ConnectedNow() {
			var err error
			if c.delegate, err = tarantool.Connect(c.addr, c.connMulti.connOpts); err != nil {
				log.Printf("Connection %s failed: %s\n", c.addr, err.Error())
			}
		}
	}
}

func (c *loadBalancedConnection) clusterMemberType() clusterMemberType {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c._type
}

func (connMulti *connectionMulti) stop() (err error) {
	connMulti.opts.Cancel()
	connMulti.lockAll()
	defer connMulti.unlockAll()
	wg := new(sync.WaitGroup)
	for _, connection := range connMulti.groupedByAddressConnections {
		wg.Add(1)
		go func(lbCon *loadBalancedConnection) {
			defer wg.Done()
			if errC := lbCon.Close(); errC != nil && err == nil {
				err = errC
			}
		}(connection)
	}
	wg.Wait()
	connMulti.synchronizeCollections(make(map[string]*loadBalancedConnection))
	return
}

func (connMulti *connectionMulti) start(addresses []string) error {
	conns := make(map[string]*loadBalancedConnection)
	for _, addr := range addresses {
		conns[addr] = &loadBalancedConnection{
			mx:        new(sync.RWMutex),
			closeMx:   new(sync.RWMutex),
			connectMx: new(sync.Mutex),
			connMulti: connMulti,
			addr:      addr,
			_type:     nonWritable,
		}
	}
	connMulti.writableLbMx = new(sync.Mutex)
	connMulti.nonWritableLbMx = new(sync.Mutex)
	connMulti.synchronizeCollections(conns)
	connMulti.sync(conns)
	go connMulti.startMonitoringConnectionStateChanges()
	go connMulti.startMonitoringClusterStructureChanges()

	ctx, cancel := context.WithTimeout(connMulti.opts.Context, connMulti.connOpts.Timeout)
	defer cancel()

	if connMulti.ConnectedNow() {
		for ctx.Err() == nil {
			allGood := true
			connMulti.lockAll()
			for _, connection := range connMulti.groupedByAddressConnections {
				if !connection.isReady() {
					allGood = false
					break
				}
			}
			connMulti.unlockAll()
			if allGood {
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	return ErrStartFailed
}

func (connMulti *connectionMulti) startMonitoringConnectionStateChanges() {
	defer func() { _ = connMulti.stop() }()

	for connMulti.opts.Context.Err() == nil {
		select {
		case <-connMulti.opts.Context.Done():
			return
		case ev, open := <-connMulti.notify:
			if !open || connMulti.opts.Context.Err() != nil {
				return
			}
			connMulti.refresh(&loadBalancedConnection{addr: ev.Conn.Addr()})
		}
	}
}

func (connMulti *connectionMulti) startMonitoringClusterStructureChanges() {
	timer := time.NewTicker(connMulti.opts.CheckTimeout)
	defer func() { _ = connMulti.stop() }()
	defer timer.Stop()

	for connMulti.opts.Context.Err() == nil {
		select {
		case <-connMulti.opts.Context.Done():
			return
		case _, open := <-timer.C:
			if !open || connMulti.opts.Context.Err() != nil {
				return
			}
			var resp [][]map[string]interface{}
			if err := connMulti.Call17Typed(connMulti.opts.NodesGetFunctionName, []interface{}{}, &resp); err == nil && len(resp) > 0 && len(resp[0]) > 0 {
				addrs := resp[0]
				conns := make(map[string]*loadBalancedConnection)
				for _, r := range addrs {
					conns[r["uri"].(string)] = &loadBalancedConnection{
						connMulti: connMulti,
						mx:        new(sync.RWMutex),
						closeMx:   new(sync.RWMutex),
						connectMx: new(sync.Mutex),
						_type:     clusterMemberType(r["type"].(string)),
						addr:      r["uri"].(string),
					}
				}
				connMulti.sync(conns)
			} else {
				log.Printf("Failed to fetch new cluster configuration %v\n", err)
			}
		}
	}
}

func (connMulti *connectionMulti) synchronizeCollections(g map[string]*loadBalancedConnection) {
	connMulti.groupedByAddressConnections = g
	connMulti.writableLoadBalancedConnections = make([]*loadBalancedConnection, 0, len(connMulti.groupedByAddressConnections))
	connMulti.nonWritableLoadBalancedConnections = make([]*loadBalancedConnection, 0, len(connMulti.groupedByAddressConnections))

	for _, connection := range connMulti.groupedByAddressConnections {
		switch connection.clusterMemberType() {
		case nonWritable:
			connMulti.nonWritableLoadBalancedConnections = append(connMulti.nonWritableLoadBalancedConnections, connection)
			break
		case writable:
			connMulti.writableLoadBalancedConnections = append(connMulti.writableLoadBalancedConnections, connection)
			break
		}
	}
}

func (connMulti *connectionMulti) sync(connections map[string]*loadBalancedConnection) {
	connMulti.lockAll()
	defer connMulti.unlockAll()
	for _, n := range connections {
		o, found := connMulti.groupedByAddressConnections[n.addr]
		if !found {
			connMulti.groupedByAddressConnections[n.addr] = n
		}
		go func(old *loadBalancedConnection, new *loadBalancedConnection) {
			if old == nil {
				new.Refresh(nil)
			} else {
				old.Refresh(new)
			}
		}(o, n)
	}

	for _, connection := range connMulti.groupedByAddressConnections {
		if _, found := connections[connection.addr]; !found {
			go func(lbCon *loadBalancedConnection) {
				_ = lbCon.Close()
			}(connection)
			delete(connMulti.groupedByAddressConnections, connection.addr)
		}
	}

	connMulti.synchronizeCollections(connMulti.groupedByAddressConnections)
}

func (connMulti *connectionMulti) refresh(connections ...*loadBalancedConnection) {
	connMulti.lockAll()
	defer connMulti.unlockAll()

	for _, c := range connections {
		if s, found := connMulti.groupedByAddressConnections[c.addr]; found {
			go func(old *loadBalancedConnection, new *loadBalancedConnection) {
				old.Refresh(new)
			}(s, c)
		}
	}
}

func (connMulti *connectionMulti) lockAll() {
	connMulti.writableLbMx.Lock()
	connMulti.nonWritableLbMx.Lock()
}

func (connMulti *connectionMulti) unlockAll() {
	connMulti.writableLbMx.Unlock()
	connMulti.nonWritableLbMx.Unlock()
}

func (connMulti *connectionMulti) roundRobinWritable() *tarantool.Connection {
	if len(connMulti.writableLoadBalancedConnections) == 0 {
		return nil
	}
	connMulti.writableLbMx.Lock()
	defer connMulti.writableLbMx.Unlock()
	if len(connMulti.writableLoadBalancedConnections) == 0 {
		return nil
	}
	noOfConnections := uint8(len(connMulti.writableLoadBalancedConnections))

	for elementsCycled := uint8(0); elementsCycled < noOfConnections; elementsCycled++ {
		connMulti.roundRobinLoadBalancerCurrentWritableIdx = (connMulti.roundRobinLoadBalancerCurrentWritableIdx + 1) % noOfConnections
		p := connMulti.writableLoadBalancedConnections[connMulti.roundRobinLoadBalancerCurrentWritableIdx]
		if p.delegate != nil && p.delegate.ConnectedNow() {
			return p.delegate
		}
	}

	return nil
}

func (connMulti *connectionMulti) roundRobinNonWritable() *tarantool.Connection {
	if len(connMulti.nonWritableLoadBalancedConnections) == 0 {
		return nil
	}
	connMulti.nonWritableLbMx.Lock()
	defer connMulti.nonWritableLbMx.Unlock()
	if len(connMulti.nonWritableLoadBalancedConnections) == 0 {
		return nil
	}
	noOfConnections := uint8(len(connMulti.nonWritableLoadBalancedConnections))

	for elementsCycled := uint8(0); elementsCycled < noOfConnections; elementsCycled++ {
		connMulti.roundRobinLoadBalancerCurrentNonWritableIdx = (connMulti.roundRobinLoadBalancerCurrentNonWritableIdx + 1) % noOfConnections
		p := connMulti.nonWritableLoadBalancedConnections[connMulti.roundRobinLoadBalancerCurrentNonWritableIdx]
		if p.delegate != nil && p.delegate.ConnectedNow() {
			return p.delegate
		}
	}

	return nil
}
