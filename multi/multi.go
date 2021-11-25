package multi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/framey-io/go-tarantool"
)

var (
	ErrEmptyAddrs        = errors.New("addrs should not be empty")
	ErrWrongCheckTimeout = errors.New("wrong check timeout, must be greater than 0")
	ErrNoConnection      = errors.New("no active connections")
	ErrStartFailed       = errors.New("failed to start")
)

type ConnectionMulti struct {
	connOpts tarantool.Opts
	opts     OptsMulti
	notify   <-chan tarantool.ConnEvent
	*lb
}

var _ = tarantool.Connector(&ConnectionMulti{}) // check compatibility with connector interface

type OptsMulti struct {
	CheckTimeout         time.Duration
	NodesGetFunctionName string
	Context              context.Context
	Cancel               context.CancelFunc
}

type BasicAuth struct {
	User, Pass string
}

func ConnectWithDefaults(ctx context.Context, cancel context.CancelFunc, auth BasicAuth, addresses ...string) (*ConnectionMulti, error) {
	conOpts := tarantool.Opts{
		Timeout:       10 * time.Second,
		MaxReconnects: 10,
		User:          auth.User,
		Pass:          auth.Pass,
		Concurrency:   128 * uint32(runtime.GOMAXPROCS(-1)),
	}
	conMultiOpts := OptsMulti{
		CheckTimeout: 2 * time.Second,
		Context:      ctx,
		Cancel:       cancel,
	}
	return ConnectWithOpts(addresses, conOpts, conMultiOpts)
}

func ConnectWithOpts(addrs []string, connOpts tarantool.Opts, opts OptsMulti) (connMulti *ConnectionMulti, err error) {
	if len(addrs) == 0 {
		return nil, ErrEmptyAddrs
	}
	if opts.CheckTimeout <= 0 {
		return nil, ErrWrongCheckTimeout
	}
	if opts.Context == nil {
		return nil, errors.New("OptsMulti.Context is required")
	}
	if opts.Cancel == nil {
		opts.Context, opts.Cancel = context.WithCancel(opts.Context)
	}
	if opts.NodesGetFunctionName == "" {
		opts.NodesGetFunctionName = "get_cluster_members"
	}
	opts.NodesGetFunctionName = fmt.Sprintf("{{%s}}%s", nonWritable, opts.NodesGetFunctionName)
	notify := make(chan tarantool.ConnEvent, 100)
	connOpts.Notify = notify
	connMulti = &ConnectionMulti{
		connOpts: connOpts,
		opts:     opts,
		notify:   notify,
		lb:       new(lb),
	}
	if err = connMulti.start(addrs); err != nil {
		_ = connMulti.Close()
		return nil, err
	}

	return connMulti, nil
}

func (connMulti *ConnectionMulti) getConnection(requiresWrite bool) *tarantool.Connection {
	ctx, cancel := context.WithTimeout(connMulti.opts.Context, connMulti.connOpts.Timeout)
	defer cancel()

	for ctx.Err() == nil {
		if !requiresWrite {
			if c := connMulti.roundRobinNonWritable(); c != nil {
				return c
			}
		}

		if c := connMulti.roundRobinWritable(); c != nil {
			return c
		}
	}

	return nil
}

func (connMulti *ConnectionMulti) ConnectedNow() bool {
	return connMulti.getConnection(true) != nil
}

func (connMulti *ConnectionMulti) Close() (err error) {
	return connMulti.stop()
}

func (connMulti *ConnectionMulti) detectType(s string) (_type clusterMemberType, script string) {
	trimmedS := strings.Trim(s, " ")
	nonWritableTemplate := fmt.Sprintf("{{%s}}", nonWritable)
	writableTemplate := fmt.Sprintf("{{%s}}", writable)

	if isNonWritable := strings.HasPrefix(trimmedS, nonWritableTemplate); isNonWritable {
		return nonWritable, strings.Replace(s, nonWritableTemplate, "", 1)
	}
	if isWritable := strings.HasPrefix(trimmedS, writableTemplate); isWritable {
		return writable, strings.Replace(s, writableTemplate, "", 1)
	}

	return "", s
}

func (connMulti *ConnectionMulti) balance(requiresWrite bool, f func(conn *tarantool.Connection) (*tarantool.Response, error)) (r *tarantool.Response, err error) {
	if connMulti.opts.Context.Err() != nil {
		return nil, connMulti.opts.Context.Err()
	}
	c := connMulti.getConnection(requiresWrite)
	if c == nil {
		return nil, ErrNoConnection
	}
	if r, err = f(c); err != nil && connMulti.shouldRetry(err) {
		ctx, cancel := context.WithTimeout(connMulti.opts.Context, connMulti.connOpts.Timeout)
		defer cancel()
		err = backoff.RetryNotify(
			func() error {
				if ctx.Err() != nil {
					err = ctx.Err()
					return backoff.Permanent(err)
				}
				c = connMulti.getConnection(requiresWrite)
				if c == nil {
					err = ErrNoConnection
					return backoff.Permanent(err)
				}
				if r, err = f(c); err != nil {
					if connMulti.shouldRetry(err) {
						return err
					} else {
						return backoff.Permanent(err)
					}
				}
				return nil
			},
			connMulti.backoff(ctx),
			func(e error, next time.Duration) {
				log.Printf("Call failed with %v. Retrying...", e)
			},
		)
	}
	return
}

func (connMulti *ConnectionMulti) backoff(ctx context.Context) backoff.BackOffContext {
	return backoff.WithContext(&backoff.ExponentialBackOff{
		InitialInterval:     time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          5,
		MaxInterval:         time.Second,
		MaxElapsedTime:      connMulti.connOpts.Timeout,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}, ctx)
}

func (connMulti *ConnectionMulti) shouldRetry(err error) (shouldRetry bool) {
	switch err.(type) {
	case tarantool.ClientError:
		code := err.(tarantool.ClientError).Code
		if code == tarantool.ErrConnectionNotReady ||
			code == tarantool.ErrConnectionClosed {
			shouldRetry = true
		}
		return
	case tarantool.Error:
		code := err.(tarantool.Error).Code
		msg := err.(tarantool.Error).Msg
		if code == tarantool.ErrNonmaster ||
			code == tarantool.ErrReadonly ||
			code == tarantool.ErrUnknownServer ||
			code == tarantool.ErrLocalServerIsNotActive ||
			code == tarantool.ErrClusterIdMismatch ||
			code == tarantool.ErrInvalidOrder ||
			code == tarantool.ErrInvalidXlog ||
			code == tarantool.ErrNoConnection ||
			code == tarantool.ErrActiveTransaction ||
			code == tarantool.ErrMissingSnapshot ||
			code == tarantool.ErrTransactionConflict ||
			code == tarantool.ErrProcC ||
			code == tarantool.ErrUnknown ||
			code == tarantool.ErrMemoryIssue ||
			code == tarantool.ErrClusterIdIsRo ||
			code == tarantool.ErrWalIo ||
			strings.Contains(msg, "read-only") {
			shouldRetry = true
		}
		return
	}
	return
}

func (connMulti *ConnectionMulti) balanceTyped(requiresWrite bool, f func(conn *tarantool.Connection) error) (err error) {
	_, err = connMulti.balance(requiresWrite, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return nil, f(conn)
	})

	return
}

func (connMulti *ConnectionMulti) ConfiguredTimeout() time.Duration {
	return connMulti.getConnection(false).ConfiguredTimeout()
}

func (connMulti *ConnectionMulti) Ping() (resp *tarantool.Response, err error) {
	return connMulti.balance(true, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Ping()
	})
}

func (connMulti *ConnectionMulti) Select(space, index interface{}, offset, limit, iterator uint32, key interface{}) (resp *tarantool.Response, err error) {
	return connMulti.balance(false, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Select(space, index, offset, limit, iterator, key)
	})
}

func (connMulti *ConnectionMulti) Insert(space interface{}, tuple interface{}) (resp *tarantool.Response, err error) {
	return connMulti.balance(true, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Insert(space, tuple)
	})
}

func (connMulti *ConnectionMulti) Replace(space interface{}, tuple interface{}) (resp *tarantool.Response, err error) {
	return connMulti.balance(true, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Replace(space, tuple)
	})
}

func (connMulti *ConnectionMulti) Delete(space, index interface{}, key interface{}) (resp *tarantool.Response, err error) {
	return connMulti.balance(true, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Delete(space, index, key)
	})
}

func (connMulti *ConnectionMulti) Update(space, index interface{}, key, ops interface{}) (resp *tarantool.Response, err error) {
	return connMulti.balance(true, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Update(space, index, key, ops)
	})
}

func (connMulti *ConnectionMulti) Upsert(space interface{}, tuple, ops interface{}) (resp *tarantool.Response, err error) {
	return connMulti.balance(true, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Upsert(space, tuple, ops)
	})
}

func (connMulti *ConnectionMulti) Call(functionName string, args interface{}) (resp *tarantool.Response, err error) {
	t, script := connMulti.detectType(functionName)
	requiresWrite := true
	switch t {
	case nonWritable:
		requiresWrite = false
		break
	}

	return connMulti.balance(requiresWrite, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Call(script, args)
	})
}

func (connMulti *ConnectionMulti) Call17(functionName string, args interface{}) (resp *tarantool.Response, err error) {
	t, script := connMulti.detectType(functionName)
	requiresWrite := true
	switch t {
	case nonWritable:
		requiresWrite = false
		break
	}

	return connMulti.balance(requiresWrite, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Call17(script, args)
	})
}

func (connMulti *ConnectionMulti) Eval(expr string, args interface{}) (resp *tarantool.Response, err error) {
	t, script := connMulti.detectType(expr)
	requiresWrite := true
	switch t {
	case nonWritable:
		requiresWrite = false
		break
	}

	return connMulti.balance(requiresWrite, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Eval(script, args)
	})
}

func (connMulti *ConnectionMulti) GetTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	return connMulti.balanceTyped(false, func(conn *tarantool.Connection) error {
		return conn.GetTyped(space, index, key, result)
	})
}

func (connMulti *ConnectionMulti) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key interface{}, result interface{}) (err error) {
	return connMulti.balanceTyped(false, func(conn *tarantool.Connection) error {
		return conn.SelectTyped(space, index, offset, limit, iterator, key, result)
	})
}

func (connMulti *ConnectionMulti) InsertTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return connMulti.balanceTyped(true, func(conn *tarantool.Connection) error {
		return conn.InsertTyped(space, tuple, result)
	})
}

func (connMulti *ConnectionMulti) ReplaceTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return connMulti.balanceTyped(true, func(conn *tarantool.Connection) error {
		return conn.ReplaceTyped(space, tuple, result)
	})
}

func (connMulti *ConnectionMulti) DeleteTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	return connMulti.balanceTyped(true, func(conn *tarantool.Connection) error {
		return conn.DeleteTyped(space, index, key, result)
	})
}

func (connMulti *ConnectionMulti) UpdateTyped(space, index interface{}, key, ops interface{}, result interface{}) (err error) {
	return connMulti.balanceTyped(true, func(conn *tarantool.Connection) error {
		return conn.UpdateTyped(space, index, key, ops, result)
	})
}

func (connMulti *ConnectionMulti) CallTyped(functionName string, args interface{}, result interface{}) (err error) {
	t, script := connMulti.detectType(functionName)
	requiresWrite := true
	switch t {
	case nonWritable:
		requiresWrite = false
		break
	}

	return connMulti.balanceTyped(requiresWrite, func(conn *tarantool.Connection) error {
		return conn.CallTyped(script, args, result)
	})
}

func (connMulti *ConnectionMulti) Call17Typed(functionName string, args interface{}, result interface{}) (err error) {
	t, script := connMulti.detectType(functionName)
	requiresWrite := true
	switch t {
	case nonWritable:
		requiresWrite = false
		break
	}

	return connMulti.balanceTyped(requiresWrite, func(conn *tarantool.Connection) error {
		return conn.Call17Typed(script, args, result)
	})
}

func (connMulti *ConnectionMulti) EvalTyped(expr string, args interface{}, result interface{}) (err error) {
	t, script := connMulti.detectType(expr)
	requiresWrite := true
	switch t {
	case nonWritable:
		requiresWrite = false
		break
	}

	return connMulti.balanceTyped(requiresWrite, func(conn *tarantool.Connection) error {
		return conn.EvalTyped(script, args, result)
	})
}

// TODO implement the same safety and retry mechanism for futures as well

func (connMulti *ConnectionMulti) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}) *tarantool.Future {
	return connMulti.getConnection(false).SelectAsync(space, index, offset, limit, iterator, key)
}

func (connMulti *ConnectionMulti) InsertAsync(space interface{}, tuple interface{}) *tarantool.Future {
	return connMulti.getConnection(true).InsertAsync(space, tuple)
}

func (connMulti *ConnectionMulti) ReplaceAsync(space interface{}, tuple interface{}) *tarantool.Future {
	return connMulti.getConnection(true).ReplaceAsync(space, tuple)
}

func (connMulti *ConnectionMulti) DeleteAsync(space, index interface{}, key interface{}) *tarantool.Future {
	return connMulti.getConnection(true).DeleteAsync(space, index, key)
}

func (connMulti *ConnectionMulti) UpdateAsync(space, index interface{}, key, ops interface{}) *tarantool.Future {
	return connMulti.getConnection(true).UpdateAsync(space, index, key, ops)
}

func (connMulti *ConnectionMulti) UpsertAsync(space interface{}, tuple interface{}, ops interface{}) *tarantool.Future {
	return connMulti.getConnection(true).UpsertAsync(space, tuple, ops)
}

func (connMulti *ConnectionMulti) CallAsync(functionName string, args interface{}) *tarantool.Future {
	return connMulti.getConnection(true).CallAsync(functionName, args)
}

func (connMulti *ConnectionMulti) Call17Async(functionName string, args interface{}) *tarantool.Future {
	return connMulti.getConnection(true).Call17Async(functionName, args)
}

func (connMulti *ConnectionMulti) EvalAsync(expr string, args interface{}) *tarantool.Future {
	return connMulti.getConnection(true).EvalAsync(expr, args)
}
