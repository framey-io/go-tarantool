package multi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"
	"unicode"

	"github.com/cenkalti/backoff/v4"
	"github.com/framey-io/go-tarantool"
)

var (
	ErrEmptyAddrs        = errors.New("addrs should not be empty")
	ErrWrongCheckTimeout = errors.New("wrong check timeout, must be greater than 0")
	ErrNoConnection      = errors.New("no active connections")
	ErrStartFailed       = errors.New("failed to start")
)

type connectionMulti struct {
	connOpts tarantool.Opts
	opts     OptsMulti
	notify   <-chan tarantool.ConnEvent
	*lb
}

var _ = tarantool.Connector(&connectionMulti{}) // check compatibility with connector interface

type OptsMulti struct {
	CheckTimeout         time.Duration
	NodesGetFunctionName string
	RequiresWrite        bool
	Context              context.Context
	Cancel               context.CancelFunc
}

type BasicAuth struct {
	User, Pass string
}

func ConnectWithDefaults(ctx context.Context, cancel context.CancelFunc, auth BasicAuth, addresses ...string) (tarantool.Connector, error) {
	return ConnectWithWritableAwareDefaults(ctx, cancel, true, auth, addresses...)
}

func ConnectToReadOnlyClusterWithDefaults(ctx context.Context, cancel context.CancelFunc, auth BasicAuth, addresses ...string) (tarantool.Connector, error) {
	return ConnectWithWritableAwareDefaults(ctx, cancel, false, auth, addresses...)
}

func ConnectWithWritableAwareDefaults(ctx context.Context, cancel context.CancelFunc, requiresWrite bool, auth BasicAuth, addresses ...string) (tarantool.Connector, error) {
	conOpts := tarantool.Opts{
		Timeout:       10 * time.Second,
		MaxReconnects: 10,
		User:          auth.User,
		Pass:          auth.Pass,
		Concurrency:   128 * uint32(runtime.GOMAXPROCS(-1)),
	}
	conMultiOpts := OptsMulti{
		CheckTimeout:  2 * time.Second,
		Context:       ctx,
		Cancel:        cancel,
		RequiresWrite: requiresWrite,
	}
	return ConnectWithOpts(addresses, conOpts, conMultiOpts)
}

func ConnectWithOpts(addrs []string, connOpts tarantool.Opts, opts OptsMulti) (connMulti tarantool.Connector, err error) {
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
	connMulti = &connectionMulti{
		connOpts: connOpts,
		opts:     opts,
		notify:   notify,
		lb:       new(lb),
	}
	if err = connMulti.(*connectionMulti).start(addrs); err != nil {
		_ = connMulti.Close()
		return nil, err
	}

	return connMulti, nil
}

func (connMulti *connectionMulti) getConnection(requiresWrite bool) *tarantool.Connection {
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

func (connMulti *connectionMulti) ConnectedNow() bool {
	return connMulti.getConnection(connMulti.opts.RequiresWrite) != nil
}

func (connMulti *connectionMulti) Close() (err error) {
	return connMulti.stop()
}

func (connMulti *connectionMulti) checkIfRequiresWrite(expr string, _default bool) (string, bool) {
	trimmedS := strings.ToLower(strings.TrimLeftFunc(expr, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}))
	nonWritableTemplate := fmt.Sprintf("{{%s}}", nonWritable)
	if isNonWritable := strings.HasPrefix(trimmedS, nonWritableTemplate); isNonWritable {
		return strings.Replace(expr, nonWritableTemplate, "", 1), false
	}

	writableTemplate := fmt.Sprintf("{{%s}}", writable)
	if isWritable := strings.HasPrefix(trimmedS, writableTemplate); isWritable {
		return strings.Replace(expr, writableTemplate, "", 1), true
	}

	if isWritable := strings.HasPrefix(trimmedS, "insert"); isWritable {
		return expr, true
	}

	if isWritable := strings.HasPrefix(trimmedS, "delete"); isWritable {
		return expr, true
	}

	if isWritable := strings.HasPrefix(trimmedS, "update"); isWritable {
		return expr, true
	}

	if isWritable := strings.HasPrefix(trimmedS, "replace"); isWritable {
		return expr, true
	}

	if isNonWritable := strings.HasPrefix(trimmedS, "values"); isNonWritable {
		return expr, false
	}

	if isNonWritable := strings.HasPrefix(trimmedS, "with"); isNonWritable {
		return expr, false
	}

	if isNonWritable := strings.HasPrefix(trimmedS, "select"); isNonWritable {
		return expr, false
	}

	return expr, _default
}

func (connMulti *connectionMulti) balance(requiresWrite bool, f func(conn *tarantool.Connection) (*tarantool.Response, error)) (r *tarantool.Response, err error) {
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

func (connMulti *connectionMulti) backoff(ctx context.Context) backoff.BackOffContext {
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

func (connMulti *connectionMulti) shouldRetry(err error) (shouldRetry bool) {
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
		if code == tarantool.ER_NONMASTER ||
			code == tarantool.ER_READONLY ||
			code == tarantool.ER_TUPLE_FORMAT_LIMIT ||
			code == tarantool.ER_UNKNOWN ||
			code == tarantool.ER_MEMORY_ISSUE ||
			code == tarantool.ER_UNKNOWN_REPLICA ||
			code == tarantool.ER_REPLICASET_UUID_MISMATCH ||
			code == tarantool.ER_REPLICASET_UUID_IS_RO ||
			code == tarantool.ER_REPLICA_ID_IS_RESERVED ||
			code == tarantool.ER_REPLICA_MAX ||
			code == tarantool.ER_INVALID_XLOG ||
			code == tarantool.ER_NO_CONNECTION ||
			code == tarantool.ER_ACTIVE_TRANSACTION ||
			code == tarantool.ER_SESSION_CLOSED ||
			code == tarantool.ER_TRANSACTION_CONFLICT ||
			code == tarantool.ER_MEMTX_MAX_TUPLE_SIZE ||
			code == tarantool.ER_VIEW_IS_RO ||
			code == tarantool.ER_NO_TRANSACTION ||
			code == tarantool.ER_SYSTEM ||
			code == tarantool.ER_LOADING ||
			code == tarantool.ER_LOCAL_INSTANCE_ID_IS_READ_ONLY ||
			code == tarantool.ER_BACKUP_IN_PROGRESS ||
			code == tarantool.ER_READ_VIEW_ABORTED ||
			code == tarantool.ER_CASCADE_ROLLBACK ||
			code == tarantool.ER_VY_QUOTA_TIMEOUT ||
			code == tarantool.ER_TRANSACTION_YIELD ||
			code == tarantool.ER_BOOTSTRAP_READONLY ||
			code == tarantool.ER_REPLICA_NOT_ANON ||
			code == tarantool.ER_CANNOT_REGISTER ||
			code == tarantool.ER_UNCOMMITTED_FOREIGN_SYNC_TXNS ||
			code == tarantool.ER_SYNC_MASTER_MISMATCH ||
			code == tarantool.ER_SYNC_QUORUM_TIMEOUT ||
			code == tarantool.ER_SYNC_ROLLBACK ||
			code == tarantool.ER_QUORUM_WAIT ||
			code == tarantool.ER_TOO_EARLY_SUBSCRIBE ||
			code == tarantool.ER_INTERFERING_PROMOTE ||
			code == tarantool.ER_ELECTION_DISABLED ||
			code == tarantool.ER_TXN_ROLLBACK ||
			code == tarantool.ER_NOT_LEADER ||
			code == tarantool.ER_SYNC_QUEUE_UNCLAIMED ||
			code == tarantool.ER_SYNC_QUEUE_FOREIGN ||
			code == tarantool.ER_WAL_IO ||
			strings.Contains(msg, "read-only") {
			shouldRetry = true
		}
		return
	}
	return
}

func (connMulti *connectionMulti) balanceTyped(requiresWrite bool, f func(conn *tarantool.Connection) error) (err error) {
	_, err = connMulti.balance(requiresWrite, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return nil, f(conn)
	})

	return
}

func (connMulti *connectionMulti) ConfiguredTimeout() time.Duration {
	return connMulti.getConnection(false).ConfiguredTimeout()
}

func (connMulti *connectionMulti) Ping() (resp *tarantool.Response, err error) {
	return connMulti.balance(connMulti.opts.RequiresWrite, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Ping()
	})
}

func (connMulti *connectionMulti) Select(space, index interface{}, offset, limit, iterator uint32, key interface{}) (resp *tarantool.Response, err error) {
	return connMulti.balance(false, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Select(space, index, offset, limit, iterator, key)
	})
}

func (connMulti *connectionMulti) Insert(space interface{}, tuple interface{}) (resp *tarantool.Response, err error) {
	return connMulti.balance(true, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Insert(space, tuple)
	})
}

func (connMulti *connectionMulti) Replace(space interface{}, tuple interface{}) (resp *tarantool.Response, err error) {
	return connMulti.balance(true, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Replace(space, tuple)
	})
}

func (connMulti *connectionMulti) Delete(space, index interface{}, key interface{}) (resp *tarantool.Response, err error) {
	return connMulti.balance(true, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Delete(space, index, key)
	})
}

func (connMulti *connectionMulti) Update(space, index interface{}, key, ops interface{}) (resp *tarantool.Response, err error) {
	return connMulti.balance(true, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Update(space, index, key, ops)
	})
}

func (connMulti *connectionMulti) Upsert(space interface{}, tuple, ops interface{}) (resp *tarantool.Response, err error) {
	return connMulti.balance(true, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Upsert(space, tuple, ops)
	})
}

func (connMulti *connectionMulti) Call(functionName string, args interface{}) (resp *tarantool.Response, err error) {
	script, requiresWrite := connMulti.checkIfRequiresWrite(functionName, true)

	return connMulti.balance(requiresWrite, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Call(script, args)
	})
}

func (connMulti *connectionMulti) Call17(functionName string, args interface{}) (resp *tarantool.Response, err error) {
	script, requiresWrite := connMulti.checkIfRequiresWrite(functionName, true)

	return connMulti.balance(requiresWrite, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Call17(script, args)
	})
}

func (connMulti *connectionMulti) Eval(expr string, args interface{}) (resp *tarantool.Response, err error) {
	script, requiresWrite := connMulti.checkIfRequiresWrite(expr, true)

	return connMulti.balance(requiresWrite, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.Eval(script, args)
	})
}

func (connMulti *connectionMulti) PrepareExecute(sql string, args map[string]interface{}) (resp *tarantool.Response, err error) {
	script, requiresWrite := connMulti.checkIfRequiresWrite(sql, true)

	return connMulti.balance(requiresWrite, func(conn *tarantool.Connection) (*tarantool.Response, error) {
		return conn.PrepareExecute(script, args)
	})
}

func (connMulti *connectionMulti) GetTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	return connMulti.balanceTyped(false, func(conn *tarantool.Connection) error {
		return conn.GetTyped(space, index, key, result)
	})
}

func (connMulti *connectionMulti) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key interface{}, result interface{}) (err error) {
	return connMulti.balanceTyped(false, func(conn *tarantool.Connection) error {
		return conn.SelectTyped(space, index, offset, limit, iterator, key, result)
	})
}

func (connMulti *connectionMulti) InsertTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return connMulti.balanceTyped(true, func(conn *tarantool.Connection) error {
		return conn.InsertTyped(space, tuple, result)
	})
}

func (connMulti *connectionMulti) ReplaceTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return connMulti.balanceTyped(true, func(conn *tarantool.Connection) error {
		return conn.ReplaceTyped(space, tuple, result)
	})
}

func (connMulti *connectionMulti) DeleteTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	return connMulti.balanceTyped(true, func(conn *tarantool.Connection) error {
		return conn.DeleteTyped(space, index, key, result)
	})
}

func (connMulti *connectionMulti) UpdateTyped(space, index interface{}, key, ops interface{}, result interface{}) (err error) {
	return connMulti.balanceTyped(true, func(conn *tarantool.Connection) error {
		return conn.UpdateTyped(space, index, key, ops, result)
	})
}

func (connMulti *connectionMulti) UpsertTyped(space, tuple, ops, result interface{}) (err error) {
	return connMulti.balanceTyped(true, func(conn *tarantool.Connection) error {
		return conn.UpsertTyped(space, tuple, ops, result)
	})
}

func (connMulti *connectionMulti) CallTyped(functionName string, args interface{}, result interface{}) (err error) {
	script, requiresWrite := connMulti.checkIfRequiresWrite(functionName, true)

	return connMulti.balanceTyped(requiresWrite, func(conn *tarantool.Connection) error {
		return conn.CallTyped(script, args, result)
	})
}

func (connMulti *connectionMulti) Call17Typed(functionName string, args interface{}, result interface{}) (err error) {
	script, requiresWrite := connMulti.checkIfRequiresWrite(functionName, true)

	return connMulti.balanceTyped(requiresWrite, func(conn *tarantool.Connection) error {
		return conn.Call17Typed(script, args, result)
	})
}

func (connMulti *connectionMulti) EvalTyped(expr string, args interface{}, result interface{}) (err error) {
	script, requiresWrite := connMulti.checkIfRequiresWrite(expr, true)

	return connMulti.balanceTyped(requiresWrite, func(conn *tarantool.Connection) error {
		return conn.EvalTyped(script, args, result)
	})
}

func (connMulti *connectionMulti) PrepareExecuteTyped(sql string, args map[string]interface{}, result interface{}) (err error) {
	script, requiresWrite := connMulti.checkIfRequiresWrite(sql, true)

	return connMulti.balanceTyped(requiresWrite, func(conn *tarantool.Connection) error {
		return conn.PrepareExecuteTyped(script, args, result)
	})
}

// TODO implement the same safety and retry mechanism for futures as well

func (connMulti *connectionMulti) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}) *tarantool.Future {
	return connMulti.getConnection(false).SelectAsync(space, index, offset, limit, iterator, key)
}

func (connMulti *connectionMulti) InsertAsync(space interface{}, tuple interface{}) *tarantool.Future {
	return connMulti.getConnection(true).InsertAsync(space, tuple)
}

func (connMulti *connectionMulti) ReplaceAsync(space interface{}, tuple interface{}) *tarantool.Future {
	return connMulti.getConnection(true).ReplaceAsync(space, tuple)
}

func (connMulti *connectionMulti) DeleteAsync(space, index interface{}, key interface{}) *tarantool.Future {
	return connMulti.getConnection(true).DeleteAsync(space, index, key)
}

func (connMulti *connectionMulti) UpdateAsync(space, index interface{}, key, ops interface{}) *tarantool.Future {
	return connMulti.getConnection(true).UpdateAsync(space, index, key, ops)
}

func (connMulti *connectionMulti) UpsertAsync(space interface{}, tuple interface{}, ops interface{}) *tarantool.Future {
	return connMulti.getConnection(true).UpsertAsync(space, tuple, ops)
}

func (connMulti *connectionMulti) CallAsync(functionName string, args interface{}) *tarantool.Future {
	script, requiresWrite := connMulti.checkIfRequiresWrite(functionName, true)
	return connMulti.getConnection(requiresWrite).CallAsync(script, args)
}

func (connMulti *connectionMulti) Call17Async(functionName string, args interface{}) *tarantool.Future {
	script, requiresWrite := connMulti.checkIfRequiresWrite(functionName, true)
	return connMulti.getConnection(requiresWrite).Call17Async(script, args)
}

func (connMulti *connectionMulti) EvalAsync(expr string, args interface{}) *tarantool.Future {
	script, requiresWrite := connMulti.checkIfRequiresWrite(expr, true)
	return connMulti.getConnection(requiresWrite).EvalAsync(script, args)
}
