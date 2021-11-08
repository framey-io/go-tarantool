package uuid_test

import (
	"fmt"
	"testing"
	"time"

	. "github.com/tarantool/go-tarantool"
	_ "github.com/tarantool/go-tarantool/uuid" 
	"gopkg.in/vmihailenco/msgpack.v2"
	"github.com/google/uuid"
)

var server = "127.0.0.1:3013"
var opts = Opts{
	Timeout: 500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
}

var space = "testUUID"
var index = "primary"

type TupleUUID struct {
	id uuid.UUID
}

func (t *TupleUUID) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 1 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	res, err := d.DecodeInterface()
	if err != nil {
		return err
	}
	t.id = res.(uuid.UUID)
	return nil
}

func connectWithValidation(t *testing.T) *Connection {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
	}
	return conn
}

func skipIfUUIDUnsupported(t *testing.T, conn *Connection) {
	resp, err := conn.Eval("return pcall(require('msgpack').encode, require('uuid').new())", []interface{}{})
	if err != nil {
		t.Errorf("Failed to Eval: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Eval")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}
	val := resp.Data[0].(bool)
	if val != true {
		t.Skip("Skipping test for Tarantool without UUID support in msgpack")
	}
}

func TestSelect(t *testing.T) {
	conn := connectWithValidation(t)
	defer conn.Close()

	skipIfUUIDUnsupported(t, conn)

	id, uuidErr := uuid.Parse("c8f0fa1f-da29-438c-a040-393f1126ad39")
	if uuidErr != nil {
		t.Errorf("Failed to prepare test uuid: %s", uuidErr)
	}

	resp, errSel := conn.Select(space, index, 0, 1, IterEq, []interface{}{ id })
	if errSel != nil {
		t.Errorf("UUID select failed: %s", errSel.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}

	var tuples []TupleUUID
	errTyp := conn.SelectTyped(space, index, 0, 1, IterEq, []interface{}{ id }, &tuples)
	if errTyp != nil {
		t.Errorf("Failed to SelectTyped: %s", errTyp.Error())
	}
	if len(tuples) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	}
	if tuples[0].id != id {
		t.Errorf("Bad value loaded from SelectTyped: %s", tuples[0].id)
	}
}

func TestReplace(t *testing.T) {
	conn := connectWithValidation(t)
	defer conn.Close()

	skipIfUUIDUnsupported(t, conn)

	id, uuidErr := uuid.Parse("64d22e4d-ac92-4a23-899a-e59f34af5479")
	if uuidErr != nil {
		t.Errorf("Failed to prepare test uuid: %s", uuidErr)
	}

	_, errRep := conn.Replace(space, []interface{}{ id })
	if errRep != nil {
		t.Errorf("UUID replace failed: %s", errRep)
	}

	resp, errSel := conn.Select(space, index, 0, 1, IterEq, []interface{}{ id })
	if errSel != nil {
		t.Errorf("UUID select failed: %s", errSel)
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
}
