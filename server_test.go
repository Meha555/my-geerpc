package geerpc

import (
	"fmt"
	"reflect"
	"testing"
)

type Foo int

type Args struct{ Num1, Num2 int }

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

// it's not a exported Method
func (f Foo) sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func TestNewService(t *testing.T) {
	s := newService(new(Foo))
	_assert(len(s.methods) == 1, "wrong service Method, expect 1, but got %d", len(s.methods))
	mType := s.methods["Sum"]
	_assert(mType != nil, "wrong Method, Sum shouldn't nil")
	_assert(mType.method.Name == "Sum", "wrong Name, expect Sum, but got %s", mType.method.Name)
}

func TestMethodType_Call(t *testing.T) {
	s := newService(new(Foo))
	mType := s.methods["Sum"]

	argv := mType.newArgv()
	replyv := mType.newReplyv()
	argv.Set(reflect.ValueOf(Args{Num1: 1, Num2: 3}))
	err := s.call(mType, argv, replyv)
	_assert(err == nil && *replyv.Interface().(*int) == 4 && mType.NumCalls() == 1, "failed to call Foo.Sum")
}
