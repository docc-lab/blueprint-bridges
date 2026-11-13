package workflow

import (
	"context"
	ctxx "context"
)

/*
Some simple services used for testing.

TestNonLeafService calls TestLeafService

No backend components are used.
*/

/*
Workflow services
*/
type (
	TestLeafService interface {
		HelloNothing(ctx ctxx.Context) error
		HelloInt(ctx context.Context, a int16) (int32, error)
		HelloObject(ctxt context.Context, obj TestLeafObject) (*TestLeafObject, error)
	}

	TestNonLeafService interface {
		Hello(ctx context.Context, a TestMyInt) (int64, error)
	}
)

/*
Types used by services
*/
type (
	TestMyInt int64

	TestNestedLeafObject struct {
		Key   string
		Value string
		Props []string
	}

	TestLeafObject struct {
		ID    int64
		Name  string
		Count int
		Props map[string]TestNestedLeafObject
	}
)

/*
Service implementation structs
*/
type (
	TestLeafServiceImpl struct {
		TestLeafService
	}

	TestNonLeafServiceImpl struct {
		TestNonLeafService
		leaf  TestLeafService
		count int
	}
)

/*
Constructors
*/

func NewNonLeafServiceImpl(ctx context.Context, leafService TestLeafService) (TestNonLeafService, error) {
	return &TestNonLeafServiceImpl{leaf: leafService}, nil
}

func NewLeafServiceImpl(ctx ctxx.Context) (*TestLeafServiceImpl, error) {
	return &TestLeafServiceImpl{}, nil
}

/*
Interface method bodies
*/

func (l *TestLeafServiceImpl) HelloNothing(ctx ctxx.Context) error {
	return nil
}

func (l *TestLeafServiceImpl) HelloInt(ctx ctxx.Context, a int16) (int32, error) {
	return int32(a * 2), nil
}

func (l *TestLeafServiceImpl) HelloObject(ctx ctxx.Context, obj TestLeafObject) (*TestLeafObject, error) {
	obj.Count += 10
	return &obj, nil
}

func (nl *TestNonLeafServiceImpl) Hello(ctx context.Context, a TestMyInt) (int64, error) {
	b, err := nl.leaf.HelloInt(ctx, int16(a))
	if err != nil {
		return 0, err
	}

	obj := TestLeafObject{
		ID:    int64(b),
		Name:  "test leaf object",
		Count: nl.count,
		Props: map[string]TestNestedLeafObject{
			"first": TestNestedLeafObject{
				Key:   "filter",
				Value: "non-chill filtered",
				Props: []string{"a1", "b1"},
			},
			"second": TestNestedLeafObject{
				Key:   "color",
				Value: "natural color",
				Props: []string{"c2", "d2"},
			},
		},
	}

	obj2, err := nl.leaf.HelloObject(ctx, obj)
	nl.count = obj2.Count

	return int64(nl.count), nil
}

/*
Non-interface functions
*/

func (l *TestLeafServiceImpl) NonServiceFunction() int64 {
	return 3
}

func (l *TestLeafServiceImpl) privateNonServiceFunction() int64 {
	return 7
}

// Tomislav-RetCtx: TestPingService and TestPongService depend on each other (a dependency cycle), for the cycle tests in
// test/wiring/cycle_test.go.
type (
	TestPingService interface {
		Ping(ctx context.Context, n int64) (int64, error)
	}

	TestPongService interface {
		Pong(ctx context.Context, n int64) (int64, error)
	}

	TestPingServiceImpl struct {
		pong TestPongService
	}

	TestPongServiceImpl struct {
		ping TestPingService
	}
)

func NewTestPingServiceImpl(ctx context.Context, pong TestPongService) (TestPingService, error) {
	return &TestPingServiceImpl{pong: pong}, nil
}

func NewTestPongServiceImpl(ctx context.Context, ping TestPingService) (TestPongService, error) {
	return &TestPongServiceImpl{ping: ping}, nil
}

func (s *TestPingServiceImpl) Ping(ctx context.Context, n int64) (int64, error) {
	if n <= 0 {
		return 0, nil
	}
	r, err := s.pong.Pong(ctx, n-1)
	return r + 1, err
}

func (s *TestPongServiceImpl) Pong(ctx context.Context, n int64) (int64, error) {
	if n <= 0 {
		return 0, nil
	}
	r, err := s.ping.Ping(ctx, n-1)
	return r + 1, err
}
