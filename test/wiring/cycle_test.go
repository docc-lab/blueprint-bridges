package wiring

import (
	"strings"
	"testing"

	"github.com/blueprint-uservices/blueprint/plugins/goproc"
	"github.com/blueprint-uservices/blueprint/plugins/grpc"
	"github.com/blueprint-uservices/blueprint/plugins/workflow"
	wf "github.com/blueprint-uservices/blueprint/test/workflow/workflow"

	"github.com/stretchr/testify/assert"
)

/*
Dependency cycles between services. Within one process the namespace breaks the cycle with a forward
reference (plugins/golang/forward); across processes over RPC no forward reference is needed, because the server
side behind an address is built lazily. Each test names the service that closes the cycle before it is defined.
*/

func TestCycleWithinSameProcess(t *testing.T) {
	spec := newWiringSpec("TestCycleWithinSameProcess")

	ping := workflow.Service[wf.TestPingService](spec, "ping", "pong")
	pong := workflow.Service[wf.TestPongService](spec, "pong", ping)

	myproc := goproc.CreateProcess(spec, "myproc", ping, pong)

	app := assertBuildSuccess(t, spec, myproc)
	ir := app.String()
	// The process builds ping's server side first; pong's request for ping reaches it while it is still being built,
	// so the cycle closes there and ping.client, which pong receives, wraps the forward reference.
	assert.Equal(t, 1, strings.Count(ir, "ForwardReference("), ir)
	assert.Contains(t, ir, "ping = TestPingService(pong.client)", ir)
	assert.Contains(t, ir, "pong = TestPongService(ping.client)", ir)
	assert.Contains(t, ir, "ping.client = ping.handler.uniqueness_check.forward", ir)
}

func TestCycleAcrossProcessesOverGRPC(t *testing.T) {
	spec := newWiringSpec("TestCycleAcrossProcessesOverGRPC")

	ping := workflow.Service[wf.TestPingService](spec, "ping", "pong")
	pong := workflow.Service[wf.TestPongService](spec, "pong", ping)
	grpc.Deploy(spec, ping)
	grpc.Deploy(spec, pong)

	pingproc := goproc.CreateProcess(spec, "pingproc", ping)
	pongproc := goproc.CreateProcess(spec, "pongproc", pong)

	app := assertBuildSuccess(t, spec, pingproc, pongproc)
	ir := app.String()
	assert.NotContains(t, ir, "ForwardReference(", ir)
	assert.Contains(t, ir, "ping.grpc_client", ir)
	assert.Contains(t, ir, "pong.grpc_client", ir)
}

func TestCycleOfThreeWithinSameProcess(t *testing.T) {
	spec := newWiringSpec("TestCycleOfThreeWithinSameProcess")

	// ping -> pong -> ping2 -> ping: the forward reference lands where the cycle closes
	ping := workflow.Service[wf.TestPingService](spec, "ping", "pong")
	ping2 := workflow.Service[wf.TestPingService](spec, "ping2", "pong2")
	pong := workflow.Service[wf.TestPongService](spec, "pong", ping2)
	pong2 := workflow.Service[wf.TestPongService](spec, "pong2", ping)

	myproc := goproc.CreateProcess(spec, "myproc", ping, ping2, pong, pong2)

	app := assertBuildSuccess(t, spec, myproc)
	ir := app.String()
	assert.Equal(t, 1, strings.Count(ir, "ForwardReference("), ir)
	assert.Contains(t, ir, "pong2 = TestPongService(ping.client)", ir)
}
