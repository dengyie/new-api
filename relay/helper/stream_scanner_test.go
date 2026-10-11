package helper

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dengyie/apihub/constant"
	"github.com/dengyie/apihub/loadbalancer"
	relaycommon "github.com/dengyie/apihub/relay/common"
	"github.com/dengyie/apihub/setting/operation_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	gin.SetMode(gin.TestMode)
	if constant.StreamingTimeout == 0 {
		constant.StreamingTimeout = 30
	}
}

func setupStreamTest(t *testing.T, body io.Reader) (*gin.Context, *http.Response, *relaycommon.RelayInfo) {
	t.Helper()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	resp := &http.Response{
		Body: io.NopCloser(body),
	}

	info := &relaycommon.RelayInfo{
		ChannelMeta: &relaycommon.ChannelMeta{},
	}

	return c, resp, info
}

func buildSSEBody(n int) string {
	var b strings.Builder
	for i := range n {
		fmt.Fprintf(&b, "data: {\"id\":%d,\"choices\":[{\"delta\":{\"content\":\"token_%d\"}}]}\n", i, i)
	}
	b.WriteString("data: [DONE]\n")
	return b.String()
}

// ---------- Basic correctness ----------

func TestStreamScannerHandler_NilInputs(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)

	info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{}}

	StreamScannerHandler(c, nil, info, func(data string, sr *StreamResult) {})
	StreamScannerHandler(c, &http.Response{Body: io.NopCloser(strings.NewReader(""))}, info, nil)
}

func TestNewStreamScanner_AllowsLargeStreamLine(t *testing.T) {
	oldBufferMB := constant.StreamScannerMaxBufferMB
	constant.StreamScannerMaxBufferMB = 1
	t.Cleanup(func() {
		constant.StreamScannerMaxBufferMB = oldBufferMB
	})

	payload := strings.Repeat("x", 128<<10)
	scanner := NewStreamScanner(strings.NewReader("data: " + payload + "\n"))
	scanner.Split(bufio.ScanLines)

	require.True(t, scanner.Scan())
	assert.Equal(t, "data: "+payload, scanner.Text())
	require.NoError(t, scanner.Err())
}

func TestStreamScannerHandler_EmptyBody(t *testing.T) {
	t.Parallel()

	c, resp, info := setupStreamTest(t, strings.NewReader(""))

	var called atomic.Bool
	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		called.Store(true)
	})

	assert.False(t, called.Load(), "handler should not be called for empty body")
}

func TestStreamScannerHandler_1000Chunks(t *testing.T) {
	t.Parallel()

	const numChunks = 1000
	body := buildSSEBody(numChunks)
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	var count atomic.Int64
	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		count.Add(1)
	})

	assert.Equal(t, int64(numChunks), count.Load())
	assert.Equal(t, numChunks, info.ReceivedResponseCount)
}

func TestStreamScannerHandler_OrderPreserved(t *testing.T) {
	t.Parallel()

	const numChunks = 500
	body := buildSSEBody(numChunks)
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	var mu sync.Mutex
	received := make([]string, 0, numChunks)

	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		mu.Lock()
		received = append(received, data)
		mu.Unlock()
	})

	require.Equal(t, numChunks, len(received))
	for i := range numChunks {
		expected := fmt.Sprintf("{\"id\":%d,\"choices\":[{\"delta\":{\"content\":\"token_%d\"}}]}", i, i)
		assert.Equal(t, expected, received[i], "chunk %d out of order", i)
	}
}

func TestStreamScannerHandler_DoneStopsScanner(t *testing.T) {
	t.Parallel()

	body := buildSSEBody(50) + "data: should_not_appear\n"
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	var count atomic.Int64
	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		count.Add(1)
	})

	assert.Equal(t, int64(50), count.Load(), "data after [DONE] must not be processed")
}

func TestStreamScannerHandler_StopStopsStream(t *testing.T) {
	t.Parallel()

	const numChunks = 200
	body := buildSSEBody(numChunks)
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	const stopAt int64 = 50
	var count atomic.Int64
	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		n := count.Add(1)
		if n >= stopAt {
			sr.Stop(fmt.Errorf("fatal at %d", n))
		}
	})

	assert.Equal(t, stopAt, count.Load())
	require.NotNil(t, info.StreamStatus)
	assert.Equal(t, relaycommon.StreamEndReasonHandlerStop, info.StreamStatus.EndReason)
}

func TestStreamScannerHandler_SkipsNonDataLines(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString(": comment line\n")
	b.WriteString("event: message\n")
	b.WriteString("id: 12345\n")
	b.WriteString("retry: 5000\n")
	for i := range 100 {
		fmt.Fprintf(&b, "data: payload_%d\n", i)
		b.WriteString(": interleaved comment\n")
	}
	b.WriteString("data: [DONE]\n")

	c, resp, info := setupStreamTest(t, strings.NewReader(b.String()))

	var count atomic.Int64
	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		count.Add(1)
	})

	assert.Equal(t, int64(100), count.Load())
}

func TestStreamScannerHandler_DataWithExtraSpaces(t *testing.T) {
	t.Parallel()

	body := "data:   {\"trimmed\":true}  \ndata: [DONE]\n"
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	var got string
	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		got = data
	})

	assert.Equal(t, "{\"trimmed\":true}", got)
}

// TestStreamScannerHandler_ClientCancelAbortsUpstreamAndReturns pins the
// disconnect contract: when the client goes away, the handler must return
// promptly (all goroutines joined, so the gin.Context can never leak into a
// pooled reuse), the upstream body must be closed to stop token generation,
// and no data received after the disconnect may be processed or written.
func TestStreamScannerHandler_ClientCancelAbortsUpstreamAndReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pr, pw := io.Pipe()
	t.Cleanup(func() {
		_ = pr.Close()
		_ = pw.Close()
	})

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)

	resp := &http.Response{Body: pr}
	info := &relaycommon.RelayInfo{
		DisablePing: true,
		ChannelMeta: &relaycommon.ChannelMeta{},
	}

	var count atomic.Int64
	firstHandled := make(chan struct{})
	done := make(chan struct{})
	go func() {
		StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
			count.Add(1)
			_ = StringData(c, data)
			if data == "first" {
				close(firstHandled)
			}
		})
		close(done)
	}()

	_, err := fmt.Fprint(pw, "data: first\n")
	require.NoError(t, err)

	select {
	case <-firstHandled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first chunk")
	}

	cancel()

	// The handler must return without any further upstream input: cleanup
	// closes resp.Body, which unblocks the scanner goroutine.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after client disconnect")
	}

	// Upstream read side must be closed so the provider stops generating
	// (and billing) for a request nobody is listening to.
	_, err = fmt.Fprint(pw, "data: second\n")
	require.ErrorIs(t, err, io.ErrClosedPipe, "upstream body should be closed after client disconnect")

	assert.Equal(t, int64(1), count.Load(), "no chunk after disconnect should be processed")
	require.NotNil(t, info.StreamStatus)
	assert.Equal(t, relaycommon.StreamEndReasonClientGone, info.StreamStatus.EndReason)

	body := recorder.Body.String()
	assert.Contains(t, body, "first")
	assert.NotContains(t, body, "second")
}

// ---------- Ping tests ----------

func TestStreamScannerHandler_PingSentDuringSlowUpstream(t *testing.T) {
	setting := operation_setting.GetGeneralSetting()
	oldEnabled := setting.PingIntervalEnabled
	oldSeconds := setting.PingIntervalSeconds
	setting.PingIntervalEnabled = true
	setting.PingIntervalSeconds = 1
	t.Cleanup(func() {
		setting.PingIntervalEnabled = oldEnabled
		setting.PingIntervalSeconds = oldSeconds
	})

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		for i := range 4 {
			fmt.Fprintf(pw, "data: chunk_%d\n", i)
			time.Sleep(400 * time.Millisecond)
		}
		fmt.Fprint(pw, "data: [DONE]\n")
	}()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	resp := &http.Response{Body: pr}
	info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{}}

	var count atomic.Int64
	done := make(chan struct{})
	go func() {
		StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
			count.Add(1)
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream to finish")
	}

	assert.Equal(t, int64(4), count.Load())

	body := recorder.Body.String()
	pingCount := strings.Count(body, ": PING")
	assert.GreaterOrEqual(t, pingCount, 1,
		"expected at least 1 ping during slow stream with 1s interval; got %d", pingCount)
}

func TestStreamScannerHandler_PingDisabledByRelayInfo(t *testing.T) {
	setting := operation_setting.GetGeneralSetting()
	oldEnabled := setting.PingIntervalEnabled
	oldSeconds := setting.PingIntervalSeconds
	setting.PingIntervalEnabled = true
	setting.PingIntervalSeconds = 1
	t.Cleanup(func() {
		setting.PingIntervalEnabled = oldEnabled
		setting.PingIntervalSeconds = oldSeconds
	})

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	resp := &http.Response{Body: io.NopCloser(strings.NewReader(buildSSEBody(5)))}
	info := &relaycommon.RelayInfo{
		DisablePing: true,
		ChannelMeta: &relaycommon.ChannelMeta{},
	}

	var count atomic.Int64
	done := make(chan struct{})
	go func() {
		StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
			count.Add(1)
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}

	assert.Equal(t, int64(5), count.Load())

	body := recorder.Body.String()
	pingCount := strings.Count(body, ": PING")
	assert.Equal(t, 0, pingCount, "pings should be disabled when DisablePing=true")
}

// ---------- StreamStatus integration ----------

func TestStreamScannerHandler_StreamStatus_DoneReason(t *testing.T) {
	t.Parallel()

	body := buildSSEBody(10)
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {})

	require.NotNil(t, info.StreamStatus)
	assert.Equal(t, relaycommon.StreamEndReasonDone, info.StreamStatus.EndReason)
	assert.Nil(t, info.StreamStatus.EndError)
	assert.True(t, info.StreamStatus.IsNormalEnd())
	assert.False(t, info.StreamStatus.HasErrors())
}

func TestStreamScannerHandler_StreamStatus_EOFWithoutDone(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	for i := range 5 {
		fmt.Fprintf(&b, "data: {\"id\":%d}\n", i)
	}
	c, resp, info := setupStreamTest(t, strings.NewReader(b.String()))

	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {})

	require.NotNil(t, info.StreamStatus)
	assert.Equal(t, relaycommon.StreamEndReasonEOF, info.StreamStatus.EndReason)
	assert.True(t, info.StreamStatus.IsNormalEnd())
}

func TestStreamScannerHandler_StreamStatus_EOFWithoutDone_RequiresTerminal(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	for i := range 5 {
		fmt.Fprintf(&b, "data: {\"choices\":[{\"delta\":{\"content\":\"token %d\"}}]}\n\n", i)
	}
	c, resp, info := setupStreamTest(t, strings.NewReader(b.String()))
	info.StreamStatus = relaycommon.NewStreamStatus()
	info.StreamStatus.RequireTerminal()

	err := StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {})

	require.Error(t, err, "当协议要求终止帧而上游无 [DONE] 即 EOF 时，应返回流中断错误")
	var brokenErr *loadbalancer.StreamBrokenError
	require.ErrorAs(t, err, &brokenErr)
	require.NotNil(t, info.StreamStatus)
	assert.Equal(t, relaycommon.StreamEndReasonEOF, info.StreamStatus.EndReason)
	assert.False(t, info.StreamStatus.IsNormalEnd())
	assert.True(t, info.StreamStatus.IsUpstreamStreamFault())
}

func TestStreamScannerHandler_StreamStatus_HandlerStop(t *testing.T) {
	t.Parallel()

	body := buildSSEBody(100)
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	var count atomic.Int64
	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		n := count.Add(1)
		if n >= 10 {
			sr.Stop(fmt.Errorf("stop at 10"))
		}
	})

	require.NotNil(t, info.StreamStatus)
	assert.Equal(t, relaycommon.StreamEndReasonHandlerStop, info.StreamStatus.EndReason)
	assert.True(t, info.StreamStatus.HasErrors())
}

func TestStreamScannerHandler_StreamStatus_HandlerDone(t *testing.T) {
	t.Parallel()

	body := buildSSEBody(20)
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	var count atomic.Int64
	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		n := count.Add(1)
		if n >= 5 {
			sr.Done()
		}
	})

	assert.Equal(t, int64(5), count.Load())
	require.NotNil(t, info.StreamStatus)
	assert.Equal(t, relaycommon.StreamEndReasonDone, info.StreamStatus.EndReason)
	assert.False(t, info.StreamStatus.HasErrors())
}

func TestStreamScannerHandler_StreamStatus_Timeout(t *testing.T) {
	// Not parallel: modifies global constant.StreamingTimeout
	oldTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 1
	t.Cleanup(func() { constant.StreamingTimeout = oldTimeout })

	pr, pw := io.Pipe()
	go func() {
		fmt.Fprint(pw, "data: {\"id\":1}\n")
		time.Sleep(2 * time.Second)
		pw.Close()
	}()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	resp := &http.Response{Body: pr}
	info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{}}

	done := make(chan struct{})
	go func() {
		StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream timeout")
	}

	require.NotNil(t, info.StreamStatus)
	assert.Equal(t, relaycommon.StreamEndReasonTimeout, info.StreamStatus.EndReason)
	assert.False(t, info.StreamStatus.IsNormalEnd())
}

func TestStreamScannerHandler_StreamStatus_SoftErrors(t *testing.T) {
	t.Parallel()

	body := buildSSEBody(10)
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		sr.Error(fmt.Errorf("soft error for chunk"))
	})

	require.NotNil(t, info.StreamStatus)
	assert.Equal(t, relaycommon.StreamEndReasonDone, info.StreamStatus.EndReason)
	assert.True(t, info.StreamStatus.HasErrors())
	assert.Equal(t, 10, info.StreamStatus.TotalErrorCount())
}

func TestStreamScannerHandler_StreamStatus_MultipleErrorsPerChunk(t *testing.T) {
	t.Parallel()

	body := buildSSEBody(5)
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		sr.Error(fmt.Errorf("error A"))
		sr.Error(fmt.Errorf("error B"))
	})

	require.NotNil(t, info.StreamStatus)
	assert.Equal(t, relaycommon.StreamEndReasonDone, info.StreamStatus.EndReason)
	assert.Equal(t, 10, info.StreamStatus.TotalErrorCount())
}

func TestStreamScannerHandler_StreamStatus_ErrorThenStop(t *testing.T) {
	t.Parallel()

	// Use a large body without [DONE] to avoid race between scanner's [DONE]
	// and handler's Stop on the sync.Once EndReason.
	var b strings.Builder
	for i := range 100 {
		fmt.Fprintf(&b, "data: {\"id\":%d}\n", i)
	}
	c, resp, info := setupStreamTest(t, strings.NewReader(b.String()))

	var count atomic.Int64
	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		count.Add(1)
		sr.Error(fmt.Errorf("soft error"))
		sr.Stop(fmt.Errorf("fatal"))
	})

	assert.Equal(t, int64(1), count.Load())
	require.NotNil(t, info.StreamStatus)
	assert.Equal(t, relaycommon.StreamEndReasonHandlerStop, info.StreamStatus.EndReason)
	assert.Equal(t, 2, info.StreamStatus.TotalErrorCount())
}

func TestStreamScannerHandler_StreamStatus_InitializedIfNil(t *testing.T) {
	t.Parallel()

	body := buildSSEBody(1)
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	assert.Nil(t, info.StreamStatus)

	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {})

	assert.NotNil(t, info.StreamStatus)
}

func TestStreamScannerHandler_StreamStatus_ReplacesPreInitialized(t *testing.T) {
	t.Parallel()

	body := buildSSEBody(5)
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	info.StreamStatus = relaycommon.NewStreamStatus()
	info.StreamStatus.RecordError("pre-existing error")

	StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {})

	assert.Equal(t, relaycommon.StreamEndReasonDone, info.StreamStatus.EndReason)
	assert.Equal(t, 0, info.StreamStatus.TotalErrorCount())
}

func TestNewStreamScannerCallerLimit(t *testing.T) {
	// The smaller buffer must actually constrain a line; a preallocated 64 KiB
	// buffer would otherwise bypass this caller's 1 KiB limit in bufio.Scanner.
	scanner := NewStreamScanner(strings.NewReader(strings.Repeat("x", 2048)+"\n"), 1024)
	assert.False(t, scanner.Scan())
	require.Error(t, scanner.Err())
	scanner = NewStreamScanner(strings.NewReader("data: ok\n"), 1024)
	require.True(t, scanner.Scan())
	assert.Equal(t, "data: ok", scanner.Text())
	require.NoError(t, scanner.Err())
}

type mockErrorReader struct {
	data []byte
	err  error
}

func (r *mockErrorReader) Read(p []byte) (n int, err error) {
	if len(r.data) > 0 {
		n = copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}

func TestStreamScannerHandler_StreamBroken_PreFirstByte(t *testing.T) {
	reader := &mockErrorReader{
		data: nil,
		err:  fmt.Errorf("stream error: stream ID 1; INTERNAL_ERROR; received from peer"),
	}
	c, resp, info := setupStreamTest(t, reader)

	err := StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {})

	require.Error(t, err)
	var brokenErr *loadbalancer.StreamBrokenError
	require.ErrorAs(t, err, &brokenErr)
	assert.True(t, info.StreamStatus.HasErrors())
	assert.Equal(t, relaycommon.StreamEndReasonScannerErr, info.StreamStatus.EndReason)
}

func TestStreamScannerHandler_StreamBroken_MidStream(t *testing.T) {
	reader := &mockErrorReader{
		data: []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\\n\\n"),
		err:  fmt.Errorf("stream error: stream ID 1; INTERNAL_ERROR; received from peer"),
	}
	c, resp, info := setupStreamTest(t, reader)

	err := StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		info.ReceivedContentBytes += 150
		info.ReceivedResponseCount++
	})

	require.Error(t, err)
	var brokenErr *loadbalancer.StreamBrokenError
	require.ErrorAs(t, err, &brokenErr)
	assert.True(t, info.StreamStatus.HasErrors())
	assert.Equal(t, relaycommon.StreamEndReasonScannerErr, info.StreamStatus.EndReason)
}

// A stream whose only frame is shorter than any plausible byte threshold still
// carries model output. The scanner measures SSE envelope size, not content, so
// a length heuristic reports it as an empty stream, the client gets a 502, and
// the whole cross-channel retry budget is burned on a healthy channel.
func TestStreamScannerHandler_ShortValidStreamIsNotEmpty(t *testing.T) {
	t.Parallel()

	// 60 bytes of payload — a well-formed one-word Gemini answer.
	frame := `data: {"candidates":[{"content":{"role":"model","parts":[{"text":"Hi"}]}}]}`
	body := frame + "\n" + "data: [DONE]\n"
	require.Less(t, len(frame)-len("data: "), 100, "precondition: the frame must sit below any length heuristic")

	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	var got []string
	err := StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		got = append(got, data)
	})

	require.NoError(t, err, "a short but valid stream must not be reported as empty")
	require.Len(t, got, 1)
	assert.Equal(t, info.ReceivedResponseCount, 1)
}

// The zero-frame case is the one the empty-stream retry exists for: the upstream
// ended cleanly without ever emitting a data frame.
func TestStreamScannerHandler_ZeroFrameStreamIsEmpty(t *testing.T) {
	t.Parallel()

	c, resp, info := setupStreamTest(t, strings.NewReader("data: [DONE]\n"))

	err := StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {})

	var emptyErr *loadbalancer.EmptyStreamError
	require.ErrorAs(t, err, &emptyErr)
	assert.Equal(t, 0, info.ReceivedResponseCount)
}

// ---------- 客户端主动断开不得被当成上游流故障 ----------

// blockedBody 在 Read 上一直阻塞，直到 Close 被调用才返回一个读错误——复刻生产上
// 「客户端断开 → 我们收尾关闭上游 body → scanner 报 read on closed response body」
// 这条路径。
//
// 必须绕过 setupStreamTest：它用 io.NopCloser 包一层，而 NopCloser 的 Close 是空
// 实现，阻塞中的 Read 永远醒不过来，cleanup 的 wg.Wait() 会一直挂到测试超时。
// 真实 http.Response.Body 的 Close 保证会解除挂起的 Read，测试替身必须保证同一件事。
type blockedBody struct {
	closed  chan struct{}
	closeMu sync.Once
}

func newBlockedBody() *blockedBody { return &blockedBody{closed: make(chan struct{})} }

func (b *blockedBody) Read([]byte) (int, error) {
	<-b.closed
	return 0, errors.New("http: read on closed response body")
}

func (b *blockedBody) Close() error {
	b.closeMu.Do(func() { close(b.closed) })
	return nil
}

// setupBlockingStreamTest 造一个上游一直不出数据、直到我们收尾才断的流。
func setupBlockingStreamTest(t *testing.T) (*gin.Context, *http.Response, *relaycommon.RelayInfo) {
	t.Helper()
	c, resp, info := setupStreamTest(t, strings.NewReader(""))
	resp.Body = newBlockedBody()
	return c, resp, info
}

// TestStreamScannerHandler_ClientAbortIsNotAStreamError 覆盖 v29.2 review 发现的
// 真实生产缺陷：客户端按 ESC 放弃请求时，我们收尾关闭 body 产生的读错误被记成流
// 错误，控制器据此把健康渠道判成「上游流中断」并立即熔断，还为这个已经没人接收的
// 请求继续换渠道重试。实测一天 112 次误熔断（渠道 #4 独占 54 次）、35 次白烧的
// 上游调用、$3.27 计费对应客户端从未收到的输出。
func TestStreamScannerHandler_ClientAbortIsNotAStreamError(t *testing.T) {
	t.Parallel()

	c, resp, info := setupBlockingStreamTest(t)
	ctx, cancel := context.WithCancel(c.Request.Context())
	c.Request = c.Request.WithContext(ctx)
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {})

	require.NoError(t, err, "客户端断开不得升级成可重试的渠道错误")
	assert.True(t, info.StreamStatus.IsClientAbort(), "precondition: 应判定为客户端断开")
	assert.Equal(t, 0, info.ReceivedResponseCount)

	// 判别力断言：收尾期我们自己关闭 body 产生的读错误不能进 ErrorCount。
	// 这条错误是本缺陷的触发器——hasErrorsLocked 的客户端豁免只写在 EndError 分支，
	// 会被前面的 ErrorCount>0 短路掉。
	assert.Equal(t, 0, info.StreamStatus.TotalErrorCount(),
		"我们主动关闭上游 body 引发的读错误不得计为流错误，实际记录: %s", info.StreamStatus.Summary())
	assert.False(t, info.StreamStatus.HasErrors(),
		"客户端断开不得让 HasErrors() 为真，实际: %s", info.StreamStatus.Summary())
	assert.False(t, info.StreamStatus.IsUpstreamStreamFault(),
		"客户端断开不得被归因为上游流故障")
}

// TestStreamScannerHandler_UpstreamScanErrorStillRecorded 反向验证：真实的上游读
// 错误（不是我们收尾引发的）仍然必须被记录并触发换渠道重试。
func TestStreamScannerHandler_UpstreamScanErrorStillRecorded(t *testing.T) {
	t.Parallel()

	c, resp, info := setupStreamTest(t, errBody{errors.New("stream error: stream ID 5; INTERNAL_ERROR; received from peer")})

	err := StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {})

	var brokenErr *loadbalancer.StreamBrokenError
	require.ErrorAs(t, err, &brokenErr, "真实的上游读错误必须触发换渠道重试")
	assert.NotNil(t, brokenErr.Err, "StreamBrokenError 必须保留底层的 endErr")
	assert.NotNil(t, errors.Unwrap(brokenErr), "StreamBrokenError 必须能解包出底层错误")
	assert.False(t, info.StreamStatus.IsClientAbort())
	assert.Greater(t, info.StreamStatus.TotalErrorCount(), 0, "真实读错误必须留痕")
	assert.True(t, info.StreamStatus.IsUpstreamStreamFault())
}

// errBody 直接返回错误、不产出任何数据，模拟上游在传输中途断开。
type errBody struct{ err error }

func (b errBody) Read([]byte) (int, error) { return 0, b.err }
func (b errBody) Close() error             { return nil }

func TestStreamScannerHandler_ContextDeadlineExceededNotClientAbort(t *testing.T) {
	t.Parallel()

	c, resp, info := setupStreamTest(t, strings.NewReader("data: {\"test\":1}\n\n"))
	deadlineCtx, cancel := context.WithTimeout(c.Request.Context(), time.Nanosecond)
	defer cancel()
	<-deadlineCtx.Done()
	c.Request = c.Request.WithContext(deadlineCtx)

	err := StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {})

	assert.False(t, info.StreamStatus.IsClientAbort(), "网关/请求超时 context.DeadlineExceeded 绝不能判定为客户端主动断开")
	endReason, _ := info.StreamStatus.EndState()
	assert.Equal(t, relaycommon.StreamEndReasonTimeout, endReason, "DeadlineExceeded 应当归因于超时")
	var brokenErr *loadbalancer.StreamBrokenError
	require.ErrorAs(t, err, &brokenErr, "网关超时引发的流中断应当返回 StreamBrokenError")
	assert.NotNil(t, brokenErr.Err, "StreamBrokenError 必须携带 context.DeadlineExceeded")
	assert.True(t, errors.Is(brokenErr, context.DeadlineExceeded), "StreamBrokenError 解包必须匹配 context.DeadlineExceeded")
}

func TestStreamScannerHandler_SSEKeepAliveCommentsIgnored(t *testing.T) {
	t.Parallel()

	// 模拟上游发送心跳注释，随后发送真正的 data 块与 [DONE]
	body := ": keep-alive\n\n: ping\n\ndata: {\"text\":\"hi\"}\n\ndata: [DONE]\n\n"
	c, resp, info := setupStreamTest(t, strings.NewReader(body))

	var receivedData []string
	_ = StreamScannerHandler(c, resp, info, func(data string, sr *StreamResult) {
		receivedData = append(receivedData, data)
	})

	assert.Equal(t, 1, info.ReceivedResponseCount, "心跳注释帧不得计入有效响应块数")
	assert.Equal(t, []string{"{\"text\":\"hi\"}"}, receivedData, "心跳注释帧不得分发给业务 handler")
	assert.True(t, info.StreamStatus.IsNormalEnd())
}
