package common

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamStatus_SetEndReason_FirstWins(t *testing.T) {
	t.Parallel()
	s := NewStreamStatus()

	s.SetEndReason(StreamEndReasonDone, nil)
	s.SetEndReason(StreamEndReasonTimeout, nil)
	s.SetEndReason(StreamEndReasonClientGone, fmt.Errorf("context canceled"))

	assert.Equal(t, StreamEndReasonDone, s.EndReason)
	assert.Nil(t, s.EndError)
}

func TestStreamStatus_SetEndReason_WithError(t *testing.T) {
	t.Parallel()
	s := NewStreamStatus()

	expectedErr := fmt.Errorf("read: connection reset")
	s.SetEndReason(StreamEndReasonScannerErr, expectedErr)

	assert.Equal(t, StreamEndReasonScannerErr, s.EndReason)
	assert.Equal(t, expectedErr, s.EndError)
}

func TestStreamStatus_SetEndReason_NilSafe(t *testing.T) {
	t.Parallel()
	var s *StreamStatus
	s.SetEndReason(StreamEndReasonDone, nil)
}

func TestStreamStatus_SetEndReason_Concurrent(t *testing.T) {
	t.Parallel()
	s := NewStreamStatus()

	reasons := []StreamEndReason{
		StreamEndReasonDone,
		StreamEndReasonTimeout,
		StreamEndReasonClientGone,
		StreamEndReasonScannerErr,
		StreamEndReasonHandlerStop,
		StreamEndReasonEOF,
		StreamEndReasonPanic,
		StreamEndReasonPingFail,
	}

	var wg sync.WaitGroup
	for _, r := range reasons {
		wg.Add(1)
		go func(reason StreamEndReason) {
			defer wg.Done()
			s.SetEndReason(reason, nil)
		}(r)
	}
	wg.Wait()

	assert.NotEqual(t, StreamEndReasonNone, s.EndReason)
}

func TestStreamStatus_RecordError_Basic(t *testing.T) {
	t.Parallel()
	s := NewStreamStatus()

	s.RecordError("bad json")
	s.RecordError("another bad json")
	s.RecordError("client gone")

	assert.True(t, s.HasErrors())
	assert.Equal(t, 3, s.TotalErrorCount())
	assert.Len(t, s.Errors, 3)
}

func TestStreamStatus_RecordError_CapAtMax(t *testing.T) {
	t.Parallel()
	s := NewStreamStatus()

	for i := range 30 {
		s.RecordError(fmt.Sprintf("error_%d", i))
	}

	assert.Equal(t, maxStreamErrorEntries, len(s.Errors))
	assert.Equal(t, 30, s.TotalErrorCount())
}

func TestStreamStatus_RecordError_NilSafe(t *testing.T) {
	t.Parallel()
	var s *StreamStatus
	s.RecordError("should not panic")
}

func TestStreamStatus_RecordError_Concurrent(t *testing.T) {
	t.Parallel()
	s := NewStreamStatus()

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			s.RecordError(fmt.Sprintf("error_%d", idx))
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 100, s.TotalErrorCount())
	assert.LessOrEqual(t, len(s.Errors), maxStreamErrorEntries)
}

func TestStreamStatus_HasErrors_Empty(t *testing.T) {
	t.Parallel()
	s := NewStreamStatus()
	assert.False(t, s.HasErrors())
	assert.Equal(t, 0, s.TotalErrorCount())
}

func TestStreamStatus_HasErrors_NilSafe(t *testing.T) {
	t.Parallel()
	var s *StreamStatus
	assert.False(t, s.HasErrors())
	assert.Equal(t, 0, s.TotalErrorCount())
}

func TestStreamStatus_HasErrors_VariousCases(t *testing.T) {
	t.Parallel()

	// Scanner error with EndError
	s1 := NewStreamStatus()
	s1.SetEndReason(StreamEndReasonScannerErr, fmt.Errorf("stream error: stream ID 1; INTERNAL_ERROR; received from peer"))
	assert.True(t, s1.HasErrors())

	// Scanner error without EndError
	s2 := NewStreamStatus()
	s2.SetEndReason(StreamEndReasonScannerErr, nil)
	assert.True(t, s2.HasErrors())

	// Timeout
	s3 := NewStreamStatus()
	s3.SetEndReason(StreamEndReasonTimeout, nil)
	assert.True(t, s3.HasErrors())

	// Panic
	s4 := NewStreamStatus()
	s4.SetEndReason(StreamEndReasonPanic, fmt.Errorf("panic"))
	assert.True(t, s4.HasErrors())

	// ClientGone with context.Canceled is NOT a channel error
	s5 := NewStreamStatus()
	s5.SetEndReason(StreamEndReasonClientGone, context.Canceled)
	assert.False(t, s5.HasErrors())

	// ClientGone with DeadlineExceeded IS an error
	s6 := NewStreamStatus()
	s6.SetEndReason(StreamEndReasonClientGone, context.DeadlineExceeded)
	assert.True(t, s6.HasErrors())

	// Normal Done
	s7 := NewStreamStatus()
	s7.SetEndReason(StreamEndReasonDone, nil)
	assert.False(t, s7.HasErrors())

	// Normal EOF
	s8 := NewStreamStatus()
	s8.SetEndReason(StreamEndReasonEOF, nil)
	assert.False(t, s8.HasErrors())
}

func TestStreamStatus_IsNormalEnd(t *testing.T) {
	t.Parallel()
	tests := []struct {
		reason StreamEndReason
		normal bool
	}{
		{StreamEndReasonDone, true},
		{StreamEndReasonEOF, true},
		{StreamEndReasonHandlerStop, true},
		{StreamEndReasonTimeout, false},
		{StreamEndReasonClientGone, false},
		{StreamEndReasonScannerErr, false},
		{StreamEndReasonPanic, false},
		{StreamEndReasonPingFail, false},
		{StreamEndReasonNone, false},
	}
	for _, tt := range tests {
		s := NewStreamStatus()
		s.SetEndReason(tt.reason, nil)
		assert.Equal(t, tt.normal, s.IsNormalEnd(), "reason=%s", tt.reason)
	}
}

func TestStreamStatus_IsNormalEnd_RequiresTerminal(t *testing.T) {
	t.Parallel()

	// 协议需要显式终止帧时：未收到终止标识的 EOF 属于异常截断
	s1 := NewStreamStatus()
	s1.RequireTerminal()
	s1.SetEndReason(StreamEndReasonEOF, nil)
	assert.False(t, s1.IsNormalEnd(), "expectsTerminal 时无 completed 的 EOF 属于非正常结束")
	assert.True(t, s1.IsUpstreamStreamFault(), "expectsTerminal 时中途 EOF 属于上游流故障")

	// 协议需要显式终止帧时：已收到完成标识（如 finish_reason: stop）但最后 EOF
	s2 := NewStreamStatus()
	s2.RequireTerminal()
	s2.MarkCompleted()
	s2.SetEndReason(StreamEndReasonEOF, nil)
	assert.True(t, s2.IsNormalEnd(), "已完成标记的 EOF 属于正常结束")
	assert.False(t, s2.IsUpstreamStreamFault(), "已完成标记的 EOF 不是上游流故障")

	// 协议需要显式终止帧时：收到明确的 [DONE] 帧
	s3 := NewStreamStatus()
	s3.RequireTerminal()
	s3.SetEndReason(StreamEndReasonDone, nil)
	assert.True(t, s3.IsNormalEnd(), "Done 帧属于正常结束")
	assert.False(t, s3.IsUpstreamStreamFault(), "Done 帧不是上游流故障")
}

func TestStreamStatus_IsNormalEnd_NilSafe(t *testing.T) {
	t.Parallel()
	var s *StreamStatus
	assert.True(t, s.IsNormalEnd())
}

func TestStreamStatus_Summary(t *testing.T) {
	t.Parallel()

	s := NewStreamStatus()
	s.SetEndReason(StreamEndReasonDone, nil)
	summary := s.Summary()
	assert.Contains(t, summary, "reason=done")
	assert.NotContains(t, summary, "soft_errors")

	s2 := NewStreamStatus()
	s2.SetEndReason(StreamEndReasonTimeout, nil)
	s2.RecordError("bad json")
	s2.RecordError("write failed")
	summary2 := s2.Summary()
	assert.Contains(t, summary2, "reason=timeout")
	assert.Contains(t, summary2, "soft_errors=2")
}

func TestStreamStatus_Summary_NilSafe(t *testing.T) {
	t.Parallel()
	var s *StreamStatus
	assert.Equal(t, "StreamStatus<nil>", s.Summary())
}

// TestStreamStatus_ClientAbortIsNotAnUpstreamFault 复刻生产上真实观测到的状态：
// 客户端断开后我们收尾关闭上游 body，scanner 记下一条读错误，于是
// EndReason=client_gone 且 ErrorCount=1。这必须仍然不算上游渠道故障。
//
// 回归背景：控制器曾用「HasErrors() || (!IsNormalEnd() && EndReason != ClientGone)」
// 判定流中断，HasErrors() 那一支没有客户端断开豁免，而 hasErrorsLocked 的豁免又
// 只写在 EndError 分支上、被前面的 ErrorCount>0 短路。结果是用户按 ESC 放弃
// 请求就会熔断一个健康渠道，并为这个死掉的请求继续换渠道重试（实测一天 112 次
// 误熔断、35 次白烧的上游调用）。
func TestStreamStatus_ClientAbortIsNotAnUpstreamFault(t *testing.T) {
	t.Parallel()
	s := NewStreamStatus()
	s.SetEndReason(StreamEndReasonClientGone, context.Canceled)
	// 收尾期我们自己关闭 body 产生的读错误
	s.RecordError("http: read on closed response body")

	assert.True(t, s.IsClientAbort(), "precondition: EndReason=client_gone")
	assert.False(t, s.IsUpstreamStreamFault(),
		"客户端断开即使带有收尾读错误也不得算作上游流故障")
}

// TestStreamStatus_UpstreamFaultStillDetected 确认收敛判据没有把真故障一起放过。
func TestStreamStatus_UpstreamFaultStillDetected(t *testing.T) {
	t.Parallel()

	scannerErr := NewStreamStatus()
	scannerErr.SetEndReason(StreamEndReasonScannerErr, fmt.Errorf("INTERNAL_ERROR"))
	scannerErr.RecordError("stream error: stream ID 5; INTERNAL_ERROR")
	assert.True(t, scannerErr.IsUpstreamStreamFault(), "scanner 错误是上游故障")

	ttft := NewStreamStatus()
	ttft.SetEndReason(StreamEndReasonTimeout, nil)
	assert.True(t, ttft.IsUpstreamStreamFault(), "首字超时是上游故障")

	truncated := NewStreamStatus()
	truncated.SetEndReason(StreamEndReasonEOF, nil)
	truncated.RecordError("error processing stream token data")
	assert.True(t, truncated.IsUpstreamStreamFault(), "带软错误的中途截断是上游故障")

	clean := NewStreamStatus()
	clean.SetEndReason(StreamEndReasonDone, nil)
	assert.False(t, clean.IsUpstreamStreamFault(), "正常结束不是故障")

	var nilStatus *StreamStatus
	assert.False(t, nilStatus.IsUpstreamStreamFault())
	assert.False(t, nilStatus.IsClientAbort())
}

func TestStreamStatus_Concurrent_ReadWrite(t *testing.T) {
	t.Parallel()
	for i := 0; i < 50; i++ {
		s := NewStreamStatus()
		s.RequireTerminal()

		var wg sync.WaitGroup
		wg.Add(4)

		go func() {
			defer wg.Done()
			s.SetEndReason(StreamEndReasonEOF, nil)
		}()
		go func() {
			defer wg.Done()
			_ = s.IsNormalEnd()
			_ = s.IsClientAbort()
		}()
		go func() {
			defer wg.Done()
			s.MarkCompleted()
			_ = s.Summary()
		}()
		go func() {
			defer wg.Done()
			_ = s.OutcomeSnapshot()
			_ = s.IsUpstreamStreamFault()
		}()

		wg.Wait()
	}
}

func TestStreamStatus_OverrideEndReason_AllowsClientAbortCorrection(t *testing.T) {
	t.Parallel()
	s := NewStreamStatus()

	// 模拟写入下游网络失败先行触发 SetEndReason
	writeErr := fmt.Errorf("write tcp broken pipe")
	s.SetEndReason(StreamEndReasonHandlerStop, writeErr)
	s.RecordError(writeErr.Error())
	assert.Equal(t, StreamEndReasonHandlerStop, s.EndReason)
	assert.False(t, s.IsClientAbort(), "普通写入错误不应判定为客户端取消")
	assert.True(t, s.IsUpstreamStreamFault(), "此时尚未纠偏，属于异常结束")

	// 权威纠偏：下游 context.Canceled 才是真正根因
	s.OverrideEndReason(StreamEndReasonClientGone, context.Canceled)
	assert.Equal(t, StreamEndReasonClientGone, s.EndReason)
	assert.True(t, s.IsClientAbort(), "权威覆写后必须判定为客户端取消")
	assert.False(t, s.IsUpstreamStreamFault(), "客户端取消绝不能算作上游传输故障")

	// 再次调用普通的 SetEndReason 不得覆盖权威终态
	s.SetEndReason(StreamEndReasonScannerErr, fmt.Errorf("late scanner err"))
	assert.Equal(t, StreamEndReasonClientGone, s.EndReason)
	assert.True(t, s.IsClientAbort())
}
