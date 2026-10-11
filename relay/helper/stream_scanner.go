package helper

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dengyie/apihub/common"
	"github.com/dengyie/apihub/constant"
	"github.com/dengyie/apihub/loadbalancer"
	"github.com/dengyie/apihub/logger"
	relaycommon "github.com/dengyie/apihub/relay/common"
	"github.com/dengyie/apihub/relaykit/types"
	"github.com/dengyie/apihub/service"
	"github.com/dengyie/apihub/setting/operation_setting"

	"github.com/bytedance/gopkg/util/gopool"

	"github.com/gin-gonic/gin"
)

const (
	InitialScannerBufferSize    = 64 << 10  // 64KB (64*1024)
	DefaultMaxScannerBufferSize = 128 << 20 // 64MB (64*1024*1024) default SSE buffer size
	DefaultPingInterval         = 10 * time.Second
	// streamWriteTimeout bounds a single blocked write to a slow client so the
	// unconditional wg.Wait() in cleanup can always finish. Without it, a slow
	// but connected client (full TCP buffer, no server WriteTimeout) could hang
	// the handler forever.
	streamWriteTimeout = 30 * time.Second
)

func getScannerBufferSize() int {
	if constant.StreamScannerMaxBufferMB > 0 {
		return constant.StreamScannerMaxBufferMB << 20
	}
	return DefaultMaxScannerBufferSize
}

// NewStreamScanner shares relay scanner configuration. Callers buffering bounded
// task state may additionally cap a line without increasing the configured limit.
func NewStreamScanner(reader io.Reader, maxBytes ...int) *bufio.Scanner {
	limit := getScannerBufferSize()
	if len(maxBytes) > 0 && maxBytes[0] > 0 {
		limit = min(limit, maxBytes[0])
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, min(InitialScannerBufferSize, limit)), limit)
	return scanner
}

func copyCodexSSEHeaders(c *gin.Context, resp *http.Response) {
	if c == nil || c.Writer == nil || resp == nil {
		return
	}
	// codex
	for _, name := range []string{"X-Reasoning-Included", "X-Codex-Turn-State"} {
		values := resp.Header.Values(name)
		if !service.ShouldCopyUpstreamHeader(c, name, values) {
			continue
		}
		for _, value := range values {
			if value != "" {
				c.Writer.Header().Add(name, value)
			}
		}
	}
}

// ExtendWriteDeadline pushes the connection write deadline forward before each
// stream write. Best-effort: writers that don't support deadlines (e.g.
// httptest recorders) are silently ignored.
func ExtendWriteDeadline(c *gin.Context) {
	if c == nil || c.Writer == nil {
		return
	}
	_ = http.NewResponseController(c.Writer).SetWriteDeadline(time.Now().Add(streamWriteTimeout))
}

func StreamScannerHandler(c *gin.Context, resp *http.Response, info *relaycommon.RelayInfo, dataHandler func(data string, sr *StreamResult)) error {

	if resp == nil || dataHandler == nil {
		return nil
	}

	// 初始化本次扫描的 StreamStatus；保留上层 adapter 已声明的协议期望（如 RequireTerminal）
	if info != nil {
		if info.StreamStatus == nil {
			info.StreamStatus = relaycommon.NewStreamStatus()
		} else {
			expectsTerminal := info.StreamStatus.ExpectsTerminal()
			info.StreamStatus = relaycommon.NewStreamStatus()
			if expectsTerminal {
				info.StreamStatus.RequireTerminal()
			}
		}
	}

	// 智能负载：跟踪本次请求的渠道状态。句柄由 controller 在每轮尝试开始时
	// 创建并存入 context，这里复用同一个，使 inflight 计数对流式与非流式
	// 一致生效；没有句柄的调用方（如 Responses WebSocket 中继）就地 Begin。
	channelID := 0
	if info != nil {
		channelID = info.GetChannelID()
	}
	attempt, _ := c.Get(loadbalancer.ContextKeyAttempt)
	lbHandle, _ := attempt.(*loadbalancer.RequestHandle)
	if lbHandle == nil {
		// 键必须是客户端请求的模型名（不是上游名）：选渠道时只知道客户端
		// 请求了什么，而 model_mapping 多对一不可反推。info 可能为 nil，
		// GetOriginModelName 返回空串 → 自动落到渠道级，等价 v29.10。
		lbHandle = loadbalancer.GlobalTracker().Begin(channelID, info.GetOriginModelName())
	}
	lbPolicy := loadbalancer.GetPolicy().Resolve(channelID)
	var lbTTFTSlow atomic.Bool
	var lbFirstByteOnce sync.Once

	// 首字超时检测：超时未收到首字则中断上游连接。
	// 动态扣减响应头建立及等待已消耗的时间，使整段首字等待严格收敛在 TTFTTimeoutMs 之内。
	var ttftTimer *time.Timer
	if loadbalancer.Enabled() && lbPolicy.TTFTTimeoutMs > 0 {
		remainingTTFT := time.Duration(lbPolicy.TTFTTimeoutMs) * time.Millisecond
		if info != nil && !info.StartTime.IsZero() {
			elapsed := time.Since(info.StartTime)
			if elapsed < remainingTTFT {
				remainingTTFT -= elapsed
			} else {
				remainingTTFT = 500 * time.Millisecond
			}
		}
		ttftTimer = time.AfterFunc(remainingTTFT, func() {
			lbFirstByteOnce.Do(func() {
				// 仍未收到首字：标记为慢，中断连接触发流结束
				lbTTFTSlow.Store(true)
				info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonTimeout,
					&loadbalancer.TTFTTimeoutError{ChannelID: channelID, TimeoutMs: lbPolicy.TTFTTimeoutMs})
				if resp.Body != nil {
					_ = resp.Body.Close()
				}
			})
		})
		defer ttftTimer.Stop()
	}
	markFirstByte := func() {
		lbFirstByteOnce.Do(func() {
			if ttftTimer != nil {
				ttftTimer.Stop()
			}
			lbHandle.MarkFirstByte()
		})
	}
	defer func() {
		// 流结束：上报跟踪。failed 取流状态的真实结果：
		// 有错误（scanner 错误/超时/panic）或异常结束都算失败，计入硬熔断计数器。
		// controller 用同一个（幂等）句柄收尾，先到者生效，不会双记。
		// 客户端主动断开是下游行为，不算渠道故障。
		failed := info.StreamStatus.IsUpstreamStreamFault()
		// 零块流视同失败：上游正常结束却一个内容块都没发，渠道确实没干活。
		// 判据与下面的 EmptyStreamError 一致，只看块数不看字节数。
		if !failed && info.StreamStatus.IsNormalEnd() && info.ReceivedResponseCount == 0 {
			failed = true
		}
		lbHandle.End(lbTTFTSlow.Load(), failed)
	}()

	ctx, cancel := context.WithCancel(context.Background())

	streamingTimeout := time.Duration(constant.StreamingTimeout) * time.Second

	var (
		stopChan    = make(chan bool, 3) // 增加缓冲区避免阻塞
		scanner     = NewStreamScanner(resp.Body)
		ticker      = time.NewTicker(streamingTimeout)
		pingTicker  *time.Ticker
		writeMutex  sync.Mutex     // Mutex to protect concurrent writes
		wg          sync.WaitGroup // 用于等待所有 goroutine 退出
		cleanupOnce sync.Once
		stopOnce    sync.Once
	)

	stop := func() {
		stopOnce.Do(func() {
			close(stopChan)
		})
	}

	generalSettings := operation_setting.GetGeneralSetting()
	pingEnabled := generalSettings.PingIntervalEnabled && !info.DisablePing
	pingInterval := time.Duration(generalSettings.PingIntervalSeconds) * time.Second
	if pingInterval <= 0 {
		pingInterval = DefaultPingInterval
	}

	if pingEnabled {
		pingTicker = time.NewTicker(pingInterval)
	}

	logger.LogDebug(c, "relay timeout seconds: %d", common.RelayTimeout)
	logger.LogDebug(c, "relay max idle conns: %d", common.RelayMaxIdleConns)
	logger.LogDebug(c, "relay max idle conns per host: %d", common.RelayMaxIdleConnsPerHost)
	logger.LogDebug(c, "streaming timeout seconds: %d", int64(streamingTimeout.Seconds()))
	logger.LogDebug(c, "ping interval seconds: %d", int64(pingInterval.Seconds()))

	cleanup := func() {
		cleanupOnce.Do(func() {
			cancel()
			stop()
			if resp.Body != nil {
				_ = resp.Body.Close()
			}

			ticker.Stop()
			if pingTicker != nil {
				pingTicker.Stop()
			}

			wg.Wait()
		})
	}
	// Ensure gin.Context is not returned to Gin's pool while any stream goroutine can still use it.
	defer cleanup()

	scanner.Split(bufio.ScanLines)
	copyCodexSSEHeaders(c, resp)
	SetEventStreamHeaders(c)

	ctx = context.WithValue(ctx, "stop_chan", stopChan)

	// 智能负载：首字节交付信号。ping 必须等首个上游字节交付后才开始，
	// 否则 ping 的写入会提前提交 HTTP 响应，关闭透明重试窗口。
	firstByteDelivered := make(chan struct{})
	var firstByteDeliveredOnce sync.Once
	signalFirstByteDelivered := func() {
		firstByteDeliveredOnce.Do(func() {
			close(firstByteDelivered)
		})
	}

	// Handle ping data sending with improved error handling
	if pingEnabled && pingTicker != nil {
		wg.Add(1)
		gopool.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					logger.LogError(c, fmt.Sprintf("ping goroutine panic: %v", r))
					info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonPanic, fmt.Errorf("ping panic: %v", r))
					stop()
				}
				logger.LogDebug(c, "ping goroutine exited")
				wg.Done()
			}()

			// 等待首字节交付后再启动 ping，避免提前提交响应关闭重试窗口
			select {
			case <-firstByteDelivered:
			case <-ctx.Done():
				return
			case <-stopChan:
				return
			}

			// 添加超时保护，防止 goroutine 无限运行
			maxPingDuration := 30 * time.Minute // 最大 ping 持续时间
			pingTimeout := time.NewTimer(maxPingDuration)
			defer pingTimeout.Stop()

			for {
				select {
				case <-pingTicker.C:
					var err error
					func() {
						writeMutex.Lock()
						defer writeMutex.Unlock()
						ExtendWriteDeadline(c)
						err = PingData(c)
					}()
					if err != nil {
						logger.LogError(c, "ping data error: "+err.Error())
						info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonPingFail, err)
						return
					}
					logger.LogDebug(c, "ping data sent")
				case <-ctx.Done():
					return
				case <-stopChan:
					return
				case <-c.Request.Context().Done():
					// 监听客户端断开连接
					return
				case <-pingTimeout.C:
					logger.LogError(c, "ping goroutine max duration reached")
					return
				}
			}
		})
	}

	dataChan := make(chan string, 10)

	wg.Add(1)
	gopool.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				logger.LogError(c, fmt.Sprintf("data handler goroutine panic: %v", r))
				info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonPanic, fmt.Errorf("handler panic: %v", r))
			}
			stop()
			wg.Done()
		}()
		sr := newStreamResult(info.StreamStatus)
		for data := range dataChan {
			sr.reset()
			func() {
				writeMutex.Lock()
				defer writeMutex.Unlock()
				ExtendWriteDeadline(c)
				dataHandler(data, sr)
				// 首个数据已交付客户端：关闭透明重试窗口，允许 ping 启动
				signalFirstByteDelivered()
			}()
			if sr.IsStopped() {
				return
			}
		}
	})

	// Scanner goroutine with improved error handling
	wg.Add(1)
	common.RelayCtxGo(ctx, func() {
		defer func() {
			close(dataChan)
			if r := recover(); r != nil {
				logger.LogError(c, fmt.Sprintf("scanner goroutine panic: %v", r))
				info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonPanic, fmt.Errorf("scanner panic: %v", r))
			}
			stop()
			logger.LogDebug(c, "scanner goroutine exited")
			wg.Done()
		}()

		for scanner.Scan() {
			// 检查是否需要停止
			select {
			case <-stopChan:
				return
			case <-ctx.Done():
				return
			default:
			}

			ticker.Reset(streamingTimeout)
			data := scanner.Text()
			logger.LogDebug(c, "stream scanner data: %s", data)

			if len(data) < 6 {
				continue
			}
			if data[:5] != "data:" && data[:6] != "[DONE]" {
				continue
			}
			data = data[5:]
			data = strings.TrimSpace(data)
			if data == "" {
				continue
			}

			// 智能负载：首个有效数据/完成帧到达即标记首字（SSE 注释/心跳空帧不计入首字）
			markFirstByte()
			if !strings.HasPrefix(data, "[DONE]") {
				info.SetFirstResponseTime()
				info.ReceivedResponseCount++
				// 累计实际内容字节数，用于检测"有块无内容"的空流
				// （如 gemini 正常结束但 completion_tokens=0 的情况）
				info.ReceivedContentBytes += len(data)

				select {
				case dataChan <- data:
				case <-ctx.Done():
					return
				case <-stopChan:
					return
				}
			} else {
				info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonDone, nil)
				logger.LogDebug(c, "received [DONE], stopping scanner")
				return
			}
		}

		if err := scanner.Err(); err != nil && err != io.EOF {
			// 收尾时我们会主动关闭上游 body 来解除 scanner 的阻塞（cleanup 里的
			// resp.Body.Close()，以及首字超时定时器里的那次），scanner 随之报出的
			// 读错误是收尾的副作用而不是上游传输故障。把它记进 ErrorCount 会经
			// HasErrors() 把「客户端主动断开」升级成「上游流中断」，控制器随即
			// 熔断一个健康渠道并为一个早已放弃的请求继续换渠道重试。
			// 收尾方总是先设定终止原因、再关闭 body（主循环与 TTFT 定时器都是
			// 这个顺序），所以这里读到的原因就是发起收尾的那一方。
			if teardownOwnedByUs(c, info) {
				logger.LogDebug(c, "scanner 随收尾退出，不计为流错误: %s", err.Error())
			} else {
				logger.LogError(c, "scanner error: "+err.Error())
				info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonScannerErr, err)
				info.StreamStatus.RecordError(err.Error())
			}
		}
		info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonEOF, nil)
	})

	// 主循环等待完成或超时
	select {
	case <-ticker.C:
		info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonTimeout, nil)
		info.StreamStatus.RecordError("streaming timeout")
	case <-stopChan:
	// EndReason already set by the goroutine that triggered stopChan
	case <-c.Request.Context().Done():
		// 客户端断开：立即 cleanup 关闭上游 resp.Body，解除 scanner 阻塞并让上游停止生成，
		// 避免为已放弃的请求继续消费上游 token。
		if errors.Is(c.Request.Context().Err(), context.Canceled) {
			info.StreamStatus.OverrideEndReason(relaycommon.StreamEndReasonClientGone, c.Request.Context().Err())
		} else {
			info.StreamStatus.OverrideEndReason(relaycommon.StreamEndReasonTimeout, c.Request.Context().Err())
		}
	}

	// 客户端断开判定：即使 stopChan 先触发（例如向客户端写响应时检测到 context done），
	// 只要客户端 context 已取消或记录了 context canceled，最终原因应纠正为 ClientGone。
	// 必须严格区分 context.Canceled 与 context.DeadlineExceeded，不能将网关超时误判为客户端放弃。
	if c != nil && c.Request != nil && errors.Is(c.Request.Context().Err(), context.Canceled) {
		info.StreamStatus.OverrideEndReason(relaycommon.StreamEndReasonClientGone, c.Request.Context().Err())
	} else if c != nil && c.Request != nil && errors.Is(c.Request.Context().Err(), context.DeadlineExceeded) {
		info.StreamStatus.OverrideEndReason(relaycommon.StreamEndReasonTimeout, c.Request.Context().Err())
	} else if _, recordedErr := info.StreamStatus.EndState(); recordedErr != nil && (errors.Is(recordedErr, context.Canceled) || strings.Contains(recordedErr.Error(), "context canceled")) {
		info.StreamStatus.OverrideEndReason(relaycommon.StreamEndReasonClientGone, recordedErr)
	}

	cleanup()
	switch {
	case info.StreamStatus.IsClientAbort():
		// 客户端主动放弃是下游行为，按 Error 记会把正常现象混进故障视图里
		// （实测一天 112 条，且此前正是靠这些噪声定位到的误熔断）。
		logger.LogInfo(c, fmt.Sprintf("客户端断开，流终止: %s, received=%d",
			info.StreamStatus.Summary(), info.ReceivedResponseCount))
	case info.StreamStatus.IsNormalEnd() && !info.StreamStatus.HasErrors():
		logger.LogInfo(c, fmt.Sprintf("stream ended: %s", info.StreamStatus.Summary()))
	default:
		logger.LogError(c, fmt.Sprintf("stream ended: %s, received=%d", info.StreamStatus.Summary(), info.ReceivedResponseCount))
	}

	// 首字节前 TTFT 超时：返回可透明重试的错误（此时客户端尚未收到任何数据）
	if lbTTFTSlow.Load() {
		return &loadbalancer.TTFTTimeoutError{ChannelID: channelID, TimeoutMs: lbPolicy.TTFTTimeoutMs}
	}
	// 空流：上游正常结束但一个有效内容块都没发，视同渠道失败。
	// 此时客户端尚未收到任何数据，返回可透明重试的错误，触发换渠道重试，
	// 而不是把空 200 返回给客户端。
	//
	// 判据只能是「零块」，不能掺入对累计字节数的阈值判断。累计字节数衡量的是
	// 整个 SSE JSON 信封的长度，不是模型产出的内容长度，两者在短回复下无法
	// 区分：`{"candidates":[{"content":{"role":"model","parts":[{"text":"Hi"}]}}]}`
	// 是 60 字节的合法内容，而 `{"candidates":[{"content":{"role":"model","parts":[{}]}}],"usageMetadata":{}}`
	// 是 66 字节的零内容帧。任何固定阈值都会把阈值之下的合法短回复判成空流，
	// 让客户端拿到 502 并烧光整轮换渠道重试预算。
	if info.StreamStatus.IsNormalEnd() && !info.StreamStatus.HasErrors() && info.ReceivedResponseCount == 0 {
		logger.LogError(c, fmt.Sprintf("空流：渠道 #%d 正常结束但零有效内容（块=%d, 字节=%d），触发换渠道重试",
			channelID, info.ReceivedResponseCount, info.ReceivedContentBytes))
		return &loadbalancer.EmptyStreamError{ChannelID: channelID}
	}

	// 流中断判断：
	// 1. 客户端主动断开除外，不视为上游错误。
	// 2. 非正常结束（scanner_error、timeout、panic）均为流中断（无论是首字节前还是传输中途）。
	// 3. handler 异常终止（handler_stop 且有非客户端 end_error）。
	// 4. 首字节前/未交付任何内容块时出现错误（received == 0 && hasErrors）。
	//
	// 注意这里不能用 IsUpstreamStreamFault()：那个判据回答的是「该不该给这次尝试
	// 记一次渠道失败」，而这里回答的是「该不该把一个已经正常收尾的流升级成可重试
	// 的渠道错误」。两者宽窄不同——图片中继会把上游错误事件作为数据帧内联交付后
	// 正常 EOF 收尾（soft_errors=1, received>0），那是成功路径，不该重试。
	isClientGone := info.StreamStatus.IsClientAbort() ||
		(c != nil && c.Request != nil && errors.Is(c.Request.Context().Err(), context.Canceled))

	streamBroken := false
	if !isClientGone {
		if !info.StreamStatus.IsNormalEnd() {
			// scanner 错误、超时、panic 等异常中断
			streamBroken = true
		} else if endReason, endErr := info.StreamStatus.EndState(); endReason == relaycommon.StreamEndReasonHandlerStop && endErr != nil {
			// handler 主动停止且携带错误
			streamBroken = true
		} else if info.ReceivedResponseCount == 0 && info.StreamStatus.HasErrors() {
			// 首块未交付即发生错误
			streamBroken = true
		}
	}

	if streamBroken {
		endReason := info.StreamStatus.Summary()
		_, endErr := info.StreamStatus.EndState()
		logger.LogError(c, fmt.Sprintf("流中断：渠道 #%d 传输异常中断（%s, 已收块=%d, 字节=%d），触发换渠道重试并熔断",
			channelID, endReason, info.ReceivedResponseCount, info.ReceivedContentBytes))
		return &loadbalancer.StreamBrokenError{ChannelID: channelID, Reason: endReason, Err: endErr}
	}
	return nil
}

// teardownOwnedByUs 判断 scanner 退出时的读错误是否由我们自己的收尾动作造成。
//
// 我们会在三种时机主动关闭上游 body 来解除 scanner 阻塞：客户端断开、首字超时
// 定时器到期、数据 handler 判定流已结束（Done/Stop）或 ping 写失败。这几种都
// 不是上游传输故障，读错误只是关闭 body 的必然反应。
func teardownOwnedByUs(c *gin.Context, info *relaycommon.RelayInfo) bool {
	if info != nil && info.StreamStatus != nil {
		switch info.StreamStatus.EndReasonValue() {
		case relaycommon.StreamEndReasonClientGone,
			relaycommon.StreamEndReasonTimeout,
			relaycommon.StreamEndReasonHandlerStop,
			relaycommon.StreamEndReasonDone,
			relaycommon.StreamEndReasonPingFail:
			return true
		}
	}
	return c != nil && c.Request != nil && c.Request.Context().Err() != nil
}

// ToNewAPIError 将 StreamScannerHandler 返回的错误转换为 *types.NewAPIError。
// TTFT 超时错误会被标记为可重试，触发 controller 层的换渠道重试。
// 由于超时发生在首字节之前，客户端尚未收到任何数据，重试对客户端透明。
func ToNewAPIError(err error) *types.NewAPIError {
	if err == nil {
		return nil
	}
	if ttftErr, ok := err.(*loadbalancer.TTFTTimeoutError); ok {
		return types.NewErrorWithStatusCode(
			ttftErr,
			types.ErrorCodeChannelResponseTimeExceeded,
			http.StatusGatewayTimeout,
		)
	}
	// 空流：上游返回了空内容，视同 502 渠道失败，触发换渠道重试并计入熔断。
	if emptyErr, ok := err.(*loadbalancer.EmptyStreamError); ok {
		return types.NewErrorWithStatusCode(
			emptyErr,
			types.ErrorCodeBadResponseBody,
			http.StatusBadGateway,
		)
	}
	// 流中断：上游在首字节前断开，视同 502 渠道失败，触发换渠道重试并计入熔断。
	if brokenErr, ok := err.(*loadbalancer.StreamBrokenError); ok {
		return types.NewErrorWithStatusCode(
			brokenErr,
			types.ErrorCodeBadResponseBody,
			http.StatusBadGateway,
		)
	}
	return types.NewError(err, types.ErrorCodeBadResponseBody)
}
