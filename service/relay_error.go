package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dengyie/apihub/common"
	"github.com/dengyie/apihub/constant"
	"github.com/dengyie/apihub/loadbalancer"
	"github.com/dengyie/apihub/logger"
	"github.com/dengyie/apihub/model"
	relaycommon "github.com/dengyie/apihub/relay/common"
	"github.com/dengyie/apihub/relaykit/types"
	"github.com/dengyie/apihub/setting/operation_setting"

	"github.com/bytedance/gopkg/util/gopool"
	"github.com/gin-gonic/gin"
)

// IsClientAbort 判断本次失败是否由客户端主动断开引起。
//
// 取消是下游行为，不是渠道故障。判据用请求级事实，不依赖错误链——
// OpenAIError 构造经常丢掉 Unwrap。DeadlineExceeded 是网关预算 / TTFT，
// 不得当成 abort。
func IsClientAbort(c *gin.Context, relayInfo *relaycommon.RelayInfo, err error) bool {
	if types.IsClientAbortedError(err) {
		return true
	}
	if relayInfo != nil && relayInfo.StreamStatus != nil && relayInfo.StreamStatus.IsClientAbort() {
		return true
	}
	if c != nil && c.Request != nil && c.Request.Context() != nil {
		ctxErr := c.Request.Context().Err()
		if ctxErr != nil && errors.Is(ctxErr, context.Canceled) {
			return true
		}
	}
	return false
}

// DecideRelayRetry is the single retry decision for relay attempts. The reason
// is recorded in the request policy decision events of the log details.
func DecideRelayRetry(c *gin.Context, err *types.NewAPIError, retryTimes int) PolicyDecision {
	if err == nil {
		return PolicyDecision{Action: "stop", Reason: "request_completed", Source: "system"}
	}
	// 响应已提交（首字节/SSE 事件已写出给下游客户端）：不能再向同一个连接缝合其他渠道的响应流，
	// 必须立即停止换渠道重试，交由终结错误处理器向客户端发送标准 SSE 错误帧。
	if c != nil && c.Writer.Written() {
		return PolicyDecision{Action: "stop", Reason: "response_committed", Source: "system"}
	}
	if ShouldSkipRetryAfterChannelAffinityFailure(c) {
		source := RequestPolicy(c).SessionModeSource
		if source == "" {
			source = "session_rule"
		}
		return PolicyDecision{Action: "stop", Reason: "strict_session", Source: source}
	}
	if GetChannelConstraints(c).SuppressesRetry() {
		return PolicyDecision{Action: "stop", Reason: "pinned_channel", Source: "channel_constraint"}
	}
	// skipRetry 是错误构造点留下的「此错误重试无意义」显式标记（本地参数校验、
	// 额度不足、令牌无权使用该模型等），必须先于负载类启发式判断。否则任何命中
	// 启发式的本地错误都会被反复换渠道重试：例如网关自身的 403（令牌模型白名单
	// 拒绝）会被 IsUpstreamPermissionError 的「403 即上游权限错误」规则当作上游
	// 故障，白白烧完整轮重试预算、消耗半开探测配额，最后返回的还是同一个 403。
	// 负载类错误（TTFT 超时/空流/断流）由 ToNewAPIError 构造，从不携带该标记，
	// 因此前移不影响它们的换渠道重试。
	if types.IsClientAbortedError(err) {
		return PolicyDecision{Action: "stop", Reason: "client_aborted", Source: "local"}
	}
	if loadbalancer.IsEmptyStreamBudget(err) {
		return PolicyDecision{Action: "stop", Reason: "empty_stream_budget_exhausted", Source: "loadbalancer"}
	}
	if types.IsSkipRetryError(err) {
		return PolicyDecision{Action: "stop", Reason: "non_retryable_error", Source: "system"}
	}
	// 重试预算检查前置：负载类的可重试错误（超时/空流/断流）同样受预算约束
	if retryTimes <= 0 {
		return PolicyDecision{Action: "stop", Reason: "attempt_budget_exhausted", Source: "global"}
	}
	// 智能负载：max_completion_tokens 超出该模型上限（如
	// "max_completion_tokens is too large: 384000. This model supports at most
	// 262144 completion tokens."）。这不是渠道故障而是请求参数与模型能力不匹配，
	// 不计入熔断（controller 的 lbAttempt.End 已把 400 排除在失败计数外）。
	//
	// 必须排在下面两道闸门之前，否则这个分支是死代码：
	//  1. alwaysSkipCodes 默认含 ErrorCodeBadResponseBody——上游 400 绝大多数
	//     落在这个错误码上，会在闸门处直接 stop；
	//  2. 即便错误码不在其中，默认 retryRanges 明确排除 400，最后也会落到
	//     status_not_retryable。
	// 死代码的代价不只是少一个 reason：controller 的重试循环是
	// 「processChannelError（学到上限）→ 继续下一次尝试」，下一轮
	// ConvertOpenAIRequest 出站前就会用刚学到的上限钳制，请求当场成功。
	// 分支死了就等于第一次请求必定把 400 打给客户端，只有第二次请求才受益。
	if _, ok := loadbalancer.ParseMaxCompletionTokensLimit(err); ok {
		return PolicyDecision{Action: "retry", Reason: "max_completion_tokens_clamped", Source: "loadbalancer"}
	}
	// 智能负载：首字超时视为可重试，触发切换到下一个渠道。
	//
	// 必须排在 IsChannelError 之前：TTFT 超时的错误码是
	// ErrorCodeChannelResponseTimeExceeded = "channel:response_time_exceeded"，
	// 而 IsChannelError 判的就是 "channel:" 前缀，所以放在它后面这个分支永远
	// 走不到——既丢掉了 ttft_timeout 这个可观测的原因，也把上面两道闸门
	// （skipRetry 标记、重试预算）一并绕过了：TTFT 超时可以在预算耗尽后继续换
	// 渠道重试，无限循环在只有一个渠道的分组上。
	if loadbalancer.IsTTFTTimeout(err) {
		return PolicyDecision{Action: "retry", Reason: "ttft_timeout", Source: "loadbalancer"}
	}
	// 其余 "channel:" 前缀错误（模型映射失效、参数/请求头覆写非法、密钥无效、
	// AWS 客户端错误等）：换渠道重试。上面的 skipRetry 与预算闸门对它们同样有效。
	if types.IsChannelError(err) {
		return PolicyDecision{Action: "retry", Reason: "channel_error", Source: "system"}
	}
	// 智能负载：空流视同渠道失败，换渠道重试（客户端尚未收到任何数据）
	if loadbalancer.IsEmptyStream(err) {
		return PolicyDecision{Action: "retry", Reason: "empty_stream", Source: "loadbalancer"}
	}
	// 智能负载：流中断（上游传输异常断开，如 RST_STREAM 等）视同渠道失败，换渠道重试
	if loadbalancer.IsStreamBroken(err) {
		return PolicyDecision{Action: "retry", Reason: "stream_broken", Source: "loadbalancer"}
	}
	// 智能负载：上游额度耗尽（即使返回 400/402/403 等，属于渠道不可用，换渠道重试）
	if loadbalancer.IsUpstreamQuotaError(err) {
		return PolicyDecision{Action: "retry", Reason: "upstream_quota_exhausted", Source: "loadbalancer"}
	}
	// 智能负载：上游路由/会话头缺失（部分网关剥离 x-opencode-session 返回 400，换渠道重试）
	if loadbalancer.IsUpstreamRoutingError(err) {
		return PolicyDecision{Action: "retry", Reason: "upstream_routing_error", Source: "loadbalancer"}
	}
	// 智能负载：上游渠道权限受限/分组无权访问/TokenPlan不支持（换渠道重试）
	if loadbalancer.IsUpstreamPermissionError(err) {
		return PolicyDecision{Action: "retry", Reason: "upstream_permission_denied", Source: "loadbalancer"}
	}
	code := err.StatusCode
	if code >= 200 && code < 300 {
		return PolicyDecision{Action: "stop", Reason: "system_retry_exclusion", Source: "system"}
	}
	if code < 100 || code > 599 {
		return PolicyDecision{Action: "retry", Reason: "unrecognized_status", Source: "system"}
	}
	if operation_setting.IsAlwaysSkipRetryCode(err.GetErrorCode()) || operation_setting.IsAlwaysSkipRetryStatusCode(code) {
		return PolicyDecision{Action: "stop", Reason: "system_retry_exclusion", Source: "system"}
	}
	// 智能负载：参数不支持的 400 错误换渠道重试（不同上游对参数的支持不同，
	// 如 "thinking" / "reasoning_effort" 等），但不计入熔断。
	if _, ok := loadbalancer.IsParamNotSupportedError(err); ok {
		return PolicyDecision{Action: "retry", Reason: "bad_request_retry", Source: "loadbalancer"}
	}
	// 智能负载：思考模式历史消息校验不兼容（如 "The `reasoning_content` in the thinking mode must be passed back"），换渠道重试
	if loadbalancer.IsThinkingModeHistoryError(err) {
		return PolicyDecision{Action: "retry", Reason: "thinking_history_incompatible", Source: "loadbalancer"}
	}
		// 智能负载：Responses 推理水合（解密）失败（如 "reasoning hydration failed: Encrypted content could not be decrypted"），换渠道并脱敏重试
		if loadbalancer.IsReasoningHydrationError(err) {
			return PolicyDecision{Action: "retry", Reason: "reasoning_hydration_failed", Source: "loadbalancer"}
		}
		// 智能负载：Codex / Responses 校验不兼容错误（如 "invalid codex request"、code="invalid_responses_request"），换渠道重试，不熔断
		if loadbalancer.IsCodexValidationBadRequest(err) {
			return PolicyDecision{Action: "retry", Reason: "codex_validation_error", Source: "loadbalancer"}
		}
	// 智能负载：上游模型不可用/已禁用/未配置（如 "model not found"、"model is disabled on this gateway"），换渠道重试
	if loadbalancer.IsUpstreamModelUnavailableError(err) {
		return PolicyDecision{Action: "retry", Reason: "model_unavailable_retry", Source: "loadbalancer"}
	}
	// 智能负载：上游中继站报告代理异常（如 "来自上游渠道的报错: bad response status code 400"，换渠道重试）
	if loadbalancer.IsUpstreamRelayError(err) {
		return PolicyDecision{Action: "retry", Reason: "upstream_relay_error", Source: "loadbalancer"}
	}
	if operation_setting.ShouldRetryByStatusCode(code) {
		return PolicyDecision{Action: "retry", Reason: "retry_status_matched", Source: "global"}
	}
	// 智能负载：上游限流/并发超限（如 400 包装的并发限制、RPM 限流），换渠道重试
	if loadbalancer.IsUpstreamRateLimitError(err) {
		return PolicyDecision{Action: "retry", Reason: "upstream_rate_limited", Source: "loadbalancer"}
	}
	return PolicyDecision{Action: "stop", Reason: "status_not_retryable", Source: "global"}
}

func ShouldRetryRelayError(c *gin.Context, openaiErr *types.NewAPIError, retryTimes int) bool {
	return DecideRelayRetry(c, openaiErr, retryTimes).Action == "retry"
}

func ProcessChannelError(c *gin.Context, channelError types.ChannelError, err *types.NewAPIError, relayInfo *relaycommon.RelayInfo) {
	if err == nil {
		return
	}
	// 智能负载：记住该渠道该上游模型的 max_completion_tokens 真实上限，
	// 后续出站前直接钳制（见 openai adaptor），客户端要的长回复一次就成。
	//
	// 放在这里而不是 DecideRelayRetry 里，两个理由都是必须的：
	//  1. 上限是「上游模型」的属性，必须用实际发出去的那个名字
	//     （relayInfo.UpstreamModelName，已经过 model_mapping 与 reasoning
	//     后缀剥离）。此前写在 DecideRelayRetry 里用的是
	//     ContextKeyOriginalModel（客户端请求名），而出站钳制读的是
	//     UpstreamModelName——渠道 #111 配了 model_mapping 把 deepseek-v4-flash
	//     映射成 Deepseek-v4-flash，两个名字就此分叉，钳制永远查不到，等于没修。
	//  2. 学习是「观测到事实」的副作用，与「这次要不要重试」无关。写在重试
	//     决策里会被 client_aborted / skipRetry / retryTimes<=0 等闸门挡掉，
	//     而恰恰是反复失败到预算耗尽的请求最需要把上限学到手。
	//
	// ChannelMeta 也要判空：UpstreamModelName 是从内嵌的 *ChannelMeta 提升上来
	// 的字段，而它是**指针**内嵌。渠道选定之前就失败（选渠道失败、令牌中途失效
	// 等）时 relayInfo 非 nil 但 ChannelMeta 仍为 nil，直接取字段会空指针 panic，
	// 把一个本该正常记账的失败变成 500。
	if relayInfo != nil && relayInfo.ChannelMeta != nil {
		if limit, ok := loadbalancer.ParseMaxCompletionTokensLimit(err); ok {
			loadbalancer.RecordMaxCompletionTokensLimit(channelError.ChannelId, relayInfo.UpstreamModelName, limit)
		}
	}
	logger.LogError(c, fmt.Sprintf("channel error (channel #%d, status code: %d): %s", channelError.ChannelId, err.StatusCode, common.LocalLogPreview(err.MaskSensitiveErrorWithStatusCode())))
	// 佐证计数只在这一处发生，四条禁用路径全部汇流到 ProcessChannelError。
	// 换到任何一处调用点去计数都会被重复触发：relay 路径上同一次失败先被
	// RecordPolicyFailure 判一次（那次不计数，见 ShouldDisableChannel 的注释），
	// 测活路径也会先算一遍 shouldBanChannel 再走到这里。
	//
	// 必须在 gopool.Go 之前把模型名取成字符串：gin.Context 随请求结束被回收，
	// 闭包里再读 c 拿到的是已复用的内存，读出来的模型名会是别的请求的。
	// modelScoped / modelName 都是值类型，进闭包是安全的。
	modelScoped := isModelScopedAutoDisable(err)
	modelName := c.GetString("original_model")
	// AutoBan 必须写在左边：ShouldDisableChannelCorroborated 不是纯读，它内部
	// 会 RecordAutoDisableSignal（计数 +1）并打出「holding back disable」那行
	// 日志。写在左边时 Go 从左往右求值，一个 auto_ban=0 的渠道也会照常累加
	// 计数、并被告知「闸门在拦我」——而它永远不会被摘掉。两处后果：日志在
	// 排障时把人引向错误方向；以及运维日后给这条渠道打开 auto_ban，第一次
	// 真实失败就顶到阈值被摘掉，恰好是佐证机制要防的「一次即禁」。
	if channelError.AutoBan && ShouldDisableChannelCorroborated(channelError.ChannelId, modelName, err) {
		reason := err.MaskSensitiveErrorWithStatusCode()
		gopool.Go(func() {
			if modelScoped {
				DisableChannelForModel(channelError, modelName, reason)
				return
			}
			DisableChannel(channelError, reason)
		})
	}

	if constant.ErrorLogEnabled && types.IsRecordErrorLog(err) {
		writeErrorLogRow(c, channelError.ChannelId, err, relayInfo)
	}
}

// writeErrorLogRow 写入 type=5 行并落去重标记。渠道失败与请求级失败共用这一条
// 记账路径，所以「谁写谁打标记」必须收在这里：调用方只负责判断要不要写。
//
// 名字里没有「只写一次」：去重靠的是 ContextKeyErrorLogRecorded 标记，而
// ProcessChannelError 每次渠道尝试都会调到这里，所以 N 次尝试仍然落 N 行。
func writeErrorLogRow(c *gin.Context, channelId int, err *types.NewAPIError, relayInfo *relaycommon.RelayInfo) {
	userId := c.GetInt("id")
	tokenName := c.GetString("token_name")
	modelName := c.GetString("original_model")
	tokenId := c.GetInt("token_id")
	userGroup := c.GetString("group")
	other := model.NewLogOther()
	if c.Request != nil && c.Request.URL != nil {
		other.SetPublic("request_path", c.Request.URL.Path)
	}
	other.SetPublic("error_type", err.GetErrorType())
	other.SetPublic("error_code", err.GetErrorCode())
	other.SetPublic("status_code", err.StatusCode)
	AppendRelayLogAdminInfo(c, relayInfo, other)
	AppendResponseModelLogInfo(relayInfo, other)
	AppendTaskPluginContextAuditInfo(c, other)
	startTime := common.GetContextKeyTime(c, constant.ContextKeyRequestStartTime)
	if startTime.IsZero() {
		startTime = time.Now()
	}
	useTimeSeconds := int(time.Since(startTime).Seconds())
	// 标记必须在写库之前落：写库失败（DB 抖动）时宁可这次不重试，也不能让同一次
	// 请求在终结 defer 里再补一行。
	common.SetContextKey(c, constant.ContextKeyErrorLogRecorded, true)
	model.RecordErrorLog(c, userId, channelId, modelName, tokenName, err.MaskSensitiveErrorWithStatusCode(), tokenId, useTimeSeconds, common.GetContextKeyBool(c, constant.ContextKeyIsStream), userGroup, other)
}

// RecordRequestErrorLog 为「没有走到渠道」就失败的请求补一条 type=5 行：
// 参数校验、模型/映射解析、选渠道、预扣费等路径都不会经过 ProcessChannelError，
// 通用日志页此前只看得到成功的 type=2，请求级失败完全不可见。
//
// 去重是硬要求：渠道重试里每次尝试失败都已由 ProcessChannelError 落一行，
// 终结路径再补一行就是 N+1 行。因此命中 ContextKeyErrorLogRecorded 就直接跳过。
//
// 渠道号一律从 c 上读（ContextKeyChannelId），不接受调用方传参：所有调用点的
// 值都来自同一份 gin context，参数化等于开一个编译器管不着的第二数据源，
// 不一致时不会编译失败，只会静默写出归因错误的日志行。
func RecordRequestErrorLog(c *gin.Context, err *types.NewAPIError, relayInfo *relaycommon.RelayInfo) {
	if c == nil || err == nil {
		return
	}
	if !constant.ErrorLogEnabled || !types.IsRecordErrorLog(err) {
		return
	}
	// 客户端主动取消不记：取消是下游行为不是上游故障，与 Relay 终结 defer 的判断同源。
	//
	// 这道闸门**不能**当成 !IsRecordErrorLog 的重复而删掉：本地构造的
	// NewClientAbortedError 确实自带 opt-out，但 errorCode 的判定依据是「错误码等于
	// client_aborted」，而不是「谁构造的」。relaykit/types/error.go 里
	// WithOpenAIError (:359) 与 WithClaudeError (:383) 会把**上游响应体里的
	// code/type 字符串**直接写成 errorCode，且都不施加 opt-out。上游于是可以自己决定
	// 「这条失败要不要进通用日志页」。真实形状见 relay/responses_websocket.go:439 的
	// 上游 error 帧拒绝：上游给什么 code 就是什么 code，没有 opt-out，也不经过
	// ProcessChannelError（所以去重标记同样拦不住），三道闸门里只有这里拦得住。
	if types.IsClientAbortedError(err) {
		return
	}
	if common.GetContextKeyBool(c, constant.ContextKeyErrorLogRecorded) {
		return
	}
	writeErrorLogRow(c, common.GetContextKeyInt(c, constant.ContextKeyChannelId), err, relayInfo)
}
