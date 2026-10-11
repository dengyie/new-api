package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dengyie/apihub/common"
	"github.com/dengyie/apihub/constant"
	"github.com/dengyie/apihub/dto"
	"github.com/dengyie/apihub/loadbalancer"
	"github.com/dengyie/apihub/model"
	relaycommon "github.com/dengyie/apihub/relay/common"
	kitdto "github.com/dengyie/apihub/relaykit/dto"
	"github.com/dengyie/apihub/relaykit/types"
	"github.com/dengyie/apihub/setting/system_setting"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestShouldRetryRelayErrorHonorsChannelPinOnChannelError(t *testing.T) {
	err := types.NewError(errors.New("channel failed"), types.ErrorCodeChannelNoAvailableKey)
	for _, test := range []struct {
		name      string
		pin       *dto.ChannelPin
		wantRetry bool
	}{
		{name: "unrestricted channel error", wantRetry: true},
		{
			name: "single attempt pin suppresses channel error retry",
			pin: &dto.ChannelPin{
				ChannelId: 1, Source: dto.PinSourceToken, Rank: dto.PinRankToken, RetryMode: dto.PinRetrySingleAttempt,
			},
			wantRetry: false,
		},
		{
			name: "origin task pin permits retry on the same channel",
			pin: &dto.ChannelPin{
				ChannelId: 1, Source: dto.PinSourceOriginTask, Rank: dto.PinRankOriginTask, RetryMode: dto.PinRetrySameChannel,
			},
			wantRetry: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			if test.pin != nil {
				GetChannelConstraints(c).AddPin(*test.pin)
			}
			assert.Equal(t, test.wantRetry, ShouldRetryRelayError(c, err, 1))
		})
	}
}

func TestProcessChannelErrorMasksDisableReasonAndNotification(t *testing.T) {
	previousDB, previousType := model.DB, common.MainDatabaseType()
	previousCache, previousRedis := common.MemoryCacheEnabled, common.RedisEnabled
	previousAutoDisable, previousErrorLog := common.AutomaticDisableChannelEnabled, constant.ErrorLogEnabled
	previousNotifyLimit := constant.NotifyLimitCount
	previousClient, previousWorker := httpClient, system_setting.WorkerUrl
	fetch := system_setting.GetFetchSetting()
	previousFetch := *fetch
	t.Cleanup(func() {
		model.DB = previousDB
		common.SetMainDatabaseType(previousType)
		common.MemoryCacheEnabled, common.RedisEnabled = previousCache, previousRedis
		common.AutomaticDisableChannelEnabled, constant.ErrorLogEnabled = previousAutoDisable, previousErrorLog
		constant.NotifyLimitCount = previousNotifyLimit
		httpClient, system_setting.WorkerUrl = previousClient, previousWorker
		*fetch = previousFetch
	})
	database, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := database.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, database.AutoMigrate(&model.Channel{}, &model.Ability{}, &model.User{}))
	model.DB = database
	common.SetMainDatabaseType(common.DatabaseTypeSQLite)
	common.MemoryCacheEnabled, common.RedisEnabled = false, false
	common.AutomaticDisableChannelEnabled, constant.ErrorLogEnabled = true, false
	constant.NotifyLimitCount = 10
	channel := &model.Channel{Name: "relay-review", Key: "fixture-key", Type: 1, Status: common.ChannelStatusEnabled, Group: "default", Models: "test-model"}
	require.NoError(t, channel.Insert())
	notifications := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		notifications <- body
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	httpClient, system_setting.WorkerUrl = server.Client(), ""
	fetch.EnableSSRFProtection = false
	settings, err := common.Marshal(kitdto.UserSetting{NotifyType: kitdto.NotifyTypeWebhook, WebhookUrl: server.URL})
	require.NoError(t, err)
	root := &model.User{Username: "notification-test-root", Role: common.RoleRootUser, Status: common.UserStatusEnabled, Setting: string(settings)}
	require.NoError(t, database.Create(root).Error)
	notifyKey := fmt.Sprintf("%d:%s:%s", root.Id, formatNotifyType(channel.Id, common.ChannelStatusAutoDisabled), time.Now().Format("2006010215"))
	notifyLimitStore.Delete(notifyKey)
	t.Cleanup(func() { notifyLimitStore.Delete(notifyKey) })
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	apiErr := types.NewErrorWithStatusCode(errors.New("upstream https://private.example.com/path?token=review-token api_key:review-secret"), types.ErrorCodeChannelNoAvailableKey, http.StatusUnauthorized)
	ProcessChannelError(c, types.ChannelError{ChannelId: channel.Id, ChannelName: channel.Name, AutoBan: true}, apiErr, nil)
	var notification WebhookPayload
	select {
	case payload := <-notifications:
		require.NoError(t, common.Unmarshal(payload, &notification))
	case <-time.After(5 * time.Second):
		t.Fatal("automatic channel-disable notification was not delivered")
	}
	loaded, err := model.GetChannelById(channel.Id, true)
	require.NoError(t, err)
	assert.Equal(t, common.ChannelStatusAutoDisabled, loaded.Status)
	wantReason := "status_code=401, upstream https://***.com/***?token=*** api_key:***"
	assert.Equal(t, wantReason, loaded.GetOtherInfo()["status_reason"])
	assert.Contains(t, notification.Content, wantReason)
	assert.NotContains(t, notification.Content, "review-token")
	assert.NotContains(t, notification.Content, "review-secret")
	assert.Equal(t, http.StatusUnauthorized, apiErr.StatusCode)
}

func TestDecideRelayRetryReasons(t *testing.T) {
	upstream := func(status int) *types.NewAPIError {
		return types.NewOpenAIError(errors.New("upstream"), types.ErrorCodeBadResponseStatusCode, status)
	}
	for _, tc := range []struct {
		name    string
		err     *types.NewAPIError
		retries int
		setup   func(*gin.Context)
		want    PolicyDecision
	}{
		{name: "retry status matched", err: upstream(http.StatusTooManyRequests), retries: 1, want: PolicyDecision{Action: "retry", Reason: "retry_status_matched", Source: "global"}},
		{name: "status outside retry rules", err: upstream(http.StatusBadRequest), retries: 1, want: PolicyDecision{Action: "stop", Reason: "status_not_retryable", Source: "global"}},
		{name: "unsupported param 400 retries", err: types.NewOpenAIError(errors.New("unsupported parameter: thinking"), types.ErrorCodeBadResponseStatusCode, http.StatusBadRequest), retries: 1, want: PolicyDecision{Action: "retry", Reason: "bad_request_retry", Source: "loadbalancer"}},
		{name: "stream broken retries", err: types.NewErrorWithStatusCode(&loadbalancer.StreamBrokenError{ChannelID: 1, Reason: "scanner error"}, types.ErrorCodeBadResponseBody, http.StatusBadGateway), retries: 1, want: PolicyDecision{Action: "retry", Reason: "stream_broken", Source: "loadbalancer"}},
		{name: "upstream quota exhausted 400 retries", err: types.NewOpenAIError(errors.New("credit insufficient balance: balance=0 required=9952"), types.ErrorCodeBadResponseStatusCode, http.StatusBadRequest), retries: 1, want: PolicyDecision{Action: "retry", Reason: "upstream_quota_exhausted", Source: "loadbalancer"}},
		{name: "upstream session routing missing 400 retries", err: types.NewOpenAIError(errors.New("Request is missing x-opencode-session and cannot be routed efficiently."), types.ErrorCodeBadResponseStatusCode, http.StatusBadRequest), retries: 1, want: PolicyDecision{Action: "retry", Reason: "upstream_routing_error", Source: "loadbalancer"}},
		{name: "upstream permission denied 403 retries", err: types.NewOpenAIError(errors.New("无权访问 按量分组 分组"), types.ErrorCodeBadResponseStatusCode, http.StatusForbidden), retries: 1, want: PolicyDecision{Action: "retry", Reason: "upstream_permission_denied", Source: "loadbalancer"}},
		{name: "upstream tokenplan model unsupported 404 retries", err: types.NewOpenAIError(errors.New("deepseek-v4-flash is not supported by TokenPlan"), types.ErrorCodeBadResponseStatusCode, http.StatusNotFound), retries: 1, want: PolicyDecision{Action: "retry", Reason: "upstream_permission_denied", Source: "loadbalancer"}},
		{name: "upstream relay bad response status code 400 retries", err: types.NewOpenAIError(errors.New("来自上游渠道的报错: bad response status code 400 (request id: 202609281247566269176407PebRmdX)"), types.ErrorCodeBadResponseStatusCode, http.StatusBadRequest), retries: 1, want: PolicyDecision{Action: "retry", Reason: "upstream_relay_error", Source: "loadbalancer"}},
		{name: "upstream thinking mode history reasoning_content 400 retries", err: types.NewOpenAIError(errors.New("The `reasoning_content` in the thinking mode must be passed back to the API. (request_id: 3392e26e-fd8c-4a6d-ba03-2982501fdef1)"), types.ErrorCodeBadResponseStatusCode, http.StatusBadRequest), retries: 1, want: PolicyDecision{Action: "retry", Reason: "thinking_history_incompatible", Source: "loadbalancer"}},
		{name: "upstream concurrency limit 400 retries", err: types.NewOpenAIError(errors.New("您已达到并发请求数限制：最多同时处理1个请求"), types.ErrorCodeBadResponseStatusCode, http.StatusBadRequest), retries: 1, want: PolicyDecision{Action: "retry", Reason: "upstream_rate_limited", Source: "loadbalancer"}},
		{name: "attempt budget exhausted", err: upstream(http.StatusTooManyRequests), retries: 0, want: PolicyDecision{Action: "stop", Reason: "attempt_budget_exhausted", Source: "global"}},
		// 504/524 曾在 always-skip 清单里永不重试；现放开为常规可重试（见
		// status_code_ranges.go 的注释）：慢上游网关超时改为换渠道故障转移。
		{name: "upstream gateway timeout 504 retries", err: upstream(http.StatusGatewayTimeout), retries: 1, want: PolicyDecision{Action: "retry", Reason: "retry_status_matched", Source: "global"}},
		{name: "success status never retries", err: upstream(http.StatusOK), retries: 1, want: PolicyDecision{Action: "stop", Reason: "system_retry_exclusion", Source: "system"}},
		{name: "skip retry error", err: types.NewErrorWithStatusCode(errors.New("local"), types.ErrorCodeInvalidRequest, http.StatusBadRequest, types.ErrOptionWithSkipRetry()), retries: 1, want: PolicyDecision{Action: "stop", Reason: "non_retryable_error", Source: "system"}},
		{name: "channel error retries", err: types.NewError(errors.New("no key"), types.ErrorCodeChannelNoAvailableKey), retries: 1, want: PolicyDecision{Action: "retry", Reason: "channel_error", Source: "system"}},
		// "channel:" 前缀错误曾经无条件换渠道重试，绕过了上面两道闸门。
		{name: "channel error respects budget", err: types.NewError(errors.New("no key"), types.ErrorCodeChannelNoAvailableKey), retries: 0, want: PolicyDecision{Action: "stop", Reason: "attempt_budget_exhausted", Source: "global"}},
		{name: "channel error respects skip retry", err: types.NewError(errors.New("local override rejected"), types.ErrorCodeChannelParamOverrideInvalid, types.ErrOptionWithSkipRetry()), retries: 1, want: PolicyDecision{Action: "stop", Reason: "non_retryable_error", Source: "system"}},
		// TTFT 超时的错误码就是 channel:response_time_exceeded（IsChannelError 判前缀），
		// 它此前被 channel_error 分支抢先命中，导致 ttft_timeout 永不可达。
		{name: "ttft timeout reports its own reason", err: types.NewErrorWithStatusCode(&loadbalancer.TTFTTimeoutError{ChannelID: 1, TimeoutMs: 30000}, types.ErrorCodeChannelResponseTimeExceeded, http.StatusGatewayTimeout), retries: 2, want: PolicyDecision{Action: "retry", Reason: "ttft_timeout", Source: "loadbalancer"}},
		{name: "ttft timeout respects budget", err: types.NewErrorWithStatusCode(&loadbalancer.TTFTTimeoutError{ChannelID: 1, TimeoutMs: 30000}, types.ErrorCodeChannelResponseTimeExceeded, http.StatusGatewayTimeout), retries: 0, want: PolicyDecision{Action: "stop", Reason: "attempt_budget_exhausted", Source: "global"}},
		{name: "single attempt pin", err: upstream(http.StatusTooManyRequests), retries: 1, setup: func(c *gin.Context) {
			GetChannelConstraints(c).AddPin(dto.ChannelPin{ChannelId: 1, Source: dto.PinSourceToken, Rank: dto.PinRankToken, RetryMode: dto.PinRetrySingleAttempt})
		}, want: PolicyDecision{Action: "stop", Reason: "pinned_channel", Source: "channel_constraint"}},
		{name: "strict session", err: upstream(http.StatusTooManyRequests), retries: 1, setup: func(c *gin.Context) {
			c.Set(ginKeyChannelAffinitySkipRetry, true)
			RequestPolicy(c).SessionModeSource = "global"
		}, want: PolicyDecision{Action: "stop", Reason: "strict_session", Source: "global"}},
		{name: "nil error", retries: 1, want: PolicyDecision{Action: "stop", Reason: "request_completed", Source: "system"}},
		{name: "client aborted never retries", err: types.NewClientAbortedError(context.Canceled), retries: 3, want: PolicyDecision{Action: "stop", Reason: "client_aborted", Source: "local"}},
		{name: "response committed never retries", err: upstream(http.StatusBadGateway), retries: 2, setup: func(c *gin.Context) {
			_, _ = c.Writer.Write([]byte("data: partial response\n\n"))
		}, want: PolicyDecision{Action: "stop", Reason: "response_committed", Source: "system"}},
		{name: "upstream model disabled on gateway 400 retries", err: types.NewOpenAIError(errors.New("model is disabled on this gateway: deepseek/deepseek-v4.1-flash"), types.ErrorCodeBadResponseStatusCode, http.StatusBadRequest), retries: 1, want: PolicyDecision{Action: "retry", Reason: "model_unavailable_retry", Source: "loadbalancer"}},
		{name: "upstream model temporarily unavailable 400 retries", err: types.NewOpenAIError(errors.New("模型 'deepseek-v4-pro-free' 暂不可用，请稍后重试。"), types.ErrorCodeBadResponseStatusCode, http.StatusBadRequest), retries: 1, want: PolicyDecision{Action: "retry", Reason: "upstream_relay_error", Source: "loadbalancer"}},
		{name: "empty stream budget exhausted never retries", err: types.NewErrorWithStatusCode(&loadbalancer.EmptyStreamBudgetError{ChannelID: 1}, types.ErrorCodeEmptyStreamBudgetExhausted, http.StatusBadGateway, types.ErrOptionWithSkipRetry()), retries: 3, want: PolicyDecision{Action: "stop", Reason: "empty_stream_budget_exhausted", Source: "loadbalancer"}},
		{name: "empty stream still retries", err: types.NewErrorWithStatusCode(&loadbalancer.EmptyStreamError{ChannelID: 1}, types.ErrorCodeBadResponseBody, http.StatusBadGateway), retries: 2, want: PolicyDecision{Action: "retry", Reason: "empty_stream", Source: "loadbalancer"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			if tc.setup != nil {
				tc.setup(c)
			}
			decision := DecideRelayRetry(c, tc.err, tc.retries)
			assert.Equal(t, tc.want, decision)
			assert.Equal(t, tc.want.Action == "retry", ShouldRetryRelayError(c, tc.err, tc.retries))
		})
	}
}

func TestRequestPolicyEventsReachLogAdminInfo(t *testing.T) {
	previousAutoDisable := common.AutomaticDisableChannelEnabled
	common.AutomaticDisableChannelEnabled = true
	t.Cleanup(func() { common.AutomaticDisableChannelEnabled = previousAutoDisable })
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Set("auto_ban", true)
	c.Set("channel_id", 7)
	state := RequestPolicy(c)
	state.BeginAttempt(&model.Channel{Id: 7}, "default")
	apiErr := types.NewOpenAIError(errors.New("invalid credential"), types.ErrorCodeBadResponseStatusCode, http.StatusUnauthorized)
	RecordPolicyFailure(c, 7, apiErr, DecideRelayRetry(c, apiErr, 0))

	failed := model.NewLogOther()
	AppendRelayLogAdminInfo(c, nil, failed)
	events, ok := failed.Snapshot()["admin_info"].(map[string]any)["request_policy"].([]PolicyEvent)
	require.True(t, ok, "a failed relay exposes its decision events to administrators")
	require.Len(t, events, 3)
	assert.Equal(t, PolicyDecision{Action: "attempt", Reason: "channel_selected", Source: "routing"}, events[0].Decision)
	assert.Equal(t, "default", events[0].Group)
	assert.Equal(t, PolicyDecision{Action: "failure", Reason: "upstream_failure", Source: "upstream"}, events[1].Decision)
	assert.Equal(t, http.StatusUnauthorized, events[1].Status)
	assert.Equal(t, PolicyDecision{Action: "stop", Reason: "attempt_budget_exhausted", Source: "global"}, events[2].Decision)
	assert.Equal(t, "channel_disable_requested", events[2].Health, "the health entry follows the automatic disable rules")
	common.SetContextKey(c, constant.ContextKeyChannelIsMultiKey, true)
	RecordPolicyFailure(c, 7, apiErr, DecideRelayRetry(c, apiErr, 0))
	assert.Equal(t, "key_disable_requested", state.Events()[4].Health)

	state.BeginAttempt(&model.Channel{Id: 8}, "default")
	c.Set("channel_id", 8)
	MarkRequestPolicySuccess(c, nil)
	MarkRequestPolicySuccess(c, nil)
	succeeded := model.NewLogOther()
	AppendRelayLogAdminInfo(c, nil, succeeded)
	events, ok = succeeded.Snapshot()["admin_info"].(map[string]any)["request_policy"].([]PolicyEvent)
	require.True(t, ok, "a successful relay exposes its decision events to administrators")
	require.Len(t, events, 7, "the outcome is recorded once")
	assert.Equal(t, PolicyDecision{Action: "success", Reason: "request_completed", Source: "upstream"}, events[6].Decision)
	assert.Equal(t, 8, events[6].ChannelID)
	assert.Equal(t, 2, events[6].Attempt)
	assert.True(t, state.Successful)

	untouched, _ := gin.CreateTestContext(httptest.NewRecorder())
	other := model.NewLogOther()
	AppendRelayLogAdminInfo(untouched, nil, other)
	assert.NotContains(t, other.Snapshot()["admin_info"], "request_policy", "requests without decisions do not carry an empty record")
}

func TestIsClientAbortUsesRequestFacts(t *testing.T) {
	gin.SetMode(gin.TestMode)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(canceled)
	assert.True(t, IsClientAbort(c, nil, types.NewError(errors.New("do request failed"), types.ErrorCodeDoRequestFailed)))

	timedOut, timeoutCancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer timeoutCancel()
	<-timedOut.Done()
	c2, _ := gin.CreateTestContext(httptest.NewRecorder())
	c2.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(timedOut)
	assert.False(t, IsClientAbort(c2, nil, types.NewError(errors.New("deadline"), types.ErrorCodeDoRequestFailed)))

	status := relaycommon.NewStreamStatus()
	status.SetEndReason(relaycommon.StreamEndReasonClientGone, context.Canceled)
	assert.True(t, IsClientAbort(nil, &relaycommon.RelayInfo{StreamStatus: status}, nil))

	assert.True(t, IsClientAbort(nil, nil, types.NewClientAbortedError(context.Canceled)))
}

func TestShouldDisableChannelSkipsClientAbortAndEmptyStream(t *testing.T) {
	previous := common.AutomaticDisableChannelEnabled
	common.AutomaticDisableChannelEnabled = true
	t.Cleanup(func() { common.AutomaticDisableChannelEnabled = previous })

	assert.False(t, ShouldDisableChannel(0, types.NewClientAbortedError(context.Canceled)))
	assert.False(t, ShouldDisableChannel(0, types.NewErrorWithStatusCode(
		&loadbalancer.EmptyStreamError{ChannelID: 1},
		types.ErrorCodeBadResponseBody,
		http.StatusBadGateway,
	)))
	assert.False(t, ShouldDisableChannel(0, types.NewErrorWithStatusCode(
		&loadbalancer.EmptyStreamBudgetError{ChannelID: 1},
		types.ErrorCodeEmptyStreamBudgetExhausted,
		http.StatusBadGateway,
		types.ErrOptionWithSkipRetry(),
	)))
}

// upstreamError 造一条生产原样的错误：上游报文经
// relay/channel/openai/relay-openai.go:314 的 types.WithOpenAIError 转成
// NewAPIError，错误码取自上游报文自己的 code 字段。
//
// **不要**换成 types.NewErrorWithStatusCode(err, types.ErrorCodeBadResponseBody, ...)
// 来"简化"这个构造：ErrorCodeBadResponseBody 在 alwaysSkipCodes 的默认名单里，
// DecideRelayRetry 会在到达参数分支之前就 system_retry_exclusion 停掉，
// 断言随之变成永远成立、什么也证明不了。生产里这类错误的码来自上游报文，
// 缺省是 unknown_error。
func upstreamError(statusCode int, msg string) *types.NewAPIError {
	return types.WithOpenAIError(types.OpenAIError{Message: msg}, statusCode)
}

// TestDecideRelayRetryTreatsParamErrorAsRequestShape pins the reason string for
// a parameter rejection no matter which status code the upstream wrapped it in.
//
// 为什么值得钉：controller 的重试循环是
// 「processChannelError（标记裁剪）→ 继续下一次尝试」，而下一轮
// ConvertOpenAIRequest 出站前就会用刚学到的参数裁剪，请求当场成功。
// 所以参数错误必须走「换渠道重试」这条路，reason 也必须是 bad_request_retry
// —— 原因字符串会进日志明细，排障时要能一眼看出这是裁剪生效而不是上游故障。
//
// v29.32 之前 5xx 在参数识别里被硬门槛挡住，于是同一条报文会掉到
// IsUpstreamRelayError 变成 upstream_relay_error：重试路径倒还对（不至于
// 直接把 500 回给客户端），但 reason 是错的，而且它同时会喂给熔断判据，
// 把一条好渠道按「上游中继故障」熔掉。
func TestDecideRelayRetryTreatsParamErrorAsRequestShape(t *testing.T) {
	const wrappedParamMsg = "Validation: Unsupported parameter(s): `enable_thinking`"

	for _, test := range []struct {
		name       string
		statusCode int
		msg        string
		wantReason string
		wantAction string
	}{
		{
			name:       "400 param error retries for param strip",
			statusCode: http.StatusBadRequest,
			msg:        wrappedParamMsg,
			wantReason: "bad_request_retry",
			wantAction: "retry",
		},
		{
			name:       "422 param error retries for param strip",
			statusCode: http.StatusUnprocessableEntity,
			msg:        wrappedParamMsg,
			wantReason: "bad_request_retry",
			wantAction: "retry",
		},
		{
			// 生产实测渠道 #238：中转层把同一条校验报文包在自己的 500 里。
			name:       "500 wrapped param error retries for param strip",
			statusCode: http.StatusInternalServerError,
			msg:        wrappedParamMsg,
			wantReason: "bad_request_retry",
			wantAction: "retry",
		},
		{
			name:       "400 invalid codex request retries for codex validation error",
			statusCode: http.StatusBadRequest,
			msg:        `{"error":{"message":"invalid codex request","type":"new_api_error","code":"invalid_responses_request"}}`,
			wantReason: "codex_validation_error",
			wantAction: "retry",
		},
		{
			name:       "500 without param wording stays an upstream fault",
			statusCode: http.StatusInternalServerError,
			msg:        "upstream request failed",
			wantReason: "upstream_relay_error",
			wantAction: "retry",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			got := DecideRelayRetry(c, upstreamError(test.statusCode, test.msg), 1)
			assert.Equal(t, test.wantReason, got.Reason)
			assert.Equal(t, test.wantAction, got.Action)
		})
	}
}

// TestIsUpstreamRelayErrorExcludesParamErrorAcross5xx 是上面那条的熔断侧断言。
//
// IsUpstreamRelayError 是熔断判据：命中就 TripBreaker。参数不支持是请求形状
// 问题、渠道本身完全健康，裁掉参数重发即可，把它算作中继故障等于用一次
// 误判把好渠道熔掉。400 上这条豁免早在 v29.19 就存在，放宽识别到 5xx 之后
// 它必须自动跟着覆盖 5xx —— 判据共用 IsParamNotSupportedError，不该有第二份。
func TestIsUpstreamRelayErrorExcludesParamErrorAcross5xx(t *testing.T) {
	const wrappedParamMsg = "Validation: Unsupported parameter(s): `enable_thinking`"

	assert.False(t, loadbalancer.IsUpstreamRelayError(upstreamError(
		http.StatusBadRequest, wrappedParamMsg)),
		"400 参数错误不得计入中继失效熔断")
	assert.False(t, loadbalancer.IsUpstreamRelayError(upstreamError(
		http.StatusInternalServerError, wrappedParamMsg)),
		"5xx 包着的参数错误同样不得计入中继失效熔断")
	assert.True(t, loadbalancer.IsUpstreamRelayError(upstreamError(
		http.StatusInternalServerError, "upstream request failed")),
		"普通的 500 仍然是中继故障 —— 豁免只能窄化到参数措辞，不能把 5xx 一刀切")
}

func TestDecideRelayRetryReasoningHydrationError(t *testing.T) {
	const hydrationMsg = "The encrypted content for item rs_01fd could not be verified. Reason: reasoning hydration failed: Encrypted content could not be decrypted or parsed."
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	got := DecideRelayRetry(c, upstreamError(http.StatusBadRequest, hydrationMsg), 2)
	assert.Equal(t, "retry", got.Action)
	assert.Equal(t, "reasoning_hydration_failed", got.Reason)
}
