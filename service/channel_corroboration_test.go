package service

import (
	"errors"
	"net/http"
	"testing"

	"github.com/dengyie/apihub/common"
	"github.com/dengyie/apihub/loadbalancer"
	"github.com/dengyie/apihub/relaykit/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// corroborationFixture 装好「自动禁用开着 + 策略可信 + 阈值 N」这套环境，
// 并保证包级佐证表在用例之间不串味。
type corroborationFixture struct {
	threshold int
}

func newCorroborationFixture(t *testing.T, threshold int) *corroborationFixture {
	t.Helper()

	restoreAutoDisable := common.AutomaticDisableChannelEnabled
	common.AutomaticDisableChannelEnabled = true

	prevPolicy := loadbalancer.GetPolicy()
	p := loadbalancer.DefaultPolicy()
	p.Enabled = true
	p.Default.Breaker.AutoDisableCorroborationThreshold = threshold
	p.Default.Breaker.AutoDisableCorroborationWindowSeconds = 600
	loadbalancer.SetPolicy(p)

	restoreFailed := loadbalancer.MarkPolicyLoadFailedForTest(false)

	t.Cleanup(func() {
		common.AutomaticDisableChannelEnabled = restoreAutoDisable
		loadbalancer.SetPolicy(prevPolicy)
		restoreFailed()
	})
	return &corroborationFixture{threshold: threshold}
}

func (f *corroborationFixture) reset(t *testing.T) {
	t.Helper()
	loadbalancer.ResetCorroborationForChannel(9001)
	loadbalancer.ResetCorroborationForChannel(1)
}

func authError() *types.NewAPIError {
	return types.NewErrorWithStatusCode(errors.New("invalid api key"), types.ErrorCodeBadResponseBody, http.StatusUnauthorized)
}

// TestShouldDisableChannelStaysPure 是这个接线里最关键的一条不变式。
//
// ShouldDisableChannel 被同一次失败评估两次（controller/relay.go 先
// RecordPolicyFailure、后 processChannelError）。若它顺手计数，一次失败
// 记两次，佐证阈值形同虚设 —— 而症状是「阈值调到 3 也挡不住一次误判」，
// 表现为机制看起来在工作、实际已经失效，排查成本极高。
func TestShouldDisableChannelStaysPure(t *testing.T) {
	newCorroborationFixture(t, 3)
	defer loadbalancer.ResetCorroborationForChannel(1)

	for i := 0; i < 50; i++ {
		require.True(t, ShouldDisableChannel(1, authError()),
			"纯判定不受调用次数影响：这是判定，不是计数")
	}
	assert.Equal(t, 0, loadbalancer.CorroborationCount(1, "", loadbalancer.CorroborationClassStatusCode),
		"ShouldDisableChannel 绝不能碰佐证计数")
}

// TestCorroborationGateHoldsBackFirstSignals 钉住新行为的主干：
// 未达阈值一律不放行，达到才放行。
func TestCorroborationGateHoldsBackFirstSignals(t *testing.T) {
	f := newCorroborationFixture(t, 3)
	f.reset(t)
	defer loadbalancer.ResetCorroborationForChannel(1)

	err := authError()
	assert.False(t, ShouldDisableChannelCorroborated(1, "gpt-4o", err),
		"第 1 次：不得执行不可逆的禁用")
	assert.False(t, ShouldDisableChannelCorroborated(1, "gpt-4o", err),
		"第 2 次：仍不得")
	assert.True(t, ShouldDisableChannelCorroborated(1, "gpt-4o", err),
		"第 3 次达到阈值才放行")
	assert.True(t, ShouldDisableChannelCorroborated(1, "gpt-4o", err),
		"超过阈值后继续放行，直到有别的东西把它摘掉")
}

// TestCorroborationGateSkipsNonDisableErrors 确认闸门只作用在「本来就该禁」
// 的错误上。其它错误连计数都不该发生 —— 否则无关失败会凭空把某个桶推上
// 阈值，等真正该禁时反而立刻被禁。
func TestCorroborationGateSkipsNonDisableErrors(t *testing.T) {
	newCorroborationFixture(t, 3)
	defer loadbalancer.ResetCorroborationForChannel(1)

	clientAbort := types.NewClientAbortedError(errors.New("context canceled"))
	for i := 0; i < 10; i++ {
		assert.False(t, ShouldDisableChannelCorroborated(1, "m", clientAbort))
	}
	assert.Equal(t, 0, loadbalancer.CorroborationCount(1, "m", loadbalancer.CorroborationClassStatusCode))
	assert.Equal(t, 0, loadbalancer.CorroborationCount(1, "m", loadbalancer.CorroborationClassChannelError))
	assert.Equal(t, 0, loadbalancer.CorroborationCount(1, "m", loadbalancer.CorroborationClassKeyword))
}

// TestCorroborationGateRespectsBreakerExempt 兜底渠道豁免必须仍然优先：
// 佐证再多也不能把 CPA 兜底渠道摘掉。
func TestCorroborationGateRespectsBreakerExempt(t *testing.T) {
	newCorroborationFixture(t, 2)
	f := &corroborationFixture{threshold: 2}
	f.reset(t)
	defer loadbalancer.ResetCorroborationForChannel(8)

	p := loadbalancer.DefaultPolicy()
	p.Enabled = true
	p.Default.Breaker.AutoDisableCorroborationThreshold = 2
	p.Default.Breaker.AutoDisableCorroborationWindowSeconds = 600
	p.Channels = map[int]loadbalancer.ChannelPolicy{}
	// 先确认不豁免时它确实会被禁
	loadbalancer.SetPolicy(p)
	assert.False(t, ShouldDisableChannelCorroborated(8, "m", authError()))
	assert.True(t, ShouldDisableChannelCorroborated(8, "m", authError()),
		"前置条件：不豁免时第二次即达阈值")
	loadbalancer.ResetCorroborationForChannel(8)

	// 加入豁免
	p.Channels[8] = loadbalancer.ChannelPolicy{BreakerExempt: true}
	loadbalancer.SetPolicy(p)
	assert.False(t, ShouldDisableChannelCorroborated(8, "m", authError()),
		"豁免渠道无论攒多少次信号都不得被自动禁用")
}

// TestCorroborationGateFailsSafeOnPolicyLoadFailure 策略加载失败时闸门必须
// 整体停摆（而不是因为「读不到阈值」把自动禁用全关掉，那会让兜底链路失效）。
func TestCorroborationGateFailsSafeOnPolicyLoadFailure(t *testing.T) {
	newCorroborationFixture(t, 3)
	defer loadbalancer.ResetCorroborationForChannel(1)

	restore := loadbalancer.MarkPolicyLoadFailedForTest(true)
	defer restore()

	for i := 0; i < 5; i++ {
		assert.False(t, ShouldDisableChannelCorroborated(1, "m", authError()),
			"判据不可信时一律不自动禁用")
	}
}

// TestCorroborationGateThresholdOneRestoresLegacy 止血档位：阈值 1 必须完全
// 退回 v29.19 的「一次即禁」，否则出问题时没有回退手段。
func TestCorroborationGateThresholdOneRestoresLegacy(t *testing.T) {
	newCorroborationFixture(t, 1)
	defer loadbalancer.ResetCorroborationForChannel(1)

	assert.True(t, ShouldDisableChannelCorroborated(1, "m", authError()),
		"阈值为 1 时第一次即放行，与旧版逐位一致")
}

// TestClassifyAutoDisableAssignsDistinctClasses 钉住类别划分：不同判据不得
// 共用一个桶。共桶的后果是「凭据失效」与「这个模型不存在」互相充当佐证，
// 而两者要修的东西完全不同。
func TestClassifyAutoDisableAssignsDistinctClasses(t *testing.T) {
	newCorroborationFixture(t, 3)
	defer loadbalancer.ResetCorroborationForChannel(1)

	authVerdict := classifyAutoDisable(1, authError())
	require.True(t, authVerdict.Disable)
	assert.Equal(t, loadbalancer.CorroborationClassStatusCode, authVerdict.Class)

	// 非禁用类必须返回空类别，否则日志与统计会把它们算进去
	for _, err := range []*types.NewAPIError{
		nil,
		types.NewClientAbortedError(errors.New("canceled")),
	} {
		v := classifyAutoDisable(1, err)
		assert.False(t, v.Disable)
		assert.Empty(t, v.Class, "未命中禁用判据时不得带类别")
	}
}

// TestClassifyAutoDisableRespectsGlobalSwitch 总开关关闭时，
// 带佐证的版本也必须返回 false —— 佐证不能绕过人工的总开关。
func TestClassifyAutoDisableRespectsGlobalSwitch(t *testing.T) {
	newCorroborationFixture(t, 1)
	defer loadbalancer.ResetCorroborationForChannel(1)

	common.AutomaticDisableChannelEnabled = false
	for i := 0; i < 5; i++ {
		assert.False(t, ShouldDisableChannelCorroborated(1, "m", authError()),
			"总开关关闭时，佐证机制不得成为绕过它的后门")
	}
}

// TestProcessChannelErrorDoesNotCountWhenAutoBanOff 钉住求值顺序。
//
// 禁用闸门写的是 `AutoBan && ShouldDisableChannelCorroborated(...)`，而不是
// 反过来。ShouldDisableChannelCorroborated 不是纯读 —— 它会
// RecordAutoDisableSignal（计数 +1）并打出「holding back disable」。写在左边
// 时 Go 从左往右求值，于是 auto_ban=0 的渠道也照常累加计数、并被告知「闸门
// 在拦我」，而它永远不会被摘掉：日志把人引向错误方向，而日后一旦打开
// auto_ban，第一次真实失败就顶到阈值被摘掉 —— 恰好是佐证要防的「一次即禁」。
func TestProcessChannelErrorDoesNotCountWhenAutoBanOff(t *testing.T) {
	newCorroborationFixture(t, 3)
	const channelID = 9101
	const model = "model-that-autoban-would-forbid"
	defer loadbalancer.ResetCorroborationForChannel(channelID)

	c := clampTestContext()
	c.Set("original_model", model)

	for i := 0; i < 5; i++ {
		ProcessChannelError(c, types.ChannelError{ChannelId: channelID, AutoBan: false}, authError(), nil)
	}

	assert.Equal(t, 0, loadbalancer.CorroborationCount(channelID, model, loadbalancer.CorroborationClassStatusCode),
		"auto_ban 关闭的渠道不得累加佐证计数")
}
