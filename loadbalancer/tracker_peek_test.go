package loadbalancer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTrackerPeekAvailableNoSideEffect(t *testing.T) {
	old := currentPolicy.Load()
	currentPolicy.Store(&Policy{
		Enabled: true,
		Default: ChannelPolicy{
			MaxInflight: 2,
			Breaker: BreakerPolicy{
				FailureThreshold: 1,
				CooldownSeconds:  1,
				HalfOpenProbes:   1,
			},
		},
	})
	defer currentPolicy.Store(old)

	tr := newTestTracker()
	channelID := 9999
	model := testModel

	// 触发一次硬故障使渠道熔断
	h := tr.Begin(channelID, model)
	h.End(false, true)

	// 刚熔断，在 1s 冷却期内
	avail, reason := tr.PeekAvailable(channelID, model)
	assert.False(t, avail)
	assert.Equal(t, ReasonCircuitOpen, reason)

	// 等待冷却期过去进入半开
	time.Sleep(1100 * time.Millisecond)

	// PeekAvailable 纯只读探测，绝不自增 halfOpenProbes
	for i := 0; i < 5; i++ {
		avail, reason = tr.PeekAvailable(channelID, model)
		assert.True(t, avail, "第 %d 次 PeekAvailable 必须返回可用", i)
		assert.Empty(t, reason)
	}

	// 确认 halfOpenProbes 计数器仍为 0
	k := breakerKey{channelID: channelID}
	s := tr.getBreaker(k)
	assert.Equal(t, int32(0), s.halfOpenProbes.Load(), "PeekAvailable 绝不得预占 halfOpenProbes")

	// 真实选路调用 IsAvailable，此时应成功并预占 1 个探测配额
	ok, r := tr.IsAvailable(channelID, model)
	assert.True(t, ok)
	assert.Empty(t, r)
	assert.Equal(t, int32(1), s.halfOpenProbes.Load())

	// 第二次 IsAvailable 会超额被拒
	ok2, r2 := tr.IsAvailable(channelID, model)
	assert.False(t, ok2)
	assert.Equal(t, ReasonHalfOpenProbesExceeded, r2)
}

func TestTrackerPeekAvailableOverloaded(t *testing.T) {
	old := currentPolicy.Load()
	currentPolicy.Store(&Policy{
		Enabled: true,
		Default: ChannelPolicy{
			MaxInflight: 1,
			Breaker: BreakerPolicy{
				FailureThreshold: 5,
				CooldownSeconds:  60,
				HalfOpenProbes:   1,
			},
		},
	})
	defer currentPolicy.Store(old)

	tr := newTestTracker()
	channelID := 8888
	model := testModel

	avail, _ := tr.PeekAvailable(channelID, model)
	assert.True(t, avail)

	h := tr.Begin(channelID, model)
	require.NotNil(t, h)

	// 并发打满后 PeekAvailable 应返回 overload
	avail, reason := tr.PeekAvailable(channelID, model)
	assert.False(t, avail)
	assert.Equal(t, ReasonOverloaded, reason)

	h.End(false, false)

	avail, _ = tr.PeekAvailable(channelID, model)
	assert.True(t, avail)
}
