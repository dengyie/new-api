package model

import (
	"testing"
	"time"

	"github.com/dengyie/apihub/common"
	"github.com/dengyie/apihub/loadbalancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannelCandidates_TopAvailablePriority(t *testing.T) {
	loadbalancer.SetPolicy(&loadbalancer.Policy{
		Enabled: true,
		Default: loadbalancer.ChannelPolicy{
			Breaker: loadbalancer.BreakerPolicy{
				FailureThreshold: 5,
				CooldownSeconds:  60,
			},
		},
	})
	t.Cleanup(func() {
		loadbalancer.SetPolicy(&loadbalancer.Policy{Enabled: false})
	})

	const modelName = "test-model-priority"
	p10 := int64(10)
	p8 := int64(8)
	p5 := int64(5)

	ch1 := &Channel{Id: 801, Status: common.ChannelStatusEnabled, Priority: &p10}
	ch2 := &Channel{Id: 802, Status: common.ChannelStatusEnabled, Priority: &p8}
	ch3 := &Channel{Id: 803, Status: common.ChannelStatusEnabled, Priority: &p5}

	candidates := &ChannelCandidates{channels: []*Channel{ch1, ch2, ch3}}

	// 1. 全部渠道健康时，返回最高优先级 10
	top, ok := candidates.TopAvailablePriority(nil, modelName)
	require.True(t, ok)
	assert.Equal(t, int64(10), top)

	// 2. 模拟 ch1 (优先级 10) 连续慢 3 次降级
	tr := loadbalancer.GlobalTracker()
	for i := 0; i < 3; i++ {
		tr.Begin(ch1.Id, modelName).End(true, false)
	}
	require.True(t, tr.IsDegraded(ch1.Id, modelName))

	// 降级后，最高健康优先级应为 ch2 (优先级 8)
	top, ok = candidates.TopAvailablePriority(nil, modelName)
	require.True(t, ok)
	assert.Equal(t, int64(8), top, "当高优先级渠道处于降级状态时，应优先使用健康渠道的最高优先级")

	// 3. 排除 ch2
	top, ok = candidates.TopAvailablePriority(map[int]struct{}{ch2.Id: {}}, modelName)
	require.True(t, ok)
	assert.Equal(t, int64(5), top)

	// 4. 当全部健康渠道排除/不可用，仅剩降级渠道时，回退到降级渠道的最高优先级
	top, ok = candidates.TopAvailablePriority(map[int]struct{}{ch2.Id: {}, ch3.Id: {}}, modelName)
	require.True(t, ok)
	assert.Equal(t, int64(10), top, "无任何健康渠道可用时，回退使用降级渠道的最高优先级")

	// 5. 渠道硬熔断后不可用
	tr.TripBreakerUntil(ch1.Id, modelName, time.Now().Add(time.Hour))
	t.Cleanup(func() {
		tr.TripBreakerUntil(ch1.Id, modelName, time.Now().Add(-time.Hour))
	})
	top, ok = candidates.TopAvailablePriority(map[int]struct{}{ch2.Id: {}, ch3.Id: {}}, modelName)
	assert.False(t, ok)
	assert.Equal(t, int64(0), top)
}
