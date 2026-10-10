package service

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dengyie/apihub/common"
	"github.com/dengyie/apihub/constant"
	"github.com/dengyie/apihub/loadbalancer"
	"github.com/dengyie/apihub/model"
	"github.com/dengyie/apihub/setting/operation_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordChannelAffinity_AntiDowngradeProtection(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 开启内存缓存并设置测试渠道
	common.MemoryCacheEnabled = true
	p10 := int64(10)
	p8 := int64(8)
	p5 := int64(5)

	chHigh := &model.Channel{
		Id:       154,
		Name:     "cpa-anyrouter",
		Status:   common.ChannelStatusEnabled,
		Priority: &p10,
	}
	chMid := &model.Channel{
		Id:       99,
		Name:     "fallback-mid",
		Status:   common.ChannelStatusEnabled,
		Priority: &p8,
	}
	chLow := &model.Channel{
		Id:       216,
		Name:     "fallback-low",
		Status:   common.ChannelStatusEnabled,
		Priority: &p5,
	}

	model.CacheUpdateChannel(chHigh)
	model.CacheUpdateChannel(chMid)
	model.CacheUpdateChannel(chLow)

	setting := operation_setting.GetChannelAffinitySetting()
	require.NotNil(t, setting)
	origEnabled := setting.Enabled
	origSwitchOnSuccess := setting.SwitchOnSuccess
	setting.Enabled = true
	setting.SwitchOnSuccess = true
	t.Cleanup(func() {
		setting.Enabled = origEnabled
		setting.SwitchOnSuccess = origSwitchOnSuccess
	})

	cacheKeySuffix := fmt.Sprintf("test-affinity-downgrade-%d", time.Now().UnixNano())
	cache := getChannelAffinityCache()
	t.Cleanup(func() {
		_, _ = cache.DeleteMany([]string{cacheKeySuffix})
	})

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	setChannelAffinityContext(c, channelAffinityMeta{
		CacheKey:   cacheKeySuffix,
		TTLSeconds: 300,
		RuleName:   "prompt_cache_key",
	})

	// 1. 首次成功落在高优先级渠道 154 (Priority 10)
	RecordChannelAffinity(c, chHigh.Id)
	val, found, err := cache.Get(cacheKeySuffix)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, chHigh.Id, val, "初始亲和度应记录为高优先级渠道 154")

	// 2. 模拟高优先级渠道临时抖动，重试由中优先级 99 (Priority 8) 兜底成功
	// 防降级保护必须阻止将缓存覆盖为 99！
	c.Set("channel_id", chMid.Id)
	RecordChannelAffinity(c, chMid.Id)
	val, found, err = cache.Get(cacheKeySuffix)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, chHigh.Id, val, "低优先级渠道 99 兜底成功不得降级覆盖高优先级亲和度 154")

	// 3. 模拟更低优先级 216 (Priority 5) 兜底成功
	c.Set("channel_id", chLow.Id)
	RecordChannelAffinity(c, chLow.Id)
	val, found, err = cache.Get(cacheKeySuffix)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, chHigh.Id, val, "低优先级渠道 216 兜底成功不得降级覆盖高优先级亲和度 154")

	// 4. 若原高优先级渠道被禁用 (ChannelStatusManuallyDisabled)，则允许由可用渠道接管
	chHigh.Status = common.ChannelStatusManuallyDisabled
	model.CacheUpdateChannel(chHigh)
	c.Set("channel_id", chMid.Id)
	RecordChannelAffinity(c, chMid.Id)
	val, found, err = cache.Get(cacheKeySuffix)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, chMid.Id, val, "当原高优先级渠道禁用时，允许更新亲和度")
}

func TestSelectChannelForRequest_TopPriorityPrecedesLowerPriorityAffinity(t *testing.T) {
	db := setupChannelSelectTest(t)
	common.MemoryCacheEnabled = false
	const modelName = "gpt-6-astra-priority-test"

	p10 := int64(10)
	p8 := int64(8)
	weight := uint(100)

	chHigh := &model.Channel{
		Id:       154,
		Type:     constant.ChannelTypeOpenAI,
		Key:      "key-154",
		Status:   common.ChannelStatusEnabled,
		Name:     "cpa-anyrouter",
		Weight:   &weight,
		Models:   modelName,
		Group:    "default",
		Priority: &p10,
	}
	chMid := &model.Channel{
		Id:       99,
		Type:     constant.ChannelTypeOpenAI,
		Key:      "key-99",
		Status:   common.ChannelStatusEnabled,
		Name:     "fallback-mid",
		Weight:   &weight,
		Models:   modelName,
		Group:    "default",
		Priority: &p8,
	}

	require.NoError(t, db.Create(chHigh).Error)
	require.NoError(t, db.Create(&model.Ability{
		Group:     "default",
		Model:     modelName,
		ChannelId: 154,
		Enabled:   true,
		Priority:  &p10,
		Weight:    weight,
	}).Error)

	require.NoError(t, db.Create(chMid).Error)
	require.NoError(t, db.Create(&model.Ability{
		Group:     "default",
		Model:     modelName,
		ChannelId: 99,
		Enabled:   true,
		Priority:  &p8,
		Weight:    weight,
	}).Error)

	loadbalancer.SetPolicy(&loadbalancer.Policy{
		Enabled: true,
		Default: loadbalancer.ChannelPolicy{
			Breaker: loadbalancer.BreakerPolicy{
				FailureThreshold: 1,
				CooldownSeconds:  60,
			},
		},
	})
	t.Cleanup(func() {
		loadbalancer.SetPolicy(&loadbalancer.Policy{Enabled: false})
	})

	rule := operation_setting.ChannelAffinityRule{
		Name:             "rule-priority-test",
		ModelRegex:       []string{fmt.Sprintf("^%s$", modelName)},
		PathRegex:        []string{"/v1/.*"},
		KeySources:       []operation_setting.ChannelAffinityKeySource{{Type: "request_header", Key: "X-Session-Key"}},
		IncludeRuleName:  true,
		IncludeModelName: true,
	}

	setting := operation_setting.GetChannelAffinitySetting()
	origEnabled := setting.Enabled
	origRules := setting.Rules
	setting.Enabled = true
	setting.Rules = append([]operation_setting.ChannelAffinityRule{rule}, origRules...)
	t.Cleanup(func() {
		setting.Enabled = origEnabled
		setting.Rules = origRules
	})

	affinityVal := fmt.Sprintf("sess-test-%d", time.Now().UnixNano())
	cacheKeySuffix := buildChannelAffinityCacheKeySuffix(rule, modelName, "default", affinityVal)
	cache := getChannelAffinityCache()
	require.NoError(t, cache.SetWithTTL(cacheKeySuffix, chMid.Id, 300*time.Second))
	t.Cleanup(func() {
		_, _ = cache.DeleteMany([]string{cacheKeySuffix})
	})

	// 1. 发起请求：缓存中记录的亲和度是渠道 99 (Priority 8)
	// 但当前渠道池中存在健康的 154 (Priority 10)，
	// 按照「保持亲和度，但 anyrouter 和 hiyo 优先级更高」原则，优先放行最高优先级 154！
	c, retry := newSelectRetryParam(modelName, nil)
	c.Request.Header.Set("X-Session-Key", affinityVal)

	selected, selectGroup, err := SelectChannelForRequest(c, modelName, retry)
	require.Nil(t, err)
	require.NotNil(t, selected)
	assert.Equal(t, "default", selectGroup)
	assert.Equal(t, chHigh.Id, selected.Id, "当候选池中有更高优先级 10 的可用渠道时，不能被低优先级 8 的亲和度劫持")

	// 2. 模拟渠道 154 熔断，此时 top available priority 变为 8
	// 亲和度渠道 99 能够正常被选用
	loadbalancer.GlobalTracker().TripBreakerUntil(chHigh.Id, modelName, time.Now().Add(time.Hour))
	t.Cleanup(func() {
		loadbalancer.GlobalTracker().TripBreakerUntil(chHigh.Id, modelName, time.Now().Add(-time.Hour))
	})

	c2, retry2 := newSelectRetryParam(modelName, nil)
	c2.Request.Header.Set("X-Session-Key", affinityVal)

	selected2, selectGroup2, err2 := SelectChannelForRequest(c2, modelName, retry2)
	require.Nil(t, err2)
	require.NotNil(t, selected2)
	assert.Equal(t, "default", selectGroup2)
	assert.Equal(t, chMid.Id, selected2.Id, "当高优先级渠道熔断不可用时，优雅回退到亲和度渠道 99")
}
