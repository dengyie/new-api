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

	origMemCache := common.MemoryCacheEnabled
	common.MemoryCacheEnabled = true
	t.Cleanup(func() {
		common.MemoryCacheEnabled = origMemCache
	})
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
	origMemCache := common.MemoryCacheEnabled
	common.MemoryCacheEnabled = false
	t.Cleanup(func() {
		common.MemoryCacheEnabled = origMemCache
	})
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

func TestSelectChannelForRequest_PreferredAffinityDoesNotLeakHalfOpenProbesWhenPreempted(t *testing.T) {
	db := setupChannelSelectTest(t)
	origMemCache := common.MemoryCacheEnabled
	common.MemoryCacheEnabled = false
	t.Cleanup(func() {
		common.MemoryCacheEnabled = origMemCache
	})
	const modelName = "gpt-6-probe-guard-test"

	p10 := int64(10)
	p8 := int64(8)
	weight := uint(100)

	chHigh := &model.Channel{
		Id:       1540,
		Type:     constant.ChannelTypeOpenAI,
		Key:      "key-1540",
		Status:   common.ChannelStatusEnabled,
		Name:     "cpa-top",
		Weight:   &weight,
		Models:   modelName,
		Group:    "default",
		Priority: &p10,
	}
	chMid := &model.Channel{
		Id:       990,
		Type:     constant.ChannelTypeOpenAI,
		Key:      "key-990",
		Status:   common.ChannelStatusEnabled,
		Name:     "affinity-mid",
		Weight:   &weight,
		Models:   modelName,
		Group:    "default",
		Priority: &p8,
	}

	require.NoError(t, db.Create(chHigh).Error)
	require.NoError(t, db.Create(&model.Ability{
		Group:     "default",
		Model:     modelName,
		ChannelId: chHigh.Id,
		Enabled:   true,
		Priority:  &p10,
		Weight:    weight,
	}).Error)

	require.NoError(t, db.Create(chMid).Error)
	require.NoError(t, db.Create(&model.Ability{
		Group:     "default",
		Model:     modelName,
		ChannelId: chMid.Id,
		Enabled:   true,
		Priority:  &p8,
		Weight:    weight,
	}).Error)

	loadbalancer.SetPolicy(&loadbalancer.Policy{
		Enabled: true,
		Default: loadbalancer.ChannelPolicy{
			Breaker: loadbalancer.BreakerPolicy{
				FailureThreshold: 1,
				CooldownSeconds:  1,
				HalfOpenProbes:   1,
			},
		},
	})
	t.Cleanup(func() {
		loadbalancer.SetPolicy(&loadbalancer.Policy{Enabled: false})
	})

	rule := operation_setting.ChannelAffinityRule{
		Name:             "rule-probe-guard-test",
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

	affinityVal := fmt.Sprintf("sess-probe-%d", time.Now().UnixNano())
	cacheKeySuffix := buildChannelAffinityCacheKeySuffix(rule, modelName, "default", affinityVal)
	cache := getChannelAffinityCache()
	require.NoError(t, cache.SetWithTTL(cacheKeySuffix, chMid.Id, 300*time.Second))
	t.Cleanup(func() {
		_, _ = cache.DeleteMany([]string{cacheKeySuffix})
	})

	// 使 chMid 触发硬故障并进入半开探测阶段
	tr := loadbalancer.GlobalTracker()
	h := tr.Begin(chMid.Id, modelName)
	h.End(false, true)
	time.Sleep(1100 * time.Millisecond)

	// 确认 chMid 处于半开，PeekAvailable 返回可用
	avail, _ := tr.PeekAvailable(chMid.Id, modelName)
	require.True(t, avail, "冷却结束后半开探测应 peek 可用")

	// 1. 发起选路请求：chMid 为亲和度渠道，但候选池有更高优先级的健康渠道 chHigh
	c, retry := newSelectRetryParam(modelName, nil)
	c.Request.Header.Set("X-Session-Key", affinityVal)

	selected, selectGroup, err := SelectChannelForRequest(c, modelName, retry)
	require.Nil(t, err)
	require.NotNil(t, selected)
	assert.Equal(t, "default", selectGroup)
	assert.Equal(t, chHigh.Id, selected.Id, "应优先放行高优先级渠道")

	// 2. 核心断言：chMid 因优先级被抢占并未被选用，其半开探测配额绝不得被提前消费
	// 若之前的逻辑过早调用了 IsAvailable，则此处调用将因配额耗尽而返回 false (ReasonHalfOpenProbesExceeded)
	ok, reason := tr.IsAvailable(chMid.Id, modelName)
	assert.True(t, ok, "未被选用的亲和渠道必须保留其半开探测配额")
	assert.Empty(t, reason)

	// 再次调用将正式消耗完唯一的探测配额
	ok2, reason2 := tr.IsAvailable(chMid.Id, modelName)
	assert.False(t, ok2)
	assert.Equal(t, loadbalancer.ReasonHalfOpenProbesExceeded, reason2)
}

func TestRecordReasoningOriginChannel_And_GetReasoningOriginChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	origMemCache := common.MemoryCacheEnabled
	common.MemoryCacheEnabled = true
	t.Cleanup(func() {
		common.MemoryCacheEnabled = origMemCache
	})

	p10 := int64(10)
	p8 := int64(8)
	chHigh := &model.Channel{Id: 154, Name: "anyrouter", Status: common.ChannelStatusEnabled, Priority: &p10}
	chMid := &model.Channel{Id: 99, Name: "fallback", Status: common.ChannelStatusEnabled, Priority: &p8}
	model.CacheUpdateChannel(chHigh)
	model.CacheUpdateChannel(chMid)

	setting := operation_setting.GetChannelAffinitySetting()
	origEnabled := setting.Enabled
	origSwitchOnSuccess := setting.SwitchOnSuccess
	setting.Enabled = true
	setting.SwitchOnSuccess = true
	t.Cleanup(func() {
		setting.Enabled = origEnabled
		setting.SwitchOnSuccess = origSwitchOnSuccess
	})

	cacheKeySuffix := fmt.Sprintf("test-origin-tracking-%d", time.Now().UnixNano())
	cache := getChannelAffinityCache()
	originCache := getChannelAffinityReasoningOriginCache()
	t.Cleanup(func() {
		_, _ = cache.DeleteMany([]string{cacheKeySuffix})
		_, _ = originCache.DeleteMany([]string{cacheKeySuffix})
	})

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	setChannelAffinityContext(c, channelAffinityMeta{
		CacheKey:   cacheKeySuffix,
		TTLSeconds: 300,
		RuleName:   "prompt_cache_key",
	})

	// 1. 第一轮：渠道 154 成功
	RecordChannelAffinity(c, chHigh.Id)
	originID, found := GetReasoningOriginChannel(c)
	require.True(t, found)
	assert.Equal(t, chHigh.Id, originID, "密文来源渠道应记录为 154")
	routeID, foundRoute, _ := cache.Get(cacheKeySuffix)
	require.True(t, foundRoute)
	assert.Equal(t, chHigh.Id, routeID, "路由亲和度应记录为 154")

	// 2. 第二轮：渠道 154 瞬时故障，重试到低优先级渠道 99 兜底成功
	c.Set("channel_id", chMid.Id)
	RecordChannelAffinity(c, chMid.Id)
	originID2, found2 := GetReasoningOriginChannel(c)
	require.True(t, found2)
	assert.Equal(t, chMid.Id, originID2, "密文来源渠道必须更新为实际兜底成功的渠道 99")
	routeID2, foundRoute2, _ := cache.Get(cacheKeySuffix)
	require.True(t, foundRoute2)
	assert.Equal(t, chHigh.Id, routeID2, "路由亲和度防降级必须保留最高优先级渠道 154")

	// 3. 清理缓存验证
	ClearCurrentChannelAffinityCache(c)
	_, foundAfterClear := GetReasoningOriginChannel(c)
	assert.False(t, foundAfterClear, "清空亲和度缓存后密文来源亦应同步清空")
	_, foundRouteAfterClear, _ := cache.Get(cacheKeySuffix)
	assert.False(t, foundRouteAfterClear, "清空亲和度缓存后路由亲和度亦应同步清空")
}

func TestSelectChannelForRequest_ReasoningDriftDetection(t *testing.T) {
	db := setupChannelSelectTest(t)
	origMemCache := common.MemoryCacheEnabled
	common.MemoryCacheEnabled = false
	t.Cleanup(func() {
		common.MemoryCacheEnabled = origMemCache
	})
	const modelName = "gpt-6-astra-drift-test"

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
	require.NoError(t, db.Create(&model.Ability{Group: "default", Model: modelName, ChannelId: 154, Enabled: true, Priority: &p10, Weight: weight}).Error)
	require.NoError(t, db.Create(chMid).Error)
	require.NoError(t, db.Create(&model.Ability{Group: "default", Model: modelName, ChannelId: 99, Enabled: true, Priority: &p8, Weight: weight}).Error)

	setting := operation_setting.GetChannelAffinitySetting()
	origSetting := *setting
	setting.Enabled = true
	setting.SessionMode = "prefer"
	setting.SwitchOnSuccess = true
	ruleName := "test-session-drift"
	setting.Rules = []operation_setting.ChannelAffinityRule{
		{
			Name:              ruleName,
			ModelRegex:        []string{".*"},
			KeySources:        []operation_setting.ChannelAffinityKeySource{{Type: "request_header", Key: "X-Session-Key"}},
			SessionMode:       "prefer",
			IncludeRuleName:   true,
			IncludeUsingGroup: true,
			IncludeModelName:  true,
		},
	}
	t.Cleanup(func() {
		*setting = origSetting
	})

	affinityVal := fmt.Sprintf("sess-drift-%d", time.Now().UnixNano())
	codexRule := setting.Rules[0]
	cacheKeySuffix := buildChannelAffinityCacheKeySuffix(codexRule, modelName, "default", affinityVal)
	originCache := getChannelAffinityReasoningOriginCache()

	// 1. 测试跨渠道漂移：密文来源为渠道 99，但本轮路由放行最高优先级 154
	require.NoError(t, originCache.SetWithTTL(cacheKeySuffix, chMid.Id, time.Minute))
	t.Cleanup(func() {
		_, _ = originCache.DeleteMany([]string{cacheKeySuffix})
	})

	c, retry := newSelectRetryParam(modelName, nil)
	c.Request.Header.Set("X-Session-Key", affinityVal)

	selected, _, err := SelectChannelForRequest(c, modelName, retry)
	require.Nil(t, err)
	require.NotNil(t, selected)
	assert.Equal(t, chHigh.Id, selected.Id, "应放行最高优先级渠道 154")
	assert.True(t, common.GetContextKeyBool(c, constant.ContextKeyStripResponsesReasoning), "跨渠道漂移时必须标记 ContextKeyStripResponsesReasoning 为 true")

	// 2. 测试同渠道无漂移：密文来源与目标渠道均为 154
	require.NoError(t, originCache.SetWithTTL(cacheKeySuffix, chHigh.Id, time.Minute))
	c2, retry2 := newSelectRetryParam(modelName, nil)
	c2.Request.Header.Set("X-Session-Key", affinityVal)

	selected2, _, err2 := SelectChannelForRequest(c2, modelName, retry2)
	require.Nil(t, err2)
	require.NotNil(t, selected2)
	assert.Equal(t, chHigh.Id, selected2.Id)
	assert.False(t, common.GetContextKeyBool(c2, constant.ContextKeyStripResponsesReasoning), "同渠道无漂移时不得标记 strip 标记，以完整保留密文")
}

func TestClearChannelAffinityCacheAll_DualCacheConsistency(t *testing.T) {
	cache := getChannelAffinityCache()
	originCache := getChannelAffinityReasoningOriginCache()

	key1 := fmt.Sprintf("test-dual-clean-1-%d", time.Now().UnixNano())
	key2 := fmt.Sprintf("test-dual-clean-2-%d", time.Now().UnixNano())

	require.NoError(t, cache.SetWithTTL(key1, 100, 300*time.Second))
	require.NoError(t, originCache.SetWithTTL(key2, 200, 300*time.Second))

	_, found1, _ := cache.Get(key1)
	require.True(t, found1)
	_, found2, _ := originCache.Get(key2)
	require.True(t, found2)

	// 全局清空必须同时清空亲和度缓存与密文来源缓存
	cleared := ClearChannelAffinityCacheAll()
	assert.GreaterOrEqual(t, cleared, 1)

	_, found1After, _ := cache.Get(key1)
	assert.False(t, found1After, "全局清空后路由亲和度主缓存必须为空")
	_, found2After, _ := originCache.Get(key2)
	assert.False(t, found2After, "全局清空后密文来源缓存必须同步为空")
}
