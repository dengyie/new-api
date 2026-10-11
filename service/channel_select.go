package service

import (
	"errors"
	"fmt"
	"math"
	"net/http"

	"github.com/dengyie/apihub/common"
	"github.com/dengyie/apihub/constant"
	"github.com/dengyie/apihub/dto"
	"github.com/dengyie/apihub/i18n"
	"github.com/dengyie/apihub/loadbalancer"
	"github.com/dengyie/apihub/logger"
	"github.com/dengyie/apihub/model"
	"github.com/dengyie/apihub/pkg/jsplugin"
	"github.com/dengyie/apihub/relaykit/types"
	"github.com/gin-gonic/gin"
)

func GetChannelConstraints(c *gin.Context) *dto.ChannelConstraints {
	if c == nil {
		return &dto.ChannelConstraints{}
	}
	if existing, ok := common.GetContextKeyType[*dto.ChannelConstraints](c, constant.ContextKeyChannelConstraints); ok && existing != nil {
		return existing
	}
	constraints := &dto.ChannelConstraints{}
	common.SetContextKey(c, constant.ContextKeyChannelConstraints, constraints)
	return constraints
}

func AppendTaskPluginIdentityFilter(c *gin.Context, pluginKey string) {
	if c == nil {
		return
	}
	channelTypes, pluginKeys := pinnedTaskPluginIdentities(c, pluginKey)
	GetChannelConstraints(c).AddFilter(dto.ChannelFilter{
		Kind:                   dto.FilterTaskPluginIdentity,
		TaskPluginKey:          pluginKey,
		TaskPluginChannelTypes: channelTypes,
		TaskPluginKeys:         pluginKeys,
	})
}

type RetryParam struct {
	Ctx         *gin.Context
	TokenGroup  string
	ModelName   string
	RequestPath string
	Retry       *int
	// StickyKey prompt 前缀 hash，用于一致性路由（提高上游 cache 命中率）。
	// 仅在首次选择（Retry==0）时生效；重试时（渠道失败后）忽略，走普通选择。
	StickyKey string
	// ExcludedIDs 选路时跳过的渠道 ID 集合（如不可用/熔断/降级/本请求已用渠道），
	// 避免选路循环篡改全局重试计数 Retry。
	ExcludedIDs  map[int]struct{}
	resetNextTry bool
}

func (p *RetryParam) GetRetry() int {
	if p.Retry == nil {
		return 0
	}
	return *p.Retry
}

func (p *RetryParam) SetRetry(retry int) {
	p.Retry = &retry
}

func (p *RetryParam) IncreaseRetry() {
	if p.resetNextTry {
		p.resetNextTry = false
		return
	}
	if p.Retry == nil {
		p.Retry = new(int)
	}
	*p.Retry++
}

func (p *RetryParam) ResetRetryNextTry() {
	p.resetNextTry = true
}

// channelCandidates memoizes the candidate set of every group a request may be
// routed to. The set is resolved once per group and then picked from in memory,
// so the caller's skip loop — which excludes a channel per pass — issues no
// further queries. Resolving per attempt instead cost two queries per skip and
// reached 150 statements on a 57-channel model.
type channelCandidates struct {
	param   *RetryParam
	filters []dto.ChannelFilter
	byGroup map[string]*model.ChannelCandidates
}

func newChannelCandidates(param *RetryParam) *channelCandidates {
	return &channelCandidates{
		param:   param,
		filters: GetChannelConstraints(param.Ctx).Filters,
		byGroup: make(map[string]*model.ChannelCandidates, 2),
	}
}

func (r *channelCandidates) pick(group string, retry int) (*model.Channel, error) {
	candidates, ok := r.byGroup[group]
	if !ok {
		resolved, err := model.GetChannelCandidates(group, r.param.ModelName, r.filters)
		if err != nil {
			return nil, err
		}
		r.byGroup[group] = resolved
		candidates = resolved
	}
	return candidates.Pick(retry, r.param.ExcludedIDs, r.param.StickyKey), nil
}

func (r *channelCandidates) topAvailablePriority(group string) (int64, bool) {
	if r == nil || r.param == nil {
		return 0, false
	}
	if group == "auto" {
		userGroup := common.GetContextKeyString(r.param.Ctx, constant.ContextKeyUserGroup)
		autoGroups := GetRequestAutoGroups(r.param.Ctx, userGroup)
		var maxP int64 = math.MinInt64
		hasAny := false
		for _, g := range autoGroups {
			if p, ok := r.topAvailablePriority(g); ok {
				if !hasAny || p > maxP {
					maxP = p
					hasAny = true
				}
			}
		}
		if !hasAny {
			return 0, false
		}
		return maxP, true
	}
	candidates, ok := r.byGroup[group]
	if !ok {
		resolved, err := model.GetChannelCandidates(group, r.param.ModelName, r.filters)
		if err != nil || resolved == nil {
			return 0, false
		}
		r.byGroup[group] = resolved
		candidates = resolved
	}
	return candidates.TopAvailablePriority(r.param.ExcludedIDs, r.param.ModelName)
}

// select tries to get a channel that satisfies the requirements.
// 尝试获取一个满足要求的渠道。
//
// For "auto" tokenGroup with cross-group Retry enabled:
// 对于启用了跨分组重试的 "auto" tokenGroup：
//
//   - Each group will exhaust all its priorities before moving to the next group.
//     每个分组会用完所有优先级后才会切换到下一个分组。
//
//   - Uses ContextKeyAutoGroupIndex to track current group index.
//     使用 ContextKeyAutoGroupIndex 跟踪当前分组索引。
//
//   - Uses ContextKeyAutoGroupRetryIndex to track the global Retry count when current group started.
//     使用 ContextKeyAutoGroupRetryIndex 跟踪当前分组开始时的全局重试次数。
//
//   - priorityRetry = Retry - startRetryIndex, represents the priority level within current group.
//     priorityRetry = Retry - startRetryIndex，表示当前分组内的优先级级别。
//
//   - When select returns nil (priorities exhausted), moves to next group.
//     当 select 返回 nil（优先级用完）时，切换到下一个分组。
//
// Example flow (2 groups, each with 2 priorities, RetryTimes=3):
// 示例流程（2个分组，每个有2个优先级，RetryTimes=3）：
//
//	Retry=0: GroupA, priority0 (startRetryIndex=0, priorityRetry=0)
//	         分组A, 优先级0
//
//	Retry=1: GroupA, priority1 (startRetryIndex=0, priorityRetry=1)
//	         分组A, 优先级1
//
//	Retry=2: GroupA exhausted → GroupB, priority0 (startRetryIndex=2, priorityRetry=0)
//	         分组A用完 → 分组B, 优先级0
//
//	Retry=3: GroupB, priority1 (startRetryIndex=2, priorityRetry=1)
//	         分组B, 优先级1
func (r *channelCandidates) resolve() (*model.Channel, string, error) {
	param := r.param
	var channel *model.Channel
	var err error
	selectGroup := param.TokenGroup
	userGroup := common.GetContextKeyString(param.Ctx, constant.ContextKeyUserGroup)

	if param.TokenGroup == "auto" {
		autoGroups := GetRequestAutoGroups(param.Ctx, userGroup)
		if len(autoGroups) == 0 {
			return nil, selectGroup, errors.New("auto groups is not enabled")
		}

		// startGroupIndex: the group index to start searching from
		// startGroupIndex: 开始搜索的分组索引
		startGroupIndex := 0
		crossGroupRetry := common.GetContextKeyBool(param.Ctx, constant.ContextKeyTokenCrossGroupRetry)

		if lastGroupIndex, exists := common.GetContextKey(param.Ctx, constant.ContextKeyAutoGroupIndex); exists {
			if idx, ok := lastGroupIndex.(int); ok {
				startGroupIndex = idx
			}
		}

		for i := startGroupIndex; i < len(autoGroups); i++ {
			autoGroup := autoGroups[i]
			// Calculate priorityRetry for current group
			// 计算当前分组的 priorityRetry
			priorityRetry := param.GetRetry()
			// If moved to a new group, reset priorityRetry and update startRetryIndex
			// 如果切换到新分组，重置 priorityRetry 并更新 startRetryIndex
			if i > startGroupIndex {
				priorityRetry = 0
			}
			logger.LogDebug(param.Ctx, "Auto selecting group: %s, priorityRetry: %d", autoGroup, priorityRetry)

			channel, _ = r.pick(autoGroup, priorityRetry)
			if channel == nil {
				// Current group has no available channel for this model, try next group
				// 当前分组没有该模型的可用渠道，尝试下一个分组
				logger.LogDebug(param.Ctx, "No available channel in group %s for model %s at priorityRetry %d, trying next group", autoGroup, param.ModelName, priorityRetry)
				// 重置状态以尝试下一个分组
				common.SetContextKey(param.Ctx, constant.ContextKeyAutoGroupIndex, i+1)
				common.SetContextKey(param.Ctx, constant.ContextKeyAutoGroupRetryIndex, 0)
				// Reset retry counter so outer loop can continue for next group
				// 重置重试计数器，以便外层循环可以为下一个分组继续
				param.SetRetry(0)
				continue
			}
			common.SetContextKey(param.Ctx, constant.ContextKeyAutoGroup, autoGroup)
			selectGroup = autoGroup
			logger.LogDebug(param.Ctx, "Auto selected group: %s", autoGroup)

			// Prepare state for next retry
			// 为下一次重试准备状态
			if crossGroupRetry && priorityRetry >= common.RetryTimes {
				// Current group has exhausted all retries, prepare to switch to next group
				// This request still uses current group, but next retry will use next group
				// 当前分组已用完所有重试次数，准备切换到下一个分组
				// 本次请求仍使用当前分组，但下次重试将使用下一个分组
				logger.LogDebug(param.Ctx, "Current group %s retries exhausted (priorityRetry=%d >= RetryTimes=%d), preparing switch to next group for next retry", autoGroup, priorityRetry, common.RetryTimes)
				common.SetContextKey(param.Ctx, constant.ContextKeyAutoGroupIndex, i+1)
				// Reset retry counter so outer loop can continue for next group
				// 重置重试计数器，以便外层循环可以为下一个分组继续
				param.SetRetry(0)
				param.ResetRetryNextTry()
			} else {
				// Stay in current group, save current state
				// 保持在当前分组，保存当前状态
				common.SetContextKey(param.Ctx, constant.ContextKeyAutoGroupIndex, i)
			}
			break
		}
	} else {
		channel, err = r.pick(param.TokenGroup, param.GetRetry())
		if err != nil {
			return nil, param.TokenGroup, err
		}
	}
	return channel, selectGroup, nil
}

// CacheGetRandomSatisfiedChannel resolves one channel for a single attempt.
// Callers that loop over attempts should hold a channelCandidates instead, so
// the candidate set is resolved once rather than per attempt.
func CacheGetRandomSatisfiedChannel(param *RetryParam) (*model.Channel, string, error) {
	return newChannelCandidates(param).resolve()
}

// SelectRetryChannel 为「换渠道重试」挑选下一个渠道。
//
// 重试路径此前直接调用 CacheGetRandomSatisfiedChannel，而它只按分组/优先级/
// 权重选路，比首轮选路少了两道过滤，于是「换渠道重试」在很多部署里名不副实：
//
//  1. 不知道本请求已经用过哪些渠道。RetryParam.ExcludedIDs 在重试路径上从未被
//     填充，唯一的真实记录 use_channel（每次 SetupContextForSelectedChannel 追加）
//     只有首轮选路的 SelectChannelForRequest 会读。所以刚失败的那个渠道在下一轮
//     仍然在候选池里，而配合 sticky 路由（同一 StickyKey → 同一索引）就是
//     确定性地再打一次同一个坏渠道。
//  2. 不看负载均衡状态。熔断已打开、并发已打满的渠道照样会被选中，于是首轮刚
//     确认失败的渠道下一轮又回来了，熔断形同虚设。
//
// 排除集写回 param 并跨轮次累积：一个渠道只要在本请求内被证明不可用（试过了或
// 被 tracker 判死），后续轮次就不再参与。终止性由 Pick 保证——候选被排除干净时
// 它返回 nil，resolve 随之返回「无可用渠道」错误，循环最迟在候选耗尽的那一轮
// 结束，无需人为设定尝试上限。
func SelectRetryChannel(param *RetryParam) (*model.Channel, string, error) {
	if param == nil || param.Ctx == nil {
		return nil, "", errors.New("retry param is nil")
	}

	// 本请求已试过的渠道。use_channel 记录每一次 SetupContextForSelectedChannel
	// 的结果，首轮由中间件写入、重试轮由 getChannel 追加。
	used := param.Ctx.GetStringSlice("use_channel")
	if len(used) > 0 || param.ExcludedIDs == nil {
		excluded := make(map[int]struct{}, len(used)+len(param.ExcludedIDs))
		for id := range param.ExcludedIDs {
			excluded[id] = struct{}{}
		}
		for _, idStr := range used {
			var id int
			if _, parseErr := fmt.Sscanf(idStr, "%d", &id); parseErr == nil && id > 0 {
				excluded[id] = struct{}{}
			}
		}
		param.ExcludedIDs = excluded
	}

	tracker := loadbalancer.GlobalTracker()
	for {
		channel, selectGroup, err := CacheGetRandomSatisfiedChannel(param)
		if err != nil {
			return nil, selectGroup, err
		}
		if channel == nil {
			return nil, selectGroup, nil
		}
		if ok, reason := tracker.IsAvailable(channel.Id, param.ModelName); !ok {
			logger.LogDebug(param.Ctx, "loadbalancer: retry 跳过渠道 #%d [model=%s] (%s)", channel.Id, param.ModelName, reason)
			param.ExcludedIDs[channel.Id] = struct{}{}
			continue
		}
		return channel, selectGroup, nil
	}
}

func pinnedTaskPluginIdentities(c *gin.Context, expected string) ([]int, []string) {
	if c == nil || expected == "" {
		return nil, nil
	}
	if value, exists := c.Get(jsplugin.ContextKeyPinnedEndpoint); exists {
		pinned, ok := value.(jsplugin.PinnedEndpoint)
		if ok && pinned.Generation != nil && len(pinned.Candidates) > 1 {
			expectedFound := false
			channelTypes := make([]int, 0, len(pinned.Candidates))
			pluginKeys := make([]string, 0, len(pinned.Candidates))
			seen := make(map[int]struct{}, len(pinned.Candidates))
			for _, candidate := range pinned.Candidates {
				if candidate.Plugin == nil {
					continue
				}
				if candidate.Plugin.Meta.Key == expected {
					expectedFound = true
				}
				pluginKeys = append(pluginKeys, candidate.Plugin.Meta.Key)
				for _, channelType := range candidate.Plugin.Meta.ChannelTypes {
					if channelType == 0 || channelType == constant.ChannelTypeTaskPlugin {
						continue
					}
					if _, duplicate := seen[channelType]; duplicate {
						continue
					}
					if plugin, indexed := pinned.Generation.GetByChannelType(channelType); indexed && plugin == candidate.Plugin {
						seen[channelType] = struct{}{}
						channelTypes = append(channelTypes, channelType)
					}
				}
			}
			if expectedFound {
				return channelTypes, pluginKeys
			}
		}
	}
	value, exists := c.Get(jsplugin.ContextKeyPinnedPlugin)
	pinned, ok := value.(jsplugin.PinnedPlugin)
	if !exists || !ok || pinned.Generation == nil || pinned.Plugin == nil || pinned.Plugin.Meta.Key != expected {
		return nil, nil
	}
	channelTypes := make([]int, 0, len(pinned.Plugin.Meta.ChannelTypes))
	for _, channelType := range pinned.Plugin.Meta.ChannelTypes {
		if channelType == 0 || channelType == constant.ChannelTypeTaskPlugin {
			continue
		}
		channelTypes = append(channelTypes, channelType)
	}
	return channelTypes, []string{expected}
}

// ChannelSelectError explains why SelectChannelForRequest found no channel.
// Callers render it for their transport: the HTTP distributor localizes
// MessageID with its own helpers and the Responses WebSocket relay wraps it in
// a NewAPIError. Message is set instead of MessageID when the text is a fixed
// error code that clients match on.
type ChannelSelectError struct {
	StatusCode int
	Code       types.ErrorCode
	MessageID  string
	Params     map[string]any
	Message    string
	// FilterKind and Channel identify a candidate rejected by request filters.
	FilterKind dto.ChannelFilterKind
	Channel    *model.Channel
	// NoAvailableChannel marks the "no channel for this group and model"
	// outcome so the distributor can name the claiming task plugin.
	NoAvailableChannel bool
}

func (e *ChannelSelectError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.MessageID != "" {
		return e.MessageID
	}
	return "channel select error"
}

// SelectChannelForRequest resolves the channel for one attempt with the rules
// shared by the HTTP distributor and the Responses WebSocket relay: a pinned
// channel wins, then session affinity (first attempt only), then a random
// eligible channel; every candidate must satisfy the request's channel
// filters. The group the channel was chosen from is returned for auto-group
// callers. The caller still applies SetupContextForSelectedChannel.
func SelectChannelForRequest(c *gin.Context, modelName string, retry *RetryParam) (*model.Channel, string, *ChannelSelectError) {
	constraints := GetChannelConstraints(c)
	if pin, found, overridden := constraints.ResolvedPin(); found {
		for _, lost := range overridden {
			logger.LogWarn(c, fmt.Sprintf(
				"channel pin overridden: winning_source=%s winning_channel_id=%d overridden_source=%s overridden_channel_id=%d",
				pin.Source, pin.ChannelId, lost.Source, lost.ChannelId,
			))
		}
		channel, err := model.CacheGetChannel(pin.ChannelId)
		if err != nil {
			return nil, "", pinnedChannelUnavailable(pin, http.StatusBadRequest, i18n.MsgDistributorInvalidChannelId)
		}
		if channel.Status != common.ChannelStatusEnabled {
			return nil, "", pinnedChannelUnavailable(pin, http.StatusForbidden, i18n.MsgDistributorChannelDisabled)
		}
		if ok, kind := model.ChannelSatisfiesFilters(channel, modelName, constraints.Filters); !ok {
			return nil, "", &ChannelSelectError{
				StatusCode: http.StatusBadRequest, Code: types.ErrorCode(kind), MessageID: i18n.MsgDistributorNoAvailableChannel,
				Params:     map[string]any{"Group": common.GetContextKeyString(c, constant.ContextKeyUsingGroup), "Model": modelName},
				FilterKind: kind, Channel: channel,
			}
		}
		return channel, "", nil
	}

	usingGroup := retry.TokenGroup
	var channel *model.Channel
	var selectGroup string

	// 智能负载：过载/熔断/本请求已用过的渠道自动跳过。
	// 候选集合整轮只解析一次；每跳过一个渠道就多排除一个，候选耗尽时
	// pick 返回 nil，循环因此在至多「渠道总数」轮内自然终止，无需人为
	// 设定尝试上限（过小的上限会让大渠道池因个别过载渠道过早中断）。
	// 降级渠道（连续慢 3 次）与过载渠道（达到并发上限）都先记为备选：
	// 过载只是瞬时并发占满，降级是已证实的持续慢，所以过载兜底优先。
	used := retry.Ctx.GetStringSlice("use_channel")
	usedSet := make(map[string]struct{}, len(used))
	for _, id := range used {
		usedSet[id] = struct{}{}
	}

	// 使用本地工作副本，绝不调用 retry.IncreaseRetry() 污染外层请求的重试计数与重试预算
	retryLocal := *retry
	retryLocal.ExcludedIDs = make(map[int]struct{}, len(used)+len(retry.ExcludedIDs))
	for id := range retry.ExcludedIDs {
		retryLocal.ExcludedIDs[id] = struct{}{}
	}
	for _, idStr := range used {
		var id int
		if _, parseErr := fmt.Sscanf(idStr, "%d", &id); parseErr == nil && id > 0 {
			retryLocal.ExcludedIDs[id] = struct{}{}
		}
	}

	// 候选集合按分组缓存；本函数被重试循环反复调用时也只解析一次
	candidates := newChannelCandidates(&retryLocal)

	if retry.GetRetry() == 0 {
		if preferredChannelID, found := GetPreferredChannelByAffinity(c, modelName, usingGroup); found {
			affinityUsable := false
			preferred, err := model.CacheGetChannel(preferredChannelID)
			channelDisabled := (err != nil || preferred == nil || preferred.Status != common.ChannelStatusEnabled)
			affinitySatisfied := false
			if !channelDisabled {
				affinitySatisfied, _ = model.ChannelSatisfiesFilters(preferred, modelName, constraints.Filters)
			}
			if affinitySatisfied {
				// 检查熔断与过载状态：使用纯只读 PeekAvailable，
				// 避免在后续被更高优先级抢占或分组不匹配时浪费半开探测名额
				if ok, _ := loadbalancer.GlobalTracker().PeekAvailable(preferred.Id, modelName); !ok {
					affinitySatisfied = false
				}
			}
			if affinitySatisfied && RequestPolicy(c).SessionMode != "strict" {
				// 保持亲和度与优先级兼顾：若候选池中存在更高优先级的健康可用渠道（例如 AnyRouter/Hiyo 优先级 10，
				// 而历史请求因故障重试被记录到了兜底优先级 8 或 5 的渠道），绝不能被低优先级渠道永久劫持，
				// 优先放行高优先级渠道探测/服务！
				if topPriority, hasTop := candidates.topAvailablePriority(usingGroup); hasTop {
					if preferred.GetPriority() < topPriority {
						logger.LogDebug(retry.Ctx, fmt.Sprintf("channel affinity: preferred #%d (priority=%d) is lower than available top priority %d, routing to top tier", preferred.Id, preferred.GetPriority(), topPriority))
						affinitySatisfied = false
					}
				}
			}
			if affinitySatisfied {
				var matchedGroup string
				if usingGroup == "auto" {
					userGroup := common.GetContextKeyString(c, constant.ContextKeyUserGroup)
					for _, g := range GetRequestAutoGroups(c, userGroup) {
						if model.IsChannelEnabledForGroupModel(g, modelName, preferred.Id) {
							matchedGroup = g
							break
						}
					}
				} else if model.IsChannelEnabledForGroupModel(usingGroup, modelName, preferred.Id) {
					matchedGroup = usingGroup
				}
				if matchedGroup != "" {
					// 最终锁定使用该亲和渠道，正式调用 IsAvailable 校验并消费探测配额
					if ok, _ := loadbalancer.GlobalTracker().IsAvailable(preferred.Id, modelName); ok {
						channel = preferred
						selectGroup = matchedGroup
						affinityUsable = true
						if usingGroup == "auto" {
							common.SetContextKey(c, constant.ContextKeyAutoGroup, matchedGroup)
						}
						MarkChannelAffinityUsed(c, matchedGroup, preferred.Id)
					}
				}
			}
			if channelDisabled && !ShouldKeepChannelAffinityOnChannelDisabled() {
				ClearCurrentChannelAffinityCache(c)
			}
			if !affinityUsable && RequestPolicy(c).SessionMode == "strict" {
				return nil, "", &ChannelSelectError{StatusCode: http.StatusServiceUnavailable, Message: "strict_session_binding_unavailable"}
			}
		}
	}

	if channel == nil {
		var err error
		var degradedFallback *model.Channel
		var degradedSelectGroup string
		var overloadedFallback *model.Channel
		var overloadedSelectGroup string
		minOverloadedInflight := math.MaxInt
		for {
			channel, selectGroup, err = candidates.resolve()
			if err != nil || channel == nil {
				break
			}
			if _, tried := usedSet[fmt.Sprintf("%d", channel.Id)]; tried {
				logger.LogDebug(retry.Ctx, "loadbalancer: skip channel #%d (already tried in this request)", channel.Id)
				retryLocal.ExcludedIDs[channel.Id] = struct{}{}
				channel = nil
				continue
			}
			if ok, reason := loadbalancer.GlobalTracker().PeekAvailable(channel.Id, modelName); !ok {
				logger.LogDebug(retry.Ctx, "loadbalancer: skip channel #%d [model=%s] (%s)", channel.Id, modelName, reason)
				if reason == loadbalancer.ReasonOverloaded {
					inflight := loadbalancer.GlobalTracker().Inflight(channel.Id)
					if inflight < minOverloadedInflight {
						minOverloadedInflight = inflight
						overloadedFallback = channel
						overloadedSelectGroup = selectGroup
					}
				}
				retryLocal.ExcludedIDs[channel.Id] = struct{}{}
				channel = nil
				continue
			}
			// 降级渠道：先记为备选，优先用非降级渠道
			if loadbalancer.GlobalTracker().IsDegraded(channel.Id, modelName) {
				if degradedFallback == nil {
					degradedFallback = channel
					degradedSelectGroup = selectGroup
				}
				logger.LogDebug(retry.Ctx, "loadbalancer: channel #%d is degraded, deprioritized", channel.Id)
				retryLocal.ExcludedIDs[channel.Id] = struct{}{}
				channel = nil
				continue
			}
			// 确定选用该非降级健康渠道，正式调用 IsAvailable 校验并消费探测配额
			if ok, _ := loadbalancer.GlobalTracker().IsAvailable(channel.Id, modelName); !ok {
				retryLocal.ExcludedIDs[channel.Id] = struct{}{}
				channel = nil
				continue
			}
			break
		}
		// 过载兜底：当所有可用渠道均达到并发上限且无其他健康渠道时，选取负载
		// 最低（inflight 最小）的渠道兜底放行，避免因并发保护而假死报 503。
		// 过载是瞬时状态，降级是已证实的慢，故排在降级兜底之前。
		if channel == nil && overloadedFallback != nil {
			channel = overloadedFallback
			selectGroup = overloadedSelectGroup
			logger.LogWarn(retry.Ctx, fmt.Sprintf(
				"loadbalancer: every candidate channel is overloaded, using least-loaded channel #%d (inflight=%d, max_inflight=%d) as fallback",
				channel.Id, minOverloadedInflight, loadbalancer.GetPolicy().Resolve(channel.Id).MaxInflight))
		}
		// 降级渠道兜底：没找到非降级渠道时，用降级渠道（总比无渠道好）
		if channel == nil && degradedFallback != nil {
			channel = degradedFallback
			selectGroup = degradedSelectGroup
			// 确定选用降级兜底渠道，正式调用 IsAvailable 消费探测配额
			_, _ = loadbalancer.GlobalTracker().IsAvailable(channel.Id, modelName)
			logger.LogDebug(retry.Ctx, "loadbalancer: using degraded channel #%d as fallback", channel.Id)
		}
		if err != nil {
			showGroup := usingGroup
			if usingGroup == "auto" {
				showGroup = fmt.Sprintf("auto(%s)", selectGroup)
			}
			return nil, selectGroup, &ChannelSelectError{
				StatusCode: http.StatusServiceUnavailable, Code: types.ErrorCodeGetChannelFailed, MessageID: i18n.MsgDistributorGetChannelFailed,
				Params: map[string]any{"Group": showGroup, "Model": modelName, "Error": err.Error()},
			}
		}
		if channel == nil {
			// 该模型在本组一个渠道都选不出来 —— 对外就是 503。
			// 熔断只负责绕开坏渠道，"这个模型已经整体挂了" 这件事网关内部
			// 原本没有任何记录，靠客户端来报障。这里留一条可聚合的证据。
			loadbalancer.RecordModelExhausted(modelName, "group="+usingGroup)
			return nil, selectGroup, &ChannelSelectError{
				StatusCode: http.StatusServiceUnavailable, Code: types.ErrorCodeNoAvailableChannel, MessageID: i18n.MsgDistributorNoAvailableChannel,
				Params: map[string]any{"Group": usingGroup, "Model": modelName}, NoAvailableChannel: true,
			}
		}
	}
	if ok, kind := model.ChannelSatisfiesFilters(channel, modelName, constraints.Filters); !ok {
		loadbalancer.RecordModelExhausted(modelName, "filter="+string(kind))
		return nil, selectGroup, &ChannelSelectError{
			StatusCode: http.StatusServiceUnavailable, Code: types.ErrorCodeNoAvailableChannel, MessageID: i18n.MsgDistributorNoAvailableChannel,
			Params:     map[string]any{"Group": common.GetContextKeyString(c, constant.ContextKeyUsingGroup), "Model": modelName},
			FilterKind: kind, Channel: channel, NoAvailableChannel: true,
		}
	}
	// 选得到渠道 = 这个模型还在正常服务，清空累计，避免「曾经短暂耗尽」
	// 和「现在真的整体挂了」混进同一条证据里。
	loadbalancer.ResetModelExhausted(modelName)
	if originID, found := GetReasoningOriginChannel(c); found && originID > 0 && channel != nil && channel.Id != originID {
		logger.LogInfo(retry.Ctx, fmt.Sprintf("channel affinity: responses reasoning drift detected (target #%d != origin #%d), marking pre-emptive strip", channel.Id, originID))
		common.SetContextKey(c, constant.ContextKeyStripResponsesReasoning, true)
	}
	return channel, selectGroup, nil
}

// Origin-task pins report a fixed code so task polling can tell a retired
// channel from a malformed request.
func pinnedChannelUnavailable(pin dto.ChannelPin, statusCode int, messageID string) *ChannelSelectError {
	if pin.Source == dto.PinSourceOriginTask {
		return &ChannelSelectError{StatusCode: http.StatusBadRequest, Message: "origin_task_channel_disabled", Code: "origin_task_channel_disabled"}
	}
	return &ChannelSelectError{StatusCode: statusCode, MessageID: messageID}
}

// AppendUsedChannel records an attempted channel in the request's channel
// trail, which the retry log and the consume log's admin_info both read.
func AppendUsedChannel(c *gin.Context, channelID int) {
	c.Set("use_channel", append(c.GetStringSlice("use_channel"), fmt.Sprintf("%d", channelID)))
}
