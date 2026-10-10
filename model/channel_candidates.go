package model

import (
	"fmt"
	"math"
	"math/rand"
	"slices"

	"github.com/dengyie/apihub/common"
	"github.com/dengyie/apihub/dto"
	"github.com/dengyie/apihub/loadbalancer"
	"github.com/dengyie/apihub/setting/ratio_setting"
	"github.com/samber/lo"
)

// ChannelCandidates is the resolved set of channels that can serve one
// (group, model) pair, across every priority tier.
//
// Selection resolves the set once and then picks from it in memory. Both the
// memory-cache source and the database source produce it, so a retry loop that
// skips unavailable channels costs no extra queries per attempt.
type ChannelCandidates struct {
	channels []*Channel
}

// Pick returns one candidate for the given retry tier, skipping excluded
// channels. Sticky routing only applies on the first attempt (retry == 0), so a
// retry after a channel failure is free to move off the sticky channel. It
// returns nil once every candidate of the tier is excluded, which is what lets
// the caller's skip loop terminate on candidate exhaustion.
func (c *ChannelCandidates) Pick(retry int, excludedIDs map[int]struct{}, stickyKey string) *Channel {
	if c == nil || len(c.channels) == 0 {
		return nil
	}
	pool := c.channels
	if len(excludedIDs) > 0 {
		pool = lo.Filter(pool, func(channel *Channel, _ int) bool {
			_, excluded := excludedIDs[channel.Id]
			return !excluded
		})
	}
	if len(pool) == 0 {
		return nil
	}

	// firstAttempt 必须在下面的 clamp 之前取值。
	//
	// 只有单一优先级分层的渠道池里 len(tiers)==1，任何 retry>0 都会被 clamp 回 0；
	// 若按 clamp 之后的值判断「retry==0 就走 sticky」，重试就会用同一个
	// StickyKey 确定性地选回刚刚失败的那个渠道——「换渠道重试」退化成对同一个
	// 坏渠道的重复调用，整轮重试预算全部烧在同一次失败上。sticky 的语义是
	// 「同一个 prompt 稳定命中同一渠道以命中上游缓存」，它与「换个渠道试试」
	// 本来就互斥，必须以调用方传入的原始 attempt 序号为准。
	firstAttempt := retry == 0

	tiers := priorityTiers(pool)
	if retry >= len(tiers) {
		retry = len(tiers) - 1
	}
	if retry < 0 {
		retry = 0
	}
	tier := lo.Filter(pool, func(channel *Channel, _ int) bool {
		return channel.GetPriority() == tiers[retry]
	})
	if len(tier) == 0 {
		return nil
	}

	// 智能负载：sticky 一致性路由。同样的 prompt 前缀总是选同一渠道，
	// 提高上游 prompt cache 命中率。仅在首次选择时生效。
	if firstAttempt {
		if idx := loadbalancer.StickyIndex(stickyKey, len(tier)); idx >= 0 {
			return tier[idx]
		}
	}

	sumWeight := 0
	for _, channel := range tier {
		sumWeight += channel.GetWeight()
	}
	// smoothing factor and adjustment
	smoothingFactor := 1
	smoothingAdjustment := 0
	switch {
	case sumWeight == 0:
		// when all channels have weight 0, set sumWeight to the number of channels and set smoothing adjustment to 100
		// each channel's effective weight = 100
		sumWeight = len(tier) * 100
		smoothingAdjustment = 100
	case sumWeight/len(tier) < 10:
		// when the average weight is less than 10, set smoothing factor to 100
		smoothingFactor = 100
	}

	// The contributions below always sum to sumWeight*smoothingFactor and
	// randomWeight starts below it, so this loop is guaranteed to return.
	randomWeight := rand.Intn(sumWeight * smoothingFactor)
	for _, channel := range tier {
		randomWeight -= channel.GetWeight()*smoothingFactor + smoothingAdjustment
		if randomWeight < 0 {
			return channel
		}
	}
	return tier[len(tier)-1]
}

// priorityTiers returns the distinct priorities of pool, highest first.
func priorityTiers(pool []*Channel) []int64 {
	tiers := make([]int64, 0, len(pool))
	for _, channel := range pool {
		if !slices.Contains(tiers, channel.GetPriority()) {
			tiers = append(tiers, channel.GetPriority())
		}
	}
	slices.Sort(tiers)
	slices.Reverse(tiers)
	return tiers
}

// TopAvailablePriority returns the highest priority among enabled, non-excluded channels
// that are currently available in the loadbalancer tracker (neither breaker-tripped nor overloaded).
func (c *ChannelCandidates) TopAvailablePriority(excludedIDs map[int]struct{}, modelName string) (int64, bool) {
	if c == nil || len(c.channels) == 0 {
		return 0, false
	}
	var maxPriority int64 = math.MinInt64
	hasAvail := false
	for _, channel := range c.channels {
		if channel.Status != common.ChannelStatusEnabled {
			continue
		}
		if len(excludedIDs) > 0 {
			if _, excluded := excludedIDs[channel.Id]; excluded {
				continue
			}
		}
		if ok, _ := loadbalancer.GlobalTracker().PeekAvailable(channel.Id, modelName); ok {
			p := channel.GetPriority()
			if !hasAvail || p > maxPriority {
				maxPriority = p
				hasAvail = true
			}
		}
	}
	if !hasAvail {
		return 0, false
	}
	return maxPriority, true
}

// GetChannelCandidates resolves every channel able to serve group and
// modelName, from the memory cache when enabled and from the abilities table
// otherwise. The returned set is a point-in-time snapshot: cache-sourced
// channels stay valid because InitChannelCache replaces the whole map instead
// of mutating the channels already handed out.
func GetChannelCandidates(group string, modelName string, filters []dto.ChannelFilter) (*ChannelCandidates, error) {
	if common.MemoryCacheEnabled {
		return cachedChannelCandidates(group, modelName, filters)
	}
	return dbChannelCandidates(group, modelName, filters)
}

func cachedChannelCandidates(group string, modelName string, filters []dto.ChannelFilter) (*ChannelCandidates, error) {
	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()

	ids, _ := filterCandidateIDs(group2model2channels[group][modelName], modelName, filters)
	if len(ids) == 0 {
		normalized := ratio_setting.RoutingMatchModelName(modelName)
		if normalized != "" && normalized != modelName {
			ids, _ = filterCandidateIDs(group2model2channels[group][normalized], modelName, filters)
		}
	}
	channels := make([]*Channel, 0, len(ids))
	for _, id := range ids {
		channel, ok := channelsIDM[id]
		if !ok {
			return nil, fmt.Errorf("数据库一致性错误，渠道# %d 不存在，请联系管理员修复", id)
		}
		channels = append(channels, channel)
	}
	return &ChannelCandidates{channels: channels}, nil
}

func dbChannelCandidates(group string, modelName string, filters []dto.ChannelFilter) (*ChannelCandidates, error) {
	// abilities is a projection of channels (see updateChannelAbilities), so
	// priority and weight are read back from the channel row itself and the two
	// sources cannot drift apart.
	abilities, err := loadEnabledAbilities(group, modelName)
	if err != nil {
		return nil, err
	}
	if len(abilities) == 0 {
		return &ChannelCandidates{}, nil
	}

	ids := make([]int, 0, len(abilities))
	seen := make(map[int]struct{}, len(abilities))
	for _, ability := range abilities {
		if _, dup := seen[ability.ChannelId]; dup {
			continue
		}
		seen[ability.ChannelId] = struct{}{}
		ids = append(ids, ability.ChannelId)
	}

	// Re-check the live channel status rather than trusting abilities.enabled.
	// The two tables are not updated in one transaction: UpdateChannelStatus
	// updates abilities from a defer whose error is only logged, so a failure
	// there leaves a disabled channel with a stale enabled row. The memory-cache
	// path already filters on channel.Status (InitChannelCache), and the two
	// sources are documented as producing the same candidate set, so the
	// database path has to defend itself the same way.
	var rows []*Channel
	if err := DB.Where("id IN ? and status = ?", ids, common.ChannelStatusEnabled).Find(&rows).Error; err != nil {
		// A task-plugin identity is a hard requirement: fail closed rather than
		// routing to a channel that may not implement the plugin.
		if identityFilterRequiresKey(filters) {
			return &ChannelCandidates{}, nil
		}
		return nil, err
	}

	channels := lo.Filter(rows, func(channel *Channel, _ int) bool {
		ok, _ := ChannelSatisfiesFilters(channel, modelName, filters)
		return ok
	})
	return &ChannelCandidates{channels: channels}, nil
}

// loadEnabledAbilities reads the enabled abilities of a model, falling back to
// the routing-normalized model name when the exact name has no rows.
func loadEnabledAbilities(group string, modelName string) ([]Ability, error) {
	var abilities []Ability
	err := DB.Where(commonGroupCol()+" = ? and model = ? and enabled = ?", group, modelName, true).
		Order("priority DESC, weight DESC").Find(&abilities).Error
	if err != nil {
		return nil, err
	}
	if len(abilities) > 0 {
		return abilities, nil
	}
	normalized := ratio_setting.RoutingMatchModelName(modelName)
	if normalized == "" || normalized == modelName {
		return nil, nil
	}
	err = DB.Where(commonGroupCol()+" = ? and model = ? and enabled = ?", group, normalized, true).
		Order("priority DESC, weight DESC").Find(&abilities).Error
	if err != nil {
		return nil, err
	}
	return abilities, nil
}

func identityFilterRequiresKey(filters []dto.ChannelFilter) bool {
	for _, filter := range filters {
		if filter.Kind == dto.FilterTaskPluginIdentity && filter.TaskPluginKey != "" {
			return true
		}
	}
	return false
}
