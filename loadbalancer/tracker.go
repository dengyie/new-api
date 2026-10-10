package loadbalancer

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// breakerState 熔断器状态
type breakerState int32

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

// ChannelStats 一份熔断状态机的状态。
//
// 键是 breakerKey 而非渠道 id：model 为空串表示「渠道级」，非空表示
// 「该渠道的这个模型」。同一个渠道的不同模型各持一份，互不影响——
// 这正是 v29.11 的目的：一个模型 404 不该让该渠道其余模型一起退出轮转。
type ChannelStats struct {
	// lastSeen 最近一次 Begin 的时刻（Unix 秒）。淘汰清扫据此丢弃
	// 长期没人用的条目，避免 (渠道 × 模型) 的组合把 map 撑大。
	lastSeen atomic.Int64
	// consecutiveFailures 连续硬失败计数（达到阈值后硬熔断）
	consecutiveFailures atomic.Int32
	// state 熔断器状态
	state atomic.Int32
	// openedAt 熔断开启时间（Unix 秒）
	openedAt atomic.Int64
	// blockedUntil 定时熔断到期时间（Unix 秒），0=未设置。
	// 用于宵禁等需要熔断到指定时间点的场景，优先级高于常规冷却。
	blockedUntil atomic.Int64
	// degradedUntil 降级到期时间（Unix 秒），0=未设置。
	// 慢渠道的软降级：仍可用，但在选渠道时排最后。10 分钟后自动恢复。
	degradedUntil atomic.Int64
	// consecutiveSlowCount 连续慢请求计数（不含硬失败），用于触发降级
	consecutiveSlowCount atomic.Int32
	// consecutiveEmptyStreams 连续空流计数。达到 empty_stream_trip_threshold
	// 后熔断该条目对应的范围（渠道级或单模型）；一次非空流成功清零。
	consecutiveEmptyStreams atomic.Int32
	// tripCount 连续熔断次数。用于冷却递增退避：第 n 次熔断的冷却为
	// 基础冷却 × min(n, escalation_cap)。一次成功请求清零。
	tripCount atomic.Int32
	// halfOpenProbes 半开状态已放行的探测数
	halfOpenProbes atomic.Int32
	// halfOpenSince 本轮半开的起始时刻（Unix 纳秒），0=未进入过半开。
	// 用于探测配额租约回收，见 halfOpenProbeLease。
	halfOpenSince atomic.Int64
	// ttftSamples 首字时间滑动窗口（毫秒），只保留最近 N 个
	mu          sync.Mutex
	ttftSamples []int64
}

const maxTTFTSamples = 100

// halfOpenProbeLease 是半开探测配额的租约时长。
//
// IsAvailable 名为「检查」，却在半开状态下以副作用方式预占一个探测配额；
// 该配额只有真正发出请求并走到 RequestHandle.End 才会归还。现实中存在大量
// 「检查通过但请求从未发出」的路径：客户端在选路后立刻断开、计费准备失败、
// Responses WebSocket 中继根本不调用 End。没有租约时，一次泄漏就会让渠道
// 永久停在半开耗尽状态——熔断器再也回不到 closed，该渠道彻底死掉且无任何
// 日志。租约让「没有结论的半开轮次」在有限时间后自动作废重来。
//
// 取值与熔断冷却同量级：远大于任何一次真实探测的时长（首字超时默认 5s），
// 不会误伤正常探测；又足够短，泄漏后渠道能在一分钟内自愈。
const halfOpenProbeLease = 60 * time.Second

// ContextKeyAttempt carries the inflight handle of the relay attempt in
// flight. The controller creates it at the start of every attempt so streams,
// non-stream replies and task submissions all count against max_inflight;
// StreamScannerHandler reuses it instead of opening a second one.
// RequestHandle.End is idempotent, so whichever layer reports first wins and
// the other is a no-op.
const ContextKeyAttempt = "loadbalancer_attempt"

// IsAvailable 拒绝渠道的原因，调用方按常量比较，避免裸字符串。
const (
	ReasonCircuitOpen            = "circuit_open"
	ReasonCircuitBlockedUntil    = "circuit_blocked_until"
	ReasonHalfOpenProbesExceeded = "circuit_half_open_probes_exhausted"
	ReasonOverloaded             = "overloaded"
)

// breakerKey 标识一份熔断状态。
//
// model 为空串表示「整渠道」状态：账号级失效（额度耗尽、宵禁、密钥失效、
// 分组无权）熔在这把键上，挡住该渠道的全部模型。model 非空则只熔这一个
// (渠道, 模型) 组合，同渠道其它模型照常轮转。
//
// 键必须是**客户端请求的模型名**（relayInfo.OriginModelName），不是上游名
// （UpstreamModelName）：熔断的写侧两个名字都拿得到，读侧（IsAvailable，
// 选渠道时）只知道客户端请求了什么，而 model_mapping 是多对一的，无法
// 从上游名反推客户端名。所以键只能落在读侧唯一知道的那个名字上。
// 代价见 docs：多个客户端名映射到同一上游名时，它们各自独立熔断。
//
// 这与 paramstrip.go 的 maxTokensLimitLearned **故意用不同的键空间**：
// 那里是纯写侧（钳制发生在中继时，上游名确定可用），不需要读侧反查。
// 不要为了「统一」把两者合并。
type breakerKey struct {
	channelID int
	model     string
}

// Tracker 跟踪所有渠道的实时状态
type Tracker struct {
	mu sync.RWMutex
	// breakers 熔断状态机，按 (渠道, 模型) 分键
	breakers map[breakerKey]*ChannelStats
	// inflight 进行中请求数，**按渠道**统计。
	//
	// max_inflight 是上游账号的并发预算，不是单模型的预算。若跟着熔断一起
	// 拆到模型级，一个渠道上 N 个并发请求分散到 N 个不同模型时每个键各自
	// 看到 inflight=0，并发上限形同虚设——这会打掉 v29.7 起对 #7 起作用的
	// 过载保护。故熔断按模型、并发按渠道，两者刻意不共用一把键。
	inflight map[int]*atomic.Int32
}

var globalTracker = &Tracker{
	breakers: make(map[breakerKey]*ChannelStats),
	inflight: make(map[int]*atomic.Int32),
}

// GlobalTracker 返回全局跟踪器
func GlobalTracker() *Tracker {
	return globalTracker
}

// getBreaker 取该键的熔断状态，必要时创建。
// 刻意不提供「只给渠道 id」的便捷版本：那会把作用域默认为渠道级，
// 而 v29.11 里选错作用域是静默的（少熔或过度熔都不会报错）。每个调用点
// 必须自己写清楚 scope。
func (t *Tracker) getBreaker(k breakerKey) *ChannelStats {
	t.mu.RLock()
	s, ok := t.breakers[k]
	t.mu.RUnlock()
	if ok {
		return s
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.breakers[k]; ok {
		return s
	}
	t.evictLocked()
	s = &ChannelStats{}
	t.breakers[k] = s
	return s
}

// getInflight 返回该渠道的并发计数器（按渠道，不按模型）。
func (t *Tracker) getInflight(channelID int) *atomic.Int32 {
	t.mu.RLock()
	c, ok := t.inflight[channelID]
	t.mu.RUnlock()
	if ok {
		return c
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.inflight[channelID]; ok {
		return c
	}
	c = &atomic.Int32{}
	t.inflight[channelID] = c
	return c
}

// 淘汰阈值：条目数超过它就在下一次建键时清扫一次。
//
// 键空间从「渠道数」变成了「渠道数 × 模型数」，条目会多一个量级。渠道被删
// 或改名后其条目本会永久残留（熔断状态不再被读，也没人再碰它），所以要有
// 这条清扫路径。阈值给得宽松：正常规模（212 渠道 × 180 模型上限 ≈ 38k）
// 远达不到，只有真出现异常增长才会触发。
const breakerEvictThreshold = 65536

// breakerIdleTTL 条目空闲多久后可以被淘汰。
const breakerIdleTTL = time.Hour

// evictLocked 丢弃「已关闭且长期无人使用」的条目。调用方必须持有写锁。
//
// 只清 closed：open / halfOpen 的条目正在挡流量或正在探测，任何一个被误删
// 都等于让故障渠道立刻重新进入轮转。halfOpen 的条目靠 lastSeen 自然老化，
// 但它在 halfOpen 时也会被 Begin 更新 lastSeen，所以会随探测流量保持新鲜。
func (t *Tracker) evictLocked() {
	if len(t.breakers) <= breakerEvictThreshold {
		return
	}
	cutoff := time.Now().Unix() - int64(breakerIdleTTL/time.Second)
	for k, s := range t.breakers {
		if breakerState(s.state.Load()) != breakerClosed {
			continue
		}
		if s.lastSeen.Load() < cutoff {
			delete(t.breakers, k)
		}
	}
}

// TripBreakerUntil 定时熔断：将该条目熔断到指定时间点（如宵禁到早 8 点）。
// 立即打开熔断器，并设置 blockedUntil，到期前不参与选渠道。
// 豁免渠道（breaker_exempt）直接返回，兜底链路不做定时熔断。
// 宵禁是账号级的时段限制，调用方应传空 model（熔整渠道）。
func (t *Tracker) TripBreakerUntil(channelID int, model string, until time.Time) {
	if !Enabled() || channelID <= 0 {
		return
	}
	if IsBreakerExempt(channelID) {
		return
	}
	s := t.getBreaker(scopeKey(channelID, model))
	s.state.Store(int32(breakerOpen))
	s.openedAt.Store(time.Now().Unix())
	s.blockedUntil.Store(until.Unix())
	s.halfOpenProbes.Store(0)
	s.halfOpenSince.Store(0)
}

// RecordEmptyStream 记一次该条目的空流。连续达到阈值则熔断（只熔断不禁用）。
// 阈值 ≤0 时关闭。成功一次非空流应调用 ClearEmptyStream。
func (t *Tracker) RecordEmptyStream(channelID int, model string) {
	if !Enabled() || channelID <= 0 {
		return
	}
	if IsBreakerExempt(channelID) {
		return
	}
	threshold := GetEmptyStreamTripThreshold()
	if threshold <= 0 {
		return
	}
	s := t.getBreaker(scopeKey(channelID, model))
	n := s.consecutiveEmptyStreams.Add(1)
	if int(n) >= threshold {
		t.TripBreaker(channelID, model)
		s.consecutiveEmptyStreams.Store(0)
	}
}

// ClearEmptyStream 一次非空流成功后清零连续空流计数。
func (t *Tracker) ClearEmptyStream(channelID int, model string) {
	if channelID <= 0 {
		return
	}
	t.getBreaker(scopeKey(channelID, model)).consecutiveEmptyStreams.Store(0)
}

// EmptyStreamStreak 返回该条目当前连续空流次数（测试用）。
func (t *Tracker) EmptyStreamStreak(channelID int, model string) int {
	if channelID <= 0 {
		return 0
	}
	return int(t.getBreaker(scopeKey(channelID, model)).consecutiveEmptyStreams.Load())
}

// TripBreaker 立即熔断（开启常规冷却周期）。
// 适用于明确的确定性或严重上游故障（如 410 EOL）。
// 冷却时长按连续熔断次数递增，见 tripBreaker。
func (t *Tracker) TripBreaker(channelID int, model string) {
	if !Enabled() || channelID <= 0 {
		return
	}
	if IsBreakerExempt(channelID) {
		return
	}
	t.getBreaker(scopeKey(channelID, model)).tripBreaker()
}

// TripBreakerForRateLimit 针对上游瞬时限流或并发超限（429、RPM 等）的短期避让熔断。
// 使用配置的 RateLimitCooldownSeconds（默认 30 秒），
// 短暂避让后自动进入半开探测，防止常规长冷却（如 300 秒）导致全渠道假死。
func (t *Tracker) TripBreakerForRateLimit(channelID int, model string) {
	if !Enabled() || channelID <= 0 {
		return
	}
	policy := GetPolicy().Resolve(channelID)
	cooldown := policy.Breaker.RateLimitCooldownSeconds
	if cooldown <= 0 {
		cooldown = 30
	}
	t.TripBreakerUntil(channelID, model, time.Now().Add(time.Duration(cooldown)*time.Second))
}

// IsDegraded 判断条目是否处于降级状态（慢 3 次后的 10 分钟软降级）。
// 降级条目仍可用，但在选渠道时排最后。
//
// 按 (渠道, 模型) 判断：首字时间取决于具体上游模型，不同模型的延迟可以
// 差一个量级。选渠道时的比较总是在同一个模型名上进行的（同一请求的候选
// 都服务于这个模型），所以按模型判降级反而比按渠道更准。
func (t *Tracker) IsDegraded(channelID int, model string) bool {
	if !Enabled() || channelID <= 0 {
		return false
	}
	s := t.getBreaker(scopeKey(channelID, model))
	if until := s.degradedUntil.Load(); until > 0 {
		if time.Now().Unix() < until {
			return true
		}
		// 到期自动清除
		s.degradedUntil.Store(0)
	}
	return false
}

// Begin 请求开始：inflight +1，返回一个用于 End 的句柄。
//
// model 是客户端请求的模型名（relayInfo.OriginModelName）：熔断状态按它
// 建键，而 inflight 始终按渠道计（见 Tracker.inflight）。
// 若 channelID <= 0（未确定渠道的占位调用），返回不污染统计的虚拟句柄。
func (t *Tracker) Begin(channelID int, model string) *RequestHandle {
	if channelID <= 0 {
		return &RequestHandle{
			tracker:   t,
			stats:     &ChannelStats{},
			inflight:  &atomic.Int32{},
			channelID: channelID,
			start:     time.Now(),
		}
	}
	// 键在 Begin 解析一次就存进句柄，End 直接用存下来的这把。
	//
	// 需要存的只有**日志要用的这把键**，不是 stats：stats 一直是 Begin 捕获的，
	// End 记的失败从来落在 Begin 定的那把上，这一点没变过。变的是新加的熔断
	// 日志要标出「熔的是 (渠道, 模型) 的哪一对」，而句柄并不保留模型名 ——
	// 若 End 为写日志重调一次 scopeKey，它读到的是**那一刻**的 policy.per_model，
	// 而这个开关会被热加载改写。于是运行中翻动它，日志会把一次按模型熔断标成
	// 「[全部模型]」（或反过来）：熔断行为仍是对的，但排障最需要的那件事
	// —— 这次熔断的影响面是几个模型还是整条渠道 —— 在日志上会说反。
	key := scopeKey(channelID, model)
	s := t.getBreaker(key)
	c := t.getInflight(channelID)
	s.lastSeen.Store(time.Now().Unix())
	c.Add(1)
	return &RequestHandle{
		tracker:   t,
		stats:     s,
		inflight:  c,
		channelID: channelID,
		key:       key,
		start:     time.Now(),
	}
}

// Inflight 返回渠道当前进行中的请求数（按渠道计，不按模型）
func (t *Tracker) Inflight(channelID int) int {
	if channelID <= 0 {
		return 0
	}
	return int(t.getInflight(channelID).Load())
}

// AvgTTFT 返回该条目最近的平均首字时间（毫秒），无样本时返回 -1
func (t *Tracker) AvgTTFT(channelID int, model string) int64 {
	if channelID <= 0 {
		return -1
	}
	s := t.getBreaker(scopeKey(channelID, model))
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ttftSamples) == 0 {
		return -1
	}
	var sum int64
	for _, v := range s.ttftSamples {
		sum += v
	}
	return sum / int64(len(s.ttftSamples))
}

// RequestHandle 单次请求的跟踪句柄
type RequestHandle struct {
	tracker *Tracker
	// stats 该 (渠道, 模型) 的熔断状态机
	stats *ChannelStats
	// inflight 该渠道的并发计数器（渠道级，与 stats 的模型级键不同）
	inflight  *atomic.Int32
	channelID int
	// key 本次请求实际落在哪把熔断键上，由 Begin 解析一次后固定下来；
	// 熔断日志用它标出 (渠道, 模型) 的哪一对，与 stats 同源。
	key       breakerKey
	start     time.Time
	firstByte time.Time
	done      atomic.Bool
}

// MarkFirstByte 记录首字到达时间
func (h *RequestHandle) MarkFirstByte() {
	if h.firstByte.IsZero() {
		h.firstByte = time.Now()
	}
}

// TTFT 返回首字时间（毫秒），未收到首字时返回 -1
func (h *RequestHandle) TTFT() int64 {
	if h.firstByte.IsZero() {
		return -1
	}
	return h.firstByte.Sub(h.start).Milliseconds()
}

// End 请求结束：inflight -1，记录 TTFT 样本，更新熔断计数。
// slow 表示是否为慢请求（首字超时），failed 表示是否失败。
func (h *RequestHandle) End(slow, failed bool) {
	if h.done.Swap(true) {
		return
	}
	if h.channelID <= 0 {
		return
	}
	h.inflight.Add(-1)

	policy := GetPolicy().Resolve(h.channelID)

	if ttft := h.TTFT(); ttft >= 0 {
		h.stats.mu.Lock()
		h.stats.ttftSamples = append(h.stats.ttftSamples, ttft)
		if len(h.stats.ttftSamples) > maxTTFTSamples {
			h.stats.ttftSamples = h.stats.ttftSamples[len(h.stats.ttftSamples)-maxTTFTSamples:]
		}
		h.stats.mu.Unlock()
	}

	if failed {
		// 硬失败：计入硬熔断计数器，达到阈值后熔断；
		// 半开探测失败时立即重新熔断，开启新冷却周期，避免卡在半开耗尽状态。
		n := h.stats.consecutiveFailures.Add(1)
		currentState := breakerState(h.stats.state.Load())
		// BreakerExempt 渠道只记失败数，不推进熔断状态机（IsAvailable 也跳过
		// 熔断判断，两端保持一致，避免留下永远读不到的死状态）。
		if !policy.BreakerExempt &&
			(currentState == breakerHalfOpen || (policy.Breaker.FailureThreshold > 0 && int(n) >= policy.Breaker.FailureThreshold)) {
			h.stats.tripBreaker()
			// 阈值驱动的熔断此前**完全没有日志**：tripBreaker 里没有、这里也没有。
			// 被自动摘掉的那 5 分钟里，运维在日志上唯一能看到的线索是渠道没流量了
			// —— 而「熔断了」和「上游就是没人用」在日志里长得一模一样。
			// controller 里那 11 处显式 Trip* 调用都有 WARN 日志，唯独这条由
			// 计数器自动走的路是哑的，正好是生产里最常走的一条。
			log.Printf("loadbalancer: channel #%d [%s] breaker tripped after %d consecutive failures (half_open_probe_failed=%t), cooling down for %ds (escalation #%d)",
				h.key.channelID, breakerKeyLabel(h.key), n, currentState == breakerHalfOpen,
				cooldownSecondsFor(h.stats, policy.Breaker), h.stats.tripCount.Load())
		}
		// 失败也重置慢计数（失败已硬处理，不再叠加软降级）
		h.stats.consecutiveSlowCount.Store(0)
	} else if slow {
		// 慢请求（非硬失败）：只软降级，不触发硬熔断。
		// 连续 3 次慢则降级 10 分钟，降级渠道仍可用（排最后），避免雪崩。
		if sn := h.stats.consecutiveSlowCount.Add(1); sn >= 3 {
			h.stats.degradedUntil.Store(time.Now().Add(10 * time.Minute).Unix())
			h.stats.consecutiveSlowCount.Store(0)
		}
	} else {
		h.stats.consecutiveFailures.Store(0)
		h.stats.consecutiveSlowCount.Store(0)
		// 任何一次成功都证明渠道已恢复：递增退避计数清零，
		// 下次熔断重新从 1 倍基础冷却开始。
		h.stats.tripCount.Store(0)
		// 成功即救活：半开探测成功关闭熔断器；开熔断期间仍在途的请求成功
		// 同样说明渠道已恢复（旧逻辑只认半开，会让递增退避的渠道在冷却期内
		// 无人问津，只能靠时间自然过期）。
		//
		// 但定时熔断（限流短避让 / 宵禁）不在此列：它走 blockedUntil 分支，
		// 一旦把状态改成 closed，IsAvailable 就再也不读 blockedUntil，剩余的
		// 避让时长被整段作废。渠道正被限流时其它在途请求成功是常态，那样等于
		// 限流避让形同虚设，「避让 → 立刻重入 → 再 429」会一直抖。
		//
		// 注意 h.stats 是**模型级**条目：某个模型成功不会碰渠道级那条，
		// 于是「A 模型成功」无法提前解掉「额度耗尽」这类账号级封锁——那正是
		// 我们要的：账号级封锁只能由它自己到期（blockedUntil）或同一条目的
		// 成功来解除。
		switch breakerState(h.stats.state.Load()) {
		case breakerHalfOpen, breakerOpen:
			if h.stats.blockedUntil.Load() > 0 {
				break
			}
			h.stats.state.Store(int32(breakerClosed))
			h.stats.halfOpenProbes.Store(0)
			h.stats.halfOpenSince.Store(0)
		}
	}
}

// EndCancelled 请求被下游客户端主动取消时收尾：仅释放 inflight 资源，
// 严禁清零连续失败计数。
//
// 背景：用户/客户端断开连接（如用户关闭窗口、客户端超时等）是下游行为，不代表渠道
// 本身恢复健康。若在此调用普通的 End(false, false) 将 consecutiveFailures 归零，
// 会导致此前已连续发生故障/慢请求的坏渠道被意外“洗白”，破坏熔断计数器。
func (h *RequestHandle) EndCancelled() {
	if h.done.Swap(true) {
		return
	}
	if h.channelID <= 0 {
		return
	}
	h.inflight.Add(-1)
}

// tripBreaker 打开熔断器并开启一轮递增冷却。
//
// 递增退避：第 n 次连续熔断的冷却 = cooldown_seconds × min(n, escalation_cap)
// （第 1 次 1 倍、第 2 次 2 倍、第 3 次 3 倍……），一次成功请求即清零计数。
// 动机是生产上观察到的「同一个渠道被反复熔断、每次冷却结束又立刻被同一个
// 故障打回」：固定 300 秒既没有惩罚递增的复发，也没有让重试更快找到别处。
// 封顶（默认 6 倍）避免反复故障的渠道被冷却到数小时而彻底退出轮转。
//
// 冷却长度仍按 openedAt + cooldown_seconds × 倍数 计算（不落绝对到期时间），
// 这样定时熔断（宵禁，blockedUntil）的优先级语义和既有到期判定路径都不变。
func (s *ChannelStats) tripBreaker() {
	s.tripCount.Add(1)
	// 已经在定时熔断中就整体让路：blockedUntil 是绝对到期时间（宵禁到早 8 点、
	// 限流避让到 +30s），常规熔断的 openedAt 是相对冷却。把 blockedUntil 清零
	// 等于用一次失败把宵禁/避让整段抹掉——而 End 的失败分支不看 blockedUntil，
	// 宵禁期间累计到 failure_threshold 次在途失败就会走到这里，渠道于是在午夜
	// 重新进入轮转，正是宵禁要防的事。
	//
	// tripCount 照常递增：这次失败是真的，只是不该拿它改写已有的绝对到期时间。
	if until := s.blockedUntil.Load(); until > 0 && time.Now().Unix() < until {
		return
	}
	// 无论之前是 closed 还是 half-open，只要判定熔断，无条件重置为 open 开启新冷却
	s.state.Store(int32(breakerOpen))
	s.openedAt.Store(time.Now().Unix())
	s.blockedUntil.Store(0)
	s.halfOpenProbes.Store(0)
	s.halfOpenSince.Store(0)
}

// escalationMultiplier 本轮熔断的冷却倍数：第 n 次连续熔断为 n 倍，在封顶处截断。
func (s *ChannelStats) escalationMultiplier(breaker BreakerPolicy) int64 {
	mult := int64(s.tripCount.Load())
	if cap := breaker.EscalationCapOrDefault(); mult > cap {
		mult = cap
	}
	if mult < 1 {
		mult = 1
	}
	return mult
}

// cooldownSecondsFor 是一次熔断的实际冷却长度：基础冷却 × 递增退避倍数。
//
// 抽出来的理由是「同一份算式只许有一份实现」：checkBreaker 的到期判定和
// 刚加的熔断日志都需要它，两处各写一遍的话，日志报出来的冷却时间会与
// 实际拒绝选择的时长对不上 —— 而那正是排障时最需要对得上的两个数。
func cooldownSecondsFor(s *ChannelStats, breaker BreakerPolicy) int64 {
	cooldown := breaker.CooldownSeconds
	if cooldown <= 0 {
		cooldown = 60
	}
	return cooldown * s.escalationMultiplier(breaker)
}

// breakerKeyLabel 给熔断日志一个和 controller 侧一致的标签：模型名为空
// 表示熔断落在整条渠道上，日志里必须写出来 —— 否则「熔了 #228 的
// claude-opus-4-8」和「熔了整条 #228」两件事在日志上长得一样。
func breakerKeyLabel(k breakerKey) string {
	if k.model == "" {
		return "[全部模型]"
	}
	return k.model
}

// scopeKey 决定一次记录/熔断实际落在哪把键上。
//
// 三个折叠规则，**所有写侧方法都必须经过它**（否则 Begin 记的条目和
// IsAvailable 查的条目不是同一把，失败计数会写进没人读的条目里）：
//  1. channelID <= 0 → 无处可记（零值键）
//  2. model 为空 → 渠道级：调用方明确要熔整渠道（额度耗尽、宵禁、401）
//  3. per_model 关闭 → 渠道级：这既是灰度开关也是回滚手段，
//     关闭时行为与 v29.10 逐位一致
func scopeKey(channelID int, model string) breakerKey {
	if channelID <= 0 {
		return breakerKey{}
	}
	if model == "" || !GetPolicy().Resolve(channelID).Breaker.PerModelOrDefault() {
		return breakerKey{channelID: channelID}
	}
	return breakerKey{channelID: channelID, model: model}
}

// IsAvailable 检查渠道当前对 model 是否可用（未过载、未熔断）。
// 返回 false 时附带原因。
func (t *Tracker) IsAvailable(channelID int, model string) (bool, string) {
	if !Enabled() || channelID <= 0 {
		return true, ""
	}
	policy := GetPolicy().Resolve(channelID)

	// 渠道级那把键永远查（账号级封锁必须挡住全部模型）；
	// 模型级那把键只在 per_model 打开且 model 非空时查。
	modelScoped := model != "" && policy.Breaker.PerModelOrDefault()
	keys := [2]breakerKey{{channelID: channelID}, {}}
	nkeys := 1
	if modelScoped {
		keys[1] = breakerKey{channelID: channelID, model: model}
		nkeys = 2
	}

	// BreakerExempt 只豁免熔断状态机，不豁免并发上限：兜底渠道（CPA）本身
	// 仍可能被并发打满，此时继续放行只会把过载原样透传给兜底链路。
	var reserved [2]bool
	if !policy.BreakerExempt {
		for i := 0; i < nkeys; i++ {
			reason, took := t.checkBreaker(keys[i], policy)
			if reason != "" {
				// 归还本次调用自己已经预占的探测配额。
				// 用局部标志而不是重读熔断状态：状态可能在两步之间被并发的
				// End 改成 open，那时重读会漏还或多还（见 overload 分支注释）。
				for j := 0; j < i; j++ {
					if reserved[j] {
						t.getBreaker(keys[j]).halfOpenProbes.Add(-1)
					}
				}
				return false, reason
			}
			reserved[i] = took
		}
	}

	// 并发上限检查（渠道级，不按模型——见 Tracker.inflight）
	if max := policy.MaxInflight; max > 0 && int(t.getInflight(channelID).Load()) >= max {
		for j := 0; j < nkeys; j++ {
			if reserved[j] {
				t.getBreaker(keys[j]).halfOpenProbes.Add(-1)
			}
		}
		return false, ReasonOverloaded
	}

	return true, ""
}

// PeekAvailable 只读检查渠道当前对 model 是否可能可用（未熔断、半开未满额、未过载）。
// 与 IsAvailable 的区别在于：PeekAvailable 是纯只读检查，绝不预占或自增半开探测配额（halfOpenProbes），
// 适用于候选池最高优先级探测、健康度扫描等无发起请求意图的只读判定。
func (t *Tracker) PeekAvailable(channelID int, model string) (bool, string) {
	if !Enabled() || channelID <= 0 {
		return true, ""
	}
	policy := GetPolicy().Resolve(channelID)

	modelScoped := model != "" && policy.Breaker.PerModelOrDefault()
	keys := [2]breakerKey{{channelID: channelID}, {}}
	nkeys := 1
	if modelScoped {
		keys[1] = breakerKey{channelID: channelID, model: model}
		nkeys = 2
	}

	if !policy.BreakerExempt {
		for i := 0; i < nkeys; i++ {
			if reason := t.peekBreaker(keys[i], policy); reason != "" {
				return false, reason
			}
		}
	}

	if max := policy.MaxInflight; max > 0 && int(t.getInflight(channelID).Load()) >= max {
		return false, ReasonOverloaded
	}

	return true, ""
}

// peekBreaker 对单把键做熔断状态机的只读检查。
// 绝不修改 s.state 或 s.halfOpenProbes，返回 reason 非空表示不可用。
func (t *Tracker) peekBreaker(k breakerKey, policy ChannelPolicy) string {
	s := t.getBreaker(k)
	switch state := breakerState(s.state.Load()); state {
	case breakerOpen:
		if until := s.blockedUntil.Load(); until > 0 {
			if time.Now().Unix() < until {
				return ReasonCircuitBlockedUntil
			}
		} else {
			cooldown := cooldownSecondsFor(s, policy.Breaker)
			if time.Now().Unix()-s.openedAt.Load() < cooldown {
				return ReasonCircuitOpen
			}
		}
		// 到期后可进入半开探测，继续检查半开容量
		fallthrough
	case breakerHalfOpen:
		maxProbes := policy.Breaker.HalfOpenProbes
		if maxProbes <= 0 {
			maxProbes = 1
		}
		if s.halfOpenProbes.Load() >= int32(maxProbes) {
			since := s.halfOpenSince.Load()
			if since > 0 && time.Since(time.Unix(0, since)) > halfOpenProbeLease {
				return ""
			}
			return ReasonHalfOpenProbesExceeded
		}
		return ""
	}
	return ""
}

	// checkBreaker 对单把键做熔断状态机检查。
// 返回 reason 非空表示不可用；returned took 为 true 表示本次调用预占了
// 一个半开探测配额，调用方放弃时必须归还。
func (t *Tracker) checkBreaker(k breakerKey, policy ChannelPolicy) (reason string, took bool) {
	s := t.getBreaker(k)
	switch state := breakerState(s.state.Load()); state {
	case breakerOpen:
		// 定时熔断（如宵禁或限流短冷却）优先级高于常规冷却：到期前一律不放行
		if until := s.blockedUntil.Load(); until > 0 {
			if time.Now().Unix() < until {
				return ReasonCircuitBlockedUntil, false
			}
			// 到期：清除定时，直接进入半开状态允许探测
			s.blockedUntil.Store(0)
			if s.state.CompareAndSwap(int32(breakerOpen), int32(breakerHalfOpen)) {
				s.halfOpenProbes.Store(0)
				s.halfOpenSince.Store(time.Now().UnixNano())
			}
		} else {
			// 递增退避：连续第 n 次熔断的冷却 = 基础冷却 × min(n, escalation_cap)
			cooldown := cooldownSecondsFor(s, policy.Breaker)
			if time.Now().Unix()-s.openedAt.Load() >= cooldown {
				// 进入半开状态
				if s.state.CompareAndSwap(int32(breakerOpen), int32(breakerHalfOpen)) {
					s.halfOpenProbes.Store(0)
					s.halfOpenSince.Store(time.Now().UnixNano())
				}
			} else {
				return ReasonCircuitOpen, false
			}
		}
		// 进入半开后继续走下面的半开逻辑
		fallthrough
	case breakerHalfOpen:
		maxProbes := policy.Breaker.HalfOpenProbes
		if maxProbes <= 0 {
			maxProbes = 1
		}
		s.reclaimExpiredProbes()
		if s.halfOpenProbes.Add(1) > int32(maxProbes) {
			s.halfOpenProbes.Add(-1)
			return ReasonHalfOpenProbesExceeded, false
		}
		// 允许这一个探测请求通过，结束时 End 会根据结果关闭或重新熔断
		return "", true
	}
	return "", false
}

// reclaimExpiredProbes 回收「已预占但从未归还」的半开探测配额。
//
// 上一轮半开在租约内没有给出任何结论，说明放行的探测请求没有走到 End
// （客户端断开、计费准备失败、Responses WebSocket 中继根本不调用 End）。
// 与其让渠道永久停在耗尽状态，不如把这轮作废、重新放行探测。
func (s *ChannelStats) reclaimExpiredProbes() {
	if s.halfOpenProbes.Load() <= 0 {
		return
	}
	since := s.halfOpenSince.Load()
	if since <= 0 {
		return
	}
	if time.Since(time.Unix(0, since)) > halfOpenProbeLease {
		// 同时把租约时钟拨回现在：这一轮从此刻重新计时。否则时钟停留在过期值上，
		// 之后每一次 IsAvailable 都会再次触发回收，探测配额上限形同虚设——
		// 本该被限流的半开渠道会被无限放行。
		s.halfOpenProbes.Store(0)
		s.halfOpenSince.Store(time.Now().UnixNano())
	}
}
