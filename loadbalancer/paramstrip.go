package loadbalancer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/dengyie/apihub/relaykit/types"
)

// 通用参数裁剪：不同上游对可选参数的支持程度不同（如 thinking、
// reasoning_effort、stream_options 等），不支持时报 400。
// 本模块按渠道维护需要裁剪的参数列表：
//  1. 配置文件中手动指定（strip_params）
//  2. 运行时从 400 错误中自动学习参数名
//
// 对标 CPA 的 payload.filter，但粒度是按渠道而非按模型。

var (
	paramStripMu     sync.RWMutex
	paramStripConfig = make(map[int]map[string]struct{})
	// paramStripLearned 按 **(渠道, 模型)** 存，不按渠道存。
	//
	// 「上游不支持某参数」是关于**那个模型**的观察，不是关于这条渠道的：
	// 渠道 #238 同时挂 gemini-3.8-flash / kimi-k3 / claude-opus-4-8 /
	// glm-5.3-flash / deepseek-v4.1-flash / deepseek-v4-flash 六个模型，
	// 按渠道存会让任一模型的一次误判外溢到另外五个，把它们本该支持的参数
	// 一起裁掉。此前它是这一串错误处理里**唯一**停在渠道级的环节——熔断自
	// v29.14 起是 ScopeModel、isModelScopedAutoDisable 存在的理由也正是
	// 「这个模型坏了 ≠ 这条渠道坏了」。
	//
	// 运维在 yaml 里显式写的 paramStripConfig 仍按渠道：那是人的决定，不是观测。
	paramStripLearned = make(map[paramStripKey]map[string]struct{})
)

// paramStripKey 是学习集合的键。两端必须传**同一个**模型名，否则学到的裁剪
// 永远不会生效（且不会有任何报错）——学习侧传 relayInfo.OriginModelName，
// 出站裁剪侧传 info.OriginModelName，与熔断键同源。
type paramStripKey struct {
	channelID int
	model     string
}

// maxTokensLimitLearned 记录「渠道 × 上游模型」真实的 max_completion_tokens 上限。
//
// 上游对超限请求一律回 400，但报文里通常直接写明它自己的上限
// （"max_completion_tokens is too large: 384000. This model supports at most
// 262144 completion tokens."）。与其每次都先失败一次再重试，不如第一次收到
// 就把这个上限学到手，之后出站前直接钳制——客户端要的长回复一次就成。
//
// key 为 channelID → model → limit。模型级而非渠道级：同一中转站的
// 不同模型上限可以差一个数量级。
var maxTokensLimitLearned sync.Map // map[int]map[string]uint

// maxTokensLimitRe 解析上游回传的 max_completion_tokens 上限。
// 兼容中英文与不同措辞：只认「超限 + 上限数字」这一组语义。
var maxTokensLimitRe = regexp.MustCompile(
	`(?is)max_completion_tokens\s+is\s+too\s+large.*?supports\s+at\s+most\s+(\d+)`)

// ParseMaxCompletionTokensLimit 从上游 400 报文里解析该模型允许的
// max_completion_tokens 上限。未命中返回 (0, false)。
func ParseMaxCompletionTokensLimit(err *types.NewAPIError) (uint, bool) {
	if err == nil {
		return 0, false
	}
	m := maxTokensLimitRe.FindStringSubmatch(err.Error())
	if m == nil {
		return 0, false
	}
	parsed, perr := strconv.ParseUint(m[1], 10, 64)
	if perr != nil || parsed == 0 {
		return 0, false
	}
	return uint(parsed), true
}

// RecordMaxCompletionTokensLimit 记住该渠道该模型的上限。
// 只在「本次学到的上限比已知更小」时覆盖：上限只会收紧，放宽会让先前
// 按更小上限做的钳制失效。
func RecordMaxCompletionTokensLimit(channelID int, modelName string, limit uint) {
	if channelID <= 0 || modelName == "" || limit == 0 {
		return
	}
	inner, _ := maxTokensLimitLearned.LoadOrStore(channelID, &sync.Map{})
	perModel, ok := inner.(*sync.Map)
	if !ok {
		return
	}
	if prev, loaded := perModel.Load(modelName); loaded {
		if old, ok := prev.(uint); ok && old <= limit {
			return
		}
	}
	perModel.Store(modelName, limit)
}

// GetMaxCompletionTokensLimit 返回该渠道该模型的已知上限；未学到返回 0。
func GetMaxCompletionTokensLimit(channelID int, modelName string) uint {
	if channelID <= 0 || modelName == "" {
		return 0
	}
	inner, ok := maxTokensLimitLearned.Load(channelID)
	if !ok {
		return 0
	}
	perModel, ok := inner.(*sync.Map)
	if !ok {
		return 0
	}
	if v, ok := perModel.Load(modelName); ok {
		if limit, ok := v.(uint); ok {
			return limit
		}
	}
	return 0
}

// ClampMaxCompletionTokens 把请求的上限钳到该渠道该模型的已知上限内。
// limit 为 0（未学到）或请求本身已在上限内时原样返回。
func ClampMaxCompletionTokens(channelID int, modelName string, requested uint) uint {
	limit := GetMaxCompletionTokensLimit(channelID, modelName)
	if limit == 0 || requested <= limit {
		return requested
	}
	return limit
}

// SetParamStripConfig 从配置加载手动指定的裁剪规则
// cfg: channelID -> 参数名列表
func SetParamStripConfig(cfg map[int][]string) {
	paramStripMu.Lock()
	defer paramStripMu.Unlock()
	paramStripConfig = make(map[int]map[string]struct{}, len(cfg))
	for id, params := range cfg {
		set := make(map[string]struct{}, len(params))
		for _, p := range params {
			if n := normalizeParamName(p); n != "" {
				set[n] = struct{}{}
			}
		}
		paramStripConfig[id] = set
	}
}

// SetThinkingStripChannels 兼容旧接口：设置需要裁剪 thinking 的渠道
func SetThinkingStripChannels(ids []int) {
	cfg := make(map[int][]string, len(ids))
	for _, id := range ids {
		cfg[id] = []string{"thinking"}
	}
	// 合并而非覆盖：保留已有的其他参数规则
	paramStripMu.Lock()
	defer paramStripMu.Unlock()
	for id, params := range cfg {
		set := paramStripConfig[id]
		if set == nil {
			set = make(map[string]struct{})
			paramStripConfig[id] = set
		}
		for _, p := range params {
			if n := normalizeParamName(p); n != "" {
				set[n] = struct{}{}
			}
		}
	}
}

// GetStripParams 返回某渠道某模型需要裁剪的参数名列表（已去重）。
//
// 运维预置按渠道生效，人写的规则不该被自动学习的作用域收窄；
// 自动学习到的按 (渠道, 模型) 生效，见 paramStripLearned 的注释。
func GetStripParams(channelID int, modelName string) []string {
	if !Enabled() || channelID <= 0 {
		return nil
	}
	paramStripMu.RLock()
	defer paramStripMu.RUnlock()
	key := paramStripKey{channelID: channelID, model: modelName}
	var out []string
	for _, set := range []map[string]struct{}{paramStripConfig[channelID], paramStripLearned[key]} {
		for p := range set {
			if !slices.Contains(out, p) {
				out = append(out, p)
			}
		}
	}
	return out
}

// MarkParamUnsupported 标记该渠道**该模型**不支持某参数（自动学习）。
func MarkParamUnsupported(channelID int, modelName string, param string) {
	param = normalizeParamName(param)
	if param == "" || !Enabled() || channelID <= 0 {
		return
	}
	paramStripMu.Lock()
	defer paramStripMu.Unlock()
	key := paramStripKey{channelID: channelID, model: modelName}
	set := paramStripLearned[key]
	if set == nil {
		set = make(map[string]struct{})
		paramStripLearned[key] = set
	}
	set[param] = struct{}{}
}

func normalizeParamName(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	// 如果全是纯大写（如 THINKING），直接转为小写
	hasLower := false
	for _, r := range p {
		if unicode.IsLower(r) {
			hasLower = true
			break
		}
	}
	if !hasLower {
		return strings.ToLower(p)
	}

	// 转换 PascalCase/camelCase 到 snake_case（例如 ReasoningEffort -> reasoning_effort）
	var b strings.Builder
	runes := []rune(p)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if unicode.IsUpper(r) {
			if i > 0 && runes[i-1] != '_' && unicode.IsLower(runes[i-1]) {
				b.WriteRune('_')
			}
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// 常见 400 参数不支持错误的模式，用于提取参数名
var unsupportedParamPatterns = []*regexp.Regexp{
	// "thinking" is not supported on /v1/chat/completions
	regexp.MustCompile(`["']([a-zA-Z_][a-zA-Z0-9_]*)["']\s+is not supported`),
	// Unsupported parameter: 'reasoning_effort'
	// Unsupported parameter(s): `enable_thinking`
	//   ↑ 复数 (s) 与反引号包裹（pydantic / FastAPI 风格，huan666 等中转站用它）。
	//   原先字面量写死 "parameter:"，遇到 "parameter(s):" 直接漏识别——生产实测
	//   渠道 #68 因这一条白烧了 68 次 400。分隔符用「非标识符字符」类匹配，
	//   不枚举引号种类（Go 原始字符串里也无法写反引号）。
	regexp.MustCompile(`[Uu]nsupported parameters?(?:\(s\))?:\s*[^a-zA-Z0-9_]{0,2}([a-zA-Z_][a-zA-Z0-9_]*)`),
	// Unrecognized request argument supplied: stream_options
	regexp.MustCompile(`[Uu]nrecognized request argument supplied:\s*["']?([a-zA-Z_][a-zA-Z0-9_]*)["']?`),
	// Additional properties are not allowed ('foo' was unexpected)
	regexp.MustCompile(`\(\s*["']([a-zA-Z_][a-zA-Z0-9_]*)["']\s+was unexpected\s*\)`),
	// does not support parameter "bar"
	regexp.MustCompile(`does not support (?:the |parameter\s+)?["']?([a-zA-Z_][a-zA-Z0-9_]*)["']?`),
	// field ReasoningEffort invalid, should be one of: ... (SenseNova / 商汤系)
	regexp.MustCompile(`(?i)field\s+['"]?([a-zA-Z_][a-zA-Z0-9_]*)['"]?\s+invalid`),
	// parameter 'xxx' is invalid / 'xxx' is invalid
	regexp.MustCompile(`(?i)(?:parameter\s+)?['"]([a-zA-Z_][a-zA-Z0-9_]*)['"]\s+is invalid`),
	// unknown parameter / unknown field
	regexp.MustCompile(`(?i)unknown\s+(?:parameter|field):\s*['"]?([a-zA-Z_][a-zA-Z0-9_]*)['"]?`),
	// invalid parameter: xxx
	regexp.MustCompile(`(?i)invalid\s+parameter:\s*['"]?([a-zA-Z_][a-zA-Z0-9_]*)['"]?`),
	// parameter xxx is not supported
	regexp.MustCompile(`(?i)parameter\s+['"]?([a-zA-Z_][a-zA-Z0-9_]*)['"]?\s+is not supported`),
	// extra fields not permitted ... loc: ['body', 'xxx']
	// pydantic v2 的 loc 是**路径数组**：首元素是定位前缀（body/query/...），
	// 末元素才是真正被拒的字段。旧正则的惰性 .*? 抓的是 loc 之后第一个带引号的
	// token，于是学到 "body" —— 而 stripOpenAIParam 是封闭白名单、没有 body 分支，
	// 裁剪成了空操作：该渠道会带着同一个参数反复重发、每次同样失败，且日志照样
	// 打出「已标记自动裁剪」这种误导性的成功信号。
	// 两条按顺序匹配，顺序不可调换：多元素先走逗号那条（抓末位），单元素
	// loc: ['xxx'] 没有逗号，落到括号那条。括号那条若先跑，多元素会退回抓首元素。
	regexp.MustCompile(`(?i)extra fields not permitted.*loc.*?,\s*['"]([a-zA-Z_][a-zA-Z0-9_]*)['"]`),
	regexp.MustCompile(`(?i)extra fields not permitted.*loc.*?\[\s*['"]([a-zA-Z_][a-zA-Z0-9_]*)['"]`),
}

// unsupportedParamPatternsStrict 是「报文里明确点名了某个参数」的措辞子集。
//
// 存在的唯一理由是要在 **400 之外**（422 / 5xx）也认参数错误，而放宽到这些
// 状态码的前提是「不会把无关故障误判成参数问题」——5xx 报文的内容不受我们
// 控制，什么都可能出现在里面。于是按「是否强制出现 parameter / field /
// argument 这类词，或强制要求带引号的参数名 + 校验框架」来划线，每一条都
// 只可能来自请求校验，不可能是通用故障文案。
//
// 反面教材是 `does not support X`：它是散文式措辞，`SMTP server does not
// support STARTTLS`、`does not support ClickHouse` 都会命中，且会提取出
// "STARTTLS"/"ClickHouse" 这种根本不是请求参数的名字。因此它留在 400-only
// 的 unsupportedParamPatterns 里，不进本集合。
var unsupportedParamPatternsStrict = []*regexp.Regexp{
	// "thinking" is not supported on /v1/chat/completions
	regexp.MustCompile(`["']([a-zA-Z_][a-zA-Z0-9_]*)["']\s+is not supported`),
	// Unsupported parameter: 'reasoning_effort'
	// Unsupported parameter(s): `enable_thinking`
	regexp.MustCompile(`[Uu]nsupported parameters?(?:\(s\))?:\s*[^a-zA-Z0-9_]{0,2}([a-zA-Z_][a-zA-Z0-9_]*)`),
	// Unrecognized request argument supplied: stream_options
	regexp.MustCompile(`[Uu]nrecognized request argument supplied:\s*["']?([a-zA-Z_][a-zA-Z0-9_]*)["']?`),
	// Additional properties are not allowed ('foo' was unexpected)
	regexp.MustCompile(`\(\s*["']([a-zA-Z_][a-zA-Z0-9_]*)["']\s+was unexpected\s*\)`),
	// unknown parameter / unknown field
	regexp.MustCompile(`(?i)unknown\s+(?:parameter|field):\s*['"]?([a-zA-Z_][a-zA-Z0-9_]*)['"]?`),
	// invalid parameter: xxx
	regexp.MustCompile(`(?i)invalid\s+parameter:\s*['"]?([a-zA-Z_][a-zA-Z0-9_]*)['"]?`),
	// parameter 'xxx' is not supported
	regexp.MustCompile(`(?i)parameter\s+['"]?([a-zA-Z_][a-zA-Z0-9_]*)['"]?\s+is not supported`),
	// parameter 'xxx' is invalid / 'xxx' is invalid（要求带引号的参数名）
	regexp.MustCompile(`(?i)(?:parameter\s+)?['"]([a-zA-Z_][a-zA-Z0-9_]*)['"]\s+is invalid`),
	// extra fields not permitted ... loc: ['body', 'xxx']
	// pydantic v2 的 loc 是**路径数组**：首元素是定位前缀（body/query/...），
	// 末元素才是真正被拒的字段。旧正则的惰性 .*? 抓的是 loc 之后第一个带引号的
	// token，于是学到 "body" —— 而 stripOpenAIParam 是封闭白名单、没有 body 分支，
	// 裁剪成了空操作：该渠道会带着同一个参数反复重发、每次同样失败，且日志照样
	// 打出「已标记自动裁剪」这种误导性的成功信号。
	// 两条按顺序匹配，顺序不可调换：多元素先走逗号那条（抓末位），单元素
	// loc: ['xxx'] 没有逗号，落到括号那条。括号那条若先跑，多元素会退回抓首元素。
	regexp.MustCompile(`(?i)extra fields not permitted.*loc.*?,\s*['"]([a-zA-Z_][a-zA-Z0-9_]*)['"]`),
	regexp.MustCompile(`(?i)extra fields not permitted.*loc.*?\[\s*['"]([a-zA-Z_][a-zA-Z0-9_]*)['"]`),
}

// matchParamPatterns 依次跑一组模式，返回第一个命中的规范化参数名。
func matchParamPatterns(msg string, patterns []*regexp.Regexp) (string, bool) {
	for _, re := range patterns {
		if m := re.FindStringSubmatch(msg); m != nil {
			return normalizeParamName(m[1]), true
		}
	}
	return "", false
}

// isParamValidationStatusCode 判断该状态码是否可能承载「参数校验失败」。
//
// 为什么不能只认 400：参数校验错误的状态码在各家上游极不统一 ——
// OpenAI 风格 400、FastAPI/pydantic 原生 422、以及**中转层把上游 400
// 包成自己的 5xx**。生产实测渠道 #238（ilovecat520-cc）就是最后一种：
// 同一句 `Validation: Unsupported parameter(s): enable_thinking`（原报文用
// 反引号把参数名包起来），直连上游回 500，网关侧于是完全没认出它是参数问题。
//
// 而 5xx 是网关侧故障的状态码，语义上「一定是上游挂了」。把它纳入进来
// 正是本函数存在的意义：这条错误其实是**请求形状问题**，换个渠道/裁掉
// 参数就能过，不该按渠道故障熔断，更不该每个请求白烧一轮。
//
// 仍刻意排除 401/403/404/429：这些码下若出现「不支持某参数」的措辞，
// 真实原因几乎必然是凭据/权限/路由，与裁剪参数无关，放进来只会误伤。
func isParamValidationStatusCode(code int) bool {
	return code == http.StatusBadRequest ||
		code == http.StatusUnprocessableEntity ||
		(code >= 500 && code <= 599)
}

// IsParamNotSupportedError 判断是否为「参数不支持」错误，是则返回参数名。
// 支持 thinking/reasoning_effort/enable_thinking/stream_options 等。
//
// 判据是**报文措辞**而非状态码：同一个校验错误，OpenAI 风格回 400、
// pydantic 原生回 422、中转层包成 5xx 都见过（见 isParamValidationStatusCode）。
// 早先这里第一行写死 `StatusCode != 400` 就返回，把「上游用什么码包装」
// 当成了「上游在说什么」，于是非 400 的上游整条自动学习链路静默失效：
// 不标记、不裁剪，每个请求稳定白烧一轮报错，且永不自愈。
func IsParamNotSupportedError(err *types.NewAPIError) (string, bool) {
	if err == nil {
		return "", false
	}
	msg := err.Error()
	msgLower := strings.ToLower(msg)

	// 400 走完整判据（含散文式措辞与 reasoning_effort 等级启发式），
	// 行为与既往逐位一致 —— 那 71 条生产 400 正是靠它自愈的，不能动。
	if err.StatusCode == http.StatusBadRequest {
		// 特殊识别 reasoning_effort 等级/参数不支持错误：
		// 例如：level "max" not supported, valid levels: low, medium, high
		if strings.Contains(msgLower, "valid levels:") ||
			(strings.Contains(msgLower, "level") && strings.Contains(msgLower, "not supported") && (strings.Contains(msgLower, "low") || strings.Contains(msgLower, "medium") || strings.Contains(msgLower, "high") || strings.Contains(msgLower, "max") || strings.Contains(msgLower, "xhigh"))) ||
			regexp.MustCompile(`(?i)level\s+["']?[a-zA-Z0-9_]+["']?\s+(?:is\s+)?not supported`).MatchString(msg) {
			return "reasoning_effort", true
		}
		return matchParamPatterns(msg, unsupportedParamPatterns)
	}

	// 400 之外只认「明确点名参数」的严格子集：5xx 报文不受我们控制，
	// 散文式措辞会把无关故障误判成参数问题。
	if !isParamValidationStatusCode(err.StatusCode) {
		return "", false
	}
	return matchParamPatterns(msg, unsupportedParamPatternsStrict)
}

// IsCurfewError 判断是否为"宵禁"类 403 错误（00:00-8:00 服务不可用）。
// 这类渠道需要熔断到早 8 点，而不是常规的几分钟冷却。
func IsCurfewError(err *types.NewAPIError) bool {
	if err == nil || err.StatusCode != 403 {
		return false
	}
	msg := err.Error()
	// provider_code=system_curfew 或中文"宵禁"提示
	return strings.Contains(msg, "system_curfew") || strings.Contains(msg, "宵禁")
}

// IsEOLError 判断是否为模型已下线 (End of Life) 或永久不可用错误（410 Gone 等）。
// 这类错误表示该渠道上的该模型已永久退役，应立即熔断该渠道并避免向下游客户端直接透传 410。
func IsEOLError(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	if err.StatusCode == 410 {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "end of life") ||
		strings.Contains(msg, "no longer available") ||
		strings.Contains(msg, "has reached its end of life")
}

// CurfewEndTime 计算宵禁结束时间（上海时间早 8 点）。
// 如果现在已过早 8 点，返回次日早 8 点。
func CurfewEndTime(now time.Time) time.Time {
	// 上海时区
	loc, _ := time.LoadLocation("Asia/Shanghai")
	if loc == nil {
		loc = time.FixedZone("CST", 8*3600)
	}
	nowShanghai := now.In(loc)
	// 当天早 8 点
	end := time.Date(nowShanghai.Year(), nowShanghai.Month(), nowShanghai.Day(), 8, 0, 0, 0, loc)
	if !nowShanghai.Before(end) {
		// 已过早 8 点，取次日
		end = end.Add(24 * time.Hour)
	}
	return end
}

// IsUpstreamQuotaError 判断是否为上游中继站或渠道自身的额度耗尽/欠费错误（通常返回 400/402/403/429）。
// 很多聚合中继站（如 One-API、New-API 等）在上游额度不足时会返回 400 且附带 "credit insufficient balance"
// 或 "insufficient_user_quota"，这属于渠道侧可用性故障而非客户端 Bad Request，应触发换渠道重试与熔断。
func IsUpstreamQuotaError(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "credit insufficient balance") ||
		strings.Contains(msg, "insufficient_user_quota") ||
		strings.Contains(msg, "insufficient_quota") ||
		strings.Contains(msg, "insufficient balance") ||
		strings.Contains(msg, "exceeded your current quota") ||
		strings.Contains(msg, "your credit balance is too low") ||
		strings.Contains(msg, "user quota not enough") ||
		strings.Contains(msg, "quota exhausted") ||
		strings.Contains(msg, "balance is not enough") ||
		strings.Contains(msg, "quota_exceeded") ||
		strings.Contains(msg, "user_quota_exhausted") ||
		strings.Contains(msg, "account_deactivated") ||
		strings.Contains(msg, "用量上限") ||
		strings.Contains(msg, "今日已用完") ||
		strings.Contains(msg, "额度已用完") ||
		strings.Contains(msg, "额度不足") ||
		strings.Contains(msg, "余额不足") ||
		strings.Contains(msg, "entitlement exhausted") ||
		strings.Contains(msg, "reached your weekly") ||
		strings.Contains(msg, "reached your daily") {
		return true
	}
	if oe, ok := err.RelayError.(types.OpenAIError); ok {
		codeStr := strings.ToLower(fmt.Sprintf("%v", oe.Code))
		typeStr := strings.ToLower(oe.Type)
		if strings.Contains(codeStr, "insufficient") || strings.Contains(codeStr, "quota") ||
			strings.Contains(typeStr, "insufficient") || strings.Contains(typeStr, "quota") {
			return true
		}
	}
	return false
}

// IsUpstreamRoutingError 判断是否为上游网关会话路由头缺失错误（通常返回 400）。
// 例如部分中间层中继（如 ioll.pp.ua）未正确透传 x-opencode-session / x-opencode-* 等请求头，
// 导致目标上游（如 OpenCode）拒绝服务并返回 "Request is missing x-opencode-session and cannot be routed efficiently."
// 这属于渠道代理配置缺陷而非客户端错误，应触发换渠道重试与熔断。
func IsUpstreamRoutingError(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "x-opencode-session") ||
		strings.Contains(msg, "missingsessionid") ||
		strings.Contains(msg, "cannot be routed efficiently")
}

// IsThinkingModeHistoryError 判断是否为上游思考模式历史消息校验错误（通常返回 400）。
// 例如 SiliconFlow 等上游在 thinking mode 下严格要求多轮对话中历史 assistant 消息必须回传 reasoning_content，
// 而标准客户端 SDK（如 ZCode、OpenCode 等）历史消息未持久化该字段，导致特定渠道拒绝请求。
// 这属于渠道特性不兼容/上游校验错误，应触发换渠道重试与熔断隔离。
func IsThinkingModeHistoryError(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return (strings.Contains(msg, "reasoning_content") && strings.Contains(msg, "thinking mode")) ||
		strings.Contains(msg, "thinking mode must be passed back") ||
		strings.Contains(msg, "content[].thinking in the thinking mode must be passed back")
}

// IsReasoningHydrationError 判断是否为上游 Responses 推理水合（解密）失败错误（通常返回 400）。
//
// 当多轮对话历史中包含某 OpenAI 账号加密的 reasoning 项（rs_*，encrypted_content），
// 被路由或重试到不同 OpenAI 账号的上游时，上游无法解密，抛出：
// "The encrypted content for item rs_* could not be verified. Reason: reasoning hydration failed: Encrypted content could not be decrypted or parsed."
// 属于跨账号会话密文不兼容，网关应剥离历史加密项并触发重试自愈，不能视为渠道损坏。
func IsReasoningHydrationError(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "reasoning hydration failed") ||
		(strings.Contains(msg, "encrypted content") && strings.Contains(msg, "could not be verified")) ||
		(strings.Contains(msg, "encrypted content could not be decrypted")) {
		return true
	}
	if oe, ok := err.RelayError.(types.OpenAIError); ok {
		relayMsg := strings.ToLower(oe.Message)
		if strings.Contains(relayMsg, "reasoning hydration failed") ||
			(strings.Contains(relayMsg, "encrypted content") && strings.Contains(relayMsg, "could not be verified")) ||
			(strings.Contains(relayMsg, "encrypted content could not be decrypted")) {
			return true
		}
	} else if poe, ok := err.RelayError.(*types.OpenAIError); ok && poe != nil {
		relayMsg := strings.ToLower(poe.Message)
		if strings.Contains(relayMsg, "reasoning hydration failed") ||
			(strings.Contains(relayMsg, "encrypted content") && strings.Contains(relayMsg, "could not be verified")) ||
			(strings.Contains(relayMsg, "encrypted content could not be decrypted")) {
			return true
		}
	}
	return false
}

// IsUpstreamToolCallStateLostError 判断是否为上游丢失工具调用会话状态错误（通常返回 400）。
//
// 客户端（如 codex CLI）续接一次工具调用时只带上一轮的 call_id，并不回传该调用的完整上下文；
// 上游若在自己的会话存储里找不到这个 call_id，就会返回「找不到可消费的工具调用状态：call_id=...」。
//
// 对本网关而言触发条件几乎总是自己造成的：渠道亲和（会话粘性）只存在于进程内存中，
// 蓝绿发布或重启后记录清空，续接请求便会被路由到当初产生该 call_id 的渠道之外，
// 上游自然认不出这个 call_id。这类错误要能被单独识别，好在回给客户端时替换成
// 有成因、有出路的说明，而不是把上游那句对使用者毫无信息量的话原样透出去。
func IsUpstreamToolCallStateLostError(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "找不到可消费的工具调用状态") {
		return true
	}
	if strings.Contains(msg, "工具调用状态") && strings.Contains(msg, "call_id") {
		return true
	}
	return strings.Contains(msg, "no consumable tool call state") ||
		strings.Contains(msg, "tool call state not found") ||
		strings.Contains(msg, "no tool call state found") ||
		strings.Contains(msg, "unable to find tool call state") ||
		strings.Contains(msg, "unknown tool call state")
}

// IsUpstreamPermissionError 判断是否为上游渠道权限/分组无权访问/TokenPlan不支持等错误（通常返回 403 或 404）。
// 例如聚合中继站（One-API/New-API 等）返回 "无权访问 按量分组 分组"、"user_group_no_permission"、
// "当前分组本时段不可调用"、"当前分组无可用渠道"，或订阅计划返回 "deepseek-v4-flash is not supported by TokenPlan"。
// 这类错误属于上游渠道配置、分组或套餐权限缺陷，而非下游客户端认证失败。
// 应触发换渠道重试与即时熔断隔离，并在透传保护中映射为网关侧故障码（503，见 controller/relay.go）
// 避免客户端把上游配置缺陷误当成自己的凭证错误而终止会话。
func IsUpstreamPermissionError(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	if err.StatusCode == http.StatusForbidden {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "无权访问") ||
		strings.Contains(msg, "按量分组") ||
		strings.Contains(msg, "user_group_no_permission") ||
		strings.Contains(msg, "group_no_permission") ||
		strings.Contains(msg, "not authorized for this group") ||
		strings.Contains(msg, "not supported by tokenplan") ||
		strings.Contains(msg, "not supported by token plan") ||
		strings.Contains(msg, "is not supported by tokenplan") ||
		strings.Contains(msg, "is not supported by token plan") ||
		strings.Contains(msg, "permission denied") ||
		strings.Contains(msg, "permission_denied") ||
		strings.Contains(msg, "operation not allowed") ||
		strings.Contains(msg, "your account is not authorized") ||
		(strings.Contains(msg, "当前分组") && (strings.Contains(msg, "不可调用") || strings.Contains(msg, "无可用渠道") || strings.Contains(msg, "无权"))) {
		return true
	}
	if oe, ok := err.RelayError.(types.OpenAIError); ok {
		codeStr := strings.ToLower(fmt.Sprintf("%v", oe.Code))
		typeStr := strings.ToLower(oe.Type)
		msgStr := strings.ToLower(oe.Message)
		if strings.Contains(codeStr, "group_no_permission") ||
			strings.Contains(codeStr, "user_group_no_permission") ||
			strings.Contains(codeStr, "permission_denied") ||
			strings.Contains(typeStr, "group_no_permission") ||
			strings.Contains(typeStr, "permission_denied") ||
			strings.Contains(msgStr, "无权访问") ||
			strings.Contains(msgStr, "not supported by tokenplan") ||
			strings.Contains(msgStr, "is not supported by tokenplan") {
			return true
		}
	} else if poe, ok := err.RelayError.(*types.OpenAIError); ok && poe != nil {
		codeStr := strings.ToLower(fmt.Sprintf("%v", poe.Code))
		typeStr := strings.ToLower(poe.Type)
		msgStr := strings.ToLower(poe.Message)
		if strings.Contains(codeStr, "group_no_permission") ||
			strings.Contains(codeStr, "user_group_no_permission") ||
			strings.Contains(codeStr, "permission_denied") ||
			strings.Contains(typeStr, "group_no_permission") ||
			strings.Contains(typeStr, "permission_denied") ||
			strings.Contains(msgStr, "无权访问") ||
			strings.Contains(msgStr, "not supported by tokenplan") ||
			strings.Contains(msgStr, "is not supported by tokenplan") {
			return true
		}
	}
	return false
}

// IsUpstreamRelayError 判断是否为上游聚合中继站（如 One-API、New-API 等）自身代理转发失败的报错（通常返回 400/404/500/502 等）。
// 很多聚合中继站向上游发送请求失败时，会将上游状态码包装并返回 HTTP 400，带有 "来自上游渠道的报错: bad response status code 400"
// 或 "bad response status code"、"unknown provider for model"、"[上游问题]"、"[渠道出错]" 等特征。
// 这属于渠道代理侧的可用性故障，而非客户端请求本身格式错误（Bad Request），应触发换渠道重试与即时熔断。
func IsUpstreamRelayError(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	// 429 或限流/并发超限属于上游频次/并发瞬时限制，绝非中继代理网关故障，不应归为中继代理失效
	if err.StatusCode == http.StatusTooManyRequests || IsUpstreamRateLimitError(err) {
		return false
	}
	// 若为参数不支持类的 400 错误（如 thinking、reasoning_effort 等），优先由参数裁剪机制处理，不计入上游中继失效熔断
	if _, ok := IsParamNotSupportedError(err); ok {
		return false
	}
		// Responses 推理水合（解密）失败属于跨账号多轮会话密文不兼容，剥离密文重试即可自愈，不得归为中继代理失效熔断
		if IsReasoningHydrationError(err) {
			return false
		}
		// 上游 400 报 invalid codex request 或 code invalid_responses_request 属于 Codex 协议/字段校验问题，
		// 绝非中继代理网关故障，不得归为中继代理失效熔断
		if IsCodexValidationBadRequest(err) {
			return false
		}
		if IsThinkingModeHistoryError(err) {
		return true
	}
	if IsUpstreamPermissionError(err) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "来自上游渠道") ||
		strings.Contains(msg, "bad response status code") ||
		strings.Contains(msg, "bad_response_status_code") ||
		strings.Contains(msg, "上游问题") ||
		strings.Contains(msg, "渠道出错") ||
		strings.Contains(msg, "upstream request failed") ||
		strings.Contains(msg, "error from provider") ||
		strings.Contains(msg, "unknown provider for model") ||
		strings.Contains(msg, "no available channel for model") ||
		strings.Contains(msg, "no available channel") ||
		strings.Contains(msg, "无可用渠道") ||
		strings.Contains(msg, "暂无可用渠道") ||
		strings.Contains(msg, "所有渠道均不可用") ||
		strings.Contains(msg, "当前分组无可用渠道") ||
		strings.Contains(msg, "暂不可用") ||
		strings.Contains(msg, "暂时不可用") ||
		strings.Contains(msg, "disabled on this gateway") ||
		strings.Contains(msg, "model is disabled") ||
		strings.Contains(msg, "model_is_disabled") ||
		strings.Contains(msg, "model is not available") ||
		strings.Contains(msg, "model not available") ||
		strings.Contains(msg, "model is not supported") ||
		strings.Contains(msg, "model not supported") ||
		strings.Contains(msg, "channel is not available") ||
		strings.Contains(msg, "reasoning_content") ||
		strings.Contains(msg, "thinking mode must be passed back") {
		return true
	}
	if oe, ok := err.RelayError.(types.OpenAIError); ok {
		typeStr := strings.ToLower(oe.Type)
		if strings.Contains(typeStr, "upstream_error") ||
			strings.Contains(typeStr, "one_api_error") ||
			strings.Contains(typeStr, "new_api_error") {
			return true
		}
	} else if poe, ok := err.RelayError.(*types.OpenAIError); ok && poe != nil {
		typeStr := strings.ToLower(poe.Type)
		if strings.Contains(typeStr, "upstream_error") ||
			strings.Contains(typeStr, "one_api_error") ||
			strings.Contains(typeStr, "new_api_error") {
			return true
		}
	}
	return false
}

// IsUpstreamRateLimitError 判断是否为上游瞬时频次限流或并发超限错误（如 429 Too Many Requests、最多同时处理1个请求、rpm/tpm 超限等）。
// 区别于配额永久耗尽（IsUpstreamQuotaError），限流通常在短时间（几秒到几十秒）后自动解除，
// 不应触发长达数百秒的常规硬熔断，而应采用短冷却（默认 30 秒）并触发换渠道重试，避免健康渠道被过度隔离导致全池瘫痪。
func IsUpstreamRateLimitError(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	// 真正的额度/欠费耗尽优先归入 QuotaError 处理
	if IsUpstreamQuotaError(err) {
		return false
	}
	if err.StatusCode == http.StatusTooManyRequests {
		return true
	}
	msg := strings.ToLower(err.Error())
		return strings.Contains(msg, "并发请求数限制") ||
			strings.Contains(msg, "最多同时处理") ||
			strings.Contains(msg, "请求数限制") ||
			strings.Contains(msg, "总请求数限制") ||
			strings.Contains(msg, "rpm") ||
			strings.Contains(msg, "tpm") ||
			strings.Contains(msg, "rate limit") ||
			strings.Contains(msg, "rate_limit") ||
			strings.Contains(msg, "too many requests") ||
			strings.Contains(msg, "超额临时冻结") ||
			strings.Contains(msg, "负载已饱和") ||
			strings.Contains(msg, "负载已经达到上限") ||
			strings.Contains(msg, "负载已达到上限")
	}

	// IsCodexValidationBadRequest 判断是否为 Codex / Responses 接口由于请求参数格式、缺失必要字段或特定后端协议校验返回的 400 Bad Request（如 "invalid codex request"、code="invalid_responses_request"）。
	// 这属于请求体校验或单后端协议匹配问题，绝非中继代理网关故障，不得触发整渠道熔断。
	func IsCodexValidationBadRequest(err *types.NewAPIError) bool {
		if err == nil {
			return false
		}
		if err.StatusCode != http.StatusBadRequest {
			return false
		}
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "invalid codex request") ||
			strings.Contains(msg, "invalid_codex_request") ||
			strings.Contains(msg, "invalid_responses_request") {
			return true
		}
		if oe, ok := err.RelayError.(types.OpenAIError); ok {
			codeStr := strings.ToLower(fmt.Sprintf("%v", oe.Code))
			if strings.Contains(codeStr, "invalid_responses_request") || strings.Contains(codeStr, "invalid_codex_request") {
				return true
			}
		} else if poe, ok := err.RelayError.(*types.OpenAIError); ok && poe != nil {
			codeStr := strings.ToLower(fmt.Sprintf("%v", poe.Code))
			if strings.Contains(codeStr, "invalid_responses_request") || strings.Contains(codeStr, "invalid_codex_request") {
				return true
			}
		}
		return false
	}

// IsUpstreamModelUnavailableError 判断是否为「这个渠道永远不会好」的上游失效。
//
// 与 IsUpstreamRelayError 那一类瞬时故障的区别：那些换个时间/换个渠道就好，
// 熔断一小时后自动恢复即可；这里的两类不会自愈，重试再多次也是同样结果，
// 于是每一次命中都要白白消耗一整轮换渠道重试预算：
//
//  1. 模型映射失效：渠道声明的某个模型在上游已不存在（"模型不存在"、
//     "model not found"、"unknown model"、"no such model"）。生产实测一天 101 次，
//     集中在 4 个渠道上，且默认自动禁用状态码只有 401，这些渠道永远不会下线。
//  2. OAuth 凭据刷新失效："oauth2: cannot fetch token"，refresh token 已被上游
//     撤销或过期，渠道再也无法取得访问令牌。
//
// 判据刻意保守：只认上游明确表达的确定性失效，不含 5xx、限流、额度耗尽——
// 那三类都会随时间自愈，误禁用会把健康渠道踢出池子。
func IsUpstreamModelUnavailableError(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "模型不存在") ||
		strings.Contains(msg, "模型不可用") ||
		strings.Contains(msg, "model not found") ||
		strings.Contains(msg, "model_not_found") ||
		strings.Contains(msg, "unknown model") ||
		strings.Contains(msg, "no such model") ||
		strings.Contains(msg, "disabled on this gateway") ||
		strings.Contains(msg, "model is disabled") ||
		strings.Contains(msg, "model_is_disabled") ||
		strings.Contains(msg, "model is not available") ||
		strings.Contains(msg, "model not available") ||
		// "The model `x` does not exist" 这类措辞只有和 model 同时出现才算数：
		// 单独的 "does not exist" 会把 "session does not exist" 之类的无关 404 一起误伤。
		(strings.Contains(msg, "does not exist") && strings.Contains(msg, "model")) ||
		strings.Contains(msg, "cannot fetch token") {
		return true
	}
	return false
}

// IsUpstreamGatewayModelDisabled 判定上游中继网关报告的模型被禁用/暂不可用（如 "disabled on this gateway"）。
// 这属于网关侧策略或临时路由状态，触发重试与按模型熔断冷却，但不得触发不可逆的自动禁用。
func IsUpstreamGatewayModelDisabled(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "disabled on this gateway") ||
		strings.Contains(msg, "model is disabled") ||
		strings.Contains(msg, "model_is_disabled") ||
		strings.Contains(msg, "model is not available") ||
		strings.Contains(msg, "model not available")
}

// IsRoutingExhaustedError 判断「选不出渠道」这一类**路由池状态**，而不是
// 「这条凭据没有这个模型」。
//
// 两者都表现为 503、都可能带上 model 字样，但因果完全相反：
//
//   - 上游说没有该模型 → 凭据级、确定性、不会自愈 → 该禁用（IsUpstreamModelUnavailableError）
//   - 池子里此刻没有能服务该模型的渠道 → 渠道可能只是被熔断/自动禁用/过载，
//     过一会儿就回来 → **绝不能禁用**
//
// 文本上两者高度相似，所以这里必须显式识别并显式排除，不能指望
// IsUpstreamModelUnavailableError 的白名单「恰好」漏掉它 —— 那是把安全
// 建立在子串巧合上：任何人日后往那张表里补一条 "no available channel"，
// 就会把一条 85% 可用的渠道整条摘掉，而线上不会有任何报错。
//
// 生产实测 #144（cpa-kuaipao）：40 分钟成功 39 次、失败 7 次，失败全是
// "No available channel for model glm-5.3-flash under group codex (distributor)"。
// 该措辞来自 new-api 家族自己的 distributor 模板（i18n/locales/en.yaml 的
// distributor.no_available_channel），本网关与上游 CPA 用的是同一句，
// 所以文本匹配对两者都成立。
func IsRoutingExhaustedError(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	// 机器可读码优先：本网关自己产出的这一类现在带 no_available_channel。
	if err.GetErrorCode() == types.ErrorCodeNoAvailableChannel {
		return true
	}
	// 上游透传的同款措辞（上游也是 new-api 家族），此时 error_code 由上游决定、
	// 未必是本码，只能回退到文本。
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no available channel for model")
}

// BreakerScope 一次上游失效的熔断范围。
type BreakerScope int

const (
	// ScopeChannel 熔整个渠道：该渠道的全部模型一起退出轮转。
	ScopeChannel BreakerScope = iota
	// ScopeModel 只熔 (该渠道, 该模型) 这一对，同渠道其它模型照常。
	ScopeModel
)

func (s BreakerScope) String() string {
	if s == ScopeChannel {
		return "channel"
	}
	return "model"
}

// BreakerScopeOf 判定一次上游失败该熔多大范围。
//
// **v29.14 起恒为 ScopeModel：熔断一律按 (渠道, 模型) 粒度。**
//
// 决策依据是生产数据。v29.11 把「模型级不可用」判据搬到了本函数最前面，
// 但 controller/relay.go 里 5 个分支绕过了本函数直接 `TripBreaker(id, "")`
// 硬编码渠道级，其中就包括中继代理异常那条 —— 而它认的关键词里恰好包含
// `no available channel for model`。结果判据在这条路径上从未生效：
// 2026-10-02 全天 257 次熔断里 33 次、当天 17:40 之后的窗口 11 次里有 6 次，
// 都是「上游这个模型没有渠道」被判成账号级故障，把该渠道其余模型一起熔掉。
//
// 为什么敢全改：账号级失效并不靠熔断兜底。401 / 额度耗尽 / 关键词命中走的是
// **自动下线**那条路（`AutomaticDisableChannelEnabled`，已在线上为 true），
// 渠道照样整体退出轮转；熔断只负责「快速绕开」，不需要承担账号级语义。
// 代价是系统性故障收敛慢 N 步（每个模型各自累计到阈值、各自升级退避），
// 终点相同，且 `inflight` 并发计数始终在渠道级，不受影响。
func BreakerScopeOf(err *types.NewAPIError) BreakerScope {
	// 保留入参与函数形状：调用点与测试都按它取范围，将来若要恢复分级
	// 只需改这里，不必再动 controller/relay.go 的调用结构。
	_ = err
	return ScopeModel
}

// EffectiveModelName 把「错误分类」与「熔断粒度开关」合成出**实际会被熔断的
// 模型名**：返回空串表示这次熔断落在渠道级。
//
// 存在的理由是 v29.11 上线后从生产日志里读出来的一个假象：日志标签原本直接
// 用 BreakerScopeOf 的分类，于是开关关闭（per_model=false，键折叠回渠道级）
// 时仍然打出「渠道 #140 [deepseek-v4-flash] 已立即熔断」——而那一刻被熔的
// 其实是**整个渠道**。熔断范围一旦打错，凌晨对账会把「这个模型有问题」当成
// 结论去查，而真相是整个渠道挂了；反过来真按模型去处置也会打空。
// 日志是排障的一手证据，不能比实际行为更聪明。
//
// 唯一的真相源必须和 scopeKey（tracker.go）同源，否则标签和状态机各说各话。
func EffectiveModelName(channelID int, originModel string, err *types.NewAPIError) string {
	if channelID <= 0 || originModel == "" {
		return ""
	}
	if BreakerScopeOf(err) == ScopeChannel {
		return ""
	}
	// 与 scopeKey 用同一个判定：开关关闭时键折叠为渠道级，标签也必须说渠道级。
	if !GetPolicy().Resolve(channelID).Breaker.PerModelOrDefault() {
		return ""
	}
	return originModel
}

// SiblingModelsFromMapping 返回与 model 映射到**同一上游模型**的其它客户端模型名。
//
// 背景：熔断键只能用客户端请求名（读侧只知���这个名字，model_mapping 多对一
// 无法反推）。代价是上游模型 X 坏了，映射到 X 的客户端模型 A、B 各自要独立
// 累计到阈值才熔断，A、B 互不感知，收敛慢一倍。
//
// 这里在**写侧**把同源的兄弟一并熔断：读侧一行都不用改，热路径零成本。
// 判据是「映射到同一个上游名」，所以 A→X-thinking、B→X-search 这种被 adaptor
// 二次改名的兄弟不会被误并（mapping 里存的是改名前的值）。
func SiblingModelsFromMapping(mappingJSON, model string) []string {
	if model == "" || mappingJSON == "" {
		return nil
	}
	var mapping map[string]string
	if err := json.Unmarshal([]byte(mappingJSON), &mapping); err != nil || len(mapping) == 0 {
		return nil
	}
	// 未出现在映射表里的模型按原样透传，上游名就是它自己。
	upstream, ok := mapping[model]
	if !ok || upstream == "" {
		upstream = model
	}
	siblings := make([]string, 0, len(mapping))
	for client, up := range mapping {
		if client != model && up == upstream {
			siblings = append(siblings, client)
		}
	}
	sort.Strings(siblings)
	return siblings
}
