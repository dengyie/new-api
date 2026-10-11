package service

import (
	"strings"
	"testing"

	"github.com/dengyie/apihub/setting/operation_setting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 这组测试把词表旁注释里那几条「刻意不收」从知识变成断言。
//
// 那些注释记录的是真实的生产决策，每一条背后都有代价算过：词表是白名单，
// 命中即触发**不可逆**的整渠道禁用（v29.20 起还需 N 次佐证，但判据本身没变）。
// 注释不会编译、不会被 review 工具扫到、日后改词表时也不会有人重读 ——
// 于是「刻意排除」事实上只以散文形式存在。任何人往表里补一条措辞相近的
// 条目，都可能在毫无察觉的情况下把一条自愈的故障变成永久下线。
//
// 断言的价值恰恰在这里：它只在**有人真的补进那条措辞**时才失败，而那正是
// 需要有人停下来想一想的时刻。

// selfHealingPhrases 是判定为「会自愈」因而必须留在词表之外的措辞。
// 每条都注明理由，因为理由才是判断依据 —— 措辞随时会变，理由不会。
var selfHealingPhrases = []struct {
	phrase string
	why    string
}{
	{"No available channel for model gpt-4", "路由池状态，不是这条凭据没有这个模型"},
	{"Upstream service temporarily unavailable", "临时不可用，熔断冷却后自动回池"},
	{"rpm exhausted", "限流，熔断的活"},
	{"您已达到并发请求数限制", "并发限流，可自愈"},
	{"您已达到请求数限制：1分钟内最多请求10次", "配额限流，可自愈"},
	{"free model quota exceeded, resets tomorrow", "日配额，到期自愈"},
	{"bad response status code", "语义不唯一，可能覆盖多种成因"},
	// quota exceeded 单独列出：它**确实**在生产出现过 90 次，且看起来很像
	// 账号级失效。但它既可能是账号超额、也可能是单请求 max_tokens 超限，
	// 两种含义的后果完全相反。宁可漏判（多烧几轮重试）也不误杀健康渠道。
	{"quota exceeded", "既可能是账号超额也可能是单请求超限，语义不唯一"},
}

func TestAutomaticDisableKeywordsExcludeSelfHealingPhrases(t *testing.T) {
	for _, tc := range selfHealingPhrases {
		t.Run(tc.phrase, func(t *testing.T) {
			hit, words := AcSearch(strings.ToLower(tc.phrase), operation_setting.AutomaticDisableKeywords, true)
			assert.False(t, hit,
				"「%s」必须留在自动禁用词表之外（%s）。把它收进来就等于把一次会自愈的故障变成不可逆的整渠道下线。命中词：%v",
				tc.phrase, tc.why, words)
		})
	}
}

// TestSelfHealingPhrasesAreNotShadowedByShorterEntries 是一张更细的网。
//
// 上面那条只验证「整句不被命中」。但真正致命的情况往往是：某条**更短**的
// 词表条目是这句的子串，于是整句照样被命中，而看词表的人完全看不出来。
// 比如往表里补一条 "quota"，上面所有含 "quota …" 的限流措辞会同时中招。
func TestSelfHealingPhrasesAreNotShadowedByShorterEntries(t *testing.T) {
	for _, tc := range selfHealingPhrases {
		lower := strings.ToLower(tc.phrase)
		for _, kw := range operation_setting.AutomaticDisableKeywords {
			kwLower := strings.ToLower(kw)
			if kwLower == "" || len(kwLower) >= len(lower) {
				continue
			}
			assert.NotContains(t, lower, kwLower,
				"词表条目 %q 是「%s」的子串，会让这条本该自愈的措辞被误判为确定性失效（%s）",
				kw, tc.phrase, tc.why)
		}
	}
}

// TestAutomaticDisableKeywordsHaveNoDuplicates 纯卫生检查，但有实际后果：
// 重复条目会让 acKey（词表哈希）无谓变化、白白重建自动机，也让 review 时
// 多出一次「这两条是不是重复了」的判断。
func TestAutomaticDisableKeywordsHaveNoDuplicates(t *testing.T) {
	seen := make(map[string]int, len(operation_setting.AutomaticDisableKeywords))
	for i, kw := range operation_setting.AutomaticDisableKeywords {
		lower := strings.ToLower(kw)
		require.NotEmpty(t, lower, "第 %d 条词表条目为空", i)
		if prev, dup := seen[lower]; dup {
			t.Errorf("词表第 %d 条与第 %d 条重复（忽略大小写）：%q", i, prev, kw)
		}
		seen[lower] = i
	}
}

// TestAutomaticDisableKeywordsNoLeadingOrTrailingSpace 空白会让子串匹配
// 悄悄失配：线上报文不会带同样的空白，于是这条词形同虚设，而它在词表里
// 看起来完全正常。
func TestAutomaticDisableKeywordsNoLeadingOrTrailingSpace(t *testing.T) {
	for i, kw := range operation_setting.AutomaticDisableKeywords {
		assert.Equal(t, strings.TrimSpace(kw), kw,
			"第 %d 条词表条目 %q 带首尾空白，线上报文不会带，会导致永远匹配不上", i, kw)
	}
}

// TestAutomaticDisableKeywordsStillMatchRealMessages 是反向确认：断言不能
// 只盯着「不该收的」，还得保证「该收的没被误伤」。这里挑几条生产实测出现
// 过、且**必须人工介入才能恢复**的措辞。
func TestAutomaticDisableKeywordsStillMatchRealMessages(t *testing.T) {
	mustMatch := []string{
		"Your credit balance is too low",
		"insufficient balance",
		"Insufficient account balance",
		"insufficient_user_quota",
		"API key 额度已用完",
		"令牌因分组倍率上调已停用",
		"Invalid token",
		"无权访问",
	}
	for _, msg := range mustMatch {
		t.Run(msg, func(t *testing.T) {
			hit, words := AcSearch(strings.ToLower(msg), operation_setting.AutomaticDisableKeywords, true)
			assert.True(t, hit,
				"%q 是生产实测出现过、且必须人工才能恢复的措辞，不得从词表中消失（命中词：%v）", msg, words)
		})
	}
}

// TestNearMissKeywordPairsStayDistinct 锁住几组「差一个词」的成对条目。
//
// 这些条目之所以要各写一条，正是因为子串匹配互相覆盖不到。日后有人
// 「顺手清理」掉其中一条看起来重复的，另一条就会静默失效 —— 而线上表现
// 只是「某家上游的渠道老是不被自动下线」，很难联想到是这里。
func TestNearMissKeywordPairsStayDistinct(t *testing.T) {
	pairs := [][2]string{
		{"insufficient balance", "Insufficient account balance"},
		{"insufficient_quota", "insufficient_user_quota"},
		{"user quota not enough", "user quota is not enough"},
		{"quota exhausted", "quota_exceeded"},
		{"credit insufficient balance", "insufficient balance"},
		{"not supported by tokenplan", "is not supported by tokenplan"},
		{"Invalid token", "The security token included in the request is invalid"},
	}
	for _, pair := range pairs {
		a, b := pair[0], pair[1]
		hitA, wordsA := AcSearch(strings.ToLower(a), operation_setting.AutomaticDisableKeywords, true)
		hitB, wordsB := AcSearch(strings.ToLower(b), operation_setting.AutomaticDisableKeywords, true)
		assert.True(t, hitA, "%q 必须在词表里（命中词：%v）", a, wordsA)
		assert.True(t, hitB, "%q 必须在词表里（与 %q 只差一两个词，子串互相覆盖不到，命中词：%v）", b, a, wordsB)
	}
}
