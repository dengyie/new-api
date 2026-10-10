package relay

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dengyie/apihub/common"
	"github.com/dengyie/apihub/constant"
	relaycommon "github.com/dengyie/apihub/relay/common"
	"github.com/dengyie/apihub/relaykit/dto"
	"github.com/dengyie/apihub/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestPrepareResponsesRequestStripsReasoningOnRetry(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	info := &relaycommon.RelayInfo{
		RetryIndex: 1, // Indicates retry attempt
	}
	info.ChannelMeta = &relaycommon.ChannelMeta{
		ChannelType: constant.ChannelTypeOpenAI,
		ApiType:     constant.ChannelTypeOpenAI,
	}

	rawInput := `[
		{"role": "user", "content": "ping"},
		{"type": "reasoning", "id": "rs_123", "encrypted_content": "gAAAAAB..."},
		{"role": "assistant", "content": "pong"}
	]`
	req := &dto.OpenAIResponsesRequest{
		Model: "gpt-6-luna",
		Input: json.RawMessage(rawInput),
	}

	adaptor, body, closer, prepErr := PrepareResponsesRequest(c, info, req)
	require.Nil(t, prepErr)
	require.NotNil(t, adaptor)
	require.NotNil(t, body)
	defer closer.Close()

	bodyBytes, err := io.ReadAll(body)
	require.NoError(t, err)

	inputStr := gjson.GetBytes(bodyBytes, "input").Raw
	assert.NotContains(t, inputStr, "encrypted_content")
	assert.NotContains(t, inputStr, "rs_123")
	assert.Contains(t, inputStr, "ping")
	assert.Contains(t, inputStr, "pong")
}

func TestPrepareResponsesRequestStripsReasoningOnContextFlag(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	common.SetContextKey(c, constant.ContextKeyStripResponsesReasoning, true)

	info := &relaycommon.RelayInfo{
		RetryIndex: 0, // Initial attempt but flag is set
	}
	info.ChannelMeta = &relaycommon.ChannelMeta{
		ChannelType: constant.ChannelTypeOpenAI,
		ApiType:     constant.ChannelTypeOpenAI,
	}

	rawInput := `[
		{"role": "user", "content": "ping"},
		{"type": "reasoning", "id": "rs_123", "encrypted_content": "gAAAAAB..."},
		{"role": "assistant", "content": "pong"}
	]`
	req := &dto.OpenAIResponsesRequest{
		Model: "gpt-6-luna",
		Input: json.RawMessage(rawInput),
	}

	adaptor, body, closer, prepErr := PrepareResponsesRequest(c, info, req)
	require.Nil(t, prepErr)
	require.NotNil(t, adaptor)
	require.NotNil(t, body)
	defer closer.Close()

	bodyBytes, err := io.ReadAll(body)
	require.NoError(t, err)

	inputStr := gjson.GetBytes(bodyBytes, "input").Raw
	assert.NotContains(t, inputStr, "encrypted_content")
	assert.NotContains(t, inputStr, "rs_123")
	assert.Contains(t, inputStr, "ping")
	assert.Contains(t, inputStr, "pong")
}

func TestPrepareResponsesRequest_DriftPreemptiveStripping(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	cacheKey := "test-drift-strip-session"
	c.Set("channel_affinity_cache_key", cacheKey)
	c.Set("channel_affinity_ttl_seconds", 300)

	// 记录密文由渠道 99 生成
	originCache := service.GetReasoningOriginChannelCacheForTest()
	require.NoError(t, originCache.SetWithTTL(cacheKey, 99, 300*time.Second))
	t.Cleanup(func() {
		_, _ = originCache.DeleteMany([]string{cacheKey})
	})

	// 当前请求由渠道 154 提供服务（发生漂移）
	info := &relaycommon.RelayInfo{
		RetryIndex: 0,
	}
	info.ChannelMeta = &relaycommon.ChannelMeta{
		ChannelId:   154,
		ChannelType: constant.ChannelTypeOpenAI,
		ApiType:     constant.ChannelTypeOpenAI,
	}

	rawInput := `[
		{"role": "user", "content": "hello from turn 2"},
		{"type": "reasoning", "id": "rs_upstream99", "encrypted_content": "gAAAAAB..."},
		{"role": "assistant", "content": "turn 1 answer"}
	]`
	req := &dto.OpenAIResponsesRequest{
		Model: "gpt-6-astra",
		Input: json.RawMessage(rawInput),
	}

	adaptor, body, closer, prepErr := PrepareResponsesRequest(c, info, req)
	require.Nil(t, prepErr)
	require.NotNil(t, adaptor)
	require.NotNil(t, body)
	defer closer.Close()

	bodyBytes, err := io.ReadAll(body)
	require.NoError(t, err)

	inputStr := gjson.GetBytes(bodyBytes, "input").Raw
	assert.NotContains(t, inputStr, "encrypted_content", "跨渠道漂移时出站必须预检清洗密文")
	assert.NotContains(t, inputStr, "rs_upstream99", "跨渠道漂移时必须清洗外国 rs_* 项")
	assert.Contains(t, inputStr, "hello from turn 2", "用户消息必须完整保留")
	assert.Contains(t, inputStr, "turn 1 answer", "助手历史文本必须完整保留")
}

func TestPrepareResponsesRequest_NoDriftPreservesReasoning(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	cacheKey := "test-no-drift-session"
	c.Set("channel_affinity_cache_key", cacheKey)
	c.Set("channel_affinity_ttl_seconds", 300)

	// 记录密文由渠道 154 生成
	originCache := service.GetReasoningOriginChannelCacheForTest()
	require.NoError(t, originCache.SetWithTTL(cacheKey, 154, 300*time.Second))
	t.Cleanup(func() {
		_, _ = originCache.DeleteMany([]string{cacheKey})
	})

	// 当前请求依然命中渠道 154（同渠道亲和）
	info := &relaycommon.RelayInfo{
		RetryIndex: 0,
	}
	info.ChannelMeta = &relaycommon.ChannelMeta{
		ChannelId:   154,
		ChannelType: constant.ChannelTypeOpenAI,
		ApiType:     constant.ChannelTypeOpenAI,
	}

	rawInput := `[
		{"role": "user", "content": "hello turn 2"},
		{"type": "reasoning", "id": "rs_same154", "encrypted_content": "gAAAAAB_valid..."},
		{"role": "assistant", "content": "turn 1 answer"}
	]`
	req := &dto.OpenAIResponsesRequest{
		Model: "gpt-6-astra",
		Input: json.RawMessage(rawInput),
	}

	adaptor, body, closer, prepErr := PrepareResponsesRequest(c, info, req)
	require.Nil(t, prepErr)
	require.NotNil(t, adaptor)
	require.NotNil(t, body)
	defer closer.Close()

	bodyBytes, err := io.ReadAll(body)
	require.NoError(t, err)

	inputStr := gjson.GetBytes(bodyBytes, "input").Raw
	assert.Contains(t, inputStr, "encrypted_content", "同渠道无漂移时必须完整保留密文以最大化 Prompt Caching 命中")
	assert.Contains(t, inputStr, "rs_same154")
}
