package service

import (
	"testing"
	"time"

	"github.com/dengyie/apihub/constant"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The retention sweep only runs on a schedule if the handler satisfies
// ScheduledSystemTaskHandler. runSystemTaskScheduler type-asserts every
// registered handler to that interface and skips anything that does not
// implement it, so a regression here silently stops all retention forever
// with no error anywhere.
var _ ScheduledSystemTaskHandler = logCleanupHandler{}

func withLogRetentionDays(t *testing.T, days int) {
	t.Helper()
	previous := constant.LogRetentionDays
	constant.LogRetentionDays = days
	t.Cleanup(func() { constant.LogRetentionDays = previous })
}

func TestLogCleanupHandlerIsScheduled(t *testing.T) {
	assert.Equal(t, 24*time.Hour, logCleanupHandler{}.Interval(),
		"保留策略按每日一次调度")
}

func TestLogCleanupHandlerEnabledFollowsRetentionSetting(t *testing.T) {
	withLogRetentionDays(t, 30)
	assert.True(t, logCleanupHandler{}.Enabled(), "保留天数为正时应自动调度")

	// 0 是关闭自动清理的开关，但不禁用手动接口，两者必须独立。
	withLogRetentionDays(t, 0)
	assert.False(t, logCleanupHandler{}.Enabled(), "LOG_RETENTION_DAYS=0 应关闭自动清理")

	withLogRetentionDays(t, -1)
	assert.False(t, logCleanupHandler{}.Enabled(), "负数保留天数应视为关闭")
}

func TestLogCleanupNewPayloadCutsAtRetentionWatermark(t *testing.T) {
	withLogRetentionDays(t, 30)

	payload, ok := logCleanupHandler{}.NewPayload().(LogCleanupPayload)
	require.True(t, ok, "NewPayload 必须返回 LogCleanupPayload")

	now := time.Now().Unix()
	expected := now - int64(30)*24*60*60
	assert.InDelta(t, expected, payload.TargetTimestamp, 5,
		"水位线应为「当前时间减保留天数」")
	assert.Positive(t, payload.BatchSize, "批量大小必须为正，否则清理循环无法推进")
}

// NewPayload 每次入队都重新计算水位线。若改成复用上一次任务的 payload，
// 第二次调度会拿着一个已经删过的旧时间戳，一行都删不掉，而且不会有任何报错。
func TestLogCleanupNewPayloadRecomputesWatermarkEveryRun(t *testing.T) {
	withLogRetentionDays(t, 30)

	first := logCleanupHandler{}.NewPayload().(LogCleanupPayload)
	time.Sleep(1100 * time.Millisecond)
	second := logCleanupHandler{}.NewPayload().(LogCleanupPayload)

	assert.Greater(t, second.TargetTimestamp, first.TargetTimestamp,
		"每次调度都必须推进水位线，否则第二次起就是空跑")
}

// 批量大小直接决定一次清理要开多少个写事务。SQLite 上每个批次是一个写事务，
// 值太小会让全量清理变成数千次串行事务并长时间占用写锁。
func TestLogCleanupBatchSizeIsTunedForLargeBacklogs(t *testing.T) {
	assert.GreaterOrEqual(t, logCleanupBatchSize, 1000,
		"批量过小会导致全量清理产生过多写事务")
}
