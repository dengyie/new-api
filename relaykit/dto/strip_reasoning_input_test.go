package dto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStripReasoningInput(t *testing.T) {
	t.Run("strips reasoning type and encrypted_content", func(t *testing.T) {
		inputJSON := `[
			{"role": "user", "content": "hello"},
			{"type": "reasoning", "id": "rs_123", "encrypted_content": "gAAAAAB..."},
			{"type": "message", "role": "assistant", "content": "hi there"},
			{"type": "custom", "encrypted_content": "gAAAAABxyz"}
		]`
		req := &OpenAIResponsesRequest{
			Input: json.RawMessage(inputJSON),
		}

		changed := req.StripReasoningInput()
		require.True(t, changed)

		var items []map[string]any
		err := json.Unmarshal(req.Input, &items)
		require.NoError(t, err)
		assert.Len(t, items, 2)
		assert.Equal(t, "user", items[0]["role"])
		assert.Equal(t, "assistant", items[1]["role"])
	})

	t.Run("no-op when input is string", func(t *testing.T) {
		req := &OpenAIResponsesRequest{
			Input: json.RawMessage(`"just a string"`),
		}
		changed := req.StripReasoningInput()
		assert.False(t, changed)
		assert.Equal(t, `"just a string"`, string(req.Input))
	})

		t.Run("no-op when no reasoning or encrypted_content", func(t *testing.T) {
			inputJSON := `[
				{"role": "user", "content": "hello"},
				{"role": "assistant", "content": "world"}
			]`
			req := &OpenAIResponsesRequest{
				Input: json.RawMessage(inputJSON),
			}
			changed := req.StripReasoningInput()
			assert.False(t, changed)
		})

		t.Run("strips all items when all are reasoning items", func(t *testing.T) {
			inputJSON := `[
				{"type": "reasoning", "id": "rs_1", "encrypted_content": "gAAAAAB..."},
				{"type": "reasoning", "id": "rs_2", "encrypted_content": "gAAAAAB..."}
			]`
			req := &OpenAIResponsesRequest{
				Input: json.RawMessage(inputJSON),
			}
			changed := req.StripReasoningInput()
			require.True(t, changed)
			assert.Equal(t, `[]`, string(req.Input))
		})

		t.Run("strips item with rs_ id prefix and does not strip user message mentioning encrypted_content", func(t *testing.T) {
			inputJSON := `[
				{"role": "user", "content": "How do I decrypt encrypted_content with rs_123 in Python?"},
				{"id": "rs_01fd9964e6696813016ac9d2f15b3c81", "summary": []}
			]`
			req := &OpenAIResponsesRequest{
				Input: json.RawMessage(inputJSON),
			}
			changed := req.StripReasoningInput()
			require.True(t, changed)

			var items []map[string]any
			err := json.Unmarshal(req.Input, &items)
			require.NoError(t, err)
			assert.Len(t, items, 1)
			assert.Equal(t, "user", items[0]["role"])
			assert.Equal(t, "How do I decrypt encrypted_content with rs_123 in Python?", items[0]["content"])
			})
		}

func TestEnsureCodexFields(t *testing.T) {
	t.Run("injects reasoning.encrypted_content when include is empty", func(t *testing.T) {
		req := &OpenAIResponsesRequest{}
		req.EnsureCodexFields()
		assert.JSONEq(t, `["reasoning.encrypted_content"]`, string(req.Include))
	})

	t.Run("appends reasoning.encrypted_content when include has other elements", func(t *testing.T) {
		req := &OpenAIResponsesRequest{
			Include: json.RawMessage(`["citations"]`),
		}
		req.EnsureCodexFields()
		assert.JSONEq(t, `["citations","reasoning.encrypted_content"]`, string(req.Include))
	})

	t.Run("does not duplicate when reasoning.encrypted_content already present", func(t *testing.T) {
		req := &OpenAIResponsesRequest{
			Include: json.RawMessage(`["reasoning.encrypted_content"]`),
		}
		req.EnsureCodexFields()
		assert.JSONEq(t, `["reasoning.encrypted_content"]`, string(req.Include))
	})
}

