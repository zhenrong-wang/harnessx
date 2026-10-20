package config

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	platformconfig "github.com/zhenrong-wang/harnessx/internal/platform/config"
)

type Config struct {
	GRPCAddress string
	Providers   []Provider `json:"providers"`
}

type Provider struct {
	Name             string            `json:"name"`
	Type             string            `json:"type"`
	BaseURL          string            `json:"base_url"`
	APIKeyEnv        string            `json:"api_key_env"`
	Models           []Model           `json:"models"`
	Headers          map[string]string `json:"headers,omitempty"`
	Timeout          string            `json:"timeout,omitempty"`
	MaxResponseBytes int64             `json:"max_response_bytes,omitempty"`
}

type Model struct {
	ID                  string `json:"id"`
	Streaming           bool   `json:"streaming"`
	ToolCalling         bool   `json:"tool_calling"`
	ParallelToolCalling bool   `json:"parallel_tool_calling"`
	StructuredOutput    bool   `json:"structured_output"`
	Vision              bool   `json:"vision"`
	MaxContextTokens    int64  `json:"max_context_tokens,omitempty"`
	MaxOutputTokens     int64  `json:"max_output_tokens,omitempty"`
}

func Load() (Config, error) {
	path, err := platformconfig.Required("MODEL_CORE_CONFIG")
	if err != nil {
		return Config{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read MODEL_CORE_CONFIG: %w", err)
	}
	var cfg Config
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode MODEL_CORE_CONFIG: %w", err)
	}
	cfg.GRPCAddress = platformconfig.String("MODEL_CORE_GRPC_ADDR", ":9101")
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if len(c.Providers) == 0 {
		return fmt.Errorf("at least one model provider is required")
	}
	seenProviders := map[string]struct{}{}
	for i, provider := range c.Providers {
		name := strings.TrimSpace(provider.Name)
		if name == "" {
			return fmt.Errorf("providers[%d].name is required", i)
		}
		if _, exists := seenProviders[name]; exists {
			return fmt.Errorf("duplicate provider name %q", name)
		}
		seenProviders[name] = struct{}{}
		if provider.Type != "openai-compatible" {
			return fmt.Errorf("provider %q: unsupported type %q", name, provider.Type)
		}
		if strings.TrimSpace(provider.BaseURL) == "" {
			return fmt.Errorf("provider %q: base_url is required", name)
		}
		if len(provider.Models) == 0 {
			return fmt.Errorf("provider %q: at least one model is required", name)
		}
		seenModels := map[string]struct{}{}
		for j, model := range provider.Models {
			if strings.TrimSpace(model.ID) == "" {
				return fmt.Errorf("provider %q models[%d].id is required", name, j)
			}
			if _, exists := seenModels[model.ID]; exists {
				return fmt.Errorf("provider %q: duplicate model %q", name, model.ID)
			}
			seenModels[model.ID] = struct{}{}
		}
		if provider.Timeout != "" {
			if _, err := time.ParseDuration(provider.Timeout); err != nil {
				return fmt.Errorf("provider %q timeout: %w", name, err)
			}
		}
	}
	return nil
}

func (p Provider) HasModel(model string) bool {
	return slices.ContainsFunc(p.Models, func(candidate Model) bool { return candidate.ID == model })
}
