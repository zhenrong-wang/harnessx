package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	platformconfig "github.com/zhenrong-wang/harnessx/internal/platform/config"
)

type Config struct {
	GRPCAddress      string
	DefaultTimeout   time.Duration
	MaxResponseBytes int64
	Tools            []Tool `json:"tools"`
}

type Tool struct {
	Name             string            `json:"name"`
	Description      string            `json:"description"`
	InputSchema      json.RawMessage   `json:"input_schema"`
	OutputSchema     json.RawMessage   `json:"output_schema,omitempty"`
	Endpoint         string            `json:"endpoint"`
	Headers          map[string]string `json:"headers,omitempty"`
	HeadersFromEnv   map[string]string `json:"headers_from_env,omitempty"`
	Timeout          string            `json:"timeout,omitempty"`
	MaxResponseBytes int64             `json:"max_response_bytes,omitempty"`
}

func Load() (Config, error) {
	path, err := platformconfig.Required("TOOL_CORE_CONFIG")
	if err != nil {
		return Config{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read TOOL_CORE_CONFIG: %w", err)
	}
	var cfg Config
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode TOOL_CORE_CONFIG: %w", err)
	}
	cfg.GRPCAddress = platformconfig.String("TOOL_CORE_GRPC_ADDR", ":9103")
	cfg.DefaultTimeout, err = platformconfig.Duration("TOOL_CORE_DEFAULT_TIMEOUT", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	cfg.MaxResponseBytes, err = platformconfig.Int64("TOOL_CORE_MAX_RESPONSE_BYTES", 8<<20)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.DefaultTimeout <= 0 {
		return fmt.Errorf("default tool timeout must be positive")
	}
	if c.MaxResponseBytes <= 0 {
		return fmt.Errorf("max response bytes must be positive")
	}
	seen := map[string]struct{}{}
	for index, tool := range c.Tools {
		if strings.TrimSpace(tool.Name) == "" {
			return fmt.Errorf("tools[%d].name is required", index)
		}
		if _, exists := seen[tool.Name]; exists {
			return fmt.Errorf("duplicate tool %q", tool.Name)
		}
		seen[tool.Name] = struct{}{}
		endpoint, err := url.Parse(tool.Endpoint)
		if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" {
			return fmt.Errorf("tool %q endpoint must be an absolute HTTP(S) URL", tool.Name)
		}
		if len(tool.InputSchema) == 0 || !json.Valid(tool.InputSchema) {
			return fmt.Errorf("tool %q input_schema must be valid JSON", tool.Name)
		}
		if len(tool.OutputSchema) > 0 && !json.Valid(tool.OutputSchema) {
			return fmt.Errorf("tool %q output_schema must be valid JSON", tool.Name)
		}
		if tool.Timeout != "" {
			if parsed, err := time.ParseDuration(tool.Timeout); err != nil || parsed <= 0 {
				return fmt.Errorf("tool %q timeout must be a positive duration", tool.Name)
			}
		}
	}
	return nil
}
