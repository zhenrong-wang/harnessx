package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	platformconfig "github.com/zhenrong-wang/harnessx/internal/platform/config"
)

type Config struct {
	HTTPAddress       string
	HarnessTarget     string
	APIKeys           map[string]string
	AllowInsecure     bool
	ReadHeaderTimeout time.Duration
	IdleTimeout       time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddress:       platformconfig.String("GATEWAY_HTTP_ADDR", ":8080"),
		HarnessTarget:     platformconfig.String("HARNESS_CORE_GRPC_TARGET", "harness-core:9100"),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		APIKeys:           map[string]string{},
	}
	if raw := strings.TrimSpace(os.Getenv("GATEWAY_API_KEYS_JSON")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &cfg.APIKeys); err != nil {
			return Config{}, fmt.Errorf("GATEWAY_API_KEYS_JSON: %w", err)
		}
	}
	cfg.AllowInsecure = strings.EqualFold(strings.TrimSpace(os.Getenv("GATEWAY_ALLOW_INSECURE")), "true")
	if len(cfg.APIKeys) == 0 && !cfg.AllowInsecure {
		return Config{}, fmt.Errorf("GATEWAY_API_KEYS_JSON is required unless GATEWAY_ALLOW_INSECURE=true")
	}
	for key, tenant := range cfg.APIKeys {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(tenant) == "" {
			return Config{}, fmt.Errorf("gateway API keys and tenant IDs cannot be empty")
		}
	}
	return cfg, nil
}
