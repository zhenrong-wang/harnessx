package config

import (
	"fmt"
	"path/filepath"

	platformconfig "github.com/zhenrong-wang/harnessx/internal/platform/config"
)

type Config struct {
	GRPCAddress string
	DatabaseURL string
	LocalRoot   string
}

func Load() (Config, error) {
	databaseURL, err := platformconfig.Required("VOLUME_CORE_DATABASE_URL")
	if err != nil {
		return Config{}, err
	}
	root, err := platformconfig.Required("VOLUME_CORE_LOCAL_ROOT")
	if err != nil {
		return Config{}, err
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return Config{}, fmt.Errorf("resolve VOLUME_CORE_LOCAL_ROOT: %w", err)
	}
	return Config{GRPCAddress: platformconfig.String("VOLUME_CORE_GRPC_ADDR", ":9105"), DatabaseURL: databaseURL, LocalRoot: absolute}, nil
}
