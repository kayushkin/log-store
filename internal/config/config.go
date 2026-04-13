package config

import "os"

type Config struct {
	ListenAddr  string
	DBPath      string
	LogstackURL string
}

func Load() Config {
	return Config{
		ListenAddr:  envOr("LOG_STORE_LISTEN_ADDR", ":8175"),
		DBPath:      envOr("LOG_STORE_DB_PATH", defaultDBPath()),
		LogstackURL: envOr("LOG_STORE_LOGSTACK_URL", "http://localhost:8081"),
	}
}

func defaultDBPath() string {
	home, _ := os.UserHomeDir()
	return home + "/.config/log-store/events.db"
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
