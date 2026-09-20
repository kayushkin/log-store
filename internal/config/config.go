// Package config declares every environment variable log-store reads, once,
// with llm-bridge servicesettings. The command reads its configuration from
// the registry built here, GET /settings describes the service from it, and a
// test holds every os.Getenv in the repo to it.
package config

import (
	"os"

	"github.com/kayushkin/llm-bridge/msg"
	"github.com/kayushkin/llm-bridge/servicesettings"
)

// ServiceName is this service's name in its own settings description, as
// healthcheck and the repo know it.
const ServiceName = "log-store"

// OwnedEnvironmentVariablePrefixes are the prefixes of the variables that are
// this service's alone. A set variable carrying one that SettingDefinitions
// does not declare stops the service from starting: it is a misspelling or a
// leftover, and either way someone believes it does something.
//
// They stop short of "LOG_STORE_" on purpose. llm-bridge-server's
// scripts/import-dedupe-canary.sh takes LOG_STORE_SRC from its caller's
// environment and then starts log-store as a child, which inherits it; owning
// the whole prefix would stop that child at boot over a variable that was
// never meant for it. So LOG_STORE_DB and LOG_STORE_LISTEN_ADDRESS are
// refused, and LOG_STORE_SRC is not.
var OwnedEnvironmentVariablePrefixes = []string{"LOG_STORE_LISTEN", "LOG_STORE_DB", "LOG_STORE_LOGSTACK"}

// Keys of the settings, as GET /settings names them.
const (
	SettingListenAddress = "listen_address"
	SettingDatabasePath  = "database_path"
	SettingLogstackURL   = "logstack_url"
)

// The values in force with nothing set.
const (
	DefaultListenAddress = ":8175"
	DefaultLogstackURL   = "http://localhost:8081"
)

// Config is the configuration the command runs on, read from a Registry.
type Config struct {
	ListenAddr  string
	DBPath      string
	LogstackURL string
}

// DefaultDatabasePath is the database the service opens with LOG_STORE_DB_PATH
// unset: ~/.config/log-store/events.db. It is empty when the home directory is
// unknown, which leaves the setting unset, and CheckRequired then refuses to
// start rather than open /.config/log-store/events.db.
func DefaultDatabasePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return home + "/.config/log-store/events.db"
}

// SettingDefinitions declares every environment variable this process reads.
//
// Nothing here is Editable, and nothing may become so while the service has no
// operator gate: GET /settings is as open as every other route.
func SettingDefinitions() []servicesettings.Definition {
	return []servicesettings.Definition{
		{Key: SettingListenAddress, EnvironmentVariable: "LOG_STORE_LISTEN_ADDR", Kind: msg.ServiceSettingKindWiring, ValueType: msg.ServiceSettingValueTypeString, Default: DefaultListenAddress,
			Description: "The address the HTTP server listens on. Changing it moves the service, so llm-bridge-server's LLMBRIDGE_LOG_STORE_URL must be told the new address."},
		{Key: SettingDatabasePath, EnvironmentVariable: "LOG_STORE_DB_PATH", Kind: msg.ServiceSettingKindPath, ValueType: msg.ServiceSettingValueTypeString, Default: DefaultDatabasePath(), Required: true,
			Description: "The SQLite file that holds every session event. Changing it starts the service on whatever database is there, or a new empty one; the old events stay where they were."},
		{Key: SettingLogstackURL, EnvironmentVariable: "LOG_STORE_LOGSTACK_URL", Kind: msg.ServiceSettingKindWiring, ValueType: msg.ServiceSettingValueTypeString, Default: DefaultLogstackURL,
			Description: "Where logstack answers. Result statistics are forwarded there; a wrong address drops every one of them, which the startup probe and /health report."},
	}
}

// NewSettingsRegistry reads this service's settings from environment. It fails
// on a set variable under an owned prefix that nobody declared.
func NewSettingsRegistry(environment servicesettings.Environment) (*servicesettings.Registry, error) {
	return servicesettings.New(ServiceName, OwnedEnvironmentVariablePrefixes, SettingDefinitions(), environment)
}

// Load reads the command's configuration from registry.
func Load(registry *servicesettings.Registry) Config {
	return Config{
		ListenAddr:  registry.String(SettingListenAddress),
		DBPath:      registry.String(SettingDatabasePath),
		LogstackURL: registry.String(SettingLogstackURL),
	}
}
