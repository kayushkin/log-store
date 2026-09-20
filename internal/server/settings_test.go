package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kayushkin/llm-bridge/msg"
	"github.com/kayushkin/llm-bridge/servicesettings"
	"github.com/kayushkin/log-store/internal/config"
)

// newTestSettings is the registry a test server describes itself from.
func newTestSettings(t *testing.T, environment map[string]string) *servicesettings.Registry {
	t.Helper()
	registry, err := config.NewSettingsRegistry(servicesettings.MapEnvironment(environment))
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestGetSettingsDescribesTheServiceAndNothingCanBeWritten(t *testing.T) {
	srv, _ := serverWithForwarder(t, "http://127.0.0.1:1")

	recorder := httptest.NewRecorder()
	srv.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/settings", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /settings = %d: %s", recorder.Code, recorder.Body)
	}
	var described msg.ServiceSettings
	if err := json.Unmarshal(recorder.Body.Bytes(), &described); err != nil {
		t.Fatal(err)
	}
	if described.Service != config.ServiceName || len(described.Settings) != len(config.SettingDefinitions()) {
		t.Fatalf("service=%q with %d settings, want %q with %d", described.Service, len(described.Settings), config.ServiceName, len(config.SettingDefinitions()))
	}
	for _, setting := range described.Settings {
		if setting.Editable {
			t.Errorf("%s is editable, and this service has no operator gate to put a write behind", setting.Key)
		}
		if setting.Kind == msg.ServiceSettingKindSecret {
			t.Errorf("%s is a secret on a service whose every route is open", setting.Key)
		}
		if setting.Key == config.SettingLogstackURL && (setting.Value != "http://127.0.0.1:1" || setting.Source != msg.ServiceSettingSourceEnvironment) {
			t.Errorf("logstack URL served as %q from %q", setting.Value, setting.Source)
		}
	}

	recorder = httptest.NewRecorder()
	srv.ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, "/settings/"+config.SettingLogstackURL, strings.NewReader(`{"value":"http://elsewhere"}`)))
	if recorder.Code == http.StatusOK {
		t.Errorf("PUT /settings/%s = 200: a write route is mounted", config.SettingLogstackURL)
	}
}

func TestNewRefusesAMissingSettingsRegistry(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New with no settings did not panic: GET /settings would fail at its first read instead of at boot")
		}
	}()
	New(nil, nil, nil)
}
