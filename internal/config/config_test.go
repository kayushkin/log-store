package config

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/kayushkin/llm-bridge/servicesettings"
)

// The registry gives the command what envOr gave it before 2026-09-20: ":8175",
// ~/.config/log-store/events.db and http://localhost:8081 with nothing set, and
// the operator's values when they are.
func TestLoadReadsTheSameValuesItAlwaysDid(t *testing.T) {
	t.Setenv("HOME", "/home/someone")
	unset, err := NewSettingsRegistry(servicesettings.MapEnvironment(map[string]string{}))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := Load(unset), (Config{ListenAddr: ":8175", DBPath: "/home/someone/.config/log-store/events.db", LogstackURL: "http://localhost:8081"}); got != want {
		t.Errorf("Load with nothing set = %+v, want %+v", got, want)
	}
	if err := unset.CheckRequired(); err != nil {
		t.Errorf("CheckRequired with a known home directory = %v", err)
	}

	set, err := NewSettingsRegistry(servicesettings.MapEnvironment(map[string]string{
		"LOG_STORE_LISTEN_ADDR":  "127.0.0.1:9999",
		"LOG_STORE_DB_PATH":      "/srv/events.db",
		"LOG_STORE_LOGSTACK_URL": "http://localhost:8088",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := Load(set), (Config{ListenAddr: "127.0.0.1:9999", DBPath: "/srv/events.db", LogstackURL: "http://localhost:8088"}); got != want {
		t.Errorf("Load = %+v, want %+v", got, want)
	}

	// A variable set to the empty string is the same as unset, as it was when
	// envOr compared os.Getenv to "".
	empty, err := NewSettingsRegistry(servicesettings.MapEnvironment(map[string]string{"LOG_STORE_LISTEN_ADDR": "", "LOG_STORE_LOGSTACK_URL": ""}))
	if err != nil {
		t.Fatal(err)
	}
	if got := Load(empty); got.ListenAddr != ":8175" || got.LogstackURL != "http://localhost:8081" {
		t.Errorf("empty variables: %+v", got)
	}
}

// Before 2026-09-20 an unknown home directory opened /.config/log-store/events.db,
// silently. Now the setting is unset and the command refuses to start.
func TestAnUnknownHomeDirectoryLeavesTheDatabaseUnsetAndCheckRequiredSaysSo(t *testing.T) {
	t.Setenv("HOME", "")
	registry, err := NewSettingsRegistry(servicesettings.MapEnvironment(map[string]string{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.CheckRequired(); err == nil || !strings.Contains(err.Error(), "LOG_STORE_DB_PATH is unset") {
		t.Fatalf("CheckRequired = %v, want it to name LOG_STORE_DB_PATH", err)
	}
}

// LOG_STORE_SRC reaches a log-store that llm-bridge-server's dedupe canary
// starts, and is not log-store's; a misspelling of a declared name is.
func TestTheRegistryRefusesAMisspellingAndNotAVariableMeantForSomethingElse(t *testing.T) {
	for _, misspelled := range []string{"LOG_STORE_DB", "LOG_STORE_LISTEN_ADDRESS", "LOG_STORE_LOGSTACK_ADDR"} {
		_, err := NewSettingsRegistry(servicesettings.MapEnvironment(map[string]string{misspelled: "x"}))
		if err == nil || !strings.Contains(err.Error(), misspelled+" is set and log-store declares no such setting") {
			t.Errorf("NewSettingsRegistry with %s = %v, want a refusal naming it", misspelled, err)
		}
	}
	if _, err := NewSettingsRegistry(servicesettings.MapEnvironment(map[string]string{"LOG_STORE_SRC": "/somewhere/log-store", "LOG_STORE_LISTEN_ADDR": ":1", "PATH": "/bin"})); err != nil {
		t.Errorf("LOG_STORE_SRC beside a declared variable was refused: %v", err)
	}
}

// Every environment variable the service's own code reads by name is declared.
// A read that is not declared is invisible on the settings page and escapes the
// startup check.
func TestEveryEnvironmentVariableTheServiceReadsIsDeclared(t *testing.T) {
	declared := map[string]bool{}
	for _, definition := range SettingDefinitions() {
		declared[definition.EnvironmentVariable] = true
	}
	// Read by name and not settings of this service.
	notSettings := map[string]bool{}
	const repositoryRoot = "../.."

	filesRead := 0
	err := filepath.WalkDir(repositoryRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		filesRead++
		ast.Inspect(file, func(node ast.Node) bool {
			call, isCall := node.(*ast.CallExpr)
			if !isCall || len(call.Args) == 0 {
				return true
			}
			selector, isSelector := call.Fun.(*ast.SelectorExpr)
			if !isSelector {
				return true
			}
			packageName, isIdentifier := selector.X.(*ast.Ident)
			if !isIdentifier || packageName.Name != "os" || (selector.Sel.Name != "Getenv" && selector.Sel.Name != "LookupEnv") {
				return true
			}
			literal, isLiteral := call.Args[0].(*ast.BasicLit)
			if !isLiteral {
				t.Errorf("%s reads an environment variable whose name is computed, which no declaration can be held to", path)
				return true
			}
			name, _ := strconv.Unquote(literal.Value)
			if !declared[name] && !notSettings[name] {
				t.Errorf("%s reads %s, which SettingDefinitions does not declare", path, name)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The walk starts two directories above this package, which is the
	// repository root. If the package moves, the walk would read the wrong tree
	// and pass.
	if _, err := os.Stat(filepath.Join(repositoryRoot, "cmd", "log-store", "main.go")); err != nil {
		t.Fatalf("the scan starts somewhere that is not the repository root: %v", err)
	}
	if filesRead < 3 {
		t.Fatalf("the scan read %d files; it is not looking at the service", filesRead)
	}
}
