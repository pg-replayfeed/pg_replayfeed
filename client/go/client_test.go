package replayfeed

import (
	"encoding/json"
	"testing"
)

func TestParseEnvelopeFast(t *testing.T) {
	raw := `{"cursor":"0/16B1978","committed_at_ms":1727690096789,"changes":[{"feed":"orders","op":"I","key":"48921","tenant":"acme"},{"feed":"orders","op":"U","key":"48920","tenant":"acme","old_tenant":"legacy_acme"},{"feed":"orders","op":"D","key":"1234"}],"truncate":["feed_archived"]}`

	var env Envelope
	ok := parseEnvelopeFast([]byte(raw), &env)
	if !ok {
		t.Fatalf("expected fast parse to succeed")
	}

	if env.Cursor != "0/16B1978" {
		t.Errorf("cursor mismatch: got %s, want 0/16B1978", env.Cursor)
	}
	if env.CommittedAtMs != 1727690096789 {
		t.Errorf("timestamp mismatch: got %d, want 1727690096789", env.CommittedAtMs)
	}
	if len(env.Changes) != 3 {
		t.Fatalf("expected 3 changes, got %d", len(env.Changes))
	}

	c0 := env.Changes[0]
	if c0.Feed != "orders" || c0.Op != OpInsert || c0.Key != "48921" || c0.Tenant == nil || *c0.Tenant != "acme" {
		t.Errorf("c0 mismatch: %+v", c0)
	}

	c1 := env.Changes[1]
	if c1.Feed != "orders" || c1.Op != OpUpdate || c1.Key != "48920" || c1.OldTenant == nil || *c1.OldTenant != "legacy_acme" {
		t.Errorf("c1 mismatch: %+v", c1)
	}

	c2 := env.Changes[2]
	if c2.Feed != "orders" || c2.Op != OpDelete || c2.Key != "1234" || c2.Tenant != nil {
		t.Errorf("c2 mismatch: %+v", c2)
	}

	if len(env.Truncate) != 1 || env.Truncate[0] != "feed_archived" {
		t.Errorf("truncate mismatch: %+v", env.Truncate)
	}

	// Verify standard json.Unmarshal gives the exact same result
	var jsonEnv Envelope
	if err := json.Unmarshal([]byte(raw), &jsonEnv); err != nil {
		t.Fatalf("json unmarshal failed: %v", err)
	}
	if jsonEnv.Cursor != env.Cursor || jsonEnv.CommittedAtMs != env.CommittedAtMs || len(jsonEnv.Changes) != len(env.Changes) {
		t.Errorf("fast parse doesn't match json.Unmarshal: fast=%+v, unmarshal=%+v", env, jsonEnv)
	}
}

func TestParseLSN(t *testing.T) {
	lsnStr := "0/16B1978"
	lsn, err := ParseLSN(lsnStr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if FormatLSN(lsn) != lsnStr {
		t.Errorf("lsn format mismatch: got %s, want %s", FormatLSN(lsn), lsnStr)
	}
}

func TestBuildConnStrings(t *testing.T) {
	// TCP URL
	cfg := Config{
		Host:     "db.example.com",
		Port:     5432,
		User:     "app_user",
		Password: "secret/pass",
		Database: "production",
	}
	qStr, rStr, err := buildConnStrings(cfg)
	if err != nil {
		t.Fatalf("buildConnStrings error: %v", err)
	}
	if qStr == "" || rStr == "" {
		t.Fatalf("empty conn strings")
	}
	if !contains(rStr, "replication=database") {
		t.Errorf("replication conn string missing replication=database: %s", rStr)
	}

	// URI override
	cfgUri := Config{
		ConnString: "postgres://myuser:mypass@localhost:55420/mydb?sslmode=disable",
	}
	qStr2, rStr2, err := buildConnStrings(cfgUri)
	if err != nil {
		t.Fatalf("buildConnStrings error: %v", err)
	}
	if contains(qStr2, "replication=") {
		t.Errorf("query conn string should not contain replication: %s", qStr2)
	}
	if !contains(rStr2, "replication=database") {
		t.Errorf("rep conn string missing replication=database: %s", rStr2)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || (len(s) > 0 && len(substr) > 0 && stringContains(s, substr)))
}

func stringContains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
