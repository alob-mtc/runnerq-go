package storage

import (
	"strings"
	"testing"
)

func TestBusinessKeyPairEncoding(t *testing.T) {
	a, b := BusinessIdempotencyKey("a-b", "c"), BusinessIdempotencyKey("a", "b-c")
	if a == b || strings.Contains(a, "-") || strings.Contains(b, "-") {
		t.Fatal("business key encoding aliases legacy or another pair")
	}
	for _, p := range [][2]string{{"a-b", "c"}, {"a", "b-c"}, {"", ""}, {"rq:step:foo", "日本語"}, {"/+:\"", "some-type"}} {
		legacy, typ, ok := LegacyBusinessIdempotencyKey(BusinessIdempotencyKey(p[0], p[1]))
		if !ok || typ != p[1] || legacy != p[0]+"-"+p[1] {
			t.Fatalf("round trip %v: %q %q %v", p, legacy, typ, ok)
		}
	}
}

func TestApplicationIdempotencyKey(t *testing.T) {
	for _, c := range []struct{ stored, typ, want string }{
		{BusinessIdempotencyKey("order-4411", "charge_card"), "charge_card", "order-4411"},
		{BusinessIdempotencyKey("zoë:1", "charge_card"), "charge_card", "zoë:1"},
		{BusinessIdempotencyKey("", "charge_card"), "charge_card", ""},
		// Encoded for another type: not this activity's claim; shown as stored.
		{BusinessIdempotencyKey("order-1", "email"), "charge_card", BusinessIdempotencyKey("order-1", "email")},
		{"order-4411-charge_card", "charge_card", "order-4411"}, // legacy
		{StepKeyPrefix + "root:parent:charge", "charge_card", ""},
		{"inv-1", "invoice", "inv-1"}, // written directly through the storage API
		{"", "invoice", ""},
		{"rq:key:v2:!!", "invoice", "rq:key:v2:!!"},
	} {
		if got := ApplicationIdempotencyKey(c.stored, c.typ); got != c.want {
			t.Errorf("ApplicationIdempotencyKey(%q, %q) = %q, want %q", c.stored, c.typ, got, c.want)
		}
	}
}
