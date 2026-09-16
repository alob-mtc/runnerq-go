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
