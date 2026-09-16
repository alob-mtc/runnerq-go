package storage

import (
	"encoding/base64"
	"strconv"
	"strings"
)

const businessKeyPrefix = "rq:key:v2:"

// BusinessIdempotencyKey encodes an ordered byte-string pair unambiguously.
// Standard base64 contains no '-', so v2 keys cannot equal a legacy key-type
// concatenation (which always contains '-'), or an rq:step key containing UUIDs.
func BusinessIdempotencyKey(key, activityType string) string {
	data := strconv.Itoa(len(key)) + ":" + key + activityType
	return businessKeyPrefix + base64.RawStdEncoding.EncodeToString([]byte(data))
}

// LegacyBusinessIdempotencyKey supports reading existing user-key claims during
// migration. Only accept a legacy row after verifying its activity type.
func LegacyBusinessIdempotencyKey(encoded string) (legacy, activityType string, ok bool) {
	if !strings.HasPrefix(encoded, businessKeyPrefix) {
		return "", "", false
	}
	data, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(encoded, businessKeyPrefix))
	if err != nil {
		return "", "", false
	}
	colon := strings.IndexByte(string(data), ':')
	if colon < 0 {
		return "", "", false
	}
	n, err := strconv.Atoi(string(data[:colon]))
	if err != nil || n < 0 || n > len(data)-colon-1 {
		return "", "", false
	}
	key, typ := string(data[colon+1:colon+1+n]), string(data[colon+1+n:])
	if BusinessIdempotencyKey(key, typ) != encoded {
		return "", "", false
	}
	return key + "-" + typ, typ, true
}
