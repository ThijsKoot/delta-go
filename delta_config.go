package delta

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	CONFIG_CHECKPOINT_INTERVAL = Config{
		Key:     "checkpointInterval",
		Default: "10",
	}

	CONFIG_TOMBSTONE_RETENTION = Config{
		Key:     "deletedFileRetentionDuration",
		Default: "interval 1 week",
	}

	CONFIG_LOG_RETENTION = Config{
		Key:     "logRetentionDuration",
		Default: "interval 30 day",
	}

	CONFIG_ENABLE_EXPIRED_LOG_CLEANUP = Config{
		Key:     "enableExpiredLogCleanup",
		Default: "true",
	}
)

// Delta table's `metadata.configuration` entry.
type Config struct {
	// The configuration name
	Key string
	// The default value if `key` is not set in `metadata.configuration`.
	Default string
}

func (d *Config) GetRawFromMetadata(metadata *TableMetadata) string {
	v, ok := metadata.Configuration[d.Key]
	if ok {
		return v
	}
	return d.Default
}

func (d *Config) GetIntFromMetadata(metadata *TableMetadata) (int32, error) {
	v := d.GetRawFromMetadata(metadata)
	c, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("unable to parse int32 from metadata: %w", err)
	}
	return int32(c), nil
}

func (d *Config) GetLongFromMetadata(metadata *TableMetadata) (int64, error) {
	v := d.GetRawFromMetadata(metadata)
	c, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unable to parse int32 from metadata: %w", err)
	}
	return c, nil
}

func (d *Config) GetBoolFromMetadata(metadata *TableMetadata) (bool, error) {
	v := d.GetRawFromMetadata(metadata)
	c, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("unable to parse int32 from metadata: %w", err)
	}
	return c, nil
}

func (d *Config) GetDurationFromMetadata(metadata *TableMetadata) (time.Duration, error) {
	v := d.GetRawFromMetadata(metadata)

	words := strings.Split(v, " ")
	if len(words) != 3 {
		return 0, fmt.Errorf("invalid input for GetDurationFromMetadata: %s", v)
	}

	quantity, err := strconv.Atoi(words[1])
	if err != nil {
		return 0, fmt.Errorf("unable to parse int from metadata: %w", err)
	}

	var unit time.Duration
	switch words[2] {
	case "nanosecond":
		unit = time.Nanosecond
	case "microsecond":
		unit = time.Microsecond
	case "millisecond":
		unit = time.Millisecond
	case "second":
		unit = time.Second
	case "minute":
		unit = time.Minute
	case "hour":
		unit = time.Hour
	case "day":
		unit = 24 * time.Hour
	case "week":
		unit = 24 * 7 * time.Hour
	default:
		return 0, fmt.Errorf("unknown time unit: %s", words[1])
	}

	return unit * time.Duration(quantity), nil
}

func (d *Config) Apply(metadata *TableMetadata, value string) {
	metadata.Configuration[d.Key] = value
}
