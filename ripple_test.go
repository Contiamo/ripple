package ripple

import "os"

var defaultDialTo = ":6379"

func testDialTo() string {
	if dialTo := os.Getenv("REDIS_DEFAULT_DIAL"); dialTo != "" {
		return dialTo
	}

	return defaultDialTo
}
