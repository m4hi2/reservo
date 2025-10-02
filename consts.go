package reservo

import "time"

const (
	PoolNamePreFix     = "reservo:pool"
	AllocatedPreFix    = "reservo:allocated"
	ResourceNamePreFix = "reservo:resource"
)

const (
	MaxWait = 30 * time.Second
)
