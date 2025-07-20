package reservo

import "errors"

var (
	ErrNoResources         = errors.New("no resource provided to pool")
	ErrUnknownResourceType = errors.New("unknown resource type")
	ErrLockNotObtained     = errors.New("lock not obtained")
)
