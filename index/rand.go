package index

import (
	"math/rand"
	"time"
)

var (
	R = rand.New(rand.NewSource(time.Now().UnixNano()))
)
