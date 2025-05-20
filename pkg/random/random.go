package random

import (
	"math/rand"
	"time"
)

const (
	idLen = 10
	chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

func ID() string {
	seed := rand.NewSource(time.Now().UnixNano())
	random := rand.New(seed)

	result := make([]byte, idLen)
	for i := range result {
		result[i] = chars[random.Intn(len(chars))]
	}

	return string(result)
}
