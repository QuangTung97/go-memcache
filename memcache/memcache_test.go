package memcache

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMemcache(t *testing.T) {
	mc, err := New("localhost:11211", 1)
	assert.Equal(t, nil, err)

	err = mc.Set(context.Background(), "KEY01", []byte("ABCD"))
	assert.Equal(t, nil, err)
}
