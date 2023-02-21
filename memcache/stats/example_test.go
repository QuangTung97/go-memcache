package stats

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExample_General(t *testing.T) {
	c := New("localhost:11211")
	defer func() { _ = c.Close() }()

	stats, err := c.GetGeneralStats()
	assert.Equal(t, nil, err)

	fmt.Printf("%+v\n", stats)
}

func TestExample_Slab(t *testing.T) {
	c := New("localhost:11211")
	defer func() { _ = c.Close() }()

	stats, err := c.GetSlabsStats()
	assert.Equal(t, nil, err)

	fmt.Printf("%+v\n", stats)
}

func TestExample_General__Then_Slab(t *testing.T) {
	c := New("localhost:11211")
	defer func() { _ = c.Close() }()

	general, err := c.GetGeneralStats()
	assert.Equal(t, nil, err)
	fmt.Printf("%+v\n", general)

	stats, err := c.GetSlabsStats()
	assert.Equal(t, nil, err)
	fmt.Printf("%+v\n", stats)

	stats, err = c.GetSlabsStats()
	assert.Equal(t, nil, err)
	fmt.Printf("%+v\n", stats)

	itemStats, err := c.GetSlabItemsStats()
	assert.Equal(t, nil, err)
	fmt.Printf("%+v\n", itemStats)
}
