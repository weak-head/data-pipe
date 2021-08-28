package agent

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAgent(t *testing.T) {
	require.Equal(t, sum(3, 4), 7)
}
