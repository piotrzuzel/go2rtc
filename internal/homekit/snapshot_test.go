package homekit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckJPEG(t *testing.T) {
	b, err := checkJPEG([]byte{0xFF, 0xD8, 0xFF, 0xE0})
	require.NoError(t, err)
	require.Len(t, b, 4)

	// privacy mode refusal
	_, err = checkJPEG([]byte(`{"status":-70412}`))
	require.EqualError(t, err, `homekit: snapshot refused: {"status":-70412}`)

	_, err = checkJPEG(nil)
	require.Error(t, err)
}

func TestSnapshotRefusedIs(t *testing.T) {
	_, err := checkJPEG([]byte(`{"status":-70412}`))
	require.ErrorIs(t, err, errSnapshotRefused)
}
