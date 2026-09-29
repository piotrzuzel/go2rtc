package hap

import (
	"io"
	"sync"
	"testing"
)

// controllers subscribe, unsubscribe and receive events from separate
// connection goroutines (crashed with "concurrent map writes")
func TestCharacterListenersConcurrent(t *testing.T) {
	char := &Character{IID: 1, Format: "bool", Value: false}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := &discard{}
			for j := 0; j < 1000; j++ {
				char.AddListener(w)
				_ = char.NotifyListeners(nil)
				char.RemoveListener(w)
			}
		}()
	}
	wg.Wait()

	if char.listeners != nil {
		t.Fatalf("listeners left: %d", len(char.listeners))
	}
}

// discard is a distinct pointer per controller, like eventWriter
type discard struct{ _ byte }

func (*discard) Write(p []byte) (int, error) { return io.Discard.Write(p) }
