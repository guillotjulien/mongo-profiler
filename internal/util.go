package internal

import (
	"fmt"
	"time"
)

func WithRetry(maxAttempt uint, sleep time.Duration, handler func() error) (err error) {
	for i := 0; i < int(maxAttempt); i++ {
		if i > 0 {
			Warn("will retry in %s after error %v", sleep.String(), err)
			time.Sleep(sleep)
			sleep *= 2 // exponential backoff
		}

		err = handler()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("failed after %v attemps. Last error: %w", maxAttempt, err)
}
