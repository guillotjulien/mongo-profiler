package mongo

import (
	"fmt"
	"time"

	"github.com/guillotjulien/mongo-profiler/internal/logger"
)

func withRetry(maxAttempt uint, sleep time.Duration, handler func() error) (err error) {
	for i := 0; i < int(maxAttempt); i++ {
		if i > 0 {
			logger.Warn("%v", err)
			logger.Warn("will retry after %s", sleep.String())
			time.Sleep(sleep)
			sleep *= 2 // exponential backoff
		}

		err = handler()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("failed after %v attempts. Last error: %v", maxAttempt, err)
}
