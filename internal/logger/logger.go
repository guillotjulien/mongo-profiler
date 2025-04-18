package logger

import (
	"fmt"
	"log"
)

var VERBOSE_LOGS = false

func Trace(format string, args ...any) {
	if !VERBOSE_LOGS {
		return
	}

	log.Printf("[TRACE] %v\n", fmt.Sprintf(format, args...))
}

func Info(format string, args ...any) {
	log.Printf("[INFO] %v\n", fmt.Sprintf(format, args...))
}

func Warn(format string, args ...any) {
	log.Printf("[WARN] %v\n", fmt.Sprintf(format, args...))
}

func Error(format string, args ...any) {
	log.Printf("[ERROR] %v\n", fmt.Sprintf(format, args...))
}

func Fatal(format string, args ...any) {
	log.Fatalf("[FATAL] %v", fmt.Sprintf(format, args...))
}
