package internal

import (
	"errors"
	"time"
)

const SYSTEM_PROFILE = "system.profile"
const SYSTEM_PROFILE_MAX_SIZE = 1024 * 1024 // 1MB
const MAX_RETRY = 3
const RETRY_AFTER = 10 * time.Second
const QUERY_SLOW_MS = 100 // Profiler's default value
const SLOWOPS_COLLECTION = "slowops"
const SLOWOPS_EXAMPLE_COLLECTION = "slowops.examples"
const SLOWOPS_EXPIRE_SECONDS = 7884000 // 3 months

type Config struct {
	Host     string `toml:"host"`
	Username string `toml:"username"`
	Password string `toml:"password"`
	Database string `toml:"database"`
	SSL      bool   `toml:"ssl"`
}

func (c Config) Validate() (valid bool, err error) {
	if c.Host == "" {
		return false, errors.New("host not specified")
	}

	if c.Database == "" {
		return false, errors.New("database not specified")
	}

	if c.Username != "" && c.Password == "" {
		return false, errors.New("username provided but password is not specified")
	}

	if c.Password != "" && c.Username == "" {
		return false, errors.New("password provided but username is not specified") // could just be a warning
	}

	return true, nil
}

func (c Config) UseAuth() bool {
	return c.Username != "" && c.Password != ""
}
