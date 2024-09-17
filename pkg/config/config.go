package config

import (
	"os"
	"strconv"
)

func GetEnv(key string) string {
	return os.Getenv(key)
}

func GetEnvOrDefault(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

func GetEnvAsBool(key string, defaultValue bool) bool {
	val := GetEnvOrDefault(key, "")
	if val == "" {
		return defaultValue
	}
	boolVal, err := strconv.ParseBool(val)
	if err != nil {
		return defaultValue
	}
	return boolVal
}

func GetInt64Env(key string) int64 {
	val := GetEnvOrDefault(key, "0")
	intVal, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0
	}
	return intVal
}
