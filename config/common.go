// Package config reads data from ".env" file and provides data into predefined
// config structure. If u need to specify config structure do it within this package.
package config

import (
	"fmt"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"log"
)

// LoadConfig  func reads data from ".env" file. Config  uses generic to provide more flexibility by generating
// specific config structures which depends on generic type.
func LoadConfig[Config DatabaseConfig]() (*Config, error) {
	err := godotenv.Load(".env")
	if err != nil {
		return nil, fmt.Errorf("error loading .env file: %v", err)
	}
	var cfg Config
	if err = envconfig.Process("", &cfg); err != nil {
		return nil, fmt.Errorf("cannot process env : %v", err)
	} else {
		log.Println("config initialized")
	}
	return &cfg, nil
}
