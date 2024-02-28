package main

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type ServerConfig struct {
	Port            string          `yaml:"port"`
	Host            string          `yaml:"host"`
	UDPPort         string          `yaml:"udp_port"`
	UDPBufferSize   int             `yaml:"udp_buffer_size"`
	DBEngineConfig  DBEngineConfig  `yaml:"db_engine,inline"`
	DiskStoreConfig DiskStoreConfig `yaml:"disk_store,inline"`
}

type DiskStoreConfig struct {
	NumOfPartitions int    `yaml:"num_of_partitions"`
	Directory       string `yaml:"directory"`
}

type DBEngineConfig struct {
	LSMTreeMaxElementsBeforeFlush int     `yaml:"max_elements_before_flush"`
	LSMTreeCompactionFrequency    int     `yaml:"compaction_frequency_in_ms"`
	BloomFilterCapacity           int     `yaml:"bloom_capacity"`
	BloomFilterErrorRate          float64 `yaml:"bloom_error_rate"`

	WalPath string `yaml:"wal_path"`
}

// ParseServerConfig parses the yaml config file.
func ParseServerConfig(filename string) (ServerConfig, error) {
	var serverConfig ServerConfig
	fname, err := filepath.Abs(filename)

	if err != nil {
		return serverConfig, err
	}

	data, err := os.ReadFile(fname)
	if err != nil {
		return serverConfig, err
	}

	err = yaml.Unmarshal(data, &serverConfig)

	if err != nil {
		return serverConfig, err
	}

	return serverConfig, nil
}
