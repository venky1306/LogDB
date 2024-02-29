package main

import (
	"os"
	"path/filepath"

	diskstore "github.com/venky1306/LogDB/disk_store"
	LsmTree "github.com/venky1306/LogDB/log_structured_merge_tree"
	wal "github.com/venky1306/LogDB/wal"
	"gopkg.in/yaml.v3"
)

// ServerConfig represents the configuration for the server.
type ServerConfig struct {
	Port            string          `yaml:"port"`
	Host            string          `yaml:"host"`
	UDPPort         string          `yaml:"udp_port"`
	UDPBufferSize   int             `yaml:"udp_buffer_size"`
	DBEngineConfig  DBEngineConfig  `yaml:"db_engine,inline"`
	DiskStoreConfig DiskStoreConfig `yaml:"disk_store,inline"`
}

// DiskStoreConfig represents the configuration for the disk store.
type DiskStoreConfig struct {
	NumOfPartitions int    `yaml:"num_of_partitions"`
	Directory       string `yaml:"directory"`
}

// DBEngineConfig represents the configuration for the database engine.
type DBEngineConfig struct {
	LSMTreeMaxElementsBeforeFlush int     `yaml:"max_elements_before_flush"`
	LSMTreeCompactionFrequency    int     `yaml:"compaction_frequency_in_ms"`
	BloomFilterCapacity           int     `yaml:"bloom_capacity"`
	BloomFilterErrorRate          float64 `yaml:"bloom_error_rate"`
	WalPath                       string  `yaml:"wal_path"`
}

// ParseServerConfig parses the YAML config file.
func ParseServerConfig(filename string) (ServerConfig, error) {
	var serverConfig ServerConfig

	// Get the absolute path of the config file
	fname, err := filepath.Abs(filename)
	if err != nil {
		return serverConfig, err
	}

	// Read the content of the config file
	data, err := os.ReadFile(fname)
	if err != nil {
		return serverConfig, err
	}

	// Unmarshal YAML data into the ServerConfig struct
	err = yaml.Unmarshal(data, &serverConfig)
	if err != nil {
		return serverConfig, err
	}

	return serverConfig, nil
}

func LoadServerConfig(configFile string) (ServerConfig, error) {
	// Parse the server configuration from the specified file
	serverConfig, err := ParseServerConfig(configFile)
	if err != nil {
		return serverConfig, err
	}

	// Apply default values for any missing or unspecified configuration parameters
	if serverConfig.Port == "" {
		serverConfig.Port = DEFAULT_TCP_PORT
	}

	if serverConfig.Host == "" {
		serverConfig.Host = DEFAULT_HOST
	}

	if serverConfig.UDPPort == "" {
		serverConfig.UDPPort = DEFAULT_UDP_PORT
	}

	if serverConfig.UDPBufferSize == 0 {
		serverConfig.UDPBufferSize = DEFAULT_UDP_BUFFER_SIZE
	}

	if serverConfig.DBEngineConfig.WalPath == "" {
		serverConfig.DBEngineConfig.WalPath = wal.DEFAULT_WAL_PATH
	}

	if serverConfig.DBEngineConfig.LSMTreeMaxElementsBeforeFlush == 0 {
		serverConfig.DBEngineConfig.LSMTreeMaxElementsBeforeFlush = LsmTree.DEFAULT_MAX_ELEMENTS_BEFORE_FLUSH
	}

	if serverConfig.DBEngineConfig.LSMTreeCompactionFrequency == 0 {
		serverConfig.DBEngineConfig.LSMTreeCompactionFrequency = LsmTree.DEFAULT_COMPACTION_FREQUENCY
	}

	if serverConfig.DBEngineConfig.BloomFilterErrorRate == 0 {
		serverConfig.DBEngineConfig.BloomFilterErrorRate = LsmTree.DEFAULT_BLOOM_FILTER_ERROR_RATE
	}

	if serverConfig.DBEngineConfig.BloomFilterCapacity == 0 {
		serverConfig.DBEngineConfig.BloomFilterCapacity = LsmTree.DEFAULT_BLOOM_FILTER_CAPACITY
	}

	if serverConfig.DiskStoreConfig.NumOfPartitions == 0 {
		serverConfig.DiskStoreConfig.NumOfPartitions = diskstore.DEFAULT_NUM_OF_PARTITIONS
	}

	if serverConfig.DiskStoreConfig.Directory == "" {
		serverConfig.DiskStoreConfig.Directory = diskstore.DEFAULT_DIRECTORY
	}

	return serverConfig, nil
}
