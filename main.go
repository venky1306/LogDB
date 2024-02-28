package main

import (
	"flag"
	"log"

	diskstore "github.com/venky1306/LogDB/disk_store"
	LsmTree "github.com/venky1306/LogDB/log_structured_merge_tree"
	"github.com/venky1306/LogDB/wal"
)

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", "config.yaml", "Path to config file")
	flag.Parse()

	serverConfig, err := initServerConfig(configFile)

	if err != nil {
		panic(err)
	}

	log.Println(serverConfig.DBEngineConfig.LSMTreeCompactionFrequency)

	lsmTreeOpts := LsmTree.LSMTreeOpts{
		MaxElementsBeforeFlush: serverConfig.DBEngineConfig.LSMTreeMaxElementsBeforeFlush,
		CompactionPeriod:       serverConfig.DBEngineConfig.LSMTreeCompactionFrequency,
		BloomFilterOpts: LsmTree.BloomFilterOpts{
			ErrorRate: serverConfig.DBEngineConfig.BloomFilterErrorRate,
			Capacity:  serverConfig.DBEngineConfig.BloomFilterCapacity,
		},
	}
	lsmTree := LsmTree.InitNewLSMTree(lsmTreeOpts)

	diskStoreOpts := diskstore.DiskStoreOpts{
		NumOfPartitions: serverConfig.DiskStoreConfig.NumOfPartitions,
		Directory:       serverConfig.DiskStoreConfig.Directory,
	}
	store := diskstore.New(diskStoreOpts)

	server := Server{
		Port:          serverConfig.Port,
		Host:          serverConfig.Host,
		UDPPort:       serverConfig.UDPPort,
		UDPBufferSize: serverConfig.UDPBufferSize,
		DBEngine: &DBEngine{
			LsmTree: lsmTree,
			Wal:     wal.InitWAL(serverConfig.DBEngineConfig.WalPath),
			Store:   store,
		},
	}

	server.Start()
}

func initServerConfig(configFile string) (ServerConfig, error) {
	serverConfig, err := ParseServerConfig(configFile)
	if err != nil {
		return serverConfig, err
	}

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
