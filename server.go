package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	diskstore "github.com/venky1306/LogDB/disk_store"
	LsmTree "github.com/venky1306/LogDB/log_structured_merge_tree"
	wal "github.com/venky1306/LogDB/wal"
)

const DEFAULT_TCP_PORT = "8080"
const DEFAULT_UDP_PORT = "1053"
const DEFAULT_UDP_BUFFER_SIZE = 1024
const DEFAULT_HOST = "localhost"

// Server represents a server instance.
type Server struct {
	Port          string    // Port for TCP connections
	Host          string    // Host address
	DBEngine      *DBEngine // Database engine
	UDPPort       string    // Port for UDP connections
	UDPBufferSize int       // Buffer size for UDP packets
}

func NewServer(config ServerConfig) (*Server, error) {
	// Initialize LSM tree options
	lsmTreeOpts := LsmTree.LSMTreeOpts{
		MaxElementsBeforeFlush: config.DBEngineConfig.LSMTreeMaxElementsBeforeFlush,
		CompactionPeriod:       config.DBEngineConfig.LSMTreeCompactionFrequency,
		BloomFilterOpts: LsmTree.BloomFilterOpts{
			ErrorRate: config.DBEngineConfig.BloomFilterErrorRate,
			Capacity:  config.DBEngineConfig.BloomFilterCapacity,
		},
	}
	lsmTree := LsmTree.New(lsmTreeOpts)

	// Initialize disk store options
	diskStoreOpts := diskstore.DiskStoreOpts{
		NumOfPartitions: config.DiskStoreConfig.NumOfPartitions,
		Directory:       config.DiskStoreConfig.Directory,
	}
	store := diskstore.New(diskStoreOpts)

	// Open or create the WAL file
	wal, err := wal.OpenOrCreateWAL(config.DBEngineConfig.WalPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open or create WAL: %v", err)
	}

	// Create a new server instance
	return &Server{
		Port:          config.Port,
		Host:          config.Host,
		UDPPort:       config.UDPPort,
		UDPBufferSize: config.UDPBufferSize,
		DBEngine: &DBEngine{
			LsmTree: lsmTree,
			Wal:     wal,
			Store:   store,
		},
	}, nil
}

// Start starts the server.
func (s *Server) Start() error {
	// Start TCP listener
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.Host, s.Port))
	if err != nil {
		fmt.Println("Error listening:", err)
		return err
	}
	defer listener.Close()

	// Start UDP listener
	udpServer, err := net.ListenPacket("udp", fmt.Sprintf("%s:%s", s.Host, s.UDPPort))
	if err != nil {
		log.Println("Error listening UDP:", err)
		return err
	}
	defer udpServer.Close()

	// Signals for data loading and persisting cycle
	dataLoadSignal := make(chan bool, 1)
	startPersistingCycleSignal := make(chan bool, 1)

	// Goroutine to load data from disk
	go func() {
		fmt.Println("Loading data from disk")
		err := s.DBEngine.LoadFromDisk(s.DBEngine.LsmTree, s.DBEngine.Wal)
		if err != nil {
			log.Println("Error loading data from disk:", err)
			panic(err)
		}
		fmt.Println("Data loaded from disk")
		dataLoadSignal <- true
		startPersistingCycleSignal <- true
	}()

	// Wait for data loading to complete
	<-dataLoadSignal

	// Start persisting cycle
	go s.DBEngine.Store.PersistToDisk(s.DBEngine.Wal, startPersistingCycleSignal)

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("Shutting down server")
		err := s.DBEngine.Wal.Persist()
		if err != nil {
			fmt.Println("Error persisting WAL:", err)
		}
		os.Exit(0)
	}()

	// Goroutine to handle TCP connections
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("Error accepting (TCP):", err)
				continue
			}
			go handleConnection(conn, s.DBEngine.LsmTree, s.DBEngine.Wal)
		}
	}()

	// Goroutine to handle UDP packets
	go func() {
		buf := make([]byte, s.UDPBufferSize)
		for {
			n, addr, err := udpServer.ReadFrom(buf)
			if err != nil {
				fmt.Println("Error reading UDP packet:", err)
				continue
			}
			go handleUDPPacket(udpServer, buf[:n], addr, s.DBEngine.LsmTree, s.DBEngine.Wal)
		}
	}()

	// Keep the main goroutine alive until a kill signal is received
	select {}
}

// handleConnection handles incoming connections and processes commands.
func handleConnection(conn net.Conn, ltree *LsmTree.LSMTree, wal *wal.WAL) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	for scanner.Scan() {
		text := scanner.Text()

		// Split the input text into individual command arguments
		cmd := strings.Fields(text)

		if len(cmd) == 0 {
			// Invalid command if no arguments are provided
			fmt.Fprintln(writer, "Invalid command")
			writer.Flush()
			continue
		}

		switch cmd[0] {
		case "PUT":
			if len(cmd) != 3 {
				// Invalid command if PUT command does not have 3 arguments
				fmt.Fprintln(writer, "Invalid command")
				writer.Flush()
				continue
			}

			// Write to WAL
			if err := wal.Write([]byte("+"), []byte(cmd[1]), []byte(cmd[2])); err != nil {
				fmt.Fprintln(writer, "Error writing to WAL:", err)
				writer.Flush()
				continue
			}

			// Update LSM tree and send response
			ltree.Put(cmd[1], cmd[2])
			fmt.Fprintln(writer, "OK")
			writer.Flush()
		case "GET":
			// Persist WAL changes
			if err := wal.Persist(); err != nil {
				fmt.Fprintln(writer, "Error persisting WAL:", err)
				writer.Flush()
				continue
			}

			// Retrieve value from LSM tree and send response
			val, exist := ltree.Get(cmd[1])
			if !exist {
				fmt.Fprintln(writer, "Data not found")
			} else {
				fmt.Fprintln(writer, val)
			}
			writer.Flush()
		case "DEL":
			if len(cmd) != 2 {
				// Invalid command if DEL command does not have 2 arguments
				fmt.Fprintln(writer, "Invalid command")
				writer.Flush()
				continue
			}

			// Write to WAL
			if err := wal.Write([]byte("-"), []byte(cmd[1])); err != nil {
				fmt.Fprintln(writer, "Error writing to WAL:", err)
				writer.Flush()
				continue
			}

			// Delete from LSM tree and send response
			ltree.Del(cmd[1])
			fmt.Fprintln(writer, "OK")
			writer.Flush()
		default:
			// Invalid command if command is not recognized
			fmt.Fprintln(writer, "Invalid command")
			writer.Flush()
		}
	}
}

// handleUDPPacket handles UDP packets.
func handleUDPPacket(udpConn net.PacketConn, packet []byte, addr net.Addr, ltree *LsmTree.LSMTree, wal *wal.WAL) {
	response := ""

	// Convert the received packet to a string
	request := string(packet)

	// Split the request into command and arguments
	cmd := strings.Fields(request) // Use Fields instead of Split to handle multiple spaces

	if len(cmd) == 0 {
		response = "Invalid command"
	} else {
		switch cmd[0] {
		case "GET":
			if len(cmd) != 2 {
				response = "Invalid command"
				break
			}

			// Persist WAL changes
			if err := wal.Persist(); err != nil {
				response = "Error persisting WAL"
				break
			}

			// Retrieve value from LSM tree
			val, exist := ltree.Get(strings.Trim(cmd[1], "\n"))
			if !exist {
				response = "Data not found"
			} else {
				response = val
			}
		case "PUT":
			if len(cmd) != 3 {
				response = "Invalid command"
				break
			}

			// Write to WAL
			if err := wal.Write([]byte("+"), []byte(cmd[1]), []byte(cmd[2])); err != nil {
				response = "Error writing to WAL: " + err.Error()
				break
			}

			// Update LSM tree
			ltree.Put(cmd[1], cmd[2])
			response = "OK"
		case "DEL":
			if len(cmd) != 2 {
				response = "Invalid command"
				break
			}

			// Write to WAL
			if err := wal.Write([]byte("-"), []byte(cmd[1])); err != nil {
				response = "Error writing to WAL: " + err.Error()
				break
			}

			// Delete from LSM tree
			ltree.Del(cmd[1])
			response = "OK"
		default:
			response = "Invalid command"
		}
	}

	// Convert response to bytes
	responseBytes := []byte(response)

	// Send response back to the client
	_, err := udpConn.WriteTo(responseBytes, addr)
	if err != nil {
		fmt.Println("Error sending UDP response:", err)
	}
}
