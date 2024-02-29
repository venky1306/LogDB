package wal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	LsmTree "github.com/venky1306/LogDB/log_structured_merge_tree"
)

// Entry represents a single entry in the WAL.
type Entry struct {
	Key    string `json:"k"`
	Value  string `json:"v"`
	Delete bool   `json:"-"`
}

// DefaultWALPath is the default path for the WAL file.
const DEFAULT_WAL_PATH = "wal.aof"

// WAL represents the Write-Ahead Log.
type WAL struct {
	filepath string
	file     *os.File
	writer   *bufio.Writer
	lock     sync.Mutex
}

// OpenOrCreateWAL opens an existing WAL file or creates a new one if it doesn't exist.
func OpenOrCreateWAL(path string) (*WAL, error) {
	var file *os.File

	// Check if the WAL file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// If it doesn't exist, create a new file
		file, err = os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("failed to create WAL file: %v", err)
		}
	} else {
		// If it exists, open the file in append mode
		file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open WAL file: %v", err)
		}
	}

	// Create a buffered writer for efficient writing
	writer := bufio.NewWriter(file)

	// Initialize and return the WAL instance
	return &WAL{
		filepath: path,
		file:     file,
		writer:   writer,
		lock:     sync.Mutex{},
	}, nil
}

// Write appends data to the Write-Ahead Log (WAL) buffer and flushes if necessary.
func (w *WAL) Write(data ...[]byte) error {
	// Lock the WAL to ensure exclusive access
	w.lock.Lock()
	defer w.lock.Unlock()

	// If the size of incoming data is more than the available buffer size,
	// flush the buffer to the file to prevent overflow
	if len(data) > w.writer.Available() {
		if err := w.flushBuffer(); err != nil {
			return fmt.Errorf("failed to flush buffer: %v", err)
		}
	}

	// Delimiter to separate entries
	delimiter := []byte("|")

	// Write each data entry to the buffer
	for _, d := range data {
		// Append delimiter to separate entries
		d = append(d, delimiter...)
		// Write data to the buffer
		_, err := w.writer.Write(d)
		if err != nil {
			return fmt.Errorf("failed to write to buffer: %v", err)
		}
	}

	// Write end byte to indicate the end of the entry
	if _, err := w.writer.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to write end byte to buffer: %v", err)
	}

	return nil
}

// flushBuffer flushes the buffer to the file.
func (w *WAL) flushBuffer() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer to file: %v", err)
	}
	return nil
}

// Persist flushes the buffer to the file and syncs the file to ensure data durability.
func (w *WAL) Persist() error {
	// Lock the WAL to ensure exclusive access
	w.lock.Lock()
	defer w.lock.Unlock()

	// Flush the buffer to write any pending data to the file
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %v", err)
	}

	// Sync the file to ensure data durability
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %v", err)
	}

	// Clear the write buffer for the next write operation
	w.writer.Reset(w.file)

	return nil
}

// ReadEntries reads entries from the WAL file and returns them as a slice of Entry.
func (w *WAL) ReadEntries() []Entry {
	// Lock the WAL to ensure exclusive access
	w.lock.Lock()
	defer w.lock.Unlock()

	// Open the WAL file for reading
	file, err := os.OpenFile(w.filepath, os.O_RDONLY, 0644)
	if err != nil {
		panic(fmt.Errorf("failed to open WAL file for reading: %v", err))
	}
	defer file.Close() // Close the file when done

	// Create a reader for reading from the file
	reader := bufio.NewReader(file)

	// Read all data from the file
	data, err := io.ReadAll(reader)
	if err != nil {
		panic(fmt.Errorf("failed to read data from WAL file: %v", err))
	}

	// Split data into individual commands
	cmds := strings.Split(string(data), "\n")

	// Initialize slice to store parsed entries
	entries := make([]Entry, 0, len(cmds))

	// Parse each command and create corresponding entry
	for _, cmd := range cmds {
		if cmd == "" {
			continue // Skip empty commands
		}

		args := strings.Split(cmd, "|")

		// Determine command type and create entry accordingly
		switch args[0] {
		case "+":
			if len(args) != 4 {
				continue // Skip malformed commands
			}
			entries = append(entries, Entry{Key: args[1], Value: args[2], Delete: false})
		case "-":
			if len(args) != 3 {
				continue // Skip malformed commands
			}
			entries = append(entries, Entry{Key: args[1], Delete: true})
		}
	}

	return entries
}

// InitDB initializes the database by replaying WAL entries onto the LSM tree.
func (w *WAL) InitDB(lsmTree *LsmTree.LSMTree) error {
	// Lock the WAL to ensure exclusive access
	w.lock.Lock()
	defer w.lock.Unlock()

	// Open the WAL file for reading
	file, err := os.OpenFile(w.filepath, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file for reading: %v", err)
	}
	defer file.Close() // Close the file when done

	// Create a reader for reading from the file
	reader := bufio.NewReader(file)

	// Read all data from the file
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read data from WAL file: %v", err)
	}

	// Split data into individual commands
	cmds := strings.Split(string(data), "\n")

	// Replay each command onto the LSM tree
	for _, cmd := range cmds {
		if cmd == "" {
			continue // Skip empty commands
		}

		args := strings.Split(cmd, "|")

		// Determine command type and apply it to the LSM tree
		switch args[0] {
		case "+":
			if len(args) != 4 {
				continue // Skip malformed commands
			}
			lsmTree.Put(args[1], args[2])
		case "-":
			if len(args) != 3 {
				continue // Skip malformed commands
			}
			lsmTree.Del(args[1])
		}
	}

	return nil
}

// Truncate truncates the WAL file, removing all entries.
func (w *WAL) Truncate() {
	// Lock the WAL to ensure exclusive access
	w.lock.Lock()
	defer w.lock.Unlock()

	// Truncate the file to remove all entries
	if err := w.file.Truncate(0); err != nil {
		fmt.Printf("failed to truncate WAL file: %v\n", err)
	}

	// Move the file pointer to the beginning of the file
	if _, err := w.file.Seek(0, 0); err != nil {
		fmt.Printf("failed to seek to beginning of WAL file: %v\n", err)
	}
}
