package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nil68657/nildb/internal/layout"
)

// Environment of the child process TestCrashKeepsAcknowledgedWrites runs.
const (
	crashDirEnv   = "NILDB_CRASH_DIR"
	crashFsyncEnv = "NILDB_CRASH_FSYNC"
	crashStartEnv = "NILDB_CRASH_START"
)

var crashLastKey = []byte("\x00crash-last")

func crashConfig(dir, fsync string) Config {
	// A small memtable makes the LSM switch and flush it while the child
	// writes. RocksDB's write buffer manager counts arena blocks of every
	// family and stalls writers under a few MiB, so it keeps the budget
	// the other store tests use.
	wb := int64(256 << 10)
	if TestEngine() == EngineRocksDB {
		wb = 32 << 20
	}
	return Config{
		Dir:              dir,
		Engine:           TestEngine(),
		Fsync:            fsync,
		BlockCacheBytes:  8 << 20,
		WriteBufferBytes: wb,
	}
}

func crashRow(i int) (key, val []byte) {
	return layout.DocKey(nil, 1, fmt.Appendf(nil, "row%07d", i)), bytes.Repeat(fmt.Appendf(nil, "%07d", i), 60)
}

// TestCrashKeepsAcknowledgedWrites kills a child process that writes to the
// store in a loop (SIGKILL, no shutdown) and checks after each reopen that
// every batch the child reported committed survived, and that the store
// holds a prefix of the batches with none in part. Every engine hands a
// commit to the operating system before the write returns, so a process
// crash loses nothing acknowledged under any fsync policy; the test runs
// "always" and "no".
func TestCrashKeepsAcknowledgedWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a child process")
	}
	for _, fsync := range []string{FsyncAlways, FsyncNo} {
		t.Run(fsync, func(t *testing.T) {
			dir := t.TempDir()
			rng := rand.New(rand.NewPCG(uint64(len(fsync)), 3))
			next, acked := 0, -1
			for cycle := range 4 {
				target := next + 40 + rng.IntN(160)
				cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1")
				cmd.Env = append(os.Environ(), crashDirEnv+"="+dir, crashFsyncEnv+"="+fsync, crashStartEnv+"="+strconv.Itoa(next))
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				out, err := cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				deadline := time.AfterFunc(60*time.Second, func() { _ = cmd.Process.Kill() })
				sc := bufio.NewScanner(out)
				for sc.Scan() {
					n, ok := strings.CutPrefix(sc.Text(), "ack ")
					if !ok {
						continue
					}
					if acked, err = strconv.Atoi(n); err != nil {
						t.Fatalf("child wrote %q", sc.Text())
					}
					if acked >= target {
						break
					}
				}
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				deadline.Stop()
				if acked < target {
					t.Fatalf("cycle %d: the child stopped at batch %d before %d:\n%s", cycle, acked, target, stderr.String())
				}

				s, err := Open(crashConfig(dir, fsync))
				if err != nil {
					t.Fatalf("cycle %d: reopen after the kill: %v", cycle, err)
				}
				v, ok := mustGet(t, s, CFMeta, crashLastKey)
				if !ok || len(v) != 8 {
					t.Fatalf("cycle %d: last-batch marker = %x, %v", cycle, v, ok)
				}
				last := int(binary.BigEndian.Uint64(v))
				if last < acked {
					t.Fatalf("cycle %d: %d batches survived, but the child acknowledged %d", cycle, last+1, acked+1)
				}
				if n, ok := mustGet(t, s, CFDefault, layout.CountKey(1)); !ok || !bytes.Equal(n, layout.AppendCount(nil, int64(last+1))) {
					t.Fatalf("cycle %d: count %x, %v; want %d, from the same batches as the marker", cycle, n, ok, last+1)
				}
				for i := range last + 1 {
					k, val := crashRow(i)
					wantValue(t, s, CFDoc, k, val)
				}
				after, _ := crashRow(last + 1)
				wantAbsent(t, s, CFDoc, after)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				t.Logf("cycle %d: killed after batch %d was acknowledged; %d survived", cycle, acked, last+1)
				next = last + 1
			}
		})
	}
}

// TestCrashChild is the writer TestCrashKeepsAcknowledgedWrites kills. Each
// batch writes one row, bumps the count by merge and records its own
// number, and "ack N" goes to stdout once batch N committed.
func TestCrashChild(t *testing.T) {
	dir := os.Getenv(crashDirEnv)
	if dir == "" {
		t.Skip("runs only as the child of TestCrashKeepsAcknowledgedWrites")
	}
	start, err := strconv.Atoi(os.Getenv(crashStartEnv))
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(crashConfig(dir, os.Getenv(crashFsyncEnv)))
	if err != nil {
		t.Fatal(err)
	}
	for i := start; ; i++ {
		k, v := crashRow(i)
		txn := s.Begin()
		txn.Put(CFDoc, k, v)
		txn.Merge(CFDefault, layout.CountKey(1), layout.AppendCount(nil, 1))
		txn.Put(CFMeta, crashLastKey, binary.BigEndian.AppendUint64(nil, uint64(i)))
		if err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("ack %d\n", i)
	}
}
