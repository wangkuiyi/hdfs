package hdfs

import (
	"bufio"
	"testing"
)

func testCreateAndRead(filename, content string, t *testing.T) {
	f, e := Create(filename)
	if e != nil {
		t.Errorf("Cannot create file %s : %v", filename, e)
	}
	w := bufio.NewWriter(f)
	w.WriteString(content)
	w.Flush()
	f.Close()

	f, e = Open(filename)
	if e != nil {
		t.Errorf("Cannot open file %s : %v", filename, e)
	}
	r := bufio.NewReader(f)
	s, e := r.ReadString('\n')
	if e != nil {
		t.Errorf("Failed reading string")
	}

	if s != content {
		t.Errorf("Expecting %s, got %s", content, s)
	}
	f.Close()
}

func TestLocalFS(t *testing.T) {
	const (
		kFilename = "/tmp/a.txt"
		kContent  = "Local FS, Go!\n"
	)
	testCreateAndRead(kFilename, kContent, t)
}

func TestHDFS(t *testing.T) {
	const (
		kFilename = "/tmp/a.txt"
		kContent  = "Hadoop, Go!\n"
	)
	// NOTE: here we assumed that HDFS had been deployed on
	// localhost:9000.  If this is not your case, please change.
	Connect("localhost", 9000, "")
	testCreateAndRead(kFilename, kContent, t)
}
