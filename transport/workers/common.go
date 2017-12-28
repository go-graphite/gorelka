package workers

import (
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"net"
)

type WorkerStats struct {
	SpentTime        int64
	SentPoints       int64
	SendErrors       int64
	MarshalErrors       int64
	ConnectErrors int64
}

func NoopCompressor(w net.Conn) (io.WriteCloser, error) {
	return w, nil
}

type GzipLeveledCompressor int

func NewGzipLeveledCompressor(level int) *GzipLeveledCompressor {
	v := GzipLeveledCompressor(level)
	return &v
}

func (c GzipLeveledCompressor) NewWriter(w net.Conn) (io.WriteCloser, error) {
	return gzip.NewWriterLevel(w, int(c))
}

func SnappyCompressor(w net.Conn) (io.WriteCloser, error) {
	return snappy.NewBufferedWriter(w), nil
}