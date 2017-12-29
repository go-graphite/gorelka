package workers

import (
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"net"
)

type WorkerStats struct {
	SpentTime     int64
	SentPoints    int64
	SendErrors    int64
	MarshalErrors int64
	ConnectErrors int64
	DroppedPoints int64
}

func NoopCompressor(w net.Conn) (io.ReadWriteCloser, error) {
	return w, nil
}

type GzipLeveledCompressor int

func NewGzipLeveledCompressor(level int) *GzipLeveledCompressor {
	v := GzipLeveledCompressor(level)
	return &v
}

func (c GzipLeveledCompressor) NewWriter(w net.Conn) (io.ReadWriteCloser, error) {
	writeCloser, err := gzip.NewWriterLevel(w, int(c))
	if err != nil {
		return nil, err
	}
	return struct {
		io.Reader
		io.WriteCloser
	}{
		Reader:      w,
		WriteCloser: writeCloser,
	}, nil
}

func SnappyCompressor(w net.Conn) (io.ReadWriteCloser, error) {
	return struct {
		io.Reader
		io.WriteCloser
	}{
		Reader:      w,
		WriteCloser: snappy.NewBufferedWriter(w),
	}, nil
}
