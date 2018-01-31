package workers

import (
	"io"
	"net"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
)

type WorkerStats struct {
	SpentTime     int64
	SentPoints    int64
	SentMetrics   int64
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

type Decompressor func(net.Conn) (io.Reader, error)

func NewDecompressor(typ string) Decompressor {
	switch typ {
	case "snappy":
		return func(c net.Conn) (io.Reader, error) {
			return snappy.NewReader(c), nil
		}
	case "gzip":
		return func(c net.Conn) (io.Reader, error) {
			return gzip.NewReader(c)
		}
	default:
		return func(c net.Conn) (io.Reader, error) {
			return c, nil
		}
	}
}
