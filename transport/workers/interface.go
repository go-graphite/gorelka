package workers

type NetWorker interface {
	TryConnect()
	IsAlive() bool
	Loop()
	GetStats() *WorkerStats
}