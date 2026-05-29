module github.com/caisc2026/harness/dst

go 1.26

require go.etcd.io/raft/v3 v3.0.0

require (
	github.com/anishathalye/porcupine v1.1.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace go.etcd.io/raft/v3 => ../../raft-upstream
