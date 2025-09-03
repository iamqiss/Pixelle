module github.com/apache/messenger/examples/go

go 1.23.0

replace github.com/apache/messenger/foreign/go => ../../foreign/go

require github.com/apache/messenger/foreign/go v0.0.0-00010101000000-000000000000

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
)
