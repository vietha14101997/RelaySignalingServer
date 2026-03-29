.PHONY: build build-linux run test clean

build:
	go build -o relay-server.exe ./cmd/server

build-linux:
	GOOS=linux GOARCH=amd64 go build -o relay-server ./cmd/server

run:
	go run ./cmd/server

test:
	go test ./... -v

clean:
	rm -f relay-server.exe relay-server
