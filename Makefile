export GO111MODULE=on

.PHONY: test

build:
	go build

test:
	docker-compose up -d
	mysql -h 127.0.0.1 -pmypass testdb < ./testdata/my.sql
	go test -v ./...
