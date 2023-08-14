.PHONY: all install clean

all:
	@go build -o bin/job-serializer-go cmd/job-serializer-go/main.go

install:
	@cp bin/job-serializer-go /usr/local/bin/job-serializer-go

clean:
	@rm -f bin/job-serializer-go /usr/local/bin/job-serializer-go

