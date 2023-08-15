.PHONY: all install clean

all:
	@go build -o bin/job-serializer cmd/job-serializer/main.go

install:
	@cp bin/job-serializer /usr/local/bin/job-serializer

clean:
	@rm -f bin/job-serializer /usr/local/bin/job-serializer

