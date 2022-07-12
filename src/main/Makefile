GO=go

WC: ../mrapps/wc.go
	${GO} build -race -buildmode=plugin ../mrapps/wc.go

wc.so: ../mrapps/wc.go
	${GO} build -race -buildmode=plugin ../mrapps/wc.go

lab1-mrcoordinator: mrcoordinator.go
	@echo "In one or more other windows, run some workers: go run -race mrworker.go wc.so"
	${GO} run -race $^ pg-*.txt

lab1: WC lab1-mrcoordinator

lab1-worker: mrworker.go wc.so
	${GO} run -race $^

.PHONY: clean
clean:
	rm -rf mr-out*
	rm -rf *.so
	rm -rf tmp
	rm -rf mr-out-*
