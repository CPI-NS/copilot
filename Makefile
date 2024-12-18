CURR_DIR = $(shell pwd)
BIN_DIR = bin
#export GOPATH=$(CURR_DIR)
GO_BUILD = GOBIN=$(CURR_DIR)/$(BIN_DIR) go install ./src/$@

all: server master clientmain clientol eaasclient eaasepaxos

server:
	$(GO_BUILD)

client:
	$(GO_BUILD)

master:
	$(GO_BUILD)

clientmain:
	$(GO_BUILD)

clientol:
	$(GO_BUILD)

eaasclient:
	$(GO_BUILD)

eaasepaxos:
	$(GO_BUILD)

.PHONY: clean

clean:
	rm -rf bin pkg
