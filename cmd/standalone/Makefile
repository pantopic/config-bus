PROJECTNAME := icarus

DATA_DIR := /var/lib/icarus/standalone

clean-data:
	@sudo mkdir -p $(DATA_DIR)
	@sudo chmod +w $(DATA_DIR)
	@rm -rf $(DATA_DIR)/node*
	@rm -rf $(DATA_DIR)/raft*

PEERS := 127.0.0.1:17013,127.0.0.1:17023,127.0.0.1:17033

test-node-1:
	@go run *.go -d $(DATA_DIR)/node1 -g 127.0.0.1:17011 -r 127.0.0.1:17012 -a 127.0.0.1:17013 -h :19001 -p $(PEERS)

test-node-2:
	@go run *.go -d $(DATA_DIR)/node2 -g 127.0.0.1:17021 -r 127.0.0.1:17022 -a 127.0.0.1:17023 -h :19002 -p $(PEERS)

test-node-3:
	@go run *.go -d $(DATA_DIR)/node3 -g 127.0.0.1:17031 -r 127.0.0.1:17032 -a 127.0.0.1:17033 -h :19003 -p $(PEERS)
