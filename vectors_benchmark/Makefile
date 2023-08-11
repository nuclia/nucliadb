BATCH_SIZE:=100
INDEX_SIZE:=10000

.PHONY: build
build:
	cargo build


.PHONY: vector-bench
vector-bench:
	cargo run --release --bin vectors_benchmark -- -b $(BATCH_SIZE) -i $(INDEX_SIZE)
