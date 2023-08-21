BATCH_SIZE:=100
INDEX_SIZE:=10000

.PHONY: build
build:
	cargo build --release --frozen

.PHONY: vector-bench
vector-bench:
	cargo run --release --bin vectors_benchmark -- -b $(BATCH_SIZE) -i $(INDEX_SIZE)

.PHONY: samply
samply:
	samply --help > /dev/null || cargo install samply

.PHONY: samply-vector-bench
samply-vector-bench: samply
	samply record ../target/release/vectors_benchmark -b $(BATCH_SIZE) -i $(INDEX_SIZE)

.PHONY: label-bench
label-bench:
	cargo run --release --bin labels_benchmark




