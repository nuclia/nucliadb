BATCH_SIZE:=100
INDEX_SIZE:=10000

.PHONY: build
build:
	cargo build --release --locked

.PHONY: nightly
nightly:
	cargo run --release --locked --bin vectors_benchmark
	cargo run --release --locked --bin 1m_stats -- --merge


.PHONY: fast-bench
fast-bench:
	cargo run --release --locked --bin vectors_benchmark -- -b $(BATCH_SIZE) -i $(INDEX_SIZE)
	cargo run --release --locked --bin 1m_stats -- -b $(BATCH_SIZE) -i $(INDEX_SIZE) --merge

.PHONY: 1m-bench
1m-bench:
	cargo run --locked --release --bin 1m_stats


.PHONY: vector-bench
vector-bench:
	cargo run --locked --release --bin vectors_benchmark -- -b $(BATCH_SIZE) -i $(INDEX_SIZE)

.PHONY: samply
samply:
	samply --help > /dev/null || cargo install samply --locked

.PHONY: samply-vector-bench
samply-vector-bench: samply
	samply record ../target/release/vectors_benchmark -b $(BATCH_SIZE) -i $(INDEX_SIZE)

.PHONY: real-data-bench
real-data-bench:
	cargo run --locked --release --bin real_data_benchmark -- --datasets datasets/dataset.json --dataset-name large
