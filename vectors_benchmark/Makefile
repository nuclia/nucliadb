BATCH_SIZE:=100
INDEX_SIZE:=10000

.PHONY: build
build:
	cargo build --release --locked


.PHONY: nightly
nightly:
	cargo run --release --locked --bin vectors_benchmark
	cargo run --release --locked --bin 1m_stats -- --merge
	cargo run --release --locked --bin labels_benchmark -- --merge


.PHONY: fast-bench
fast-bench:
	cargo run --release --locked --bin vectors_benchmark -- -b $(BATCH_SIZE) -i $(INDEX_SIZE)
	cargo run --release --locked --bin 1m_stats -- -b $(BATCH_SIZE) -i $(INDEX_SIZE) --merge
	cargo run --release --locked --bin labels_benchmark -- --merge

.PHONY: samply
samply:
	samply --help > /dev/null || cargo install samply

.PHONY: samply-vector-bench
samply-vector-bench: samply
	samply record ../target/release/vectors_benchmark -b $(BATCH_SIZE) -i $(INDEX_SIZE)

.PHONY: label-bench
label-bench:
	cargo run --release --locked --bin labels_benchmark

.PHONY: real-data
real-data:
	cargo run --locked --release --bin real_data_benchmark -- --datasets datasets/dataset.json --dataset-name large
