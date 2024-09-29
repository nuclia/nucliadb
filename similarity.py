import math
import random
import time
from nucliadb_node_binding import Similarity


latencies = {}

def measure_latency(func):
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        latencies.setdefault(func.__name__, []).append(end_time - start_time)
        return result
    return wrapper


def print_latencies():
    # Print tabulated table with all latencies
    print("\nLatencies table:")
    print("{:<20} | {:>10} | {:>10} | {:>10}".format("Function", "p50 (ms)", "p90 (ms)", "p99 (ms)"))
    print("{:<20} | {:>10} | {:>10} | {:>10}".format("---------", "----------", "----------", "---------"))
    for func_name, latencies_list in sorted(latencies.items()):
        sorted_latencies = sorted(latencies_list)
        p50 = sorted_latencies[len(sorted_latencies) // 2] * 1000
        p90 = sorted_latencies[int(len(sorted_latencies) * 0.9)] * 1000
        p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)] * 1000
        print("{:<20} | {:>10.2f} | {:>10.2f} | {:>10.2f}".format(func_name, p50, p90, p99))


@measure_latency
def python_dot_product(vec1: list[float], vec2: list[float]) -> float:
    # Calculate the dot product of the two vectors
    return sum(a * b for a, b in zip(vec1, vec2))


@measure_latency
def python_cosine(vec1: list[float], vec2: list[float]) -> float:
    # Step 1: Calculate the dot product of the two vectors
    dp = python_dot_product(vec1, vec2)

    # Step 2: Calculate the magnitude of each vector
    magnitude_vec1 = math.sqrt(sum(a * a for a in vec1))
    magnitude_vec2 = math.sqrt(sum(b * b for b in vec2))

    # Step 3: Calculate the cosine similarity
    if magnitude_vec1 == 0 or magnitude_vec2 == 0:
        return 0.0  # To handle the case where one of the vectors is zero
    return dp / (magnitude_vec1 * magnitude_vec2)


@measure_latency
def rust_dot_product(vec1: list[float], vec2: list[float]) -> float:
    return Similarity().dot_product(vec1, vec2)


@measure_latency
def rust_cosine(vec1: list[float], vec2: list[float]) -> float:
    return Similarity().cosine(vec1, vec2)


def main():
    n_vectors = 500
    vector_dimension = 1024

    vectors = [[random.random() for _ in range(vector_dimension)] for _ in range(n_vectors)]

    n_calculations = 10000
    for _ in range(n_calculations):
        vec1 = random.choice(vectors)
        vec2 = random.choice(vectors)
        python_cosine(vec1, vec2)
        python_dot_product(vec1, vec2)
        rust_cosine(vec1, vec2)
        rust_dot_product(vec1, vec2)

    print_latencies()


if __name__ == "__main__":
    main()
