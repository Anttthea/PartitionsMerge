#include <iostream>
#include <queue>
#include <random>
#include "concurrentqueue/concurrentqueue.h"
#include "concurrentqueue/blockingconcurrentqueue.h"
#include "Partition.h"

using namespace std;

#define NPARTITIONS 10

shared_ptr<Partition<int>>
generate_random_partition(const int num_elements, const int num_blocks, const int partition_id) {
    const int elements_per_block = round(num_elements / num_blocks) + 1;
    shared_ptr<Partition<int>> partition = make_shared<Partition<int>>(partition_id);
    for (int i = 0; i < num_blocks; i++) {
        shared_ptr<Block<int>> current_block = make_shared<Block<int>>();
        int base_val = 1;
        if (!partition->get_blocks().empty()) {
            base_val += partition->get_blocks().back()->get_data().back();
        }
        for (int j = 0; j < elements_per_block; j++) {
            int element_val = base_val + rand() % 10000;
            current_block->insert(element_val);
        }
        partition->add(move(current_block));
    }
    return partition;
}

void generate_random_partitions(vector<shared_ptr<Partition<int>>> &result) {
    const int num_partitions = NPARTITIONS; // number of partitions to be generated
    const int num_blocks_per_partition = 2; // median number of blocks per partition
    const int num_elements_per_partition = 100; // median number of elements per partition

    random_device rd;
    mt19937 gen(rd());

    normal_distribution<> num_blocks_distribution(num_blocks_per_partition, 1);
    normal_distribution<> num_elements_distribution(num_elements_per_partition, 30);

    for (int i = 0; i < num_partitions; i++) {
        const int num_elements = abs(round(num_elements_distribution(gen))) + 1;
        const int num_blocks = abs(round(num_blocks_distribution(gen))) + 1;
        result.push_back(move(generate_random_partition(num_elements, num_blocks, i)));
    }
}

void print_partitions(deque<shared_ptr<Partition<int>>> const &partitions) {
    for (int i = 0; i < partitions.size(); i++) {
        cout << "Partition " << i << " :";
        partitions[i]->printBlocks();
        cout << "\n\n";
    }
}

void print_partitions(vector<shared_ptr<Partition<int>>> const &partitions) {
    for (int i = 0; i < partitions.size(); i++) {
        cout << "Partition " << i << " :";
        partitions[i]->printBlocks();
        cout << "\n\n";
    }
}

void merge_k_partitions_single_thread(deque<shared_ptr<Partition<int>>> &partitions) {
    if (partitions.empty()) {
        return;
    }
    // Merge the partitions in pairs consecutively. Time complexity of the iterations of the while loop should be log(N).
    // N is the size of partitions.
    while (partitions.size() > 1) {
        shared_ptr<Partition<int>> current_partition = move(partitions.front());
        partitions.pop_front();
        if (!partitions.empty()) {
            shared_ptr<Partition<int>> next_partition = move(partitions.front());
            current_partition->merge(move(next_partition));
            partitions.pop_front();
            partitions.push_back(move(current_partition));
        }
    }
    partitions.front()->mergeBlocks();
}

// thread-safe
bool if_more_or_equal_then_decrement(atomic<int> &value, const int compare_value, const int decrement) {
    auto current_value = value.load(memory_order_acquire);
    while (current_value >= compare_value) {
        if (value.compare_exchange_weak(current_value, current_value - decrement, memory_order_release,
                                        memory_order_relaxed))
            return true;
    }
    return false;
}

void merge_k_partitions_producer_consumer(vector<shared_ptr<Partition<int>>> &partitions) {
    if (partitions.empty()) {
        return;
    }
    if (partitions.size() == 1) {
        partitions[0]->mergeBlocks();
        return;
    }

    moodycamel::BlockingConcurrentQueue<int> input_partitions_queue;

    const int num_producer_threads = 10;
    const int num_consumer_threads = 10;

    // Producer of initial input partitions.
    thread input_producers[num_producer_threads];
    // NPARTITIONS / num_producer_threads should not be fractional
    assert(NPARTITIONS % num_producer_threads == 0);
    int num_jobs = NPARTITIONS / num_producer_threads;
    for (int i = 0; i < num_producer_threads; i++) {
        input_producers[i] = thread([&input_partitions_queue, num_jobs, i]() {
            for (int j = 0; j < num_jobs; ++j) {
                int index = i * num_jobs + j;
                input_partitions_queue.enqueue(index);
            }
        });
    }

    shared_ptr<Partition<int>> result;
    // whether the ultimate merged single partition result is produced
    atomic<bool> has_produced_merged_partition_result(false);
    atomic<int> num_remaining_partitions(NPARTITIONS);

    // Consumers of output from input_producers.
    thread consumers[num_consumer_threads];
    for (int i = 0; i < num_consumer_threads; ++i) {
        consumers[i] = thread([&]() {
            while (!has_produced_merged_partition_result.load(memory_order_acquire)) {
                int index1, index2;
                while (if_more_or_equal_then_decrement(num_remaining_partitions, 2, 2)) {
                    input_partitions_queue.wait_dequeue(index1);
                    input_partitions_queue.wait_dequeue(index2);
                    partitions[index1]->merge(partitions[index2]);
                    const int num_partitions_merged = partitions[index1]->get_num_partitions_merged();
                    if (num_partitions_merged == NPARTITIONS) {
                        has_produced_merged_partition_result.store(true, memory_order_release);
                        result = partitions[index1];
                        return;
                    }
                    input_partitions_queue.enqueue(index1);
                    num_remaining_partitions.fetch_add(1, memory_order_release);
                }
            }
        });
    }

    // Wait for all producer threads
    for (int i = 0; i < num_producer_threads; i++) {
        input_producers[i].join();
    }

    // Wait for all consumer threads
    for (int i = 0; i != num_consumer_threads; ++i) {
        consumers[i].join();
    }

    partitions.clear();
    partitions.push_back(result);

}

int main() {
    vector<shared_ptr<Partition<int>>> partitions;
    generate_random_partitions(partitions);
    deque<shared_ptr<Partition<int>>> partitions_deque;
    for (const shared_ptr<Partition<int>> &item_ptr: partitions) {
        shared_ptr<Partition<int>> item_copy = make_shared<Partition<int>>(*item_ptr);
        partitions_deque.push_back(item_copy);
    }

    merge_k_partitions_single_thread(partitions_deque);
    cout << "Result of single-threaded partitions merge:";
    print_partitions(partitions_deque);

    merge_k_partitions_producer_consumer(partitions);
    cout << "Result of multi-threaded partitions merge:";
    print_partitions(partitions);
    return 0;
}
