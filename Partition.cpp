//
// Created by Anttthea on 1/19/21.
//

#include "Partition.h"
#include <iostream>

template<class T>
void Block<T>::insert(T element) {
    data.insert(lower_bound(data.begin(), data.end(), element), element);
}

template void Block<int>::insert(int element);

template<class T>
void Block<T>::set_data(unique_ptr<vector<T>> new_data) {
    if (!new_data) {
        throw invalid_argument("nullptr data is set for Block!");
    }
    data = *new_data.release();
}

template void Block<int>::set_data(unique_ptr<vector<int>> new_data);

template<class T>
void Block<T>::merge(const Block<T> &another_block) {
    const vector<T> &another_block_data = another_block.get_data();
    if (!data.empty() && !another_block_data.empty()) {
        assert(another_block_data.front() > data.back());
    }
    data.insert(data.end(), another_block_data.begin(),
                another_block_data.end());
}

template void Block<int>::merge(const Block<int> &another_block);

template<class T>
const vector<T> &Block<T>::get_data() const {
    return data;
}

template const vector<int> &Block<int>::get_data() const;

template<class T>
int Partition<T>::get_id() {
    return id;
}

template int Partition<int>::get_id();

template<class T>
int Partition<T>::get_num_partitions_merged() {
    return num_partitions_merged;
}

template int Partition<int>::get_num_partitions_merged();

template<class T>
void Partition<T>::add(shared_ptr<Block<T>> block) {
    assert(block);
    assert(!block->get_data().empty());
    if (!blocks.empty()) {
        assert(block->get_data().front() > blocks.back()->get_data().back());
    }
    blocks.push_back(move(block));
}

template void Partition<int>::add(shared_ptr<Block<int>> block);

template<class T>
void Partition<T>::mergeBlocks() {
    if (blocks.size() <= 1) {
        return;
    }
    shared_ptr<Block<T>> merged_block = make_shared<Block<T>>();
    for (int i = 0; i < blocks.size(); i++) {
        merged_block->merge(*blocks[i]);
    }

    blocks.clear();
    blocks.push_back(move(merged_block));
}

template void Partition<int>::mergeBlocks();

template<class T>
void Partition<T>::merge(shared_ptr<Partition<T>> another_partition) {
    assert(another_partition);
    assert(!blocks.empty());
    assert(!another_partition->get_blocks().empty());
    // Merge blocks within the partition
    if (blocks.size() > 1) {
        mergeBlocks();
    }
    if (another_partition->get_blocks().size() > 1) {
        another_partition->mergeBlocks();
    }

    const vector<T> &this_data = blocks.front()->get_data();
    const vector<T> &another_data = another_partition->get_blocks().front()->get_data();
    auto this_begin_iterator = this_data.begin();
    auto this_end_iterator = this_data.end();
    auto another_begin_iterator = another_data.begin();
    auto another_end_iterator = another_data.end();
    unique_ptr<vector<T>> result_block_data = make_unique<vector<T>>();

    // Merge 2 sorted data array in a manner similar to quick sort. This optimization
    // finds the pivot point by binary search and takes turns to concatenate them.
    while (this_begin_iterator != this_end_iterator && another_begin_iterator != another_end_iterator) {
        if (*this_begin_iterator <= *another_begin_iterator) {
            auto upper_bound_iterator = upper_bound(this_begin_iterator, this_end_iterator, *another_begin_iterator);
            result_block_data->insert(result_block_data->end(), this_begin_iterator, upper_bound_iterator);
            this_begin_iterator = upper_bound_iterator;
        } else {
            auto upper_bound_iterator = upper_bound(another_begin_iterator, another_end_iterator, *this_begin_iterator);
            result_block_data->insert(result_block_data->end(), another_begin_iterator, upper_bound_iterator);
            another_begin_iterator = upper_bound_iterator;
        }
    }
    if (this_begin_iterator != this_end_iterator) {
        result_block_data->insert(result_block_data->end(), this_begin_iterator, this_end_iterator);
    }
    if (another_begin_iterator != another_end_iterator) {
        result_block_data->insert(result_block_data->end(), another_begin_iterator, another_end_iterator);
    }
    num_partitions_merged += another_partition->get_num_partitions_merged();
    shared_ptr<Block<T>> result_block = make_shared<Block<T>>();
    result_block->set_data(move(result_block_data));
    blocks.clear();
    blocks.push_back(move(result_block));
    another_partition.reset();
}

template void Partition<int>::merge(shared_ptr<Partition<int>> another_partition);

template<class T>
const vector<shared_ptr<Block<T>>> &Partition<T>::get_blocks() {
    return blocks;
}

template const vector<shared_ptr<Block<int>>> &Partition<int>::get_blocks();

template<class T>
void Partition<T>::printBlocks() const {
    int i = 0;
    for (auto &block : blocks) {
        cout << "\nBlock " << i << " :\n";
        for (int iter : block->get_data())
            cout << iter << ' ';
        i++;
    }
}

template void Partition<int>::printBlocks() const;