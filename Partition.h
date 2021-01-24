#include <vector>

using namespace std;

#ifndef PINGCAPINTERVIEW_PARTITION_H
#define PINGCAPINTERVIEW_PARTITION_H

/**
 * Block is an ascending ordered array of elements.
 *
 */
template<typename T>
class Block {
private:
    // data is an int array of ascending order
    vector<T> data;

public:
    Block() : data() {}

    //Block(vector<T> new_data) { set_data(new_data);}

    // Insert a element to Block data
    void insert(T element);

    // Set the new_data of this Block
    void set_data(unique_ptr<vector<T>> new_data);

    // Merge this Block with another_block
    void merge(const Block<T> &another_block);

    // Return the data of this Block
    const vector<T> &get_data() const;
};

/**
 * Partitions consists of several non-overlapping data @class Block in ascending order.
 * The ascending order of @class Block is maintained in such a way that the largest element
 * in the previous block should be smaller or equal to the smallest one in the succeeding block.
 *
 */
template<typename T>
class Partition {
private:
    // partition consists of several non-overlapping data blocks in ascending order
    vector<shared_ptr<Block<T>>> blocks;
    // partition id
    const int id;
    // number of partitions merged into this partition
    int num_partitions_merged;

public:
    explicit Partition(int partition_id) : blocks(vector<shared_ptr<Block<T>>>()), id(partition_id),
                                           num_partitions_merged(1) {}

    // Return id of the partition
    int get_id();

    // Return number of partitions merged into this partition
    int get_num_partitions_merged();

    // Add a Block to Partition
    void add(shared_ptr<Block<T>> block);

    // Merge all Blocks within this Partition into one single Block
    void mergeBlocks();

    // Merge this Partition with another partition
    void merge(shared_ptr<Partition<T>> another_partition);

    // Return the blocks of this partition
    const vector<shared_ptr<Block<T>>> &get_blocks();

    // Print the contents of Blocks whthin the Partition
    void printBlocks() const;
};

#endif //PINGCAPINTERVIEW_PARTITION_H
