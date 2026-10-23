#pragma once

#include "rdmapp/rdma.hpp"
#include "types.hpp"
#include "config.hpp"

#include <atomic>
#include <span>




class Shuffle {
    Row* rows;
public:
    /**
     * rows is a pointer to RDMA registered memory of size cfg.mem_size.
     * The first cfg.num_rows*sizeof(Row) bytes are filled with the local tuples before the shuffle.
     * The remaining bytes (up to cfg.mem_size) can be used by you, for data structures of your choice,
     * as well as the local tuples after the shuffle. 
     */
    Shuffle(Row* rows);

    /**
     * Shuffle the rows according to the part_id and node_id of the row's key.
     * Return the ptr to where the local rows start after shuffling, as well as the number of local rows.
     */
    std::span<Row> run();

    void inter_node_barrier(std::vector<rdma::Connection*>& connections, uint64_t& round);
};