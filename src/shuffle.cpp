#include "shuffle.hpp"

#include "comm_helper.hpp"
#include "config.hpp"

#include "threadpool.hpp"

#include <vector>
#include <barrier>
#include <cstring>
#include <iostream>




Shuffle::Shuffle(Row* rows) : rows(rows) {}

std::span<Row> Shuffle::run() {
    auto& cfg = Config::get();
    auto& helper = CommHelper::get();

    std::cout << "(node " << cfg.my_id << ") start shuffle\n";

    

    // connect my node to the other nodes
    std::vector<rdma::Connection*> connections(cfg.num_nodes, nullptr);
    for (uint32_t i = 0; i < cfg.num_nodes; i++) {
        connections[i] = helper.connect_to_node(i);
    }
    std::cout << "(node " << cfg.my_id << ") connected to peers\n";

    uint64_t round = 0;
    inter_node_barrier(connections, round);

    std::vector<std::atomic<uint32_t>> nr_rows_transfer(cfg.num_nodes);
    ThreadPool pool1;
    pool1.parallel_n(4, [&](std::stop_token, int tid) {
        uint64_t rows_nr = cfg.num_rows / 4;
        uint64_t start_idx = rows_nr * tid;
        uint64_t stop_idx = rows_nr * tid + rows_nr;

        if (tid == 3) {
            stop_idx = cfg.num_rows;
        }

        std::vector<uint32_t> nr_rows_transfer_thread(cfg.num_nodes, 0);

        for (uint64_t i = start_idx; i < stop_idx; i++) {
            uint32_t part_id = cfg.get_part_id(rows[i].key);
            uint32_t node_id = cfg.part_to_node_id(part_id);

            nr_rows_transfer_thread[node_id]++;
        }

        for (uint64_t i = 0; i < cfg.num_nodes; i++) {
            nr_rows_transfer[i] += nr_rows_transfer_thread[i];
        }

    });

    pool1.join();
    std::cout << "(node " << cfg.my_id << ") counted rows per partition\n";

      uint8_t* buffer = reinterpret_cast<uint8_t *>(helper.get_local_mem());

    uint32_t* nr_rows_received = reinterpret_cast<uint32_t *>(
        buffer + cfg.num_rows * sizeof(Row)
    );
    uint32_t* nr_rows_send_buf = reinterpret_cast<uint32_t *>(
        buffer + cfg.num_rows * sizeof(Row) + 2 * cfg.num_nodes * sizeof(uint32_t)
    );

    for (uint32_t i = 0; i < cfg.num_nodes; i++) {
        if (i == cfg.my_id) {
            nr_rows_received[i] = nr_rows_transfer[i].load();
            continue;
        }

        size_t offset = cfg.num_rows * sizeof(Row) + cfg.my_id * sizeof(uint32_t);
        nr_rows_send_buf[i] = nr_rows_transfer[i].load();

        connections[i]->write(&nr_rows_send_buf[i], sizeof(uint32_t), offset, rdma::Flags().signaled());
        connections[i]->sync_signaled(1);
    }
    std::cout << "(node " << cfg.my_id << ") sent row counts\n";

    inter_node_barrier(connections, round);

    std::vector<uint64_t> rows_offsets_recv(cfg.num_nodes, 0);
    std::vector<uint64_t> rows_offsets_send(cfg.num_nodes, 0);
    std::vector<std::atomic<uint64_t>> rows_offsets_send_at(cfg.num_nodes);
    rows_offsets_send_at[0].store(0);

    for (uint32_t i = 1; i < cfg.num_nodes; i++) {
        rows_offsets_recv[i] = rows_offsets_recv[i - 1] + nr_rows_received[i - 1];
        rows_offsets_send[i] = rows_offsets_send[i - 1] + nr_rows_transfer[i - 1].load();
        rows_offsets_send_at[i].store(rows_offsets_send[i]);
    }

    uint32_t* offsets_write = reinterpret_cast<uint32_t *>(
        buffer + cfg.num_rows * sizeof(Row) + cfg.num_nodes * sizeof(uint32_t)
    );
    uint32_t* offsets_send_buf = reinterpret_cast<uint32_t *>(
        buffer + cfg.num_rows * sizeof(Row) + 3 * cfg.num_nodes * sizeof(uint32_t)
    );

    for (uint32_t i = 0; i < cfg.num_nodes; i++) {
        if (i == cfg.my_id) {
            offsets_write[i] = static_cast<uint32_t>(rows_offsets_recv[i]);
            continue;
        }

        size_t offset = cfg.num_rows * sizeof(Row) + cfg.num_nodes * sizeof(uint32_t)
                        + cfg.my_id * sizeof(uint32_t);
        offsets_send_buf[i] = static_cast<uint32_t>(rows_offsets_recv[i]);

        connections[i]->write(&offsets_send_buf[i], sizeof(uint32_t), offset, rdma::Flags().signaled());
        connections[i]->sync_signaled(1);
    }
    std::cout << "(node " << cfg.my_id << ") sent offsets\n";

    Row* group_rows = reinterpret_cast<Row*>(
        buffer + cfg.num_rows * sizeof(Row) + 4 * cfg.num_nodes * sizeof(uint32_t)
    );
    ThreadPool pool2;
    pool2.parallel_n(4, [&](std::stop_token, int tid) {
        uint64_t rows_nr = cfg.num_rows / 4;
        uint64_t start_idx = rows_nr * tid;
        uint64_t stop_idx = rows_nr * tid + rows_nr;

        if (tid == 3) {
            stop_idx = cfg.num_rows;
        }

        for (uint64_t i = start_idx; i < stop_idx; i++) {
            uint32_t part_id = cfg.get_part_id(rows[i].key);
            uint32_t node_id = cfg.part_to_node_id(part_id);

            uint64_t idx = rows_offsets_send_at[node_id].fetch_add(1, std::memory_order_relaxed);

            group_rows[idx] = rows[i];
        }
    });

    pool2.join();
    std::cout << "(node " << cfg.my_id << ") grouped local rows\n";
    inter_node_barrier(connections, round);

    Row* output_rows = reinterpret_cast<Row*>(
        buffer + 2 * cfg.num_rows * sizeof(Row) + 4 * cfg.num_nodes * sizeof(uint32_t)
    );

    for (uint32_t i = 0; i < cfg.num_nodes; i++) {
        uint32_t nr_rows = nr_rows_transfer[i].load();
        Row* ptr = &group_rows[rows_offsets_send[i]];
        size_t dim = nr_rows * sizeof(Row);

        if (nr_rows == 0) {
            continue;
        }

        if (i == cfg.my_id) {
            std::memcpy(output_rows + rows_offsets_recv[i], ptr, dim);
            continue;
        }

        size_t offset = 2 * cfg.num_rows * sizeof(Row) + 4 * cfg.num_nodes * sizeof(uint32_t)
                        + offsets_write[i] * sizeof(Row);

        constexpr size_t MAX_CHUNK = 256 * 1024;
        size_t sent = 0;
        while (sent < dim) {
            uint32_t chunkSize = static_cast<uint32_t>(std::min(dim - sent, MAX_CHUNK));
            connections[i]->write(reinterpret_cast<uint8_t*>(ptr) + sent, chunkSize, offset + sent, rdma::Flags().signaled());
            connections[i]->sync_signaled(1);
            sent += chunkSize;
        }
    }
    std::cout << "(node " << cfg.my_id << ") transferred rows\n";

    inter_node_barrier(connections, round);

    auto res = std::span<Row>(output_rows,
        rows_offsets_recv[cfg.num_nodes - 1] + nr_rows_received[cfg.num_nodes - 1]);

    for (uint32_t i = 0; i < cfg.num_nodes; i++) {
        if (!connections[i]) {
            continue;
        }

        helper.close_connection(connections[i]);
    }
    std::cout << "(node " << cfg.my_id << ") done shuffle\n";
    return res;
}

void Shuffle::inter_node_barrier(std::vector<rdma::Connection*>& connections, uint64_t& round) {
    auto& cfg = Config::get();
    round++;
    std::cout << "(node " << cfg.my_id << ") barrier " << round << " wait\n";

    auto* buffer = reinterpret_cast<char*>(CommHelper::get().get_local_mem());
    volatile uint64_t* contor = reinterpret_cast<volatile uint64_t*>(buffer + cfg.mem_size - sizeof(uint64_t));
    volatile uint64_t* flag_val = reinterpret_cast<volatile uint64_t*>(buffer + cfg.mem_size - sizeof(uint64_t) * 2);

    uint64_t* empty_val = reinterpret_cast<uint64_t*>(buffer + cfg.mem_size - sizeof(uint64_t) * 3);
    *empty_val = 0;

    // increment contor on node 0
    connections[0]->fetch_add(empty_val, 1, cfg.mem_size - sizeof(uint64_t), rdma::Flags().signaled());
    connections[0]->sync_signaled(1);

    if (cfg.my_id != 0) {
        while (*flag_val < round) {}
        std::cout << "(node " << cfg.my_id << ") barrier " << round << " done\n";
        return;
    }

    while (*contor < round * cfg.num_nodes) {}
    *empty_val = round;

    for (uint32_t i = 1; i < cfg.num_nodes; i++) {
        connections[i]->write(empty_val, sizeof(uint64_t), cfg.mem_size - sizeof(uint64_t) * 2, rdma::Flags().signaled());
        connections[i]->sync_signaled(1);
    }
    std::cout << "(node " << cfg.my_id << ") barrier " << round << " done\n";
}
