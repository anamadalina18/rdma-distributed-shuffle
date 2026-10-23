# Distributed Shuffle over RDMA

Distributed row shuffle between cluster nodes using one-sided RDMA verbs, written in C++17.

**Context.** This is the implementation part of *Project 3 — Distributed RDMA Shuffle* from
**Advanced Data Management Systems** (TU Darmstadt, FG Data Management, 2026). The course provided
a larger codebase: the RDMA connection helper (`CommHelper`), the thread pool, config parsing, and
the distributed test/benchmark harness. This repository contains **only the files I wrote** — the
`Shuffle` class. The course framework is not included, so the code does not build standalone.

## Architecture

### Memory layout

Everything runs out of the single RDMA-registered region handed to `Shuffle`, so no allocation
happens on the data path. The region is partitioned into fixed slices:

```
[ input rows ][ recv counts ][ send counts ][ recv offsets ][ send offsets ][ grouped rows ][ output rows ]
     n*Row      N*u32          N*u32          N*u32           N*u32           n*Row          n*Row
```

with `n = cfg.num_rows` and `N = cfg.num_nodes`. The `u32` slices sit right after the input rows and
are addressed by explicit pointer arithmetic, so every node computes the same layout independently.

### Phases

`Shuffle::run()` is a pipeline of five phases separated by barriers:

1. **Count** — the local rows are split into 4 contiguous chunks; each thread counts, into a local
   vector, how many rows belong to every destination node (`part_to_node_id(get_part_id(key))`).
   The per-thread vectors are then reduced into `nr_rows_transfer[node]`, an atomic array.
2. **Publish counts** — node `i` writes its count for destination `j` directly into `j`'s region at
   `recv_counts + i * sizeof(u32)`. After the barrier every node knows exactly how many rows it will
   receive from each peer.
3. **Offsets** — prefix sums over the received counts give the destination offset of each sender;
   the same values are pushed to the peers so senders know where to write.
4. **Group** — a second parallel pass copies local rows into contiguous, per-destination blocks using
   `rows_offsets_send_at[node]` as an atomic bump allocator: no locks, no per-row messaging.
5. **Transfer** — each block is RDMA-written into the receiver's output area in chunks of at most
   256 KB, while rows that stay local are `memcpy`'d. The result is returned as a `std::span<Row>`
   over the received region.

### Barrier

`inter_node_barrier()` is sense-reversing and built only from RDMA atomics:

- every node `fetch_add`s 1 into node 0's counter slot and waits for the completion notification;
- node 0 polls until `counter == round * num_nodes`, then writes the new round number into every
  peer's flag slot;
- each node spins on its own flag until it reaches the round number.

The round counter lives in thread-local (registered) memory, so the spin is a plain load, not an
RDMA operation.

## Limitations

- **Centralized barrier.** All arrivals target node 0 and node 0 writes every flag back, so barrier
  cost is O(N) traffic from a single node and node 0 is both the bottleneck and a single point of
  failure. A dissemination or tree barrier would scale better.
- **No timeout or failure handling.** Peers spin forever if a node dies mid-round; the code assumes
  every node participates in every round.
- **Synchronous transfers.** Each 256 KB chunk is followed by `sync_signaled(1)`, so transfer and
  computation do not overlap and the chunk size caps throughput on low-latency links.
- **Fixed parallelism.** The count and group phases hard-code 4 threads instead of using all cores
  from `Config`.
- **`uint32_t` row counts.** At most 4 Gi rows per sender per shuffle.
- **Connection locality.** `rdma::Connection` is not thread-safe, so all RDMA traffic is issued by
  the coordinating thread; only local memory work is parallel.
- **Pre-registered memory.** The whole protocol depends on the input buffer being RDMA-registered
  and large enough for both the incoming and outgoing rows.

## Performance

**Not benchmarked here.** The shuffle needs the course's distributed harness: two development VMs
connected over soft-RDMA, the `CommHelper` connection layer, and a driver that starts both sides.
The assignment explicitly warns against running it on a single machine, so no numbers are published
for this project — anything measured without real RDMA would be meaningless.

## Files

| Path | Contents |
|---|---|
| `src/shuffle.cpp` | `Shuffle::run()` (count → publish → offsets → group → transfer) and `inter_node_barrier()` |
| `src/shuffle.hpp` | `Shuffle` class declaration and buffer-offset bookkeeping |

## Notes

- Written against the course `Config` (`num_nodes`, `num_rows`, `mem_size`, `get_part_id`,
  `part_to_node_id`) and `ThreadPool`.
- Order inside a received block is not preserved, which the shuffle contract allows.
