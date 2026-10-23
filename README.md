# Distributed Shuffle over RDMA

Distributed row shuffle between cluster nodes using one-sided RDMA verbs, written in C++17.

**Context.** This is the implementation part of *Project 3 — Distributed RDMA Shuffle* from
**Advanced Data Management Systems** (TU Darmstadt, FG Data Management, 2026). The course provided
a larger codebase: the RDMA connection helper (`CommHelper`), the thread pool, config parsing, and
the distributed test/benchmark harness. This repository contains **only the files I wrote** — the
`Shuffle` class. The course framework is not included, so the code does not build standalone.

## What I implemented

`Shuffle::run()` performs a multi-phase distributed shuffle where every node ends up owning all
rows whose partition maps to it:

1. **Count** — the local input rows are split across 4 threads; each thread counts how many rows
   belong to every destination node, and the per-thread counts are reduced into one vector.
2. **Exchange counts** — each node RDMA-writes its per-destination counts into every peer's
   registered memory (`write` + `sync_signaled`), so every node knows how many rows it will receive.
3. **Exchange offsets** — prefix sums over the received counts give the destination offset of every
   sender inside the receiver buffer; those offsets are pushed to the peers the same way.
4. **Group** — a second parallel pass copies local rows into contiguous, per-destination groups
   using an atomic `fetch_add` bump allocator (`rows_offsets_send_at`), avoiding locks.
5. **Transfer** — each group is RDMA-written into the receiver's buffer in chunks of at most
   256 KB; rows destined for the local node are `memcpy`'d directly, and the local result is
   returned as a `std::span<Row>` starting at the first received row.

`Shuffle::inter_node_barrier()` is a sense-reversing distributed barrier built on RDMA atomics:
every node does `fetch_add` on node 0's counter and spins on its own flag word; node 0 waits for
`round * num_nodes` arrivals and then writes the new round number back to all peers.

The whole protocol reuses the single RDMA-registered memory region by laying out
`input rows | received counts | counts to send | received offsets | offsets to send | grouped rows | output rows`,
with no dynamic allocation on the data path.

## Files

| Path | Contents |
|---|---|
| `src/shuffle.cpp` | `Shuffle::run()` (count → exchange → group → transfer) and `inter_node_barrier()` |
| `src/shuffle.hpp` | `Shuffle` class declaration and the buffer-offset bookkeeping members |

## Notes

- `rdma::Connection` is not thread-safe, so connections are only touched from the coordinating
  thread; the parallel phases work purely on local memory.
- Written against the course `Config` (`num_nodes`, `num_rows`, `mem_size`, `get_part_id`,
  `part_to_node_id`) and `ThreadPool`.
