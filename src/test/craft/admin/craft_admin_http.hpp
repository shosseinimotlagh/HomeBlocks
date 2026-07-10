/*********************************************************************************
 * Modifications Copyright 2026 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/
#pragma once

// A read-only REST view of a running CRAFT replica set, for craft_ublk. It reports the per-replica LSNs
// (commit_lsn, last_append_lsn, Missing slots) and the client's dLSN bookkeeping, so the commit frontier can
// be watched advancing while I/O flows through the block device:
//
//   sudo ./craft_ublk --vol_size_mb 1024 --replicas 3 --http_port 8080
//   watch -n1 'curl -s localhost:8080/api/v1/status | jq'
//
// This is the ONLY place in the craft tree that pulls in sisl::http / httplib / nlohmann. The model and the
// client stay free of both (test_craft_memory and test_craft_dlsn_tracker link without HomeStore, and this
// TU is compiled only into craft_ublk).
//
// Handlers run on httplib's own thread, never on an iomgr reactor, so they only ever call the plain
// mutex-guarded / atomic snapshot accessors (MemCraftReplica::stats, craft_client::dlsn_stats). They never
// co_await and never sync_get.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <homeblks/home_blocks.hpp> // volume_id_t

namespace sisl {
class HttpServer;
} // namespace sisl

namespace homeblocks::craft {

class craft_client;
class MemCraftReplica;
class MemTransport;

// The volume facts the endpoint reports. Passed explicitly because volume_info is move-only and the CLI has
// already handed its own into make_memory_replica_set by the time the server starts.
struct volume_geometry {
    volume_id_t id{};
    std::string name;
    uint64_t size_bytes{0};
    uint64_t page_size{0};
};

// Build the server, register the routes, and start it. Returns the running server so the caller can stop()
// it during shutdown, before the replica set is torn down. Handlers keep the passed shared_ptrs alive.
//
// `replicas` must be in handle order (index 0 is the default leader), matching MemReplicaHandles::replicas.
// A bind failure is logged by sisl and leaves the server not listening; the returned pointer is still valid
// and stop()-able.
std::shared_ptr< sisl::HttpServer > start_admin_http(uint16_t port, std::shared_ptr< craft_client > client,
                                                     std::vector< std::shared_ptr< MemCraftReplica > > replicas,
                                                     std::shared_ptr< MemTransport > net, volume_geometry geo);

} // namespace homeblocks::craft
