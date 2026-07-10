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

#include "craft_admin_http.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <optional>

#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <nlohmann/json.hpp>
#include <sisl/http/http_server.hpp>
#include <sisl/logging/logging.h>

#include "client/craft_client.hpp"
#include "model/mem_craft_cluster.hpp"
#include "model/mem_craft_replica.hpp"

namespace homeblocks::craft {

namespace {

using json = nlohmann::json;

// Everything the handlers read. Held by shared_ptr in each lambda, so the replica set outlives the routes.
struct admin_ctx {
    std::shared_ptr< craft_client > client;
    std::vector< std::shared_ptr< MemCraftReplica > > replicas;
    std::shared_ptr< MemTransport > net;
    volume_geometry geo;
};

std::string uuid_str(peer_id_t const& id) { return boost::uuids::to_string(id); }

void reply_json(httplib::Response& res, int status, json const& j) {
    res.status = status;
    res.set_content(j.dump(2), "application/json");
}
void reply_error(httplib::Response& res, int status, std::string msg) {
    reply_json(res, status, json{{"error", std::move(msg)}});
}

json volume_json(volume_geometry const& g) {
    json j;
    j["id"] = uuid_str(g.id);
    j["name"] = g.name;
    j["size_bytes"] = g.size_bytes;
    j["page_size"] = g.page_size;
    j["blocks"] = (g.page_size != 0) ? (g.size_bytes / g.page_size) : 0;
    return j;
}

json client_json(craft_client const& c, tracker_stats const& cs) {
    json j;
    j["term"] = c.term();
    j["lba_size"] = c.lba_size();
    j["leader_index"] = c.leader_index();
    j["replica_count"] = c.replica_count();

    j["commit_lsn"] = cs.frontier; // F: the contiguous resolved prefix the client stamps on every IO
    j["highest_acked"] = cs.highest_acked;
    j["read_horizon"] = std::max(cs.frontier, cs.highest_acked);
    j["issued_dlsn"] = cs.issued;

    // The slots between F and issued that are neither acked nor Empty. These are what hold the frontier
    // down; at rest this must fall to 0, and a write that never resolves shows up here.
    j["unresolved_count"] = cs.unresolved_count;
    j["unresolved_sample"] = cs.unresolved_sample;
    j["winner_scans"] = cs.winner_scans;
    return j;
}

json replica_json(std::size_t index, MemCraftReplica const& r, MemTransport const& net, tracker_stats const& cs,
                  peer_id_t const& leader) {
    auto const s = r.stats();

    json j;
    j["index"] = index;
    j["id"] = uuid_str(s.id);
    j["addr"] = s.addr;
    j["leader"] = (s.id == leader);
    j["up"] = net.is_up(s.id);
    j["write_allowed"] = net.write_allowed(s.id);
    j["delay_ms"] = net.delay_for(s.id).count(); // injected straggler latency; 0 == healthy

    j["term"] = s.term;
    j["client_token"] = s.client_token;

    j["commit_lsn"] = s.commit_lsn;
    j["last_append_lsn"] = s.last_append_lsn;
    j["uncommitted_tail"] = s.last_append_lsn - s.commit_lsn;

    // How far this replica trails the client. These stay honest in the case missing_count cannot see: a
    // replica that never received a suffix of writes has last_append_lsn frozen, hence no Missing slots,
    // yet an arbitrarily large commit_lag.
    j["commit_lag"] = cs.frontier - s.commit_lsn;
    j["append_lag"] = cs.issued - s.last_append_lsn;

    j["missing_count"] = s.missing_count;
    j["missing_sample"] = s.missing_sample;

    j["journal_slots"] = s.journal_slots;
    j["journal_first_dlsn"] = s.journal_first_dlsn;
    j["journal_last_dlsn"] = s.journal_last_dlsn;
    j["zero_write_slots"] = s.zero_write_slots;
    j["empty_slots"] = s.empty_slots;
    j["journal_data_bytes"] = s.journal_data_bytes;

    j["mapped_blocks"] = s.mapped_blocks;
    j["mapped_bytes"] = static_cast< uint64_t >(s.mapped_blocks) * s.page_size;
    return j;
}

json replicas_json(admin_ctx const& ctx, tracker_stats const& cs) {
    auto const leader = ctx.net->leader();
    auto arr = json::array();
    for (std::size_t i = 0; i < ctx.replicas.size(); ++i) {
        arr.push_back(replica_json(i, *ctx.replicas[i], *ctx.net, cs, leader));
    }
    return arr;
}

json cluster_json(admin_ctx const& ctx) {
    auto const leader = ctx.net->leader();

    // Report the two fault sets we can derive without reaching into MemTransport's private state:
    // `down` (unreachable) and `write_blocked` (reachable, but a forced sub-quorum excludes its writes).
    // Both are always empty unless something in C++ injected a fault; this endpoint cannot.
    auto down = json::array();
    auto write_blocked = json::array();
    for (auto const& r : ctx.replicas) {
        auto const id = r->id();
        if (!ctx.net->is_up(id)) {
            down.push_back(uuid_str(id));
        } else if (!ctx.net->write_allowed(id)) {
            write_blocked.push_back(uuid_str(id));
        }
    }

    json j;
    j["leader"] = uuid_str(leader);
    j["leader_index"] = ctx.client->leader_index();
    j["member_count"] = ctx.net->member_count();
    j["op_timeout_ms"] = ctx.net->op_timeout().count(); // 0 == the transport waits forever
    j["faults"] = json{{"down", down}, {"write_blocked", write_blocked}};
    return j;
}

// Resolve ?id= to an index into `replicas`: either a decimal index or a full replica uuid.
std::optional< std::size_t > resolve_replica(std::string const& v,
                                             std::vector< std::shared_ptr< MemCraftReplica > > const& replicas) {
    bool const numeric =
        !v.empty() && std::all_of(v.begin(), v.end(), [](unsigned char c) { return std::isdigit(c) != 0; });
    if (numeric) {
        auto const idx = std::stoull(v);
        if (idx >= replicas.size()) return std::nullopt;
        return static_cast< std::size_t >(idx);
    }

    peer_id_t want;
    try {
        want = boost::uuids::string_generator()(v);
    } catch (std::exception const&) { return std::nullopt; }

    for (std::size_t i = 0; i < replicas.size(); ++i) {
        if (replicas[i]->id() == want) return i;
    }
    return std::nullopt;
}

} // namespace

std::shared_ptr< sisl::HttpServer > start_admin_http(uint16_t port, std::shared_ptr< craft_client > client,
                                                     std::vector< std::shared_ptr< MemCraftReplica > > replicas,
                                                     std::shared_ptr< MemTransport > net, volume_geometry geo) {
    auto ctx = std::make_shared< admin_ctx >();
    ctx->client = std::move(client);
    ctx->replicas = std::move(replicas);
    ctx->net = std::move(net);
    ctx->geo = std::move(geo);

    auto server = std::make_shared< sisl::HttpServer >(port);

    // Every route is a literal path. A parameterized resource ("/api/v1/replica/:id") would register its
    // PATTERN in the server's safelist while do_auth() tests the concrete request path against it, so the
    // route could never match its own url_type once a token verifier is installed. Hence ?id= instead.
    try {
        server->setup_routes({
            {sisl::http_method::Get, "/api/v1/status",
             [ctx](httplib::Request const&, httplib::Response& res) {
                 auto const cs = ctx->client->dlsn_stats();
                 json j;
                 j["volume"] = volume_json(ctx->geo);
                 j["client"] = client_json(*ctx->client, cs);
                 j["cluster"] = cluster_json(*ctx);
                 j["replicas"] = replicas_json(*ctx, cs);
                 reply_json(res, 200, j);
             },
             sisl::url_type::safe},

            {sisl::http_method::Get, "/api/v1/client",
             [ctx](httplib::Request const&, httplib::Response& res) {
                 reply_json(res, 200, client_json(*ctx->client, ctx->client->dlsn_stats()));
             },
             sisl::url_type::safe},

            {sisl::http_method::Get, "/api/v1/cluster",
             [ctx](httplib::Request const&, httplib::Response& res) { reply_json(res, 200, cluster_json(*ctx)); },
             sisl::url_type::safe},

            {sisl::http_method::Get, "/api/v1/replicas",
             [ctx](httplib::Request const&, httplib::Response& res) {
                 reply_json(res, 200, replicas_json(*ctx, ctx->client->dlsn_stats()));
             },
             sisl::url_type::safe},

            // One replica, addressed by index ("?id=0") or by uuid ("?id=3f2a...").
            {sisl::http_method::Get, "/api/v1/replica",
             [ctx](httplib::Request const& req, httplib::Response& res) {
                 if (!req.has_param("id")) {
                     reply_error(res, 400, "missing 'id' query parameter (replica index or uuid)");
                     return;
                 }
                 auto const v = req.get_param_value("id");
                 auto const idx = resolve_replica(v, ctx->replicas);
                 if (!idx) {
                     reply_error(res, 404, fmt::format("no such replica '{}'", v));
                     return;
                 }
                 auto const cs = ctx->client->dlsn_stats();
                 reply_json(res, 200, replica_json(*idx, *ctx->replicas[*idx], *ctx->net, cs, ctx->net->leader()));
             },
             sisl::url_type::safe},

            // The one mutating route: inject straggler latency on a replica. localhost-only.
            //
            //   PUT /api/v1/replica/delay?id=2&ms=20000   raise it
            //   PUT /api/v1/replica/delay?id=2&ms=0       clear it
            //
            // With --op_timeout_ms set, a delay past the timeout makes the client give up on that peer and
            // commit at quorum while the peer still applies the write later. Raising the delay only produces
            // uniform lag; the Missing slots appear when it is CLEARED with writes still in flight.
            {sisl::http_method::Put, "/api/v1/replica/delay",
             [ctx](httplib::Request const& req, httplib::Response& res) {
                 if (!req.has_param("id") || !req.has_param("ms")) {
                     reply_error(res, 400, "need 'id' (replica index or uuid) and 'ms' query parameters");
                     return;
                 }
                 auto const idx = resolve_replica(req.get_param_value("id"), ctx->replicas);
                 if (!idx) {
                     reply_error(res, 404, fmt::format("no such replica '{}'", req.get_param_value("id")));
                     return;
                 }
                 int64_t ms = 0;
                 try {
                     ms = std::stoll(req.get_param_value("ms"));
                 } catch (std::exception const&) {
                     reply_error(res, 400, fmt::format("bad 'ms' value '{}'", req.get_param_value("ms")));
                     return;
                 }
                 if (ms < 0) {
                     reply_error(res, 400, "'ms' must be >= 0");
                     return;
                 }

                 auto const id = ctx->replicas[*idx]->id();
                 // The read router now fails over off a leader that missed acked writes, so this no longer
                 // yields a stale read -- it demonstrates the leader falling behind and being routed around.
                 if ((ms > 0) && (id == ctx->net->leader())) {
                     LOGINFO("CRAFT admin: delaying the LEADER ({} ms). Its writes miss quorum; the client "
                             "routes reads of the affected LBAs to a follower that holds them.",
                             ms);
                 }
                 ctx->net->set_delay(id, std::chrono::milliseconds{ms});
                 LOGINFO("CRAFT admin: replica {} delay set to {} ms", *idx, ms);

                 auto const cs = ctx->client->dlsn_stats();
                 reply_json(res, 200, replica_json(*idx, *ctx->replicas[*idx], *ctx->net, cs, ctx->net->leader()));
             },
             sisl::url_type::localhost},
        });
    } catch (std::runtime_error const& e) { LOGERROR("CRAFT admin: route setup failed: {}", e.what()); }

    server->start(); // binds 0.0.0.0:port; on bind failure sisl logs and leaves the server not listening
    return server;
}

} // namespace homeblocks::craft
