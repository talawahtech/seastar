/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2014 Cloudius Systems
 */

#include <seastar/core/reactor.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/distributed.hh>

using namespace seastar;

static std::string str_json = "HTTP/1.1 200 OK\r\nServer: httpserver\r\nDate: Thu, 01 Jan 1970 00:00:00 GMT\r\nContent-Type: application/json\r\nContent-Length: 27\r\n\r\n{\"message\":\"Hello, World!\"}";

class tcp_server {
    std::vector<server_socket> _tcp_listeners;
public:
    future<> listen(ipv4_addr addr) {
        listen_options lo;
        lo.proto = transport::TCP;
        lo.reuse_address = true;
        lo.reuse_port = true;
        lo.reuse_port_cbpf = true;
        _tcp_listeners.push_back(seastar::listen(make_ipv4_address(addr), lo));
        do_accepts(_tcp_listeners);

        return make_ready_future<>();
    }

    // FIXME: We should properly tear down the service here.
    future<> stop() {
        return make_ready_future<>();
    }

    void do_accepts(std::vector<server_socket>& listeners) {
        int which = listeners.size() - 1;
        // Accept in the background.
        (void)listeners[which].accept().then([this, &listeners] (accept_result ar) mutable {
            connected_socket fd = std::move(ar.connection);
            socket_address addr = std::move(ar.remote_address);
            auto conn = new connection(*this, std::move(fd), addr);
            (void)conn->process().then_wrapped([conn] (auto&& f) {
                delete conn;
                try {
                    f.get();
                } catch (std::exception& ex) {
                    //std::cout << "request error " << ex.what() << "\n";
                }
            });
            do_accepts(listeners);
        }).then_wrapped([] (auto&& f) {
            try {
                f.get();
            } catch (std::exception& ex) {
                std::cout << "accept failed: " << ex.what() << "\n";
            }
        });
    }

    class connection {
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
    public:
        connection(tcp_server& server, connected_socket&& fd, socket_address addr)
            : _fd(std::move(fd))
            , _read_buf(_fd.input())
            , _write_buf(_fd.output()) {}
        future<> process() {
            if (_read_buf.eof()) {
                return make_ready_future();
            }

            return _read_buf.read().then([this] (temporary_buffer<char> buf) {
                if (buf.size() == 0) {
                    return make_ready_future();
                }

                // send the same response for all requests
                return _write_buf.write(str_json).then([this] {
                    return _write_buf.flush();
                }).then([this] {
                    return this->process();
                });
            });
        }
    };
};

namespace bpo = boost::program_options;

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(8080), "TCP server port");

    return app.run_deprecated(ac, av, [&] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();

        auto server = new distributed<tcp_server>;
        (void)server->start().then([server = std::move(server), port] () mutable {
            engine().at_exit([server] {
                return server->stop();
            });

            // Start listening in the background.
            // Use cpu_id->shard_id mapping to ensure that socket 0 is started on cpu 0 etc for SO_ATTACH_REUSEPORT_CBPF to work efficiently
            std::optional<std::vector<unsigned>> cpu_to_shard = smp::get_cpu_to_shard_mapping();
            if (cpu_to_shard){                
                auto all_cpus = smp::all_cpus();
                return do_for_each(all_cpus, [server = std::move(server), cpu_to_shard = std::move(cpu_to_shard), port] (auto cpu_id) {
                    auto shard_id = cpu_to_shard.value()[cpu_id];
                    std::cout << "CPU: " << cpu_id << ", Socket: " << cpu_id << ", Shard: " << shard_id << "\n";
                    return server->invoke_on(shard_id, &tcp_server::listen, ipv4_addr{port});
                });
            } else {
                return server->invoke_on_all(&tcp_server::listen, ipv4_addr{port});
            }
        }).then([port] {
            std::cout << "Seastar TCP server listening on port " << port << " ...\n";
        });
    });
}
