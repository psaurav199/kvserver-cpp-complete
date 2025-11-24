#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <ctime>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace std;

enum class WorkloadType {
    PUT_ALL,
    GET_ALL,
    GET_POPULAR,
    MIX
};

struct ThreadStats {
    uint64_t requests = 0;
    uint64_t successes = 0;
    uint64_t errors = 0;
    long double total_latency_ns = 0.0L;
};

static bool parse_workload(const string& s, WorkloadType& out) {
    if (s == "put_all") { out = WorkloadType::PUT_ALL; return true; }
    if (s == "get_all") { out = WorkloadType::GET_ALL; return true; }
    if (s == "get_popular") { out = WorkloadType::GET_POPULAR; return true; }
    if (s == "mix") { out = WorkloadType::MIX; return true; }
    return false;
}

int connect_to_server(const string& host, uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &sa.sin_addr) != 1) {
        close(fd);
        return -1;
    }
    if (::connect(fd, (sockaddr*)&sa, sizeof(sa)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

bool write_all(int fd, const string& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = send(fd, data.data() + sent, data.size() - sent, 0);
        if (n <= 0) return false;
        sent += (size_t)n;
    }
    return true;
}

bool read_response(int fd, string& out) {
    out.clear();
    char buf[4096];
    while (true) {
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n < 0) return false;
        if (n == 0) break;
        out.append(buf, buf + n);
    }
    return !out.empty();
}

bool is_success_response(const string& resp) {
    auto pos = resp.find("\r\n");
    if (pos == string::npos) return false;
    string status_line = resp.substr(0, pos);
    return (status_line.find("HTTP/1.1 2") == 0) ||
           (status_line.find("HTTP/1.0 2") == 0);
}

string make_json_body(const string& key, const string& value) {
    return string("{\"key\":\"") + key + "\",\"value\":\"" + value + "\"}";
}

string make_key(uint32_t thread_id, uint64_t counter) {
    return "k_" + to_string(thread_id) + "_" + to_string(counter);
}

string popular_key() {
    return "hot_key";
}

void client_thread_func(
    int thread_id,
    const string& host,
    uint16_t port,
    WorkloadType workload,
    chrono::steady_clock::time_point end_time,
    ThreadStats* stats
) {
    mt19937_64 rng(random_device{}() ^ ((uint64_t)thread_id << 32));
    uniform_int_distribution<int> mix_dist(0, 99);

    while (chrono::steady_clock::now() < end_time) {
        string req;
        string key;
        string body;

        uint64_t req_id = stats->requests;
        WorkloadType eff_workload = workload;

        if (workload == WorkloadType::MIX) {
            int r = mix_dist(rng);
            if (r < 50) eff_workload = WorkloadType::GET_POPULAR;
            else if (r < 80) eff_workload = WorkloadType::GET_ALL;
            else eff_workload = WorkloadType::PUT_ALL;
        }

        if (eff_workload == WorkloadType::PUT_ALL) {
            key = make_key(thread_id, req_id);
            body = make_json_body(key, "val_" + to_string(req_id));
            req = "POST /kv HTTP/1.1\r\n"
                  "Host: " + host + "\r\n"
                  "Connection: close\r\n"
                  "Content-Type: application/json\r\n"
                  "Content-Length: " + to_string(body.size()) + "\r\n"
                  "\r\n" + body;
        } else if (eff_workload == WorkloadType::GET_ALL) {
            key = make_key(thread_id, req_id);
            req = "GET /kv/" + key + " HTTP/1.1\r\n"
                  "Host: " + host + "\r\n"
                  "Connection: close\r\n"
                  "\r\n";
        } else if (eff_workload == WorkloadType::GET_POPULAR) {
            key = popular_key();
            req = "GET /kv/" + key + " HTTP/1.1\r\n"
                  "Host: " + host + "\r\n"
                  "Connection: close\r\n"
                  "\r\n";
        }

        auto t_start = chrono::steady_clock::now();

        int fd = connect_to_server(host, port);
        if (fd < 0) {
            stats->errors++;
            continue;
        }

        bool ok = write_all(fd, req);
        string resp;
        if (ok) ok = read_response(fd, resp);
        ::close(fd);

        auto t_end = chrono::steady_clock::now();
        auto ns = chrono::duration_cast<chrono::nanoseconds>(t_end - t_start).count();

        stats->requests++;
        stats->total_latency_ns += (long double)ns;

        if (!ok || !is_success_response(resp)) stats->errors++;
        else stats->successes++;
    }
}

int main(int argc, char** argv) {
    if (argc != 6) {
        cerr << "Usage: " << argv[0] << " <server_ip> <port> <num_clients> <duration_sec> <workload>\n";
        cerr << "  workload = put_all | get_all | get_popular | mix\n";
        return 1;
    }

    string host = argv[1];
    uint16_t port = (uint16_t)stoi(argv[2]);
    int num_clients = stoi(argv[3]);
    int duration_sec = stoi(argv[4]);
    string workload_str = argv[5];

    if (num_clients <= 0 || duration_sec <= 0) {
        cerr << "num_clients and duration_sec must be > 0\n";
        return 1;
    }

    WorkloadType workload;
    if (!parse_workload(workload_str, workload)) {
        cerr << "Unknown workload: " << workload_str << "\n";
        return 1;
    }

    cout << "[loadgen] host=" << host
         << " port=" << port
         << " clients=" << num_clients
         << " duration=" << duration_sec << "s"
         << " workload=" << workload_str << "\n";

    vector<thread> threads;
    vector<ThreadStats> stats(num_clients);

    auto start_time = chrono::steady_clock::now();
    auto end_time = start_time + chrono::seconds(duration_sec);

    for (int i = 0; i < num_clients; ++i)
        threads.emplace_back(client_thread_func, i, host, port, workload, end_time, &stats[i]);

    for (auto& t : threads) t.join();

    auto actual_end = chrono::steady_clock::now();
    long double total_test_sec = chrono::duration_cast<chrono::duration<long double>>(actual_end - start_time).count();

    uint64_t total_req = 0, total_ok = 0, total_err = 0;
    long double total_lat_ns = 0.0L;

    for (const auto& s : stats) {
        total_req += s.requests;
        total_ok  += s.successes;
        total_err += s.errors;
        total_lat_ns += s.total_latency_ns;
    }

    long double avg_throughput = total_ok / total_test_sec;
    long double avg_resp_ms = total_ok > 0 ? (total_lat_ns / total_ok) / 1e6L : 0.0L;

    cout << "==== Load test summary ====\n";
    cout << "Total duration (s):    " << total_test_sec << "\n";
    cout << "Total requests sent:   " << total_req << "\n";
    cout << "Total successes:       " << total_ok << "\n";
    cout << "Total errors:          " << total_err << "\n";
    cout << "Average throughput:    " << avg_throughput << " req/s\n";
    cout << "Average response time: " << avg_resp_ms << " ms\n";

    const char* csv_name = "load
