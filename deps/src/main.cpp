#ifdef USE_JLCXX
#include <jlcxx/jlcxx.hpp>
#include <jlcxx/stl.hpp>
#endif

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstddef>
#include <cstdio>
#include <ctime>
#include <deque>
#include <exception>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <julia.h>

#include "../include/kafka/Properties.h"
#include "../include/kafka/ProducerRecord.h"
#include "../include/kafka/ConsumerRecord.h"
#include "../include/kafka/Header.h"
#include "../include/kafka/Error.h"
#include "../include/kafka/KafkaProducer.h"
#include "../include/kafka/KafkaConsumer.h"
#include "../include/kafka/Log.h"

#ifdef USE_JLCXX

namespace {
inline void append_u32_le(std::vector<std::uint8_t>& out, std::uint32_t v)
{
    out.push_back(static_cast<std::uint8_t>(v & 0xFF));
    out.push_back(static_cast<std::uint8_t>((v >> 8) & 0xFF));
    out.push_back(static_cast<std::uint8_t>((v >> 16) & 0xFF));
    out.push_back(static_cast<std::uint8_t>((v >> 24) & 0xFF));
}

inline void append_i32_le(std::vector<std::uint8_t>& out, std::int32_t v)
{
    append_u32_le(out, static_cast<std::uint32_t>(v));
}

inline void append_i64_le(std::vector<std::uint8_t>& out, std::int64_t v)
{
    const std::uint64_t u = static_cast<std::uint64_t>(v);
    out.push_back(static_cast<std::uint8_t>(u & 0xFF));
    out.push_back(static_cast<std::uint8_t>((u >> 8) & 0xFF));
    out.push_back(static_cast<std::uint8_t>((u >> 16) & 0xFF));
    out.push_back(static_cast<std::uint8_t>((u >> 24) & 0xFF));
    out.push_back(static_cast<std::uint8_t>((u >> 32) & 0xFF));
    out.push_back(static_cast<std::uint8_t>((u >> 40) & 0xFF));
    out.push_back(static_cast<std::uint8_t>((u >> 48) & 0xFF));
    out.push_back(static_cast<std::uint8_t>((u >> 56) & 0xFF));
}

enum class LogSink
{
    JuliaLogger,
    Stdout,
    File
};

struct LogState
{
    std::mutex mutex;
    bool enabled = true;
    bool use_default_format = true;
    std::string format = "{timestamp} [{level}] {message}";
    LogSink sink = LogSink::JuliaLogger;
    std::string file_path;
    bool file_append = true;
    std::unique_ptr<std::ofstream> file;
    std::deque<std::string> pending_julia_logs;
};

LogState& log_state()
{
    static LogState state;
    return state;
}

std::string format_log_line(const std::string& fmt, int level, const char* filename, int lineno, const char* msg)
{
    auto replace_all = [](std::string& str, const std::string& from, const std::string& to) {
        if (from.empty()) return;
        std::size_t start_pos = 0;
        while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
            str.replace(start_pos, from.length(), to);
            start_pos += to.length();
        }
    };

    std::string line = fmt;
    replace_all(line, "{timestamp}", kafka::utility::getCurrentTime());
    replace_all(line, "{level}", kafka::Log::levelString(static_cast<std::size_t>(level)));
    replace_all(line, "{level_int}", std::to_string(level));
    replace_all(line, "{file}", filename ? filename : "");
    replace_all(line, "{line}", std::to_string(lineno));
    replace_all(line, "{message}", msg ? msg : "");

    return line;
}

std::ostream* log_stream(LogState& state)
{
    if (state.sink == LogSink::File) {
        if (!state.file || !state.file->is_open()) {
            if (state.file_path.empty()) {
                return nullptr;
            }
            auto mode = std::ios::out | (state.file_append ? std::ios::app : std::ios::trunc);
            state.file = std::make_unique<std::ofstream>(state.file_path, mode);
        }
        if (!state.file || !state.file->is_open()) {
            return nullptr;
        }
        return state.file.get();
    }
    return nullptr;
}

void write_to_julia_stdout(const std::string& line)
{
    std::fwrite(line.data(), 1, line.size(), stdout);
    std::fputc('\n', stdout);
    std::fflush(stdout);
}

std::string encode_julia_log(int level, const char* filename, int lineno, const char* msg)
{
    constexpr char sep = '\x1f';
    std::ostringstream oss;
    oss << level << sep
        << (filename ? filename : "") << sep
        << lineno << sep
        << (msg ? msg : "");
    return oss.str();
}

void emit_log(int level, const char* filename, int lineno, const char* msg)
{
    auto& state = log_state();
    std::lock_guard<std::mutex> guard(state.mutex);
    if (!state.enabled) {
        return;
    }

    if (state.sink == LogSink::JuliaLogger) {
        static constexpr std::size_t MAX_PENDING_LOGS = 10000;
        if (state.pending_julia_logs.size() >= MAX_PENDING_LOGS) {
            state.pending_julia_logs.pop_front();
        }
        state.pending_julia_logs.push_back(encode_julia_log(level, filename, lineno, msg));
        return;
    }

    if (state.sink == LogSink::Stdout) {
        std::string line;
        if (state.use_default_format) {
            std::ostringstream oss;
            oss << "[" << kafka::utility::getCurrentTime() << "]"
                << kafka::Log::levelString(static_cast<std::size_t>(level)) << " "
                << (msg ? msg : "");
            line = oss.str();
        } else {
            line = format_log_line(state.format, level, filename, lineno, msg);
        }
        write_to_julia_stdout(line);
        return;
    }

    std::string line;
    if (state.use_default_format) {
        std::ostringstream oss;
        oss << "[" << kafka::utility::getCurrentTime() << "]"
            << kafka::Log::levelString(static_cast<std::size_t>(level)) << " "
            << (msg ? msg : "");
        line = oss.str();
    } else {
        line = format_log_line(state.format, level, filename, lineno, msg);
    }

    std::ostream* out = log_stream(state);
    if (out) {
        (*out) << line << std::endl;
    }
}

kafka::clients::LogCallback& global_log_callback()
{
    static kafka::clients::LogCallback cb = emit_log;
    return cb;
}
}  // namespace

JLCXX_MODULE define_julia_module(jlcxx::Module& mod)
{

    static std::map<int, std::shared_ptr<kafka::Properties>> properties_store;
    static std::map<int, std::shared_ptr<kafka::clients::producer::KafkaProducer>> producer_store;
    static std::map<int, std::shared_ptr<kafka::clients::consumer::KafkaConsumer>> consumer_store;
    static std::mutex store_mutex;
    static std::atomic<int> next_id{1};

    (void)jlcxx::julia_type<std::vector<std::string>>();
    (void)jlcxx::julia_type<std::vector<int>>();
    (void)jlcxx::julia_type<std::vector<long long>>();

    kafka::setGlobalLogger(global_log_callback());

    mod.method("create_properties", []() -> int {
        const int id = next_id.fetch_add(1);
        std::lock_guard<std::mutex> guard(store_mutex);
        auto props = std::make_shared<kafka::Properties>();
        props->put("log_cb", global_log_callback());
        properties_store[id] = std::move(props);
        return id;
    });

    mod.method("properties_put", [](int props_id, const std::string& key, const std::string& value) -> bool {
        std::lock_guard<std::mutex> guard(store_mutex);
        auto it = properties_store.find(props_id);
        if (it == properties_store.end()) {
            return false;
        }
        try {
            it->second->put(key, value);
        } catch (const std::exception& e) {
            emit_log(3, __FILE__, __LINE__, e.what());
            return false;
        } catch (...) {
            emit_log(3, __FILE__, __LINE__, "properties_put: unknown exception");
            return false;
        }
        return true;
    });

    mod.method("properties_destroy", [](int props_id) -> bool {
        std::lock_guard<std::mutex> guard(store_mutex);
        return properties_store.erase(props_id) > 0;
    });
    mod.method("create_kafka_producer", [](int props_id) -> int {
        std::shared_ptr<kafka::Properties> props;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = properties_store.find(props_id);
            if (it == properties_store.end()) return 0;
            props = it->second;
            properties_store.erase(it);
        }
        if (!props->contains("log_cb")) {
            props->put("log_cb", global_log_callback());
        }
        try {
            auto producer = std::make_shared<kafka::clients::producer::KafkaProducer>(*props);
            const int id = next_id.fetch_add(1);
            {
                std::lock_guard<std::mutex> guard(store_mutex);
                producer_store[id] = std::move(producer);
            }
            return id;
        } catch (const std::exception& e) {
            emit_log(3, __FILE__, __LINE__, e.what());
            return 0;
        } catch (...) {
            emit_log(3, __FILE__, __LINE__, "create_kafka_producer: unknown exception");
            return 0;
        }
    });

    mod.method("producer_close", [](int producer_id) -> bool {
        std::shared_ptr<kafka::clients::producer::KafkaProducer> producer;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = producer_store.find(producer_id);
            if (it == producer_store.end()) {
                return false;
            }
            producer = it->second;
            producer_store.erase(it);
        }
        try {
            producer->close();
        } catch (const std::exception& e) {
            emit_log(4, __FILE__, __LINE__, e.what());
        } catch (...) {
            emit_log(4, __FILE__, __LINE__, "producer_close: unknown exception");
        }
        return true;
    });

    mod.method("producer_set_log_level", [](int producer_id, int level) -> bool {
        std::shared_ptr<kafka::clients::producer::KafkaProducer> producer;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = producer_store.find(producer_id);
            if (it == producer_store.end()) {
                return false;
            }
            producer = it->second;
        }
        producer->setLogLevel(level);
        return true;
    });

    mod.method("logging_disable", []() {
        auto& state = log_state();
        std::lock_guard<std::mutex> guard(state.mutex);
        state.enabled = false;
        state.pending_julia_logs.clear();
    });

    mod.method("logging_set_format", [](const std::string& fmt) {
        auto& state = log_state();
        std::lock_guard<std::mutex> guard(state.mutex);
        state.enabled = true;
        state.use_default_format = false;
        state.format = fmt;
    });

    mod.method("logging_set_stdout", []() {
        auto& state = log_state();
        std::lock_guard<std::mutex> guard(state.mutex);
        state.sink = LogSink::Stdout;
        state.file.reset();
        state.file_path.clear();
        state.enabled = true;
        state.pending_julia_logs.clear();
    });

    mod.method("logging_set_julia", []() {
        auto& state = log_state();
        std::lock_guard<std::mutex> guard(state.mutex);
        state.sink = LogSink::JuliaLogger;
        state.file.reset();
        state.file_path.clear();
        state.enabled = true;
    });

    mod.method("logging_drain", []() -> std::vector<std::string> {
        auto& state = log_state();
        std::lock_guard<std::mutex> guard(state.mutex);
        std::vector<std::string> out;
        out.reserve(state.pending_julia_logs.size());
        while (!state.pending_julia_logs.empty()) {
            out.emplace_back(std::move(state.pending_julia_logs.front()));
            state.pending_julia_logs.pop_front();
        }
        return out;
    });

    mod.method("logging_set_file", [](const std::string& path, bool append) -> bool {
        auto& state = log_state();
        std::lock_guard<std::mutex> guard(state.mutex);
        if (path.empty()) {
            return false;
        }
        auto mode = std::ios::out | (append ? std::ios::app : std::ios::trunc);
        auto file = std::make_unique<std::ofstream>(path, mode);
        if (!file->is_open()) {
            return false;
        }
        state.file_path = path;
        state.file_append = append;
        state.file = std::move(file);
        state.sink = LogSink::File;
        state.enabled = true;
        state.pending_julia_logs.clear();
        return true;
    });

    mod.method("logging_enable_default", []() {
        auto& state = log_state();
        std::lock_guard<std::mutex> guard(state.mutex);
        state.enabled = true;
        state.use_default_format = true;
        state.format = "{timestamp} [{level}] {message}";
        state.sink = LogSink::JuliaLogger;
        state.file.reset();
        state.file_path.clear();
    });

    mod.method("get_bootstrap_servers", []() -> std::string {
        return kafka::clients::Config::BOOTSTRAP_SERVERS;
    });

    mod.method("produce", [](int producer_id, const std::string& topic, int partition,
                             const std::string& key, jlcxx::ArrayRef<uint8_t> value,
                             jlcxx::ArrayRef<uint8_t> headers_blob) -> std::string {
        std::shared_ptr<kafka::clients::producer::KafkaProducer> producer;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = producer_store.find(producer_id);
            if (it == producer_store.end()) {
                return "KafkaProducer handle not found. It may be closed or invalid.";
            }
            producer = it->second;
        }
        kafka::clients::producer::ProducerRecord record(
            topic, kafka::Partition(partition),
            kafka::Key(key.data(), key.size()),
            kafka::Value(value.data(), value.size())
        );

        const auto* blob = headers_blob.data();
        const std::size_t blob_len = headers_blob.size();
        if (blob_len >= 4) {
            std::size_t pos = 0;
            auto read_u32 = [&]() -> std::uint32_t {
                if (pos + 4 > blob_len) throw std::runtime_error("headers_blob truncated");
                std::uint32_t v = static_cast<std::uint32_t>(blob[pos])
                    | (static_cast<std::uint32_t>(blob[pos + 1]) << 8)
                    | (static_cast<std::uint32_t>(blob[pos + 2]) << 16)
                    | (static_cast<std::uint32_t>(blob[pos + 3]) << 24);
                pos += 4;
                return v;
            };

            const std::uint32_t count = read_u32();
            if (count > (blob_len - pos) / 8) {
                throw std::runtime_error("headers_blob: header count exceeds available bytes");
            }
            record.headers().reserve(count);
            for (std::uint32_t i = 0; i < count; ++i) {
                const std::uint32_t key_len = read_u32();
                if (pos + key_len > blob_len) throw std::runtime_error("headers_blob truncated");
                std::string hkey(reinterpret_cast<const char*>(blob + pos), key_len);
                pos += key_len;

                const std::uint32_t val_len = read_u32();
                if (pos + val_len > blob_len) throw std::runtime_error("headers_blob truncated");
                // ConstBuffer points into headers_blob — data is alive until syncSend copies it
                record.headers().emplace_back(std::move(hkey),
                    kafka::ConstBuffer(blob + pos, val_len));
                pos += val_len;
            }
        }

        try {
            producer->syncSend(record);
            return "";
        } catch (const std::exception& e) {
            return e.what();
        } catch (...) {
            return "Unknown exception";
        }
    });

    mod.method("create_kafka_consumer", [](int props_id) -> int {
        std::shared_ptr<kafka::Properties> props;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = properties_store.find(props_id);
            if (it == properties_store.end()) return 0;
            props = it->second;
            properties_store.erase(it);
        }
        if (!props->contains("log_cb")) {
            props->put("log_cb", global_log_callback());
        }
        try {
            auto consumer = std::make_shared<kafka::clients::consumer::KafkaConsumer>(*props);
            const int id = next_id.fetch_add(1);
            {
                std::lock_guard<std::mutex> guard(store_mutex);
                consumer_store[id] = std::move(consumer);
            }
            return id;
        } catch (const std::exception& e) {
            emit_log(3, __FILE__, __LINE__, e.what());
            return 0;
        } catch (...) {
            emit_log(3, __FILE__, __LINE__, "create_kafka_consumer: unknown exception");
            return 0;
        }
    });

    mod.method("consumer_set_log_level", [](int consumer_id, int level) -> bool {
        std::shared_ptr<kafka::clients::consumer::KafkaConsumer> consumer;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = consumer_store.find(consumer_id);
            if (it == consumer_store.end()) {
                return false;
            }
            consumer = it->second;
        }
        consumer->setLogLevel(level);
        return true;
    });

    mod.method("consumer_subscribe", [](int consumer_id, const std::set<std::string>& topics) -> bool {
        std::shared_ptr<kafka::clients::consumer::KafkaConsumer> consumer;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = consumer_store.find(consumer_id);
            if (it == consumer_store.end()) {
                return false;
            }
            consumer = it->second;
        }
        try {
            consumer->subscribe(topics);
        } catch (const std::exception& e) {
            emit_log(3, __FILE__, __LINE__, e.what());
            return false;
        } catch (...) {
            emit_log(3, __FILE__, __LINE__, "consumer_subscribe: unknown exception");
            return false;
        }
        return true;
    });

    mod.method("consumer_poll_raw", [](int consumer_id, int timeout_ms) -> std::vector<std::uint8_t> {
        std::shared_ptr<kafka::clients::consumer::KafkaConsumer> consumer;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = consumer_store.find(consumer_id);
            if (it == consumer_store.end()) {
                throw std::runtime_error("KafkaConsumer handle not found. It may be closed or invalid.");
            }
            consumer = it->second;
        }
        auto records = consumer->poll(std::chrono::milliseconds(timeout_ms));
        std::vector<std::uint8_t> out;
        out.reserve(records.size() * 256);

        for (const auto& record : records) {
            if (record.error()) {
                continue;
            }

            auto topic = record.topic();
            auto hdrs = record.headers();
            const auto key = record.key();
            const auto value = record.value();

            if (topic.empty()) {
                continue;
            }
            if (topic.size() > std::numeric_limits<std::uint32_t>::max() ||
                key.size() > std::numeric_limits<std::uint32_t>::max() ||
                value.size() > std::numeric_limits<std::uint32_t>::max() ||
                hdrs.size() > std::numeric_limits<std::uint32_t>::max()) {
                throw std::runtime_error("Record field too large for raw encoding.");
            }

            append_u32_le(out, static_cast<std::uint32_t>(topic.size()));
            out.insert(out.end(), topic.begin(), topic.end());
            append_i32_le(out, record.partition());
            append_i64_le(out, record.offset());
            append_i64_le(out, record.timestamp().msSinceEpoch);
            append_u32_le(out, static_cast<std::uint32_t>(key.size()));
            if (key.size() > 0) {
                const auto* key_ptr = static_cast<const std::uint8_t*>(key.data());
                out.insert(out.end(), key_ptr, key_ptr + key.size());
            }
            append_u32_le(out, static_cast<std::uint32_t>(value.size()));
            if (value.size() > 0) {
                const auto* value_ptr = static_cast<const std::uint8_t*>(value.data());
                out.insert(out.end(), value_ptr, value_ptr + value.size());
            }

            append_u32_le(out, static_cast<std::uint32_t>(hdrs.size()));
            for (const auto& hdr : hdrs) {
                append_u32_le(out, static_cast<std::uint32_t>(hdr.key.size()));
                out.insert(out.end(), hdr.key.begin(), hdr.key.end());
                append_u32_le(out, static_cast<std::uint32_t>(hdr.value.size()));
                if (hdr.value.size() > 0) {
                    auto* p = static_cast<const std::uint8_t*>(hdr.value.data());
                    out.insert(out.end(), p, p + hdr.value.size());
                }
            }
        }
        return out;
    });

    mod.method("consumer_commit_sync", [](int consumer_id) -> int {
        std::shared_ptr<kafka::clients::consumer::KafkaConsumer> consumer;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = consumer_store.find(consumer_id);
            if (it == consumer_store.end()) {
                return -1;
            }
            consumer = it->second;
        }
        try {
            consumer->commitSync();
        } catch (const std::exception& e) {
            emit_log(3, __FILE__, __LINE__, e.what());
            return -2;
        } catch (...) {
            emit_log(3, __FILE__, __LINE__, "consumer_commit_sync: unknown exception");
            return -2;
        }
        return 0;
    });

    mod.method("consumer_close", [](int consumer_id) -> bool {
        std::shared_ptr<kafka::clients::consumer::KafkaConsumer> consumer;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = consumer_store.find(consumer_id);
            if (it == consumer_store.end()) {
                return false;
            }
            consumer = it->second;
            consumer_store.erase(it);
        }
        try {
            consumer->close();
        } catch (const std::exception& e) {
            emit_log(4, __FILE__, __LINE__, e.what());
        } catch (...) {
            emit_log(4, __FILE__, __LINE__, "consumer_close: unknown exception");
        }
        return true;
    });


    mod.method("consumer_assign_many", [](int consumer_id, const std::vector<std::string>& topics,
                                          const std::vector<int>& partitions, const std::vector<long long>& offsets) -> bool {
        if (topics.size() != partitions.size() || topics.size() != offsets.size()) {
            throw std::runtime_error("consumer_assign_many: topics/partitions/offsets length mismatch.");
        }
        std::shared_ptr<kafka::clients::consumer::KafkaConsumer> consumer;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = consumer_store.find(consumer_id);
            if (it == consumer_store.end()) {
                return false;
            }
            consumer = it->second;
        }

        try {
            kafka::TopicPartitions tps;
            for (std::size_t i = 0; i < topics.size(); ++i) {
                tps.emplace(topics[i], partitions[i]);
            }
            consumer->assign(tps);

            for (std::size_t i = 0; i < topics.size(); ++i) {
                if (offsets[i] != RD_KAFKA_OFFSET_INVALID) {
                    consumer->seek(kafka::TopicPartition(topics[i], partitions[i]), offsets[i]);
                }
            }
        } catch (const std::exception& e) {
            emit_log(3, __FILE__, __LINE__, e.what());
            return false;
        } catch (...) {
            emit_log(3, __FILE__, __LINE__, "consumer_assign_many: unknown exception");
            return false;
        }
        return true;
    });

    mod.method("consumer_seek_to_beginning", [](int consumer_id, const std::string& topic, int partition) -> bool {
        std::shared_ptr<kafka::clients::consumer::KafkaConsumer> consumer;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = consumer_store.find(consumer_id);
            if (it == consumer_store.end()) {
                return false;
            }
            consumer = it->second;
        }
        try {
            kafka::TopicPartitions partitions{kafka::TopicPartition(topic, partition)};
            consumer->seekToBeginning(partitions);
        } catch (const std::exception& e) {
            emit_log(3, __FILE__, __LINE__, e.what());
            return false;
        } catch (...) {
            emit_log(3, __FILE__, __LINE__, "consumer_seek_to_beginning: unknown exception");
            return false;
        }
        return true;
    });

    mod.method("consumer_seek_to_end", [](int consumer_id, const std::string& topic, int partition) -> bool {
        std::shared_ptr<kafka::clients::consumer::KafkaConsumer> consumer;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = consumer_store.find(consumer_id);
            if (it == consumer_store.end()) {
                return false;
            }
            consumer = it->second;
        }
        try {
            kafka::TopicPartitions partitions{kafka::TopicPartition(topic, partition)};
            consumer->seekToEnd(partitions);
        } catch (const std::exception& e) {
            emit_log(3, __FILE__, __LINE__, e.what());
            return false;
        } catch (...) {
            emit_log(3, __FILE__, __LINE__, "consumer_seek_to_end: unknown exception");
            return false;
        }
        return true;
    });

    mod.method("consumer_commit_record", [](int consumer_id, const std::string& topic, int partition, long long offset) -> bool {
        std::shared_ptr<kafka::clients::consumer::KafkaConsumer> consumer;
        {
            std::lock_guard<std::mutex> guard(store_mutex);
            auto it = consumer_store.find(consumer_id);
            if (it == consumer_store.end()) {
                return false;
            }
            consumer = it->second;
        }
        try {
            kafka::TopicPartitionOffsets tpos;
            tpos[kafka::TopicPartition(topic, partition)] = offset + 1;
            consumer->commitSync(tpos);
        } catch (const std::exception& e) {
            emit_log(3, __FILE__, __LINE__, e.what());
            return false;
        } catch (...) {
            emit_log(3, __FILE__, __LINE__, "consumer_commit_record: unknown exception");
            return false;
        }
        return true;
    });
}

#else

#include <iostream>
#include <string>

using namespace kafka;
using namespace kafka::clients::producer;
using namespace kafka::clients::consumer;

int main() {
    Properties props;
    props.put("bootstrap.servers", "localhost:9092");
    ProducerRecord record("test-topic", Key("test-key"), Value("test-value"));
    Error error;
    return 0;
}

#endif
