#ifdef USE_JLCXX
#include <jlcxx/jlcxx.hpp>
#include <jlcxx/stl.hpp>
#endif

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstddef>
#include <ctime>
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

#include "../include/kafka/Properties.h"
#include "../include/kafka/ProducerRecord.h"
#include "../include/kafka/ConsumerRecord.h"
#include "../include/kafka/Error.h"
#include "../include/kafka/KafkaProducer.h"
#include "../include/kafka/KafkaConsumer.h"
#include "../include/kafka/Log.h"

#ifdef USE_JLCXX

namespace {
static const char kBase64Alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string base64_encode(const void* data, size_t len)
{
    if (len == 0) {
        return std::string();
    }
    const unsigned char* bytes = static_cast<const unsigned char*>(data);
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    for (size_t i = 0; i < len; i += 3) {
        const unsigned int b0 = bytes[i];
        const unsigned int b1 = (i + 1 < len) ? bytes[i + 1] : 0;
        const unsigned int b2 = (i + 2 < len) ? bytes[i + 2] : 0;

        out.push_back(kBase64Alphabet[(b0 >> 2) & 0x3F]);
        out.push_back(kBase64Alphabet[((b0 & 0x3) << 4) | ((b1 >> 4) & 0xF)]);
        if (i + 1 < len) {
            out.push_back(kBase64Alphabet[((b1 & 0xF) << 2) | ((b2 >> 6) & 0x3)]);
        } else {
            out.push_back('=');
        }
        if (i + 2 < len) {
            out.push_back(kBase64Alphabet[b2 & 0x3F]);
        } else {
            out.push_back('=');
        }
    }
    return out;
}

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
    Stdout,
    File
};

struct LogState
{
    std::mutex mutex;
    bool enabled = true;
    bool use_default_format = true;
    std::string format = "{timestamp} [{level}] {message}";
    LogSink sink = LogSink::Stdout;
    std::string file_path;
    bool file_append = true;
    std::unique_ptr<std::ofstream> file;
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
    return &std::cout;
}

void emit_log(int level, const char* filename, int lineno, const char* msg)
{
    auto& state = log_state();
    std::lock_guard<std::mutex> guard(state.mutex);
    if (!state.enabled) {
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
    if (!out) {
        return;
    }
    (*out) << line << std::endl;
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
    static int next_id = 1;

    (void)jlcxx::julia_type<std::vector<std::string>>();

    kafka::setGlobalLogger(global_log_callback());

    mod.method("create_properties", []() -> int {
        int id = next_id++;
        properties_store[id] = std::make_shared<kafka::Properties>();
        properties_store[id]->put("log_cb", global_log_callback());
        return id;
    });

    mod.method("properties_put", [](int props_id, const std::string& key, const std::string& value) {
        if (properties_store.count(props_id)) {
            properties_store[props_id]->put(key, value);
        }
    });
    mod.method("create_kafka_producer", [](int props_id) -> int {
        if (!properties_store.count(props_id)) return 0;
        if (!properties_store[props_id]->contains("log_cb")) {
            properties_store[props_id]->put("log_cb", global_log_callback());
        }
        int id = next_id++;
        producer_store[id] = std::make_shared<kafka::clients::producer::KafkaProducer>(*properties_store[props_id]);
        return id;
    });

    mod.method("producer_close", [](int producer_id) -> bool {
        auto it = producer_store.find(producer_id);
        if (it == producer_store.end()) {
            return false;
        }
        try {
            it->second->close();
        } catch (...) {
        }
        producer_store.erase(it);
        return true;
    });

    mod.method("producer_set_log_level", [](int producer_id, int level) -> bool {
        if (!producer_store.count(producer_id)) {
            return false;
        }
        producer_store[producer_id]->setLogLevel(level);
        return true;
    });

    mod.method("logging_disable", []() {
        auto& state = log_state();
        std::lock_guard<std::mutex> guard(state.mutex);
        state.enabled = false;
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
        return true;
    });

    mod.method("logging_enable_default", []() {
        auto& state = log_state();
        std::lock_guard<std::mutex> guard(state.mutex);
        state.enabled = true;
        state.use_default_format = true;
        state.format = "{timestamp} [{level}] {message}";
        state.sink = LogSink::Stdout;
        state.file.reset();
        state.file_path.clear();
    });

    mod.method("get_bootstrap_servers", []() -> std::string {
        return kafka::clients::Config::BOOTSTRAP_SERVERS;
    });

    mod.method("produce", [](int producer_id, const std::string& topic, int partition,
                            const std::string& key, const std::string& value) -> std::string {
        if (!producer_store.count(producer_id)) {
            return "KafkaProducer handle not found. It may be closed or invalid.";
        }
        kafka::clients::producer::ProducerRecord record(
            topic, kafka::Partition(partition),
            kafka::Key(key.data(), key.size()),
            kafka::Value(value.data(), value.size())
        );
        try {
            producer_store[producer_id]->syncSend(record);
            return "";
        } catch (const std::exception& e) {
            return e.what();
        } catch (...) {
            return "Unknown exception";
        }
    });

    mod.method("produce_binary", [](int producer_id, const std::string& topic, int partition,
                                   const std::string& key, jlcxx::ArrayRef<uint8_t> value) -> std::string {
        if (!producer_store.count(producer_id)) {
            return "KafkaProducer handle not found. It may be closed or invalid.";
        }
        kafka::clients::producer::ProducerRecord record(
            topic, kafka::Partition(partition),
            kafka::Key(key.data(), key.size()),
            kafka::Value(value.data(), value.size())
        );
        try {
            producer_store[producer_id]->syncSend(record);
            return "";
        } catch (const std::exception& e) {
            return e.what();
        } catch (...) {
            return "Unknown exception";
        }
    });

    mod.method("create_kafka_consumer", [](int props_id) -> int {
        if (!properties_store.count(props_id)) return 0;
        if (!properties_store[props_id]->contains("log_cb")) {
            properties_store[props_id]->put("log_cb", global_log_callback());
        }
        int id = next_id++;
        consumer_store[id] = std::make_shared<kafka::clients::consumer::KafkaConsumer>(*properties_store[props_id]);
        return id;
    });

    mod.method("consumer_set_log_level", [](int consumer_id, int level) -> bool {
        if (!consumer_store.count(consumer_id)) {
            return false;
        }
        consumer_store[consumer_id]->setLogLevel(level);
        return true;
    });

    mod.method("consumer_subscribe", [](int consumer_id, const std::set<std::string>& topics) {
        if (consumer_store.count(consumer_id)) {
            consumer_store[consumer_id]->subscribe(topics);
        }
    });

    mod.method("consumer_poll", [](int consumer_id, int timeout_ms) -> std::string {
        std::string result;
        if (!consumer_store.count(consumer_id)) {
            return "ERROR: KafkaConsumer handle not found. It may be closed or invalid.";
        }
        auto records = consumer_store[consumer_id]->poll(std::chrono::milliseconds(timeout_ms));
        for (const auto& record : records) {
            result.append(record.topic()).push_back('\t');
            result.append(std::to_string(record.partition())).push_back('\t');
            result.append(std::to_string(record.offset())).push_back('\t');
            const auto& key = record.key();
            const auto& value = record.value();
            const std::string key_b64 = base64_encode(key.data(), key.size());
            const std::string value_b64 = base64_encode(value.data(), value.size());
            result.append(key_b64).push_back('\t');
            result.append(value_b64).push_back('\t');
            result.append(std::to_string(record.timestamp().msSinceEpoch));
            result.push_back('\n');
        }
        return result;
    });

    mod.method("consumer_poll_raw", [](int consumer_id, int timeout_ms) -> std::vector<std::uint8_t> {
        if (!consumer_store.count(consumer_id)) {
            throw std::runtime_error("KafkaConsumer handle not found. It may be closed or invalid.");
        }
        auto records = consumer_store[consumer_id]->poll(std::chrono::milliseconds(timeout_ms));
        std::vector<std::uint8_t> out;
        std::size_t estimate = 0;
        for (const auto& record : records) {
            const auto& topic = record.topic();
            const auto key = record.key();
            const auto value = record.value();
            estimate += 4 + topic.size() + 4 + 8 + 8 + 4 + key.size() + 4 + value.size();
        }
        out.reserve(estimate);
        for (const auto& record : records) {
            const auto& topic = record.topic();
            const auto key = record.key();
            const auto value = record.value();

            if (topic.size() > std::numeric_limits<std::uint32_t>::max()) {
                throw std::runtime_error("Topic name too large for raw encoding.");
            }
            if (key.size() > std::numeric_limits<std::uint32_t>::max()) {
                throw std::runtime_error("Key too large for raw encoding.");
            }
            if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
                throw std::runtime_error("Value too large for raw encoding.");
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
        }
        return out;
    });

    mod.method("consumer_commit_sync", [](int consumer_id) -> int {
        if (consumer_store.count(consumer_id)) {
            consumer_store[consumer_id]->commitSync();
            return 0;
        }
        return -1;
    });

    mod.method("consumer_close", [](int consumer_id) -> bool {
        auto it = consumer_store.find(consumer_id);
        if (it == consumer_store.end()) {
            return false;
        }
        try {
            it->second->close();
        } catch (...) {
        }
        consumer_store.erase(it);
        return true;
    });


    mod.method("consumer_assign", [](int consumer_id, const std::string& topic, int partition, long long offset) {
        if (consumer_store.count(consumer_id)) {
            kafka::TopicPartitionOffsets tpos;
            tpos[kafka::TopicPartition(topic, partition)] = offset;
            consumer_store[consumer_id]->assign(kafka::TopicPartitions{tpos.begin()->first});
            if (offset != RD_KAFKA_OFFSET_INVALID) {
                consumer_store[consumer_id]->seek(kafka::TopicPartition(topic, partition), offset);
            }
        }
    });

    mod.method("consumer_seek_to_beginning", [](int consumer_id, const std::string& topic, int partition) {
        if (consumer_store.count(consumer_id)) {
            kafka::TopicPartitions partitions{kafka::TopicPartition(topic, partition)};
            consumer_store[consumer_id]->seekToBeginning(partitions);
        }
    });

    mod.method("consumer_seek_to_end", [](int consumer_id, const std::string& topic, int partition) {
        if (consumer_store.count(consumer_id)) {
            kafka::TopicPartitions partitions{kafka::TopicPartition(topic, partition)};
            consumer_store[consumer_id]->seekToEnd(partitions);
        }
    });

    mod.method("consumer_commit_record", [](int consumer_id, const std::string& topic, int partition, long long offset) {
        if (consumer_store.count(consumer_id)) {
            kafka::TopicPartitionOffsets tpos;
            tpos[kafka::TopicPartition(topic, partition)] = offset + 1;
            consumer_store[consumer_id]->commitSync(tpos);
        }
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
