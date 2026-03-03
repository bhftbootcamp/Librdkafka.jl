using Logging

# Helper: encode a u32 as little-endian bytes
function _encode_u32_le(v::UInt32)
    UInt8[v & 0xff, (v >> 8) & 0xff, (v >> 16) & 0xff, (v >> 24) & 0xff]
end

# Helper: encode an i32 as little-endian bytes
function _encode_i32_le(v::Int32)
    _encode_u32_le(reinterpret(UInt32, v))
end

# Helper: encode an i64 as little-endian bytes
function _encode_i64_le(v::Int64)
    u = reinterpret(UInt64, v)
    UInt8[(u >> (8i)) & 0xff for i in 0:7]
end

# Build a binary record matching the C++ wire format:
# u32 topic_len, topic_bytes, i32 partition, i64 offset, i64 timestamp,
# u32 key_len, key_bytes, u32 value_len, value_bytes
function _build_raw_record(; topic="t", partition=Int32(0), offset=Int64(0),
                            timestamp=Int64(0), key="k", value=UInt8[0x01])
    buf = UInt8[]
    topic_bytes = Vector{UInt8}(codeunits(topic))
    key_bytes = Vector{UInt8}(codeunits(key))
    append!(buf, _encode_u32_le(UInt32(length(topic_bytes))))
    append!(buf, topic_bytes)
    append!(buf, _encode_i32_le(partition))
    append!(buf, _encode_i64_le(offset))
    append!(buf, _encode_i64_le(timestamp))
    append!(buf, _encode_u32_le(UInt32(length(key_bytes))))
    append!(buf, key_bytes)
    append!(buf, _encode_u32_le(UInt32(length(value))))
    append!(buf, value)
    return buf
end

@testset "Librdkafka unit" begin
    @testset "constants" begin
        @test Librdkafka.BOOTSTRAP_SERVERS == "bootstrap.servers"
        @test Librdkafka.CLIENT_ID == "client.id"
        @test Librdkafka.GROUP_ID == "group.id"
        @test Librdkafka.AUTO_OFFSET_RESET == "auto.offset.reset"
        @test Librdkafka.ENABLE_AUTO_COMMIT == "enable.auto.commit"
        @test Librdkafka.RD_KAFKA_OFFSET_INVALID == -1001
        @test occursin("timestamp", Librdkafka.DEFAULT_LOG_FORMAT)
    end

    @testset "Topic" begin
        t = Librdkafka.Topic("my-topic")
        @test t.name == "my-topic"
        @test_throws ArgumentError Librdkafka.Topic("")
    end

    @testset "Partition" begin
        p = Librdkafka.Partition(0)
        @test p.id == 0
        @test_throws DomainError Librdkafka.Partition(-1)
    end

    @testset "TopicPartition" begin
        tp = Librdkafka.TopicPartition("t", 1)
        @test tp.topic.name == "t"
        @test tp.partition.id == 1
    end

    @testset "Assignment" begin
        tp = Librdkafka.TopicPartition("t", 0)
        a = Librdkafka.Assignment(tp)
        @test a.topic_partition == tp
        @test a.offset == Librdkafka.RD_KAFKA_OFFSET_INVALID
        a2 = Librdkafka.Assignment(tp; offset=100)
        @test a2.offset == 100
    end

    @testset "ConsumerRecord" begin
        t = Librdkafka.Topic("t")
        p = Librdkafka.Partition(0)
        value = collect(codeunits("value"))
        r = Librdkafka.ConsumerRecord(t, p, 42, "key", value, 0)
        @test r.topic == t
        @test r.partition == p
        @test r.offset == 42
        @test r.key == "key"
        @test r.value == value
        @test r.timestamp_ms == 0
    end

    @testset "Record parser" begin
        @testset "empty input" begin
            records = Librdkafka._parse_records(UInt8[])
            @test isempty(records)
        end

        @testset "single record" begin
            raw = _build_raw_record(topic="my-topic", partition=Int32(2),
                                    offset=Int64(42), timestamp=Int64(1000),
                                    key="hello", value=UInt8[0xca, 0xfe])
            records = Librdkafka._parse_records(raw)
            @test length(records) == 1
            r = records[1]
            @test r.topic.name == "my-topic"
            @test r.partition.id == 2
            @test r.offset == 42
            @test r.timestamp_ms == 1000
            @test r.key == "hello"
            @test r.value == UInt8[0xca, 0xfe]
        end

        @testset "multiple records" begin
            r1 = _build_raw_record(topic="a", partition=Int32(0), offset=Int64(1),
                                   timestamp=Int64(100), key="k1", value=UInt8[0x01])
            r2 = _build_raw_record(topic="b", partition=Int32(1), offset=Int64(2),
                                   timestamp=Int64(200), key="k2", value=UInt8[0x02, 0x03])
            raw = vcat(r1, r2)
            records = Librdkafka._parse_records(raw)
            @test length(records) == 2
            @test records[1].topic.name == "a"
            @test records[1].key == "k1"
            @test records[2].topic.name == "b"
            @test records[2].value == UInt8[0x02, 0x03]
        end

        @testset "empty key and value" begin
            raw = _build_raw_record(topic="t", key="", value=UInt8[])
            records = Librdkafka._parse_records(raw)
            @test length(records) == 1
            @test records[1].key == ""
            @test isempty(records[1].value)
        end

        @testset "malformed input — truncated" begin
            @test_throws Librdkafka.RecordParseError Librdkafka._parse_records(UInt8[0x01, 0x00])
        end
    end

    @testset "Logging helpers" begin
        @testset "_native_log_level mapping" begin
            @test Librdkafka._native_log_level(0) == Logging.Error
            @test Librdkafka._native_log_level(3) == Logging.Error
            @test Librdkafka._native_log_level(4) == Logging.Warn
            @test Librdkafka._native_log_level(5) == Logging.Info
            @test Librdkafka._native_log_level(6) == Logging.Info
            @test Librdkafka._native_log_level(7) == Logging.Debug
        end

        @testset "_emit_native_log_entry! with malformed entry" begin
            # Malformed entry (no separators) should log a warning, not throw
            logger = Test.TestLogger(min_level=Logging.Debug)
            with_logger(logger) do
                Librdkafka._emit_native_log_entry!("no-separators-here")
            end
            @test length(logger.logs) == 1
            @test logger.logs[1].level == Logging.Warn
            @test occursin("Malformed", logger.logs[1].message)
        end

        @testset "_emit_native_log_entry! with valid entry" begin
            sep = '\x1f'
            entry = "6$(sep)myfile.cpp$(sep)42$(sep)Test message"
            logger = Test.TestLogger(min_level=Logging.Debug)
            with_logger(logger) do
                Librdkafka._emit_native_log_entry!(entry)
            end
            @test length(logger.logs) == 1
            @test logger.logs[1].level == Logging.Info
            @test logger.logs[1].message == "Test message"
        end
    end
end
