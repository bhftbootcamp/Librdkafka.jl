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
        hdrs = Pair{String,Vector{UInt8}}["x-type" => Vector{UInt8}("json")]
        r = Librdkafka.ConsumerRecord(t, p, 42, "key", value, 0, hdrs)
        @test r.topic == t
        @test r.partition == p
        @test r.offset == 42
        @test r.key == "key"
        @test r.value == value
        @test r.timestamp_ms == 0
        @test r.headers == hdrs
        @test length(r.headers) == 1
        @test r.headers[1].first == "x-type"
        @test r.headers[1].second == Vector{UInt8}("json")

        # No headers
        r2 = Librdkafka.ConsumerRecord(t, p, 0, "", UInt8[], 0, Pair{String,Vector{UInt8}}[])
        @test isempty(r2.headers)
    end

    @testset "KafkaHeaders alias" begin
        @test Librdkafka.KafkaHeaders === Vector{Pair{String,Vector{UInt8}}}
    end

    @testset "_serialize_headers round-trip" begin
        headers = Pair{String,Vector{UInt8}}[
            "content-type" => Vector{UInt8}("application/json"),
            "symbol" => Vector{UInt8}("BTCUSDT"),
            "empty-val" => UInt8[],
        ]
        blob = Librdkafka._serialize_headers(headers)

        # Manual parse of the blob
        function read_u32_le(buf, i)
            v = UInt32(buf[i]) | (UInt32(buf[i+1]) << 8) | (UInt32(buf[i+2]) << 16) | (UInt32(buf[i+3]) << 24)
            return v, i + 4
        end

        pos = 1
        count, pos = read_u32_le(blob, pos)
        @test count == 3
        parsed = Pair{String,Vector{UInt8}}[]
        for _ in 1:count
            klen, pos = read_u32_le(blob, pos)
            k = String(blob[pos:pos+Int(klen)-1])
            pos += Int(klen)
            vlen, pos = read_u32_le(blob, pos)
            v = vlen == 0 ? UInt8[] : copy(blob[pos:pos+Int(vlen)-1])
            pos += Int(vlen)
            push!(parsed, k => v)
        end
        @test parsed == headers
        @test pos == length(blob) + 1  # consumed everything
    end

    @testset "_serialize_headers empty" begin
        blob = Librdkafka._serialize_headers(Pair{String,Vector{UInt8}}[])
        @test length(blob) == 4
        count = UInt32(blob[1]) | (UInt32(blob[2]) << 8) | (UInt32(blob[3]) << 16) | (UInt32(blob[4]) << 24)
        @test count == 0
    end

    @testset "record parser with headers" begin
        # Build a raw binary blob as C++ would produce it
        buf = UInt8[]
        function put_u32_le!(buf, v::UInt32)
            push!(buf, v % UInt8, (v >> 8) % UInt8, (v >> 16) % UInt8, (v >> 24) % UInt8)
        end
        function put_i32_le!(buf, v::Int32)
            put_u32_le!(buf, reinterpret(UInt32, v))
        end
        function put_i64_le!(buf, v::Int64)
            u = reinterpret(UInt64, v)
            for s in 0:8:56
                push!(buf, (u >> s) % UInt8)
            end
        end

        topic = "test-topic"
        key = "k1"
        value = Vector{UInt8}("hello")
        hdrs = ["h1" => Vector{UInt8}("v1"), "h2" => UInt8[]]

        # topic
        put_u32_le!(buf, UInt32(length(topic)))
        append!(buf, codeunits(topic))
        # partition
        put_i32_le!(buf, Int32(2))
        # offset
        put_i64_le!(buf, Int64(100))
        # timestamp
        put_i64_le!(buf, Int64(1700000000000))
        # key
        put_u32_le!(buf, UInt32(length(key)))
        append!(buf, codeunits(key))
        # value
        put_u32_le!(buf, UInt32(length(value)))
        append!(buf, value)
        # headers
        put_u32_le!(buf, UInt32(length(hdrs)))
        for (hk, hv) in hdrs
            put_u32_le!(buf, UInt32(length(hk)))
            append!(buf, codeunits(hk))
            put_u32_le!(buf, UInt32(length(hv)))
            append!(buf, hv)
        end

        records = Librdkafka._parse_records(buf)
        @test length(records) == 1
        r = records[1]
        @test r.topic.name == topic
        @test r.partition.id == 2
        @test r.offset == 100
        @test r.timestamp_ms == 1700000000000
        @test r.key == key
        @test r.value == value
        @test length(r.headers) == 2
        @test r.headers[1] == ("h1" => Vector{UInt8}("v1"))
        @test r.headers[2] == ("h2" => UInt8[])
    end

    @testset "record parser no headers" begin
        buf = UInt8[]
        function put_u32!(buf, v::UInt32)
            push!(buf, v % UInt8, (v >> 8) % UInt8, (v >> 16) % UInt8, (v >> 24) % UInt8)
        end
        function put_i64!(buf, v::Int64)
            u = reinterpret(UInt64, v)
            for s in 0:8:56
                push!(buf, (u >> s) % UInt8)
            end
        end

        topic = "t"
        # topic
        put_u32!(buf, UInt32(length(topic)))
        append!(buf, codeunits(topic))
        # partition 0
        put_u32!(buf, UInt32(0))
        # offset 0
        put_i64!(buf, Int64(0))
        # timestamp 0
        put_i64!(buf, Int64(0))
        # key ""
        put_u32!(buf, UInt32(0))
        # value empty
        put_u32!(buf, UInt32(0))
        # 0 headers
        put_u32!(buf, UInt32(0))

        records = Librdkafka._parse_records(buf)
        @test length(records) == 1
        @test isempty(records[1].headers)
    end
end
