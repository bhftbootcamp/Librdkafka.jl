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

        r2 = Librdkafka.ConsumerRecord(t, p, 0, "", UInt8[], 0)
        @test isempty(r2.headers)
    end

    @testset "KafkaHeaders alias" begin
        @test Librdkafka.KafkaHeaders === Vector{Pair{String,Vector{UInt8}}}
    end

    @testset "headers binary protocol" begin
        function put_u32_le!(buf, v::UInt32)
            push!(buf, v % UInt8, (v >> 8) % UInt8, (v >> 16) % UInt8, (v >> 24) % UInt8)
        end
        put_i32_le!(buf, v::Int32) = put_u32_le!(buf, reinterpret(UInt32, v))
        function put_i64_le!(buf, v::Int64)
            u = reinterpret(UInt64, v)
            for s in 0:8:56
                push!(buf, (u >> s) % UInt8)
            end
        end

        @testset "_serialize_headers round-trip" begin
            headers = Pair{String,Vector{UInt8}}[
                "content-type" => Vector{UInt8}("application/json"),
                "symbol" => Vector{UInt8}("BTCUSDT"),
                "empty-val" => UInt8[],
            ]
            blob = Librdkafka._serialize_headers(headers)

            pos = 1
            count_u, pos = Librdkafka._read_u32_le(blob, pos)
            @test count_u == 3
            parsed = Pair{String,Vector{UInt8}}[]
            for _ in 1:count_u
                klen_u, pos = Librdkafka._read_u32_le(blob, pos)
                klen = Int(klen_u)
                k = String(blob[pos:pos+klen-1])
                pos += klen
                vlen_u, pos = Librdkafka._read_u32_le(blob, pos)
                vlen = Int(vlen_u)
                v = vlen == 0 ? UInt8[] : copy(blob[pos:pos+vlen-1])
                pos += vlen
                push!(parsed, k => v)
            end
            @test parsed == headers
            @test pos == length(blob) + 1
        end

        @testset "_serialize_headers empty" begin
            blob = Librdkafka._serialize_headers(Pair{String,Vector{UInt8}}[])
            @test length(blob) == 4
            count_u, _ = Librdkafka._read_u32_le(blob, 1)
            @test count_u == 0
        end

        @testset "record parser with headers" begin
            buf = UInt8[]
            topic = "test-topic"
            key = "k1"
            value = Vector{UInt8}("hello")
            hdrs = ["h1" => Vector{UInt8}("v1"), "h2" => UInt8[]]

            put_u32_le!(buf, UInt32(length(topic)))
            append!(buf, codeunits(topic))
            put_i32_le!(buf, Int32(2))
            put_i64_le!(buf, Int64(100))
            put_i64_le!(buf, Int64(1700000000000))
            put_u32_le!(buf, UInt32(length(key)))
            append!(buf, codeunits(key))
            put_u32_le!(buf, UInt32(length(value)))
            append!(buf, value)
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
            topic = "t"
            put_u32_le!(buf, UInt32(length(topic)))
            append!(buf, codeunits(topic))
            put_u32_le!(buf, UInt32(0))
            put_i64_le!(buf, Int64(0))
            put_i64_le!(buf, Int64(0))
            put_u32_le!(buf, UInt32(0))
            put_u32_le!(buf, UInt32(0))
            put_u32_le!(buf, UInt32(0))

            records = Librdkafka._parse_records(buf)
            @test length(records) == 1
            @test isempty(records[1].headers)
        end

        function _emit_record!(buf, topic, partition, offset, ts, key, value, hdrs)
            put_u32_le!(buf, UInt32(ncodeunits(topic)))
            append!(buf, codeunits(topic))
            put_i32_le!(buf, Int32(partition))
            put_i64_le!(buf, Int64(offset))
            put_i64_le!(buf, Int64(ts))
            put_u32_le!(buf, UInt32(ncodeunits(key)))
            append!(buf, codeunits(key))
            put_u32_le!(buf, UInt32(length(value)))
            append!(buf, value)
            put_u32_le!(buf, UInt32(length(hdrs)))
            for (hk, hv) in hdrs
                put_u32_le!(buf, UInt32(ncodeunits(hk)))
                append!(buf, codeunits(hk))
                put_u32_le!(buf, UInt32(length(hv)))
                append!(buf, hv)
            end
        end

        @testset "parser handles multi-record buffer with mixed headers" begin
            buf = UInt8[]
            _emit_record!(buf, "alpha", 0, 1, 1_700_000_000_000, "k1", Vector{UInt8}("v1"),
                ["a" => Vector{UInt8}("1")])
            _emit_record!(buf, "beta", 3, 7, 1_700_000_000_500, "", UInt8[],
                Pair{String,Vector{UInt8}}[])
            _emit_record!(buf, "alpha", 0, 2, 1_700_000_001_000, "k3", Vector{UInt8}("v3"),
                ["a" => Vector{UInt8}("3"), "b" => UInt8[], "c" => Vector{UInt8}("ccc")])

            recs = Librdkafka._parse_records(buf)
            @test length(recs) == 3
            @test recs[1].topic.name == "alpha" && recs[1].offset == 1 && length(recs[1].headers) == 1
            @test recs[2].topic.name == "beta"  && recs[2].offset == 7 && isempty(recs[2].headers)
            @test recs[3].topic.name == "alpha" && recs[3].offset == 2 && length(recs[3].headers) == 3
            @test recs[3].headers[1] == ("a" => Vector{UInt8}("3"))
            @test recs[3].headers[2] == ("b" => UInt8[])
            @test recs[3].headers[3] == ("c" => Vector{UInt8}("ccc"))
        end

        @testset "parser handles UTF-8 header keys and large values" begin
            buf = UInt8[]
            big = rand(UInt8, 1024)
            hdrs = ["ключ" => Vector{UInt8}("значение"),
                    "🚀" => Vector{UInt8}("rocket"),
                    "big" => big]
            _emit_record!(buf, "t", 0, 0, 0, "k", UInt8[], hdrs)
            recs = Librdkafka._parse_records(buf)
            @test length(recs) == 1
            @test recs[1].headers[1] == ("ключ" => Vector{UInt8}("значение"))
            @test recs[1].headers[2] == ("🚀" => Vector{UInt8}("rocket"))
            @test recs[1].headers[3] == ("big" => big)
        end

        @testset "parser rejects truncated header section" begin
            buf = UInt8[]
            _emit_record!(buf, "t", 0, 0, 0, "k", UInt8[],
                ["h" => Vector{UInt8}("v")])
            for trunc in (length(buf) - 1, length(buf) - 3, length(buf) - 5)
                @test_throws Librdkafka.RecordParseError Librdkafka._parse_records(buf[1:trunc])
            end
        end

        @testset "parser rejects header_count read past end" begin
            buf = UInt8[]
            put_u32_le!(buf, UInt32(1)); append!(buf, codeunits("t"))
            put_i32_le!(buf, Int32(0))
            put_i64_le!(buf, Int64(0))
            put_i64_le!(buf, Int64(0))
            put_u32_le!(buf, UInt32(0))
            put_u32_le!(buf, UInt32(0))
            push!(buf, 0x00, 0x00)
            @test_throws Librdkafka.RecordParseError Librdkafka._parse_records(buf)
        end
    end

    @testset "_to_header_value dispatch" begin
        bytes = UInt8[0x61, 0x62]
        @test Librdkafka._to_header_value(bytes) === bytes
        @test Librdkafka._to_header_value("ab") == bytes
        view = @view UInt8[0x61, 0x62, 0x63][1:2]
        @test Librdkafka._to_header_value(view) == bytes
        @test Librdkafka._to_header_value(view) isa Vector{UInt8}
        @test Librdkafka._to_header_value(42) == Vector{UInt8}("42")
        @test Librdkafka._to_header_value(3.14) == Vector{UInt8}(string(3.14))
        @test Librdkafka._to_header_value(:sym) == Vector{UInt8}("sym")
    end

    @testset "_normalize_headers accepts varied inputs" begin
        canonical = Pair{String,Vector{UInt8}}["a" => Vector{UInt8}("1")]
        @test Librdkafka._normalize_headers(canonical) === canonical

        as_pairs = ["a" => "1", "b" => 2, :c => UInt8[0x03]]
        norm = Librdkafka._normalize_headers(as_pairs)
        @test norm isa Librdkafka.KafkaHeaders
        @test norm[1] == ("a" => Vector{UInt8}("1"))
        @test norm[2] == ("b" => Vector{UInt8}("2"))
        @test norm[3] == ("c" => UInt8[0x03])

        d = Dict("k" => "v")
        nd = Librdkafka._normalize_headers(d)
        @test length(nd) == 1
        @test nd[1] == ("k" => Vector{UInt8}("v"))

        tup = (("x" => Vector{UInt8}("y")),)
        nt = Librdkafka._normalize_headers(tup)
        @test nt == Pair{String,Vector{UInt8}}["x" => Vector{UInt8}("y")]

        @test isempty(Librdkafka._normalize_headers(Pair{String,Vector{UInt8}}[]))
        @test isempty(Librdkafka._normalize_headers(Dict{String,String}()))
    end

    @testset "_headers_blob dispatch" begin
        @test Librdkafka._headers_blob(nothing) === Librdkafka._EMPTY_HEADERS_BLOB
        @test Librdkafka._headers_blob(Pair{String,Vector{UInt8}}[]) === Librdkafka._EMPTY_HEADERS_BLOB
        @test Librdkafka._headers_blob(Dict{String,String}()) === Librdkafka._EMPTY_HEADERS_BLOB

        blob = Librdkafka._headers_blob(["k" => "v"])
        @test !isempty(blob)
        count_u, pos = Librdkafka._read_u32_le(blob, 1)
        @test count_u == 1
        klen, pos = Librdkafka._read_u32_le(blob, pos)
        @test String(blob[pos:pos+Int(klen)-1]) == "k"
        pos += Int(klen)
        vlen, pos = Librdkafka._read_u32_le(blob, pos)
        @test blob[pos:pos+Int(vlen)-1] == Vector{UInt8}("v")
    end

    @testset "ConsumerRecord show" begin
        t = Librdkafka.Topic("t")
        p = Librdkafka.Partition(0)
        no_hdr = Librdkafka.ConsumerRecord(t, p, 1, "k", UInt8[0x01], 0)
        @test !occursin("headers=", sprint(show, no_hdr))

        with_hdr = Librdkafka.ConsumerRecord(t, p, 1, "k", UInt8[0x01], 0,
            Pair{String,Vector{UInt8}}["a" => UInt8[0x02]])
        @test occursin("headers=1", sprint(show, with_hdr))
    end

    @testset "_EMPTY_HEADERS sentinel reused across records" begin
        t = Librdkafka.Topic("t")
        p = Librdkafka.Partition(0)
        r1 = Librdkafka.ConsumerRecord(t, p, 0, "", UInt8[], 0)
        r2 = Librdkafka.ConsumerRecord(t, p, 1, "", UInt8[], 0)
        @test r1.headers === r2.headers
        @test r1.headers === Librdkafka._EMPTY_HEADERS
    end
end
