@testset "Librdkafka integration" begin
    if !haskey(ENV, "KAFKA_BOOTSTRAP_SERVERS")
        @test true # no broker configured, skip
        return
    end

    bootstrap = ENV["KAFKA_BOOTSTRAP_SERVERS"]
    # NOTE: @testset resets the global RNG to a deterministic seed at the start of
    # each testset, so `randstring` returns identical values across testsets and
    # cannot be used for uniqueness here. Use `time_ns()` instead.
    run_id = string(getpid(), "-", time_ns())

    fresh_topic() = "librdkafka-jl-test-$(run_id)-$(time_ns())"

    function make_consumer(topic; group_suffix = "")
        c = KafkaConsumer(bootstrap;
            group_id = "librdkafka-jl-test-$(run_id)-$(group_suffix)-$(time_ns())",
            config = Dict(
                AUTO_OFFSET_RESET => "earliest",
                ENABLE_AUTO_COMMIT => "false",
            ),
        )
        assign!(c, topic, 0; offset = RD_KAFKA_OFFSET_BEGINNING)
        return c
    end

    function drain(c, min_count; deadline_ms = 15_000, poll_ms = 500)
        out = ConsumerRecord[]
        start = time()
        while length(out) < min_count && (time() - start) * 1000 < deadline_ms
            for r in poll(c; timeout_ms = poll_ms)
                push!(out, r)
            end
        end
        return out
    end

    @testset "produce/consume round-trip (no headers)" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        try
            produce(p, topic, 0, "k1", "hello world")
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "noh")
        try
            records = drain(c, 1)
            @test length(records) >= 1
            r = records[1]
            @test r.key == "k1"
            @test String(copy(r.value)) == "hello world"
            @test isempty(r.headers)
        finally
            close(c)
        end
    end

    @testset "produce with single header" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        try
            produce(p, topic, 0, "k", "payload";
                headers = ["x-type" => "application/json"])
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "single")
        try
            records = drain(c, 1)
            @test length(records) >= 1
            r = records[1]
            @test length(r.headers) == 1
            @test r.headers[1].first == "x-type"
            @test String(copy(r.headers[1].second)) == "application/json"
        finally
            close(c)
        end
    end

    @testset "header ordering preserved across vector input" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        hdrs = Pair{String,Any}[
            "z" => "first",
            "a" => "second",
            "m" => "third",
        ]
        try
            produce(p, topic, 0, "k", "v"; headers = hdrs)
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "order")
        try
            records = drain(c, 1)
            r = records[1]
            @test length(r.headers) == 3
            @test [h.first for h in r.headers] == ["z", "a", "m"]
            @test String(copy(r.headers[1].second)) == "first"
            @test String(copy(r.headers[2].second)) == "second"
            @test String(copy(r.headers[3].second)) == "third"
        finally
            close(c)
        end
    end

    @testset "header value types: bytes, string, integer, symbol key" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        bytes = UInt8[0xDE, 0xAD, 0xBE, 0xEF]
        try
            produce(p, topic, 0, "k", "v"; headers = [
                "bytes" => bytes,
                "string" => "abc",
                "int" => 42,
                :symkey => "from-symbol",
            ])
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "types")
        try
            records = drain(c, 1)
            r = records[1]
            d = Dict(h.first => copy(h.second) for h in r.headers)
            @test d["bytes"] == bytes
            @test d["string"] == Vector{UInt8}("abc")
            @test d["int"] == Vector{UInt8}("42")
            @test d["symkey"] == Vector{UInt8}("from-symbol")
        finally
            close(c)
        end
    end

    @testset "empty header value" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        try
            produce(p, topic, 0, "k", "v"; headers = ["flag" => UInt8[]])
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "emptyv")
        try
            records = drain(c, 1)
            r = records[1]
            @test length(r.headers) == 1
            @test r.headers[1].first == "flag"
            @test isempty(r.headers[1].second)
        finally
            close(c)
        end
    end

    @testset "Dict input (single header — Dict iteration order not asserted)" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        try
            produce(p, topic, 0, "k", "v"; headers = Dict("only" => "value"))
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "dict")
        try
            records = drain(c, 1)
            r = records[1]
            @test length(r.headers) == 1
            @test r.headers[1].first == "only"
            @test String(copy(r.headers[1].second)) == "value"
        finally
            close(c)
        end
    end

    @testset "AbstractString-value produce overload forwards headers" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        try
            produce(p, topic, 0, "k", "string-payload";
                headers = ["forwarded" => "yes"])
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "strval")
        try
            records = drain(c, 1)
            r = records[1]
            @test String(copy(r.value)) == "string-payload"
            @test length(r.headers) == 1
            @test r.headers[1] == ("forwarded" => Vector{UInt8}("yes"))
        finally
            close(c)
        end
    end

    @testset "many headers stress" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        hdrs = ["h$(i)" => "v$(i)" for i in 1:50]
        try
            produce(p, topic, 0, "k", "v"; headers = hdrs)
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "many")
        try
            records = drain(c, 1)
            r = records[1]
            @test length(r.headers) == 50
            for i in 1:50
                @test r.headers[i].first == "h$(i)"
                @test String(copy(r.headers[i].second)) == "v$(i)"
            end
        finally
            close(c)
        end
    end

    @testset "UTF-8 keys and values" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        try
            produce(p, topic, 0, "k", "v"; headers = [
                "ключ" => "значение",
                "🚀" => "rocket",
            ])
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "utf8")
        try
            records = drain(c, 1)
            r = records[1]
            d = Dict(h.first => String(copy(h.second)) for h in r.headers)
            @test d["ключ"] == "значение"
            @test d["🚀"] == "rocket"
        finally
            close(c)
        end
    end

    @testset "large header value (8 KiB)" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        big = rand(UInt8, 8 * 1024)
        try
            produce(p, topic, 0, "k", "v"; headers = ["payload" => big])
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "big")
        try
            records = drain(c, 1)
            r = records[1]
            @test length(r.headers) == 1
            @test copy(r.headers[1].second) == big
        finally
            close(c)
        end
    end

    @testset "multiple records, mixed headers, single poll" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        try
            produce(p, topic, 0, "a", "first")
            produce(p, topic, 0, "b", "second"; headers = ["h" => "h-b"])
            produce(p, topic, 0, "c", "third";  headers = ["h" => "h-c", "extra" => "yes"])
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "multi")
        try
            records = drain(c, 3)
            @test length(records) >= 3
            by_key = Dict(r.key => r for r in records)
            @test isempty(by_key["a"].headers)
            @test by_key["b"].headers == Pair{String,Vector{UInt8}}["h" => Vector{UInt8}("h-b")]
            @test length(by_key["c"].headers) == 2
            @test by_key["c"].headers[1] == ("h" => Vector{UInt8}("h-c"))
            @test by_key["c"].headers[2] == ("extra" => Vector{UInt8}("yes"))
        finally
            close(c)
        end
    end

    @testset "headers = nothing is equivalent to no kwarg" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        try
            produce(p, topic, 0, "k", "v"; headers = nothing)
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "nilkw")
        try
            records = drain(c, 1)
            @test isempty(records[1].headers)
        finally
            close(c)
        end
    end

    @testset "commit_record by topic/partition/offset works" begin
        topic = fresh_topic()
        p = KafkaProducer(bootstrap)
        try
            produce(p, topic, 0, "k", "v"; headers = ["x" => "y"])
        finally
            close(p)
        end
        c = make_consumer(topic; group_suffix = "commit")
        try
            records = drain(c, 1)
            r = records[1]
            commit_record(c, r.topic.name, r.partition.id, r.offset)
            @test true # no exception thrown
        finally
            close(c)
        end
    end
end
