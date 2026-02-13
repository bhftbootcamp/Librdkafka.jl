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
        r = Librdkafka.ConsumerRecord(t, p, 42, "key", "value", 0)
        @test r.topic == t
        @test r.partition == p
        @test r.offset == 42
        @test r.key == "key"
        @test r.value == "value"
        @test r.timestamp_ms == 0
    end
end
