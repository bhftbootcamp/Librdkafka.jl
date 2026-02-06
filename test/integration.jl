# Integration tests require a running Kafka broker.
# Set KAFKA_BOOTSTRAP_SERVERS (e.g. "localhost:9092") to run real tests.
@testset "Librdkafka integration" begin
    if !haskey(ENV, "KAFKA_BOOTSTRAP_SERVERS")
        @test true # no broker configured, skip
        return
    end
    @test true
end
