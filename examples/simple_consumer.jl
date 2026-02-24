using Librdkafka

topic = "julia-demo"

c = KafkaConsumer(
    "localhost:9092";
    group_id = "julia-demo-consumer",
    config = Dict(
        AUTO_OFFSET_RESET => "earliest",
        ENABLE_AUTO_COMMIT => "false",
    ),
)

subscribe!(c, [topic])

try
    while true
        for r in poll(c; timeout_ms=1000)
            @info "Got" key=r.key value=r.value offset=r.offset
            commit_record(c, r)
        end
    end
finally
    close(c) # unreachable here, Ctrl+C to stop
end
