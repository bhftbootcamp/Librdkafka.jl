using Librdkafka

topic = "julia-demo"

consumer = KafkaConsumer(
    "localhost:9092";
    group_id = "julia-consumer-group",
    config = Dict(
        CLIENT_ID => "julia-consumer",
        AUTO_OFFSET_RESET => "earliest",
        ENABLE_AUTO_COMMIT => "false",
        "enable.auto.offset.store" => "false",
    ),
)

subscribe(consumer, ["quickstart-events"])
try
    while true
        for r in poll(consumer; timeout_ms=1000)
            @info "Got" key=r.key value=r.value offset=r.offset
        end
    end
catch e
    println("Error during consumption: $e")
finally
    close(consumer) # unreachable here, Ctrl+C to stop
end
