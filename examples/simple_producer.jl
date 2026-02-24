using Librdkafka

topic = "julia-demo"

p = KafkaProducer("localhost:9092")

try
    i = 1
    while true
        msg = "hello #$i from julia"
        produce(p, topic, 0, "key", msg)
        @info "Sent" msg
        i += 1
        sleep(1)
    end
finally
    close(p) # unreachable here, Ctrl+C to stop
end
