using Librdkafka

function simple_consumer_no_commits_example()
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

    println("Listening for messages")
    while true
        try
            message_count = 0
            records = poll(consumer; timeout_ms=1000)

            for record in records
                message_count += 1

                println("Message $message_count:")
                println("   Topic: $(record.topic)")
                println("   Partition: $(record.partition)")
                println("   Offset: $(record.offset)")
                println("   Key: $(record.key)")
                println("   Value: $(record.value)")
                println("   Timestamp: $(record.timestamp_ms)")
                println()
            end

        catch e
            println("Error during consumption: $e")
            break
        end
    end

    close(consumer)
end

simple_consumer_no_commits_example()