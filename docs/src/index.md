# Librdkafka.jl

Julia wrapper for [librdkafka](https://github.com/edenhill/librdkafka).

## Installation

If you haven't installed our local registry yet, do that first:

```
] registry add https://github.com/bhftbootcamp/Green.git
```

To install Librdkafka, simply use the Julia package manager:

```
] add Librdkafka
```

## Usage

### Producer

```julia
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
```

### Consumer

```julia
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
```

## Examples

Ready-to-run scripts are in `examples/`. See:
- `examples/simple_producer.jl`
- `examples/simple_consumer.jl`

## API

See the [Julia REPL help](https://docs.julialang.org/en/v1/stdlib/REPL/) (e.g. `?KafkaProducer`) for function and type documentation.

## License

MIT License. See [LICENSE](https://github.com/bhftbootcamp/Librdkafka.jl/blob/master/LICENSE) in the repository.
