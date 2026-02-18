"""
    KafkaProducer

Create a Kafka producer connected to the given brokers.

The `config` dictionary accepts any librdkafka configuration property as
`String => String` pairs.  The producer is automatically closed by a finalizer
but can also be closed explicitly with `close(p)`.

# Example
```julia
p = KafkaProducer("localhost:9092")
produce(p, "my-topic", 0, "key", "hello")
close(p)
```
"""
mutable struct KafkaProducer
    id::Int
    bootstrap_servers::String
    closed::Bool

    function KafkaProducer(bootstrap_servers::AbstractString; config::AbstractDict=Dict{String,String}())
        props_id = _build_properties(bootstrap_servers; config=config)
        id = _B.create_kafka_producer(props_id)
        id == 0 && throw(ErrorException("Failed to create KafkaProducer (native returned null handle)."))
        _flush_native_logs!()
        p = new(Int(id), String(bootstrap_servers), false)
        finalizer(close, p)
        return p
    end
end

Base.isopen(p::KafkaProducer) = !p.closed

function Base.show(io::IO, p::KafkaProducer)
    state = p.closed ? "closed" : "open"
    print(io, "KafkaProducer(", p.bootstrap_servers, ", id=", p.id, ", ", state, ")")
end

@inline function _checkopen(p::KafkaProducer)
    p.closed && _closed(:KafkaProducer, p.id)
    return nothing
end

function Base.close(p::KafkaProducer)
    p.closed && return nothing
    _B.producer_close(p.id)
    _flush_native_logs!()
    p.closed = true
    return nothing
end

"""
    produce

Send a message to Kafka.  `value` may be raw bytes or a UTF-8 string.
`topic` and `partition` accept both typed (`Topic`/`Partition`) and plain values.
"""
function produce(p::KafkaProducer, topic::Topic, partition::Partition,
                 key::AbstractString, value::Vector{UInt8})
    _checkopen(p)
    err = _B.produce(p.id, topic.name, partition.id, key, value)
    _flush_native_logs!()
    err_s = String(err)
    isempty(err_s) && return nothing
    throw(ErrorException("Kafka produce failed: $err_s"))
end

produce(p::KafkaProducer, topic::AbstractString, partition::Integer,
        key::AbstractString, value::Vector{UInt8}) =
    produce(p, Topic(topic), Partition(partition), key, value)

produce(p::KafkaProducer, topic, partition, key::AbstractString, value::AbstractString) =
    produce(p, topic, partition, key, Vector{UInt8}(codeunits(value)))

"""
    log_level!

Set the librdkafka log verbosity level (0-7) for this producer.
"""
function log_level!(p::KafkaProducer, level::Integer)
    _checkopen(p)
    _B.producer_set_log_level(p.id, Int(level))
    _flush_native_logs!()
    return p
end
