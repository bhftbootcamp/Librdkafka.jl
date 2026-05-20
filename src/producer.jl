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

    function KafkaProducer(bootstrap_servers::AbstractString; config::AbstractDict = Dict{String,String}())
        props_id = _build_properties(bootstrap_servers; config = config)
        id = _B.create_kafka_producer(props_id)
        id == 0 && _throw_error(:state, :create_producer, "Failed to create KafkaProducer (native returned null handle).")
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

_to_header_value(v::Vector{UInt8}) = v
_to_header_value(v::AbstractString) = Vector{UInt8}(codeunits(v))
_to_header_value(v::AbstractVector{UInt8}) = Vector{UInt8}(v)
_to_header_value(v) = Vector{UInt8}(string(v))

"""
    _normalize_headers(headers) -> KafkaHeaders

Convert user-friendly headers to the canonical `KafkaHeaders` format.
Values can be strings, numbers, or raw bytes — anything gets converted to `Vector{UInt8}`.
"""
function _normalize_headers(headers)::KafkaHeaders
    result = Pair{String,Vector{UInt8}}[]
    sizehint!(result, length(headers))
    for (k, v) in headers
        push!(result, String(k) => _to_header_value(v))
    end
    return result
end
_normalize_headers(headers::KafkaHeaders) = headers

"""
    _serialize_headers(headers) -> Vector{UInt8}

Serialize a `KafkaHeaders` list into the flat binary format expected by the C++ layer:
`u32_le(count) ++ N × (u32_le(key_len) ++ key_bytes ++ u32_le(val_len) ++ val_bytes)`.
"""
function _serialize_headers(headers::KafkaHeaders)
    n = 4
    for (k, v) in headers
        n += 4 + sizeof(k) + 4 + length(v)
    end
    buf = Vector{UInt8}(undef, n)
    pos = 1
    @inline function _put_u32_le!(val::UInt32)
        @inbounds buf[pos]     = val % UInt8
        @inbounds buf[pos + 1] = (val >> 8) % UInt8
        @inbounds buf[pos + 2] = (val >> 16) % UInt8
        @inbounds buf[pos + 3] = (val >> 24) % UInt8
        pos += 4
    end
    _put_u32_le!(UInt32(length(headers)))
    for (k, v) in headers
        kb = codeunits(k)
        _put_u32_le!(UInt32(length(kb)))
        copyto!(buf, pos, kb, 1, length(kb))
        pos += length(kb)
        _put_u32_le!(UInt32(length(v)))
        if !isempty(v)
            copyto!(buf, pos, v, 1, length(v))
            pos += length(v)
        end
    end
    return buf
end

"""
    produce

Send a message to Kafka.  `value` may be raw bytes or a UTF-8 string.
`topic` and `partition` accept both typed (`Topic`/`Partition`) and plain values.

Optional `headers` keyword accepts a list of `key => value` pairs.
Values can be `String`, `Vector{UInt8}`, numbers, or anything convertible to string.

# Example
```julia
produce(p, "topic", 0, "key", "payload";
    headers = ["content-type" => "application/json", "version" => 2])
```
"""
function produce(p::KafkaProducer, topic::Topic, partition::Partition,
    key::AbstractString, value::Vector{UInt8};
    headers = Pair{String,Vector{UInt8}}[],
)
    _checkopen(p)
    if isempty(headers)
        err = _B.produce(p.id, topic.name, partition.id, key, value)
    else
        norm = _normalize_headers(headers)
        err = _B.produce_with_headers(p.id, topic.name, partition.id, key, value,
                                       _serialize_headers(norm))
    end
    _flush_native_logs!()
    err_s = String(err)
    isempty(err_s) && return nothing
    _throw_error(:operation, :produce, "Kafka produce failed.",
        details = _details(:topic => topic.name, :partition => partition.id, :message => err_s))
end

produce(p::KafkaProducer, topic::AbstractString, partition::Integer,
    key::AbstractString, value::Vector{UInt8};
    headers = Pair{String,Vector{UInt8}}[],
) =
    produce(p, Topic(topic), Partition(partition), key, value; headers)

produce(p::KafkaProducer, topic, partition, key::AbstractString, value::AbstractString;
    headers = Pair{String,Vector{UInt8}}[],
) =
    produce(p, topic, partition, key, Vector{UInt8}(codeunits(value)); headers)

"""
    log_level!

Set the librdkafka log verbosity level (0-7) for this producer.
"""
function log_level!(p::KafkaProducer, level::Integer)
    _checkopen(p)
    ok = _B.producer_set_log_level(p.id, Int(level))
    _flush_native_logs!()
    ok || _throw_error(:state, :producer_set_log_level, "Producer handle is invalid (maybe closed).")
    return p
end
