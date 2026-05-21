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

function _to_header_value(v::Integer)
    if typemin(Int64) <= v <= typemax(Int64)
        iv = Int64(v)
        neg = iv < 0
        u = neg ? (UInt64(-(iv + 1)) + 1) : UInt64(iv)  # safe at typemin(Int64)
        n = ndigits(u) + (neg ? 1 : 0)
        buf = Vector{UInt8}(undef, n)
        i = n
        @inbounds while u >= 10
            buf[i] = UInt8(u % 10) + 0x30
            u ÷= 10
            i -= 1
        end
        @inbounds buf[i] = UInt8(u) + 0x30
        neg && (@inbounds buf[1] = UInt8('-'))
        return buf
    end
    return Vector{UInt8}(string(v))
end
_to_header_value(v) = Vector{UInt8}(string(v))

function _normalize_headers(headers)::KafkaHeaders
    result = KafkaHeaders()
    sizehint!(result, length(headers))
    for (k, v) in headers
        push!(result, String(k) => _to_header_value(v))
    end
    return result
end
_normalize_headers(headers::KafkaHeaders) = headers

function _serialize_headers(headers::KafkaHeaders)
    n = 4
    for (k, v) in headers
        n += 4 + ncodeunits(k) + 4 + length(v)
    end
    buf = Vector{UInt8}(undef, n)
    pos = _write_u32_le!(buf, 1, UInt32(length(headers)))
    for (k, v) in headers
        kb = codeunits(k)
        kl = length(kb)
        pos = _write_u32_le!(buf, pos, UInt32(kl))
        copyto!(buf, pos, kb, 1, kl)
        pos += kl
        vl = length(v)
        pos = _write_u32_le!(buf, pos, UInt32(vl))
        copyto!(buf, pos, v, 1, vl)
        pos += vl
    end
    return buf
end

const _EMPTY_HEADERS_BLOB = UInt8[]

_headers_blob(::Nothing) = _EMPTY_HEADERS_BLOB
_headers_blob(headers) = isempty(headers) ?
    _EMPTY_HEADERS_BLOB :
    _serialize_headers(_normalize_headers(headers))

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
    headers = nothing,
)
    _checkopen(p)
    err = _B.produce(p.id, topic.name, partition.id, key, value, _headers_blob(headers))
    err_s = String(err)
    isempty(err_s) && return p
    _throw_error(:operation, :produce, "Kafka produce failed.",
        details = _details(:topic => topic.name, :partition => partition.id, :message => err_s))
end

produce(p::KafkaProducer, topic::AbstractString, partition::Integer,
    key::AbstractString, value::Vector{UInt8};
    headers = nothing,
) =
    produce(p, Topic(topic), Partition(partition), key, value; headers)

function produce(p::KafkaProducer, topic, partition, key::AbstractString, value::String;
    headers = nothing,
)
    n = ncodeunits(value)
    GC.@preserve value begin
        wrapped = unsafe_wrap(Vector{UInt8}, pointer(value), n; own = false)
        produce(p, topic, partition, key, wrapped; headers)
    end
end

produce(p::KafkaProducer, topic, partition, key::AbstractString, value::AbstractString;
    headers = nothing,
) =
    produce(p, topic, partition, key, Vector{UInt8}(codeunits(value)); headers)

"""
    log_level!

Set the librdkafka log verbosity level (0-7) for this producer.
"""
function log_level!(p::KafkaProducer, level::Integer)
    _checkopen(p)
    ok = _B.producer_set_log_level(p.id, Int(level))
    ok || _throw_error(:state, :producer_set_log_level, "Producer handle is invalid (maybe closed).")
    return p
end
