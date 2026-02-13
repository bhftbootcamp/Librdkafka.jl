mutable struct KafkaProducer
    id::Int
    bootstrap_servers::String
    closed::Bool

    function KafkaProducer(bootstrap_servers::AbstractString; config::AbstractDict=Dict())
        bs = String(bootstrap_servers)
        isempty(bs) && throw(ArgumentError("bootstrap_servers is empty."))
        props_id = _B.create_properties()
        _B.properties_put(props_id, BOOTSTRAP_SERVERS, bs)
        for (k, v) in config
            _B.properties_put(props_id, string(k), string(v))
        end
        id = _B.create_kafka_producer(props_id)
        id == 0 && throw(ErrorException("Failed to create KafkaProducer (native returned null handle)."))
        _flush_native_logs!()
        p = new(Int(id), bs, false)
        finalizer(close, p)
        return p
    end
end

Base.isopen(p::KafkaProducer) = !p.closed

function Base.show(io::IO, p::KafkaProducer)
    print(io, "KafkaProducer(", p.bootstrap_servers, ", id=", p.id, p.closed ? ", closed)" : ", open)")
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

function produce(p::KafkaProducer, topic::Topic, partition::Partition, key::AbstractString, value::Vector{UInt8})
    _checkopen(p)
    err = _B.produce(p.id, topic.name, partition.id, key, value)
    _flush_native_logs!()
    err_s = String(err)
    isempty(err_s) && return nothing
    throw(ErrorException("Kafka produce failed: $err_s"))
end

produce(p::KafkaProducer, topic::AbstractString, partition::Integer, key::AbstractString, value::Vector{UInt8}) =
    produce(p, Topic(topic), Partition(partition), key, value)

log_level!(p::KafkaProducer, level::Integer) = (_checkopen(p); _B.producer_set_log_level(p.id, Int(level)); _flush_native_logs!(); p)
