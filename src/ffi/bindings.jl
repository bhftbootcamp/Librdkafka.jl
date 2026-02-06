module Bindings

export create_properties,
    properties_put

export create_kafka_producer,
    producer_close,
    produce,
    produce_binary,
    producer_set_log_level

export create_kafka_consumer,
    consumer_close,
    consumer_subscribe,
    consumer_assign,
    consumer_poll,
    consumer_commit_sync,
    consumer_commit_record,
    consumer_seek_to_beginning,
    consumer_set_log_level

export logging_disable,
    logging_set_format,
    logging_set_stdout,
    logging_set_file,
    logging_enable_default

export get_bootstrap_servers,
    make_topics_set

include("native.jl")
using .Native

const _FN_CACHE = Dict{Symbol,Function}()

@inline function _raw(sym::Symbol)
    get!(_FN_CACHE, sym) do
        getfield(Native, sym)
    end
end

@inline _cpp_call(sym::Symbol, args...) = (_raw(sym))(args...)

create_properties() = _cpp_call(:create_properties)
properties_put(props_id, key::AbstractString, val::AbstractString) =
    _cpp_call(:properties_put, props_id, String(key), String(val))

create_kafka_producer(props_id) = _cpp_call(:create_kafka_producer, props_id)
producer_close(id::Integer) = _cpp_call(:producer_close, Int(id))

produce(id::Integer, topic::AbstractString, partition::Integer, key::AbstractString, value::AbstractString) =
    _cpp_call(:produce, Int(id), String(topic), Int(partition), String(key), String(value))

produce_binary(id::Integer, topic::AbstractString, partition::Integer, key::AbstractString, value::Vector{UInt8}) =
    _cpp_call(:produce_binary, Int(id), String(topic), Int(partition), String(key), value)

producer_set_log_level(id::Integer, level::Integer) =
    _cpp_call(:producer_set_log_level, Int(id), Int(level))

create_kafka_consumer(props_id) = _cpp_call(:create_kafka_consumer, props_id)
consumer_close(id::Integer) = _cpp_call(:consumer_close, Int(id))

consumer_subscribe(id::Integer, topics_set) =
    _cpp_call(:consumer_subscribe, Int(id), topics_set)

consumer_assign(id::Integer, topic::AbstractString, partition::Integer, offset::Integer) =
    _cpp_call(:consumer_assign, Int(id), String(topic), Int(partition), Int(offset))

consumer_poll(id::Integer, timeout_ms::Integer) =
    _cpp_call(:consumer_poll, Int(id), Int(timeout_ms))

consumer_commit_sync(id::Integer) = _cpp_call(:consumer_commit_sync, Int(id))

consumer_commit_record(id::Integer, topic::AbstractString, partition::Integer, offset::Integer) =
    _cpp_call(:consumer_commit_record, Int(id), String(topic), Int(partition), Int(offset))

consumer_seek_to_beginning(id::Integer, topic::AbstractString, partition::Integer) =
    _cpp_call(:consumer_seek_to_beginning, Int(id), String(topic), Int(partition))

consumer_set_log_level(id::Integer, level::Integer) =
    _cpp_call(:consumer_set_log_level, Int(id), Int(level))

logging_disable() = _cpp_call(:logging_disable)
logging_set_format(fmt::AbstractString) = _cpp_call(:logging_set_format, String(fmt))
logging_set_stdout() = _cpp_call(:logging_set_stdout)
logging_set_file(path::AbstractString, append::Bool) = _cpp_call(:logging_set_file, String(path), append)
logging_enable_default() = _cpp_call(:logging_enable_default)

get_bootstrap_servers() = _cpp_call(:get_bootstrap_servers)

end
