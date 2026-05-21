"""
    Bindings

Thin Julia wrappers around the CxxWrap-generated native functions.  Every public
function here maps 1-to-1 to a C++ export from `libkafka`; the wrapper exists
only to coerce Julia argument types into the concrete types the C++ side expects.
"""
module Bindings

export create_properties,
    properties_put,
    properties_destroy

export create_kafka_producer,
    producer_close,
    produce,
    producer_set_log_level

export create_kafka_consumer,
    consumer_close,
    consumer_subscribe,
    consumer_assign_many,
    consumer_poll,
    consumer_commit_sync,
    consumer_commit_record,
    consumer_seek_to_beginning,
    consumer_seek_to_end,
    consumer_set_log_level

export logging_disable,
    logging_set_format,
    logging_set_stdout,
    logging_set_julia,
    logging_set_file,
    logging_drain,
    logging_enable_default

export get_bootstrap_servers,
    make_topics_set

include("native.jl")
using .Native

create_properties() = Native.create_properties()
properties_put(props_id, key::AbstractString, val::AbstractString) =
    Native.properties_put(props_id, String(key), String(val))
properties_destroy(props_id) = Native.properties_destroy(props_id)

create_kafka_producer(props_id) = Native.create_kafka_producer(props_id)
producer_close(id::Integer) = Native.producer_close(Int(id))

produce(id::Integer, topic::AbstractString, partition::Integer, key::AbstractString,
        value::Vector{UInt8}, headers_blob::Vector{UInt8}) =
    Native.produce(Int(id), String(topic), Int(partition), String(key), value, headers_blob)

producer_set_log_level(id::Integer, level::Integer) =
    Native.producer_set_log_level(Int(id), Int(level))

create_kafka_consumer(props_id) = Native.create_kafka_consumer(props_id)
consumer_close(id::Integer) = Native.consumer_close(Int(id))

consumer_subscribe(id::Integer, topics_set) =
    Native.consumer_subscribe(Int(id), topics_set)

consumer_assign_many(id::Integer, topics::Vector{String}, partitions::Vector{Int32}, offsets::Vector{Int64}) =
    Native.consumer_assign_many(Int(id),
                                Native.to_std_vector_string(topics),
                                Native.to_std_vector_int32(partitions),
                                Native.to_std_vector_longlong(offsets))

function consumer_poll(id::Integer, timeout_ms::Integer)
    raw = Native.consumer_poll_raw(Int(id), Int(timeout_ms))
    return Vector{UInt8}(raw)
end

consumer_commit_sync(id::Integer) = Native.consumer_commit_sync(Int(id))

consumer_commit_record(id::Integer, topic::AbstractString, partition::Integer, offset::Integer) =
    Native.consumer_commit_record(Int(id), String(topic), Int(partition), Int(offset))

consumer_seek_to_beginning(id::Integer, topic::AbstractString, partition::Integer) =
    Native.consumer_seek_to_beginning(Int(id), String(topic), Int(partition))

consumer_seek_to_end(id::Integer, topic::AbstractString, partition::Integer) =
    Native.consumer_seek_to_end(Int(id), String(topic), Int(partition))

consumer_set_log_level(id::Integer, level::Integer) =
    Native.consumer_set_log_level(Int(id), Int(level))

logging_disable() = Native.logging_disable()
logging_set_format(fmt::AbstractString) = Native.logging_set_format(String(fmt))
logging_set_stdout() = Native.logging_set_stdout()
logging_set_julia() = Native.logging_set_julia()
logging_set_file(path::AbstractString, append::Bool) = Native.logging_set_file(String(path), append)
logging_drain() = Native.logging_drain()
logging_enable_default() = Native.logging_enable_default()

get_bootstrap_servers() = Native.get_bootstrap_servers()

end
