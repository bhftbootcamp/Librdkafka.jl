using Dates

"""
    KafkaProducer

Client handle for producing messages to Kafka.

## Fields
- `id::Int`: Native producer handle identifier.
- `bootstrap_servers::String`: Bootstrap broker list used to create the producer.
- `closed::Bool`: Whether the producer has been closed.
"""
mutable struct KafkaProducer
    id::Int
    bootstrap_servers::String
    closed::Bool
end

"""
    KafkaConsumer

Client handle for consuming messages from Kafka.

## Fields
- `id::Int`: Native consumer handle identifier.
- `bootstrap_servers::String`: Bootstrap broker list used to create the consumer.
- `group_id::Union{Nothing,String}`: Consumer group id, if configured.
- `closed::Bool`: Whether the consumer has been closed.
- `subscription_mode::Symbol`: `:none`, `:subscribe`, or `:assign`.
- `subscribed_topics::Vector{String}`: Topics passed to `subscribe`.
- `assigned_partitions::Vector{Tuple{String,Int}}`: Partitions assigned via `assign`.
"""
mutable struct KafkaConsumer
    id::Int
    bootstrap_servers::String
    group_id::Union{Nothing,String}
    closed::Bool
    subscription_mode::Symbol
    subscribed_topics::Vector{String}
    assigned_partitions::Vector{Tuple{String,Int}}
end

"""
    ConsumerRecord

Represents a Kafka message returned by `poll` or `poll_one`.

## Fields
- `topic::String`: Topic name.
- `partition::Int`: Partition number.
- `offset::Int`: Record offset.
- `key::String`: Message key (decoded from base64).
- `value::String`: Message value (decoded from base64).
- `timestamp_ms::Int`: Timestamp in milliseconds since Unix epoch.
"""
struct ConsumerRecord
    topic::String
    partition::Int
    offset::Int
    key::String
    value::String
    timestamp_ms::Int
end

function Base.show(io::IO, p::KafkaProducer)
    state = p.closed ? "closed" : "open"
    print(io, "KafkaProducer(", p.bootstrap_servers, ", id=", p.id, ", ", state, ")")
end

function Base.show(io::IO, c::KafkaConsumer)
    state = c.closed ? "closed" : "open"
    group = isnothing(c.group_id) ? "no-group" : c.group_id
    print(io, "KafkaConsumer(", c.bootstrap_servers, ", group=", group, ", id=", c.id, ", ", state, ")")
end

function Base.show(io::IO, r::ConsumerRecord)
    ts = Dates.unix2datetime(r.timestamp_ms / 1000)
    print(io, "Message(", r.topic, ":", r.partition, " @", r.offset,", key=\"", r.key, "\", value=\"", r.value, "\", ts=", ts, ")")
end
