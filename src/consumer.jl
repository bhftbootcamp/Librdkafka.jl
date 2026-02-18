"""
    KafkaConsumer

Create a Kafka consumer connected to the given brokers.

After creation, call [`subscribe!`](@ref) (group-based) or [`assign!`](@ref)
(manual partition assignment) before calling [`poll`](@ref).

# Example
```julia
c = KafkaConsumer("localhost:9092"; group_id="my-group",
                  config=Dict(AUTO_OFFSET_RESET => "earliest"))
subscribe!(c, ["my-topic"])
records = poll(c; timeout_ms=1000)
close(c)
```
"""
mutable struct KafkaConsumer
    id::Int
    bootstrap_servers::String
    group_id::Union{Nothing,String}
    closed::Bool
    subscription_log_mode::Symbol # :none | :subscribe | :assign
    subscribed_topics::Vector{Topic}
    assignment::Union{Nothing,Assignment}

    function KafkaConsumer(bootstrap_servers::AbstractString;
                           group_id::Union{Nothing,AbstractString}=nothing,
                           config::AbstractDict=Dict{String,String}())
        gid = group_id === nothing ? nothing : String(group_id)
        props_id = _build_properties(bootstrap_servers; group_id=gid, config=config)
        id = _B.create_kafka_consumer(props_id)
        bs = String(bootstrap_servers)
        id == 0 && throw(ErrorException("Failed to create KafkaConsumer (native returned null handle)."))
        _flush_native_logs!()
        c = new(Int(id), bs, gid, false, :none, Topic[], nothing)
        finalizer(close, c)
        return c
    end
end

Base.isopen(c::KafkaConsumer) = !c.closed

function Base.show(io::IO, c::KafkaConsumer)
    group = c.group_id === nothing ? "no-group" : c.group_id
    state = c.closed ? "closed" : "open"
    print(io, "KafkaConsumer(", c.bootstrap_servers,
          ", group=", group, ", id=", c.id, ", ", state, ")")
end

@inline function _checkopen(c::KafkaConsumer)
    c.closed && _closed(:KafkaConsumer, c.id)
    return nothing
end

@inline function _checkready(c::KafkaConsumer)
    c.subscription_log_mode == :none && throw(ArgumentError("KafkaConsumer is not subscribed/assigned. Call subscribe!(...) or assign!(...) first."))
    return nothing
end

function Base.close(c::KafkaConsumer)
    c.closed && return nothing
    _B.consumer_close(c.id)
    _flush_native_logs!()
    c.closed = true
    return nothing
end

"""
    subscribe!(

Subscribe to one or more topics using Kafka's group protocol.
`topics` may be a `Vector{Topic}`, `Vector{String}`, or a single `String`.
"""
function subscribe!(c::KafkaConsumer, topics::Vector{Topic})
    _checkopen(c)
    isempty(topics) && throw(ArgumentError("At least one topic is required."))
    names = [t.name for t in topics]
    topics_set = _B.make_topics_set(names)
    _B.consumer_subscribe(c.id, topics_set)
    _flush_native_logs!()
    c.subscription_log_mode = :subscribe
    c.subscribed_topics = topics
    c.assignment = nothing
    return c
end

subscribe!(c::KafkaConsumer, topics::AbstractVector{<:AbstractString}) =
    subscribe!(c, Topic.(topics))
subscribe!(c::KafkaConsumer, topic::AbstractString) =
    subscribe!(c, [Topic(topic)])

"""
    assign!

Manually assign a specific topic-partition (and optional offset) to the consumer.
"""
function assign!(c::KafkaConsumer, a::Assignment)
    _checkopen(c)
    tp = a.topic_partition
    _B.consumer_assign(c.id, tp.topic.name, tp.partition.id, a.offset)
    _flush_native_logs!()
    c.subscription_log_mode = :assign
    c.subscribed_topics = Topic[]
    c.assignment = a
    return c
end

assign!(c::KafkaConsumer, tp::TopicPartition; offset::Integer=RD_KAFKA_OFFSET_INVALID) =
    assign!(c, Assignment(tp; offset=offset))

assign!(c::KafkaConsumer, topic::AbstractString, partition::Integer;
        offset::Integer=RD_KAFKA_OFFSET_INVALID) =
    assign!(c, TopicPartition(topic, partition); offset=offset)

"""
    poll

Poll for new messages. Returns an empty vector if nothing is available within
`timeout_ms` milliseconds.
"""
function poll(c::KafkaConsumer; timeout_ms::Integer=1000)
    _checkopen(c)
    _checkready(c)
    timeout_ms < 0 && throw(DomainError(timeout_ms, "timeout_ms must be non-negative."))
    raw = _B.consumer_poll(c.id, Int(timeout_ms))
    _flush_native_logs!()
    isempty(raw) && return ConsumerRecord[]
    return _parse_records(raw)
end

"""
    commit

Synchronously commit the current consumer offsets for all assigned partitions.
"""
function commit(c::KafkaConsumer)
    _checkopen(c)
    _checkready(c)
    err = _B.consumer_commit_sync(c.id)
    _flush_native_logs!()
    err == -1 && throw(InvalidStateException("Consumer handle is invalid (maybe closed).", :closed))
    err != 0 && throw(ErrorException("Offset commit failed (error_code=$(err))."))
    return c
end

"""
    commit_record

Commit the offset of a specific record.
"""
function commit_record(c::KafkaConsumer, r::ConsumerRecord)
    _checkopen(c)
    _checkready(c)
    _B.consumer_commit_record(c.id, r.topic.name, r.partition.id, r.offset)
    _flush_native_logs!()
    return c
end

commit_record(c::KafkaConsumer, topic::AbstractString, partition::Integer, offset::Integer) =
    commit_record(c, ConsumerRecord(Topic(topic), Partition(partition), Int(offset), "", UInt8[], 0))

"""
    seek_to_beginning!

Seek the consumer to the beginning of the given partition.
"""
function seek_to_beginning!(c::KafkaConsumer, tp::TopicPartition)
    _checkopen(c)
    _checkready(c)
    _B.consumer_seek_to_beginning(c.id, tp.topic.name, tp.partition.id)
    _flush_native_logs!()
    return c
end

seek_to_beginning!(c::KafkaConsumer, topic::AbstractString, partition::Integer) =
    seek_to_beginning!(c, TopicPartition(topic, partition))

"""
    seek_to_end!

Seek the consumer to the end (latest offset) of the given partition.
"""
function seek_to_end!(c::KafkaConsumer, tp::TopicPartition)
    _checkopen(c)
    _checkready(c)
    _B.consumer_seek_to_end(c.id, tp.topic.name, tp.partition.id)
    _flush_native_logs!()
    return c
end

seek_to_end!(c::KafkaConsumer, topic::AbstractString, partition::Integer) =
    seek_to_end!(c, TopicPartition(topic, partition))

"""
    log_level!

Set the librdkafka log verbosity level (0-7) for this consumer.
"""
function log_level!(c::KafkaConsumer, level::Integer)
    _checkopen(c)
    _B.consumer_set_log_level(c.id, Int(level))
    _flush_native_logs!()
    return c
end
