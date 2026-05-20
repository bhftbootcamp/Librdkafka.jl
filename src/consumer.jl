"""
    KafkaConsumer

Create a Kafka consumer connected to the given brokers.

After creation, call [`subscribe!`](@ref) (group-based) or [`assign!`](@ref)
(manual partition assignment) before calling [`poll`](@ref).

# Example
```julia
c = KafkaConsumer("localhost:9092"; group_id = "my-group",
    config = Dict(AUTO_OFFSET_RESET => "earliest"))
subscribe!(c, ["my-topic"])
records = poll(c; timeout_ms = 1000)
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
    assignments::Vector{Assignment}

    function KafkaConsumer(bootstrap_servers::AbstractString;
        group_id::Union{Nothing,AbstractString} = nothing,
        config::AbstractDict = Dict{String,String}(),
    )
        gid = group_id === nothing ? nothing : String(group_id)
        props_id = _build_properties(bootstrap_servers; group_id = gid, config = config)
        id = _B.create_kafka_consumer(props_id)
        bs = String(bootstrap_servers)
        id == 0 && _throw_error(:state, :create_consumer, "Failed to create KafkaConsumer (native returned null handle).")
        _flush_native_logs!()
        c = new(Int(id), bs, gid, false, :none, Topic[], Assignment[])
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
    ok = _B.consumer_subscribe(c.id, topics_set)
    _flush_native_logs!()
    ok || _throw_error(:state, :consumer_subscribe, "Consumer handle is invalid (maybe closed).")
    c.subscription_log_mode = :subscribe
    c.subscribed_topics = topics
    c.assignments = Assignment[]
    return c
end

subscribe!(c::KafkaConsumer, topics::AbstractVector{<:AbstractString}) =
    subscribe!(c, Topic.(topics))
subscribe!(c::KafkaConsumer, topic::AbstractString) =
    subscribe!(c, [Topic(topic)])

"""
    assign!

Manually assign one or more topic-partitions (with optional offsets) to the consumer.
"""
function assign!(c::KafkaConsumer, assignments::AbstractVector{Assignment})
    _checkopen(c)
    isempty(assignments) && throw(ArgumentError("At least one assignment is required."))

    topics = String[]
    partitions = Int[]
    offsets = Int64[]
    for a in assignments
        tp = a.topic_partition
        push!(topics, tp.topic.name)
        push!(partitions, tp.partition.id)
        push!(offsets, Int64(a.offset))
    end

    ok = _B.consumer_assign_many(c.id, topics, partitions, offsets)
    _flush_native_logs!()
    ok || _throw_error(:state, :consumer_assign, "Consumer handle is invalid (maybe closed).")
    c.subscription_log_mode = :assign
    c.subscribed_topics = Topic[]
    c.assignments = collect(assignments)
    return c
end

assign!(c::KafkaConsumer, a::Assignment) = assign!(c, [a])

assign!(c::KafkaConsumer, tp::TopicPartition; offset::Integer = RD_KAFKA_OFFSET_INVALID) =
    assign!(c, Assignment(tp; offset = offset))

assign!(c::KafkaConsumer, topic::AbstractString, partition::Integer;
        offset::Integer = RD_KAFKA_OFFSET_INVALID) =
    assign!(c, TopicPartition(topic, partition); offset = offset)

"""
    poll

Poll for new messages. Returns an empty vector if nothing is available within
`timeout_ms` milliseconds.
"""
function poll(c::KafkaConsumer; timeout_ms::Integer = 1000)
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
    err == -1 && _throw_error(:state, :consumer_commit, "Consumer handle is invalid (maybe closed).")
    err != 0 && _throw_error(:operation, :consumer_commit, "Offset commit failed.",
        details = _details(:error_code => err))
    return c
end

"""
    commit_record

Commit the offset of a specific record.
"""
function commit_record(c::KafkaConsumer, r::ConsumerRecord)
    _checkopen(c)
    _checkready(c)
    ok = _B.consumer_commit_record(c.id, r.topic.name, r.partition.id, r.offset)
    _flush_native_logs!()
    ok || _throw_error(:state, :consumer_commit_record, "Consumer handle is invalid (maybe closed).")
    return c
end

commit_record(c::KafkaConsumer, topic::AbstractString, partition::Integer, offset::Integer) =
    commit_record(c, ConsumerRecord(Topic(topic), Partition(partition), Int(offset), "", UInt8[], 0, Pair{String,Vector{UInt8}}[]))

"""
    seek_to_beginning!

Seek the consumer to the beginning of the given partition.
"""
function seek_to_beginning!(c::KafkaConsumer, tp::TopicPartition)
    _checkopen(c)
    _checkready(c)
    ok = _B.consumer_seek_to_beginning(c.id, tp.topic.name, tp.partition.id)
    _flush_native_logs!()
    ok || _throw_error(:state, :consumer_seek_to_beginning, "Consumer handle is invalid (maybe closed).")
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
    ok = _B.consumer_seek_to_end(c.id, tp.topic.name, tp.partition.id)
    _flush_native_logs!()
    ok || _throw_error(:state, :consumer_seek_to_end, "Consumer handle is invalid (maybe closed).")
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
    ok = _B.consumer_set_log_level(c.id, Int(level))
    _flush_native_logs!()
    ok || _throw_error(:state, :consumer_set_log_level, "Consumer handle is invalid (maybe closed).")
    return c
end
