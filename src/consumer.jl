mutable struct KafkaConsumer
    id::Int
    bootstrap_servers::String
    group_id::Union{Nothing,String}
    closed::Bool
    log_mode::Symbol
    subscribed_topics::Vector{Topic}
    assignment::Union{Nothing,Assignment}

    function KafkaConsumer(bootstrap_servers::AbstractString; group_id::Union{Nothing,AbstractString}=nothing, config::AbstractDict=Dict())
        bs = String(bootstrap_servers)
        isempty(bs) && throw(ArgumentError("bootstrap_servers is empty."))
        gid = group_id === nothing ? nothing : String(group_id)
        props_id = _B.create_properties()
        _B.properties_put(props_id, BOOTSTRAP_SERVERS, bs)
        if gid !== nothing && !isempty(gid)
            _B.properties_put(props_id, GROUP_ID, gid)
        end
        for (k, v) in config
            _B.properties_put(props_id, string(k), string(v))
        end
        id = _B.create_kafka_consumer(props_id)
        id == 0 && throw(ErrorException("Failed to create KafkaConsumer (native returned null handle)."))
        c = new(Int(id), bs, gid, false, :none, Topic[], nothing)
        finalizer(close, c)
        return c
    end
end

Base.isopen(c::KafkaConsumer) = !c.closed

function Base.show(io::IO, c::KafkaConsumer)
    group = c.group_id === nothing ? "no-group" : c.group_id
    print(io, "KafkaConsumer(", c.bootstrap_servers, ", group=", group, ", id=", c.id, c.closed ? ", closed)" : ", open)")
end

@inline function _checkopen(c::KafkaConsumer)
    c.closed && _closed(:KafkaConsumer, c.id)
    return nothing
end

@inline function _checkready(c::KafkaConsumer)
    c.log_mode == :none && throw(ArgumentError("KafkaConsumer is not subscribed/assigned. Call subscribe!(...) or assign!(...) first."))
    return nothing
end

function Base.close(c::KafkaConsumer)
    c.closed && return nothing
    _B.consumer_close(c.id)
    c.closed = true
    return nothing
end

function subscribe!(c::KafkaConsumer, topics::Vector{Topic})
    _checkopen(c)
    isempty(topics) && throw(ArgumentError("No topics provided."))
    names = [t.name for t in topics]
    topics_set = _B.make_topics_set(names)
    _B.consumer_subscribe(c.id, topics_set)
    c.log_mode = :subscribe
    c.subscribed_topics = topics
    c.assignment = nothing
    return c
end

subscribe!(c::KafkaConsumer, topics::AbstractVector{<:AbstractString}) = subscribe!(c, Topic.(topics))
subscribe!(c::KafkaConsumer, topic::AbstractString) = subscribe!(c, [Topic(topic)])

function assign!(c::KafkaConsumer, a::Assignment)
    _checkopen(c)
    tp = a.topic_partition
    _B.consumer_assign(c.id, tp.topic.name, tp.partition.id, a.offset)
    c.log_mode = :assign
    c.subscribed_topics = Topic[]
    c.assignment = a
    return c
end

assign!(c::KafkaConsumer, tp::TopicPartition; offset::Integer=RD_KAFKA_OFFSET_INVALID) =
    assign!(c, Assignment(tp; offset=offset))

assign!(c::KafkaConsumer, topic::AbstractString, partition::Integer; offset::Integer=RD_KAFKA_OFFSET_INVALID) =
    assign!(c, TopicPartition(topic, partition); offset=offset)

function poll(c::KafkaConsumer; timeout_ms::Integer=1000)
    _checkopen(c)
    _checkready(c)
    timeout_ms < 0 && throw(DomainError(timeout_ms, "timeout_ms must be non-negative."))
    raw = _B.consumer_poll(c.id, Int(timeout_ms))
    isempty(raw) && return ConsumerRecord[]
    return _parse_records(raw)
end

function commit(c::KafkaConsumer)
    _checkopen(c)
    _checkready(c)
    err = _B.consumer_commit_sync(c.id)
    err == -1 && throw(InvalidStateException("Consumer handle is invalid (maybe closed).", :closed))
    err != 0 && throw(ErrorException("Offset commit failed (error_code=$(err))."))
    return c
end

function commit_record(c::KafkaConsumer, r::ConsumerRecord)
    _checkopen(c)
    _checkready(c)
    _B.consumer_commit_record(c.id, r.topic.name, r.partition.id, r.offset)
    return c
end

commit_record(c::KafkaConsumer, topic::AbstractString, partition::Integer, offset::Integer) =
    commit_record(c, ConsumerRecord(Topic(topic), Partition(partition), Int(offset), "", UInt8[], 0))

function seek_to_beginning!(c::KafkaConsumer, tp::TopicPartition)
    _checkopen(c)
    _checkready(c)
    _B.consumer_seek_to_beginning(c.id, tp.topic.name, tp.partition.id)
    return c
end

seek_to_beginning!(c::KafkaConsumer, topic::AbstractString, partition::Integer) =
    seek_to_beginning!(c, TopicPartition(topic, partition))

function seek_to_end!(c::KafkaConsumer, tp::TopicPartition)
    _checkopen(c)
    _checkready(c)
    _B.consumer_seek_to_end(c.id, tp.topic.name, tp.partition.id)
    return c
end

seek_to_end!(c::KafkaConsumer, topic::AbstractString, partition::Integer) =
    seek_to_end!(c, TopicPartition(topic, partition))

log_level!(c::KafkaConsumer, level::Integer) = (_checkopen(c); _B.consumer_set_log_level(c.id, Int(level)); c)
