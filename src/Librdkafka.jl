module Librdkafka

export BOOTSTRAP_SERVERS,
    CLIENT_ID,
    GROUP_ID,
    AUTO_OFFSET_RESET,
    ENABLE_AUTO_COMMIT,
    RD_KAFKA_OFFSET_INVALID,
    RD_KAFKA_OFFSET_BEGINNING,
    RD_KAFKA_OFFSET_END

export KafkaProducer, KafkaConsumer
export Topic, Partition, TopicPartition, Assignment, ConsumerRecord, KafkaHeaders
export KafkaClientError, KafkaError

export produce
export subscribe!, assign!
export poll
export commit, commit_record
export seek_to_beginning!, seek_to_end!
export log_level!
export disable_logs!, log_format!, log_stdout!, log_julia!, log_file!, enable_default_logs!
export get_bootstrap_servers

include("constants.jl")

using Dates

struct Topic
    name::String
    function Topic(name::AbstractString)
        s = String(name)
        isempty(s) && throw(ArgumentError("Topic name must be non-empty."))
        new(s)
    end
end
Base.print(io::IO, t::Topic) = print(io, t.name)
Base.show(io::IO, t::Topic) = print(io, "Topic(\"", t.name, "\")")

struct Partition
    id::Int
    function Partition(id::Integer)
        i = Int(id)
        i < 0 && throw(DomainError(i, "Partition id must be non-negative."))
        i > typemax(Int32) && throw(DomainError(i, "Partition id exceeds Int32 range."))
        new(i)
    end
end
Base.print(io::IO, p::Partition) = print(io, p.id)
Base.show(io::IO, p::Partition) = print(io, "Partition(", p.id, ")")

struct TopicPartition
    topic::Topic
    partition::Partition
end
Base.print(io::IO, tp::TopicPartition) = print(io, tp.topic.name, ":", tp.partition.id)
Base.show(io::IO, tp::TopicPartition) = print(io, "TopicPartition(\"", tp.topic.name, "\", ", tp.partition.id, ")")
TopicPartition(topic::AbstractString, partition::Integer) = TopicPartition(Topic(topic), Partition(partition))

struct Assignment
    topic_partition::TopicPartition
    offset::Int
    function Assignment(tp::TopicPartition; offset::Integer = RD_KAFKA_OFFSET_INVALID)
        new(tp, Int(offset))
    end
end
Base.show(io::IO, a::Assignment) = print(io, "Assignment(", a.topic_partition, " @", a.offset, ")")

const KafkaHeaders = Vector{Pair{String,Vector{UInt8}}}
const _EMPTY_HEADERS = KafkaHeaders()
const _NO_TIMESTAMP_MS = -1

struct ConsumerRecord
    topic::Topic
    partition::Partition
    offset::Int
    key::String
    value::Vector{UInt8}
    timestamp_ms::Int
    headers::KafkaHeaders
end

ConsumerRecord(topic::Topic, partition::Partition, offset::Int, key::String,
               value::Vector{UInt8}, timestamp_ms::Int) =
    ConsumerRecord(topic, partition, offset, key, value, timestamp_ms, _EMPTY_HEADERS)

function Base.show(io::IO, r::ConsumerRecord)
    print(io, "ConsumerRecord(", r.topic.name, ":", r.partition.id, " @", r.offset,
        ", key=\"", r.key, "\", value_bytes=", length(r.value))
    isempty(r.headers) || print(io, ", headers=", length(r.headers))
    if r.timestamp_ms == _NO_TIMESTAMP_MS
        print(io, ", ts=n/a")
    else
        print(io, ", ts=", Dates.unix2datetime(r.timestamp_ms / 1000))
    end
    print(io, ")")
end

include("errors.jl")

@noinline _closed(kind::Symbol, id::Int) =
    _throw_error(:state, kind, "$(kind) is closed (id=$(id))."; details = _details(:reason => "closed"))

include("ffi/bindings.jl")
using .Bindings
const _B = Bindings

include("record_parser.jl")
include("properties.jl")
include("producer.jl")
include("consumer.jl")
include("logging.jl")

function __init__()
    enable_default_logs!()
end

function get_bootstrap_servers()
    v = _B.get_bootstrap_servers()
    return v isa AbstractString ? String(v) : string(v)
end

end
