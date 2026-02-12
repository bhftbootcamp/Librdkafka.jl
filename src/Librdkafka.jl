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
export Topic, Partition, TopicPartition, Assignment, ConsumerRecord

export produce
export subscribe!, assign!
export poll
export commit, commit_record
export seek_to_beginning!, seek_to_end!
export log_level!
export disable_logs!, log_format!, log_stdout!, log_file!, enable_default_logs!
export get_bootstrap_servers

const BOOTSTRAP_SERVERS = "bootstrap.servers"
const CLIENT_ID = "client.id"
const GROUP_ID = "group.id"
const AUTO_OFFSET_RESET = "auto.offset.reset"
const ENABLE_AUTO_COMMIT = "enable.auto.commit"

const RD_KAFKA_OFFSET_BEGINNING = -2
const RD_KAFKA_OFFSET_END = -1
const RD_KAFKA_OFFSET_INVALID = -1001
const DEFAULT_LOG_FORMAT = "{timestamp} [{level}] {message}"

using Dates

struct Topic
    name::String
    function Topic(name::AbstractString)
        s = String(name)
        isempty(s) && throw(ArgumentError("Topic name must be non-empty."))
        new(s)
    end
end
Base.show(io::IO, t::Topic) = print(io, t.name)

struct Partition
    id::Int
    function Partition(id::Integer)
        i = Int(id)
        i < 0 && throw(DomainError(i, "Partition id must be non-negative."))
        new(i)
    end
end
Base.show(io::IO, p::Partition) = print(io, p.id)

struct TopicPartition
    topic::Topic
    partition::Partition
end
Base.show(io::IO, tp::TopicPartition) = print(io, tp.topic.name, ":", tp.partition.id)
TopicPartition(topic::AbstractString, partition::Integer) = TopicPartition(Topic(topic), Partition(partition))

struct Assignment
    topic_partition::TopicPartition
    offset::Int
    function Assignment(tp::TopicPartition; offset::Integer=RD_KAFKA_OFFSET_INVALID)
        new(tp, Int(offset))
    end
end
Base.show(io::IO, a::Assignment) = print(io, a.topic_partition, " @", a.offset)

struct ConsumerRecord
    topic::Topic
    partition::Partition
    offset::Int
    key::String
    value::Vector{UInt8}
    timestamp_ms::Int
end

function Base.show(io::IO, r::ConsumerRecord)
    ts = Dates.unix2datetime(r.timestamp_ms / 1000)
    print(io, "ConsumerRecord(", r.topic.name, ":", r.partition.id, " @", r.offset,
          ", key=\"", r.key, "\", value_bytes=", length(r.value), ", ts=", ts, ")")
end

@noinline _closed(kind::Symbol, id::Int) =
    throw(InvalidStateException("$(kind) is closed (id=$(id)).", :closed))

include("ffi/bindings.jl")
using .Bindings
const _B = Bindings

include("record_parser.jl")
include("producer.jl")
include("consumer.jl")
include("logging.jl")

function get_bootstrap_servers()
    v = _B.get_bootstrap_servers()
    return v isa AbstractString ? String(v) : string(v)
end

end
