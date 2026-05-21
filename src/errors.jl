"""
    KafkaClientError <: Exception

Abstract base type for errors raised by Librdkafka.jl.
"""
abstract type KafkaClientError <: Exception end

"""
    KafkaError <: KafkaClientError

Represents a Kafka client error raised by the wrapper.

## Fields
- `kind::Symbol`: High-level error kind. Currently one of:
    * `:state`     — operation called on a closed/uninitialized handle, or
                     native handle construction failed.
    * `:operation` — the librdkafka call returned a runtime error (broker
                     failure, parse failure, protocol skew, etc.).
- `operation::Symbol`: Operation that failed (for example `:produce`, `:poll`).
- `message::String`: Human-readable error message.
- `details::Vector{String}`: Additional details used for diagnostics (rendered
  as `key=value` pairs).
"""
struct KafkaError <: KafkaClientError
    kind::Symbol
    operation::Symbol
    message::String
    details::Vector{String}
end

function Base.showerror(io::IO, e::KafkaError)
    print(io, "Kafka ", e.kind, " error during ", e.operation, ": ", e.message)
    if !isempty(e.details)
        print(io, " (", join(e.details, "; "), ")")
    end
end

_detail_value(v::AbstractVector) = "[" * join(string.(v), ", ") * "]"
_detail_value(v) = string(v)

function _details(pairs::Pair{Symbol, <:Any}...)
    out = String[]
    for (k, v) in pairs
        v === nothing && continue
        s = _detail_value(v)
        isempty(s) && continue
        push!(out, string(k, "=", s))
    end
    return out
end

function _throw_error(kind::Symbol, operation::Symbol, message::AbstractString; details::Vector{String} = String[])
    _flush_native_logs!()
    throw(KafkaError(kind, operation, String(message), details))
end
