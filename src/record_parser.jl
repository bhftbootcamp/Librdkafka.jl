struct RecordParseError <: Exception
    pos::Int
    msg::String
end

Base.showerror(io::IO, e::RecordParseError) =
    print(io, "RecordParseError(pos=", e.pos, "): ", e.msg)

@noinline _parse_error(msg::AbstractString, pos::Int) =
    throw(RecordParseError(pos, String(msg)))

mutable struct _Cursor{V<:AbstractVector{UInt8}}
    raw::V
    i::Int
end

@inline function _read_u32_le(buf::AbstractVector{UInt8}, i::Int)
    i + 3 <= lastindex(buf) || _parse_error("unexpected end", i)
    @inbounds v = UInt32(buf[i]) | (UInt32(buf[i + 1]) << 8) | (UInt32(buf[i + 2]) << 16) | (UInt32(buf[i + 3]) << 24)
    return v, i + 4
end

@inline function _read_i32_le(buf::AbstractVector{UInt8}, i::Int)
    v, j = _read_u32_le(buf, i)
    return reinterpret(Int32, v), j
end

@inline function _read_i64_le(buf::AbstractVector{UInt8}, i::Int)
    i + 7 <= lastindex(buf) || _parse_error("unexpected end", i)
    @inbounds v = UInt64(buf[i]) |
                 (UInt64(buf[i + 1]) << 8) |
                 (UInt64(buf[i + 2]) << 16) |
                 (UInt64(buf[i + 3]) << 24) |
                 (UInt64(buf[i + 4]) << 32) |
                 (UInt64(buf[i + 5]) << 40) |
                 (UInt64(buf[i + 6]) << 48) |
                 (UInt64(buf[i + 7]) << 56)
    return reinterpret(Int64, v), i + 8
end

function _parse_one_record!(c::_Cursor)
    raw = c.raw
    i = c.i
    n = lastindex(raw)
    i > n && _parse_error("unexpected end", i)

    topic_len_u, i = _read_u32_le(raw, i)
    topic_len = Int(topic_len_u)
    i + topic_len - 1 <= n || _parse_error("unexpected end", i)
    topic = topic_len == 0 ? "" : String(view(raw, i:(i + topic_len - 1)))
    i += topic_len

    partition_i32, i = _read_i32_le(raw, i)
    partition = Int(partition_i32)
    partition < 0 && _parse_error("negative partition", i)

    offset_i64, i = _read_i64_le(raw, i)
    timestamp_i64, i = _read_i64_le(raw, i)

    key_len_u, i = _read_u32_le(raw, i)
    key_len = Int(key_len_u)
    i + key_len - 1 <= n || _parse_error("unexpected end", i)
    key = key_len == 0 ? "" : String(view(raw, i:(i + key_len - 1)))
    i += key_len

    value_len_u, i = _read_u32_le(raw, i)
    value_len = Int(value_len_u)
    i + value_len - 1 <= n || _parse_error("unexpected end", i)
    value = value_len == 0 ? UInt8[] : Vector{UInt8}(view(raw, i:(i + value_len - 1)))
    i += value_len

    c.i = i
    return ConsumerRecord(Topic(topic), Partition(partition), Int(offset_i64), key, value, Int(timestamp_i64))
end

function _parse_records(raw::AbstractVector{UInt8})
    records = ConsumerRecord[]
    isempty(raw) && return records
    c = _Cursor(raw, firstindex(raw))
    n = lastindex(raw)
    while c.i <= n
        push!(records, _parse_one_record!(c))
    end
    return records
end
