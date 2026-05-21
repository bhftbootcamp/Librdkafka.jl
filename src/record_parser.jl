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

@inline function _write_u32_le!(buf::AbstractVector{UInt8}, i::Int, v::UInt32)
    @inbounds buf[i]     =  v        % UInt8
    @inbounds buf[i + 1] = (v >> 8)  % UInt8
    @inbounds buf[i + 2] = (v >> 16) % UInt8
    @inbounds buf[i + 3] = (v >> 24) % UInt8
    return i + 4
end

@inline _write_i32_le!(buf::AbstractVector{UInt8}, i::Int, v::Int32) =
    _write_u32_le!(buf, i, reinterpret(UInt32, v))

@inline function _write_i64_le!(buf::AbstractVector{UInt8}, i::Int, v::Int64)
    u = reinterpret(UInt64, v)
    @inbounds buf[i]     =  u        % UInt8
    @inbounds buf[i + 1] = (u >> 8)  % UInt8
    @inbounds buf[i + 2] = (u >> 16) % UInt8
    @inbounds buf[i + 3] = (u >> 24) % UInt8
    @inbounds buf[i + 4] = (u >> 32) % UInt8
    @inbounds buf[i + 5] = (u >> 40) % UInt8
    @inbounds buf[i + 6] = (u >> 48) % UInt8
    @inbounds buf[i + 7] = (u >> 56) % UInt8
    return i + 8
end

@inline function _take_string(raw::Vector{UInt8}, i::Int, len::Int)
    len == 0 && return ""
    GC.@preserve raw unsafe_string(pointer(raw, i), len)
end

@inline function _take_bytes(raw::Vector{UInt8}, i::Int, len::Int)
    len == 0 && return UInt8[]
    out = Vector{UInt8}(undef, len)
    GC.@preserve raw unsafe_copyto!(pointer(out), pointer(raw, i), len)
    return out
end

function _parse_one_record!(c::_Cursor{Vector{UInt8}})
    raw = c.raw
    i = c.i
    n = lastindex(raw)
    i > n && _parse_error("unexpected end", i)

    topic_len_u, i = _read_u32_le(raw, i)
    topic_len = Int(topic_len_u)
    i + topic_len - 1 <= n || _parse_error("unexpected end", i)
    topic = _take_string(raw, i, topic_len)
    i += topic_len

    partition_i32, i = _read_i32_le(raw, i)
    partition = Int(partition_i32)
    partition < 0 && _parse_error("negative partition", i)

    offset_i64, i = _read_i64_le(raw, i)
    timestamp_i64, i = _read_i64_le(raw, i)

    key_len_u, i = _read_u32_le(raw, i)
    key_len = Int(key_len_u)
    i + key_len - 1 <= n || _parse_error("unexpected end", i)
    key = _take_string(raw, i, key_len)
    i += key_len

    value_len_u, i = _read_u32_le(raw, i)
    value_len = Int(value_len_u)
    i + value_len - 1 <= n || _parse_error("unexpected end", i)
    value = _take_bytes(raw, i, value_len)
    i += value_len

    header_count_u, i = _read_u32_le(raw, i)
    header_count = Int(header_count_u)
    if header_count == 0
        c.i = i
        return ConsumerRecord(Topic(topic), Partition(partition), Int(offset_i64), key, value, Int(timestamp_i64), _EMPTY_HEADERS)
    end
    headers = KafkaHeaders()
    sizehint!(headers, header_count)
    for _ in 1:header_count
        hkey_len_u, i = _read_u32_le(raw, i)
        hkey_len = Int(hkey_len_u)
        i + hkey_len - 1 <= n || _parse_error("unexpected end", i)
        hkey = _take_string(raw, i, hkey_len)
        i += hkey_len

        hval_len_u, i = _read_u32_le(raw, i)
        hval_len = Int(hval_len_u)
        i + hval_len - 1 <= n || _parse_error("unexpected end", i)
        hval = _take_bytes(raw, i, hval_len)
        i += hval_len

        push!(headers, hkey => hval)
    end

    c.i = i
    return ConsumerRecord(Topic(topic), Partition(partition), Int(offset_i64), key, value, Int(timestamp_i64), headers)
end

function _parse_records(raw::Vector{UInt8})
    records = ConsumerRecord[]
    isempty(raw) && return records
    # Each record is at least ~32 bytes (lengths + tiny payload).
    sizehint!(records, max(8, length(raw) >> 7))
    c = _Cursor{Vector{UInt8}}(raw, firstindex(raw))
    n = lastindex(raw)
    while c.i <= n
        push!(records, _parse_one_record!(c))
    end
    return records
end
