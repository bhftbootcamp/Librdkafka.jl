struct RecordParseError <: Exception
    pos::Int
    msg::String
end

Base.showerror(io::IO, e::RecordParseError) =
    print(io, "RecordParseError(pos=", e.pos, "): ", e.msg)

@noinline _parse_error(msg::AbstractString, pos::Int) =
    throw(RecordParseError(pos, String(msg)))

@inline _base64decode_bytes(s::AbstractString) = base64decode(Vector{UInt8}(codeunits(s)))

@inline function _decode_base64_string(s::AbstractString)
    isempty(s) && return ""
    bytes = _base64decode_bytes(s)
    try
        return String(bytes)
    catch
        return String(Char.(bytes))
    end
end

@inline function _parse_int_field(field::AbstractString, field_name::Symbol, pos::Int)
    try
        return parse(Int, field)
    catch
        _parse_error("malformed $(field_name): $(repr(field))", pos)
    end
end

@inline function _isdigit_range(raw::AbstractString, a::Int, b::Int)
    a > b && return false
    i = a
    while i <= b
        isdigit(raw[i]) || return false
        i = nextind(raw, i)
    end
    return true
end

function _find_record_end_with_ts(raw::AbstractString, value_start::Int)
    newline = findnext('\n', raw, value_start)
    while newline !== nothing
        last_tab = findprev('\t', raw, prevind(raw, newline))
        if last_tab !== nothing && last_tab >= value_start
            ts_a = nextind(raw, last_tab)
            ts_b = prevind(raw, newline)
            if _isdigit_range(raw, ts_a, ts_b)
                return (newline::Int, last_tab::Int)
            end
        end
        newline = findnext('\n', raw, nextind(raw, newline))
    end
    _parse_error("could not find record terminator with timestamp", value_start)
end

mutable struct _Cursor{S<:AbstractString}
    raw::S
    i::Int
end

function _parse_one_record!(c::_Cursor)
    raw = c.raw
    i = c.i
    n = lastindex(raw)
    i > n && _parse_error("unexpected end", i)
    t1 = findnext('\t', raw, i);                t1 === nothing && _parse_error("expected tab (t1)", i)
    t2 = findnext('\t', raw, nextind(raw, t1)); t2 === nothing && _parse_error("expected tab (t2)", t1)
    t3 = findnext('\t', raw, nextind(raw, t2)); t3 === nothing && _parse_error("expected tab (t3)", t2)
    t4 = findnext('\t', raw, nextind(raw, t3)); t4 === nothing && _parse_error("expected tab (t4)", t3)
    topic_ss = SubString(raw, i, prevind(raw, t1))
    isempty(topic_ss) && _parse_error("empty topic", i)
    part_ss = SubString(raw, nextind(raw, t1), prevind(raw, t2))
    off_ss = SubString(raw, nextind(raw, t2), prevind(raw, t3))
    key_ss = SubString(raw, nextind(raw, t3), prevind(raw, t4))
    value_start = nextind(raw, t4)
    newline, last_tab = _find_record_end_with_ts(raw, value_start)
    value_ss = last_tab > value_start ? SubString(raw, value_start, prevind(raw, last_tab)) : ""
    ts_ss = SubString(raw, nextind(raw, last_tab), prevind(raw, newline))
    partition = _parse_int_field(part_ss, :partition, t1)
    partition < 0 && _parse_error("negative partition", t1)
    offset = _parse_int_field(off_ss, :offset, t2)
    timestamp_ms = _parse_int_field(ts_ss, :timestamp_ms, last_tab)
    key = _decode_base64_string(key_ss)
    value = _decode_base64_string(value_ss)
    rec = ConsumerRecord(Topic(String(topic_ss)),
                         Partition(partition),
                         offset, key, value, timestamp_ms)
    c.i = nextind(raw, newline)
    return rec
end

function _parse_records(raw::AbstractString)
    records = ConsumerRecord[]
    isempty(raw) && return records
    c = _Cursor(raw, firstindex(raw))
    n = lastindex(raw)
    while c.i <= n
        push!(records, _parse_one_record!(c))
    end
    return records
end

mutable struct _RawCursor
    raw::Vector{UInt8}
    i::Int
end

@inline function _read_u32_le(buf::Vector{UInt8}, i::Int)
    i + 3 <= lastindex(buf) || _parse_error("unexpected end", i)
    @inbounds v = UInt32(buf[i]) | (UInt32(buf[i + 1]) << 8) | (UInt32(buf[i + 2]) << 16) | (UInt32(buf[i + 3]) << 24)
    return v, i + 4
end

@inline function _read_i32_le(buf::Vector{UInt8}, i::Int)
    v, j = _read_u32_le(buf, i)
    return reinterpret(Int32, v), j
end

@inline function _read_i64_le(buf::Vector{UInt8}, i::Int)
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

function _parse_one_record_raw!(c::_RawCursor)
    raw = c.raw
    i = c.i
    n = lastindex(raw)
    i > n && _parse_error("unexpected end", i)

    topic_len_u, i = _read_u32_le(raw, i)
    topic_len = Int(topic_len_u)
    topic_len < 0 && _parse_error("negative topic length", i)
    i + topic_len - 1 <= n || _parse_error("unexpected end", i)
    topic = topic_len == 0 ? "" : String(copy(@view raw[i:(i + topic_len - 1)]))
    i += topic_len

    partition_i32, i = _read_i32_le(raw, i)
    partition = Int(partition_i32)
    partition < 0 && _parse_error("negative partition", i)

    offset_i64, i = _read_i64_le(raw, i)
    timestamp_i64, i = _read_i64_le(raw, i)

    key_len_u, i = _read_u32_le(raw, i)
    key_len = Int(key_len_u)
    key_len < 0 && _parse_error("negative key length", i)
    i + key_len - 1 <= n || _parse_error("unexpected end", i)
    key = key_len == 0 ? UInt8[] : copy(@view raw[i:(i + key_len - 1)])
    i += key_len

    value_len_u, i = _read_u32_le(raw, i)
    value_len = Int(value_len_u)
    value_len < 0 && _parse_error("negative value length", i)
    i + value_len - 1 <= n || _parse_error("unexpected end", i)
    value = value_len == 0 ? UInt8[] : copy(@view raw[i:(i + value_len - 1)])
    i += value_len

    c.i = i
    return ConsumerRecordRaw(Topic(topic), Partition(partition), Int(offset_i64), key, value, Int(timestamp_i64))
end

function _parse_records_raw(raw::Vector{UInt8})
    records = ConsumerRecordRaw[]
    isempty(raw) && return records
    c = _RawCursor(raw, firstindex(raw))
    n = lastindex(raw)
    while c.i <= n
        push!(records, _parse_one_record_raw!(c))
    end
    return records
end
