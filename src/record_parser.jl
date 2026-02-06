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
