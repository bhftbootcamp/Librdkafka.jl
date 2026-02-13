using Logging: @logmsg, Error, Warn, Info, Debug

const _NATIVE_LOG_SEPARATOR = '\x1f'
const _NATIVE_LOG_TIMER = Ref{Union{Nothing,Timer}}(nothing)

_native_log_level(level::Integer) = level <= 3 ? Error : level == 4 ? Warn : level <= 6 ? Info : Debug

function _emit_native_log_entry!(entry::AbstractString)
    parts = split(String(entry), _NATIVE_LOG_SEPARATOR; limit=4)
    if length(parts) != 4
        @logmsg Warn "Malformed native log entry" entry=String(entry)
        return nothing
    end

    level = try
        parse(Int, parts[1])
    catch
        6
    end

    file = isempty(parts[2]) ? "LIBRDKAFKA" : parts[2]
    line = try
        parse(Int, parts[3])
    catch
        1
    end
    msg = parts[4]

    @logmsg _native_log_level(level) msg _file=file _line=max(line, 1)
    return nothing
end

function _drain_native_logs!()
    entries = _B.logging_drain()
    isempty(entries) && return nothing
    for entry in entries
        _emit_native_log_entry!(entry)
    end
    return nothing
end

function _drain_native_logs_safe!()
    try
        _drain_native_logs!()
    catch
    end
    return nothing
end

_flush_native_logs!() = _drain_native_logs_safe!()

function _start_native_log_timer!()
    _NATIVE_LOG_TIMER[] === nothing || return nothing
    _NATIVE_LOG_TIMER[] = Timer(_ -> _drain_native_logs_safe!(), 0.0; interval=0.05)
    return nothing
end

function _stop_native_log_timer!()
    t = _NATIVE_LOG_TIMER[]
    t === nothing && return nothing
    close(t)
    _NATIVE_LOG_TIMER[] = nothing
    return nothing
end

function disable_logs!()
    _B.logging_disable()
    _stop_native_log_timer!()
    return nothing
end

log_format!(format::AbstractString=DEFAULT_LOG_FORMAT) = (_B.logging_set_format(String(format)); nothing)

function log_stdout!()
    _B.logging_set_stdout()
    _stop_native_log_timer!()
    return nothing
end

function log_julia!()
    _B.logging_set_julia()
    _start_native_log_timer!()
    return nothing
end

function log_file!(path::AbstractString; append::Bool=true)
    ok = _B.logging_set_file(String(path), append)
    ok || throw(ErrorException("Failed to open log file for writing: $(path)"))
    _stop_native_log_timer!()
    return nothing
end

function enable_default_logs!()
    _B.logging_enable_default()
    _start_native_log_timer!()
    return nothing
end
