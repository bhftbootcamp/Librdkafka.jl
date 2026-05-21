# The C++ side can buffer log entries as ASCII-delimited strings
# (level \x1f file \x1f line \x1f message).  A background Timer drains them
# into Julia's Logging infrastructure.

using Logging: @logmsg, Error, Warn, Info, Debug

const _NATIVE_LOG_SEPARATOR = '\x1f'
const _NATIVE_LOG_TIMER = Ref{Union{Nothing,Timer}}(nothing)
const _NATIVE_LOG_TIMER_LOCK = ReentrantLock()

_native_log_level(level::Integer) =
    level <= 3 ? Error : level == 4 ? Warn : level <= 6 ? Info : Debug

function _emit_native_log_entry!(entry::AbstractString)
    parts = split(String(entry), _NATIVE_LOG_SEPARATOR; limit = 4)
    if length(parts) != 4
        @logmsg Warn "Malformed native log entry" entry = String(entry)
        return nothing
    end

    level = tryparse(Int, parts[1])
    level === nothing && (level = 6)

    file = isempty(parts[2]) ? "LIBRDKAFKA" : String(parts[2])
    line = tryparse(Int, parts[3])
    line = line === nothing ? 1 : max(line, 1)

    @logmsg _native_log_level(level) parts[4] _file = file _line = line
    return nothing
end

function _drain_native_logs!()
    entries = _B.logging_drain()
    for entry in entries
        _emit_native_log_entry!(entry)
    end
    return nothing
end

function _flush_native_logs!()
    try
        _drain_native_logs!()
    catch e
        @logmsg Warn "Failed to flush native logs" exception = (e, catch_backtrace())
    end
    return nothing
end

function _start_native_log_timer!()
    @lock _NATIVE_LOG_TIMER_LOCK begin
        _NATIVE_LOG_TIMER[] === nothing || return nothing
        _NATIVE_LOG_TIMER[] = Timer(_ -> _flush_native_logs!(), 0.0; interval = 0.05)
    end
    return nothing
end

function _stop_native_log_timer!()
    @lock _NATIVE_LOG_TIMER_LOCK begin
        t = _NATIVE_LOG_TIMER[]
        t === nothing && return nothing
        close(t)
        _NATIVE_LOG_TIMER[] = nothing
    end
    return nothing
end

"""
    disable_logs!

Stop all native log output and tear down the background log timer.
"""
function disable_logs!()
    _B.logging_disable()
    _stop_native_log_timer!()
    return nothing
end

"""
    log_format!

Set the native log format string.
"""
log_format!(format::AbstractString = DEFAULT_LOG_FORMAT) =
    (_B.logging_set_format(String(format)); nothing)

"""
    log_stdout!

Route native logs directly to stdout (bypasses Julia logging).
"""
function log_stdout!()
    _B.logging_set_stdout()
    _stop_native_log_timer!()
    return nothing
end

"""
    log_julia!

Route native logs through Julia's `Logging` framework.
"""
function log_julia!()
    _B.logging_set_julia()
    _start_native_log_timer!()
    return nothing
end

"""
    log_file!

Route native logs to a file at `path`.
"""
function log_file!(path::AbstractString; append::Bool = true)
    ok = _B.logging_set_file(String(path), append)
    ok || _throw_error(:operation, :logging_set_file, "Failed to open log file for writing.",
        details = _details(:path => String(path), :append => append))
    _stop_native_log_timer!()
    return nothing
end

"""
    enable_default_logs!

Restore the default logging behaviour (Julia bridge with background timer).
"""
function enable_default_logs!()
    _B.logging_enable_default()
    _start_native_log_timer!()
    return nothing
end
