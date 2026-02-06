disable_logs!() = (_B.logging_disable(); nothing)
log_format!(format::AbstractString=DEFAULT_LOG_FORMAT) = (_B.logging_set_format(String(format)); nothing)
log_stdout!() = (_B.logging_set_stdout(); nothing)

function log_file!(path::AbstractString; append::Bool=true)
    ok = _B.logging_set_file(String(path), append)
    ok || throw(ErrorException("Failed to open log file for writing: $(path)"))
    return nothing
end

enable_default_logs!() = (_B.logging_enable_default(); nothing)
