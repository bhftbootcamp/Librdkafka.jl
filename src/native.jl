using CxxWrap
using Libdl
using librdkafka_jll
using CyrusSASL_jll

const _libkafka_name = "libkafka.$(Libdl.dlext)"
const _libkafka_candidates = (
    joinpath(@__DIR__, "..", "deps", "src", "build", "lib"),
    joinpath(@__DIR__, "..", "lib"),
)

function _locate_libkafka()
    for dir in _libkafka_candidates
        path = normpath(joinpath(dir, _libkafka_name))
        if isfile(path)
            return path
        end
    end
    error("Could not locate $(_libkafka_name). Run `cmake -S deps/src -B deps/src/build && cmake --build deps/src/build` first.")
end

const _dlopen_handles = Ref{Vector{Ptr{Nothing}}}(Ptr{Nothing}[])

function _ensure_native_deps_loaded()
    if !isempty(_dlopen_handles[])
        return
    end
    local_paths = (
        librdkafka_jll.librdkafka_path,
        CyrusSASL_jll.libsasl2_path,
    )
    for path in local_paths
        if !isempty(path) && isfile(path)
            handle = Libdl.dlopen(path, Libdl.RTLD_LAZY | Libdl.RTLD_DEEPBIND)
            push!(_dlopen_handles[], handle)
        end
    end
end

module NativeBindings
using CxxWrap
import .._locate_libkafka
@wrapmodule(_locate_libkafka)
function __init__()
    @initcxx
end
end

using .NativeBindings: create_properties, properties_put, create_producer_record, producer_record_topic,
    create_kafka_producer, create_kafka_consumer, producer_close, consumer_close,
    consumer_subscribe, consumer_poll, consumer_commit_sync,
    consumer_assign, consumer_seek_to_beginning, consumer_commit_record,
    producer_set_log_level, consumer_set_log_level,
    get_bootstrap_servers, StdString, StdSet,
    logging_disable, logging_set_format, logging_set_stdout, logging_set_julia,
    logging_set_file, logging_drain, logging_enable_default
"""
    get_bootstrap_servers

Return the Kafka configuration key for bootstrap servers as defined by the native library.
"""
get_bootstrap_servers

function __init__()
    _ensure_native_deps_loaded()
    NativeBindings.__init__()
end
