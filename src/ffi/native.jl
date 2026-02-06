module Native

export make_topics_set

using CxxWrap, Libdl
using librdkafka_jll, CyrusSASL_jll

const _LIBNAME = "libkafka.$(Libdl.dlext)"
const _CANDIDATES = (
    joinpath(@__DIR__, "..", "..", "deps", "lib"),
    joinpath(@__DIR__, "..", "..", "deps", "src", "build", "lib"),
)

function _locate()
    for dir in _CANDIDATES
        path = normpath(joinpath(dir, _LIBNAME))
        isfile(path) && return path
    end
    error("Could not locate $(_LIBNAME). Build first: cmake -S deps/src -B deps/src/build && cmake --build deps/src/build")
end

const _handles = Ptr{Nothing}[]
function _preload_deps()
    isempty(_handles) || return
    for p in (librdkafka_jll.librdkafka_path, CyrusSASL_jll.libsasl2_path)
        if !isempty(p) && isfile(p)
            push!(_handles, Libdl.dlopen(p, Libdl.RTLD_LAZY | Libdl.RTLD_DEEPBIND))
        end
    end
end

@wrapmodule(_locate)

function __init__()
    _preload_deps()
    @initcxx
end

function make_topics_set(topic_names::Vector{String})
    StdSet = getfield(@__MODULE__, :StdSet)
    StdString = getfield(@__MODULE__, :StdString)
    s = Base.invokelatest(StdSet{StdString})
    for t in topic_names
        Base.invokelatest(push!, s, Base.invokelatest(StdString, t))
    end
    return s
end

end
