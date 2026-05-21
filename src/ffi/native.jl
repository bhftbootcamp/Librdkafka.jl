module Native

export make_topics_set, to_std_vector_string, to_std_vector_int32, to_std_vector_longlong

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
    error(
        "Could not locate $(_LIBNAME). " *
        "Build first: cmake -S deps/src -B deps/src/build && cmake --build deps/src/build"
    )
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

const _STDSTRING_T          = Ref{Any}()
const _STDSET_STDSTRING_T   = Ref{Any}()
const _STDVEC_STDSTRING_T   = Ref{Any}()
const _STDVEC_INT32_T       = Ref{Any}()
const _STDVEC_CXXLL_T       = Ref{Any}()
const _CXXLONGLONG_T        = Ref{Any}()

function __init__()
    _preload_deps()
    @initcxx
    _STDSTRING_T[]         = StdString
    _STDSET_STDSTRING_T[]  = StdSet{StdString}
    _STDVEC_STDSTRING_T[]  = StdVector{StdString}
    _STDVEC_INT32_T[]      = StdVector{Int32}
    _CXXLONGLONG_T[]       = CxxWrap.CxxWrapCore.CxxLongLong
    _STDVEC_CXXLL_T[]      = StdVector{CxxWrap.CxxWrapCore.CxxLongLong}
end

function make_topics_set(topic_names::Vector{String})
    SetT = _STDSET_STDSTRING_T[]::Type
    StringT = _STDSTRING_T[]::Type
    s = SetT()
    for t in topic_names
        push!(s, StringT(t))
    end
    return s
end

function to_std_vector_string(xs::Vector{String})
    VecT = _STDVEC_STDSTRING_T[]::Type
    StringT = _STDSTRING_T[]::Type
    v = VecT()
    for x in xs
        push!(v, StringT(x))
    end
    return v
end

function to_std_vector_int32(xs::Vector{Int32})
    VecT = _STDVEC_INT32_T[]::Type
    v = VecT()
    for x in xs
        push!(v, x)
    end
    return v
end

function to_std_vector_longlong(xs::Vector{Int64})
    VecT = _STDVEC_CXXLL_T[]::Type
    LLT = _CXXLONGLONG_T[]::Type
    v = VecT()
    for x in xs
        push!(v, LLT(x))
    end
    return v
end

end
