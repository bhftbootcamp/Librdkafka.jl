using TOML
using Printf
using EasyCurl

const ENV_LIB_PATH = "LIBRDKAFKA_JL_LIB_PATH"

@inline lib_name() = Sys.iswindows() ? "libkafka.dll" :
                     Sys.isapple()   ? "libkafka.dylib" : "libkafka.so"

function platform_tag()
    if Sys.islinux()
        if Sys.ARCH === :x86_64
            return "linux-x86_64"
        elseif Sys.ARCH === :aarch64
            return "linux-aarch64"
        else
            error("Unsupported Linux architecture: $(Sys.ARCH)")
        end
    elseif Sys.isapple()
        if Sys.ARCH === :aarch64
            "macos-aarch64"
        else
            "macos-x86_64"
        end
    elseif Sys.iswindows()
        return "windows-x86_64"
    else
        error("Unsupported platform: $(Sys.KERNEL)")
    end
end

function platform_pretty()
    if Sys.islinux()
        return "Linux"
    elseif Sys.isapple()
        return "macOS"
    elseif Sys.iswindows()
        return "Windows"
    else
        return string(Sys.KERNEL)
    end
end

@inline julia_minor() = "$(VERSION.major).$(VERSION.minor)"

function project_version(pkg_dir::AbstractString)
    toml = TOML.parsefile(joinpath(pkg_dir, "Project.toml"))
    v = get(toml, "version", nothing)
    v === nothing && error("Project.toml has no `version` entry")
    return String(v)
end

function ensure_tar()
    tarbin = Sys.which("tar")
    tarbin === nothing && return nothing
    return tarbin
end

function try_download_release!(; pkg_dir::String, lib_dir::String, lib_path::String)
    mkpath(lib_dir)
    ver = project_version(pkg_dir)
    plat = platform_tag()
    jver = julia_minor()
    url = "https://github.com/bhftbootcamp/Librdkafka.jl/releases/download/v$ver/$plat-julia$jver.tar.gz"
    @info "Attempting to download pre-built binary from GitHub releases" url
    resp = http_request("GET", url; read_timeout=30, connect_timeout=10)
    if http_status(resp) != 200
        @warn "Pre-built binary not found" status=http_status(resp) version=ver platform=plat julia=jver
        return false
    end
    tarbin = ensure_tar()
    if tarbin === nothing
        @warn "`tar` not found on PATH; cannot extract downloaded archive automatically" platform=platform_pretty()
        return false
    end
    tmp = tempname() * ".tar.gz"
    try
        write(tmp, http_body(resp))
        run(`$tarbin -xzf $tmp -C $lib_dir`)
        if isfile(lib_path)
            @info "Successfully downloaded and extracted pre-built binary" lib_path
            return true
        else
            @warn "Archive extracted but library file not found where expected" lib_path
            return false
        end
    finally
        rm(tmp; force=true)
    end
end

function try_copy_local_build!(; pkg_dir::String, lib_dir::String, lib_path::String, lib_name::String)
    built = joinpath(pkg_dir, "deps", "src", "build", "lib", lib_name)
    if isfile(built)
        mkpath(lib_dir)
        cp(built, lib_path; force=true)
        @info "Using local build: copied library into deps/lib" from=built to=lib_path
        return true
    end
    return false
end

function try_copy_env_override!(; lib_path::String)
    p = get(ENV, ENV_LIB_PATH, "")
    isempty(p) && return false
    if !isfile(p)
        @warn "ENV override path does not exist" env=ENV_LIB_PATH path=p
        return false
    end
    mkpath(dirname(lib_path))
    cp(p, lib_path; force=true)
    @info "Using library from ENV override" env=ENV_LIB_PATH from=p to=lib_path
    return true
end

function main()
    pkg_dir = dirname(@__DIR__)
    lib_dir = joinpath(pkg_dir, "deps", "lib")
    name = lib_name()
    lib_path = joinpath(lib_dir, name)
    if isfile(lib_path)
        @info "Library already present" lib_path
        return
    end
    if try_copy_env_override!(; lib_path)
        return
    end
    ok = false
    try
        ok = try_download_release!(; pkg_dir=String(pkg_dir), lib_dir=String(lib_dir), lib_path=String(lib_path))
    catch e
        @warn "Failed to download binary" exception=(e, catch_backtrace())
    end
    if ok
        return
    end
    if try_copy_local_build!(; pkg_dir=String(pkg_dir), lib_dir=String(lib_dir), lib_path=String(lib_path), lib_name=name)
        return
    end
    plat = platform_pretty()
    jver = julia_minor()
    error("""
    Could not locate $name for $plat (Julia $jver).

    Tried in order:
    1) Existing file: $lib_path
    2) ENV override: $ENV_LIB_PATH
    3) GitHub release download for platform tag: $(platform_tag()), Julia: $jver
    4) Local build: deps/src/build/lib/$name

    To build from source on $plat:
      1. cd ~/.julia/packages/Librdkafka/*/
      2. cmake -S deps/src -B deps/src/build
      3. cmake --build deps/src/build

    Or install from dev mode:
      julia> using Pkg
      julia> Pkg.develop(url="https://github.com/bhftbootcamp/Librdkafka.jl")
      Then:
      cd ~/.julia/dev/Librdkafka
      cmake -S deps/src -B deps/src/build
      cmake --build deps/src/build

    Tip:
      You can also provide a prebuilt library via:
        ENV["$ENV_LIB_PATH"] = "/full/path/to/$name"
    """)
end

main()
