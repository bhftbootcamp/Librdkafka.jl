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

function project_uuid_string(pkg_dir::AbstractString)
    toml = TOML.parsefile(joinpath(pkg_dir, "Project.toml"))
    u = get(toml, "uuid", nothing)
    u === nothing && error("Project.toml has no `uuid` entry")
    return String(u)
end

function ensure_tar()
    tarbin = Sys.which("tar")
    tarbin === nothing && return nothing
    return tarbin
end

function _repo_url_to_https_base(repo_url::AbstractString)
    s = strip(String(repo_url))
    isempty(s) && return nothing
    endswith(s, ".git") && (s = s[1:end-4])

    if startswith(s, "git@")
        m = match(r"^git@([^:]+):(.+)$", s)
        m === nothing && return nothing
        return "https://$(m.captures[1])/$(m.captures[2])"
    elseif startswith(s, "ssh://git@")
        m = match(r"^ssh://git@([^/]+)/(.+)$", s)
        m === nothing && return nothing
        return "https://$(m.captures[1])/$(m.captures[2])"
    elseif startswith(s, "https://") || startswith(s, "http://")
        return rstrip(s, '/')
    end

    return nothing
end

function _find_repo_url_in_manifest(manifest::Dict{String, Any}, target_uuid_s::String)
    deps_tbl = get(manifest, "deps", nothing)
    if deps_tbl isa Dict
        for (_, entries_any) in deps_tbl
            entries = entries_any isa Vector ? entries_any : Any[entries_any]
            for entry_any in entries
                entry_any isa Dict || continue
                entry = entry_any::Dict{String, Any}
                get(entry, "uuid", nothing) == target_uuid_s || continue
                repo_url = get(entry, "repo-url", nothing)
                repo_url === nothing && continue
                return String(repo_url)
            end
        end
    end

    for (k, v) in manifest
        (k == "manifest_format" || k == "project_hash" || k == "julia_version") && continue
        v isa Dict || continue
        entry = v::Dict{String, Any}
        get(entry, "uuid", nothing) == target_uuid_s || continue
        repo_url = get(entry, "repo-url", nothing)
        repo_url === nothing && continue
        return String(repo_url)
    end

    return nothing
end

function infer_release_base_url(pkg_dir::String)
    target_uuid_s = project_uuid_string(pkg_dir)
    active_project = Base.active_project()
    active_project === nothing && return nothing
    manifest_path = joinpath(dirname(String(active_project)), "Manifest.toml")
    isfile(manifest_path) || return nothing

    manifest = try
        TOML.parsefile(manifest_path)
    catch e
        @warn "Failed to parse active Manifest.toml for release URL inference" manifest_path exception=(e, catch_backtrace())
        return nothing
    end

    repo_url = _find_repo_url_in_manifest(manifest, target_uuid_s)
    repo_url === nothing && return nothing
    https_base = _repo_url_to_https_base(repo_url)
    https_base === nothing && return nothing
    return "$https_base/releases/download"
end

function try_download_release!(; pkg_dir::String, lib_dir::String, lib_path::String)
    mkpath(lib_dir)
    ver = project_version(pkg_dir)
    plat = platform_tag()
    jver = julia_minor()
    release_base_url = infer_release_base_url(pkg_dir)
    if release_base_url === nothing
        @warn "Could not infer repository release URL from package metadata; skipping release download"
        return false
    end
    tarbin = ensure_tar()
    if tarbin === nothing
        @warn "`tar` not found on PATH; cannot extract downloaded archive automatically" platform=platform_pretty()
        return false
    end
    url = "$release_base_url/v$ver/$plat-julia$jver.tar.gz"
    @info "Attempting to download pre-built binary from GitHub releases" url
    tmp = tempname() * ".tar.gz"
    try
        resp = http_request("GET", url; read_timeout=30, connect_timeout=10)
        if http_status(resp) != 200
            @warn "Pre-built binary not found" status=http_status(resp) version=ver platform=plat julia=jver url
            return false
        end
        write(tmp, http_body(resp))
        run(`$tarbin -xzf $tmp -C $lib_dir`)
        if isfile(lib_path)
            @info "Successfully downloaded and extracted pre-built binary" lib_path
            return true
        else
            @warn "Archive extracted but library file not found where expected" lib_path
            return false
        end
    catch e
        @warn "Failed to download or extract pre-built binary" url exception=(e, catch_backtrace())
        return false
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

function try_build_local!(; pkg_dir::String, lib_dir::String, lib_path::String, lib_name::String)
    cmake = Sys.which("cmake")
    if cmake === nothing
        @warn "cmake not found; cannot build native wrapper locally"
        return false
    end

    src_dir = joinpath(pkg_dir, "deps", "src")
    build_dir = joinpath(src_dir, "build")
    if !isdir(src_dir)
        @warn "CMake source directory not found" src_dir
        return false
    end

    try
        withenv("JULIA_PKG_PRECOMPILE_AUTO" => "0") do
            run(`$(Base.julia_cmd()) --project=$pkg_dir -e 'import Pkg; Pkg.instantiate()'`)
        end
        run(`$cmake -S $src_dir -B $build_dir -DCMAKE_BUILD_TYPE=Release`)
        run(`$cmake --build $build_dir --config Release`)
        if try_copy_local_build!(; pkg_dir, lib_dir, lib_path, lib_name)
            @info "Successfully built and staged library" lib_path
            return true
        end
    catch e
        @warn "Failed to build from source" exception=(e, catch_backtrace())
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
    if try_build_local!(; pkg_dir=String(pkg_dir), lib_dir=String(lib_dir), lib_path=String(lib_path), lib_name=name)
        return
    end
    plat = platform_pretty()
    jver = julia_minor()
    error("""
    Could not locate $name for $plat (Julia $jver).

    Tried in order:
    1) Existing file: $lib_path
    2) ENV override: $ENV_LIB_PATH
    3) GitHub release download inferred from repository URL (if available)
       platform tag: $(platform_tag()), Julia: $jver
    4) Existing local build: deps/src/build/lib/$name
    5) Local build from source via CMake (automatic fallback)

    To build from source on $plat:
      1. cd ~/.julia/packages/Librdkafka/*/
      2. cmake -S deps/src -B deps/src/build
      3. cmake --build deps/src/build

    Or install from dev mode:
      julia> using Pkg
      julia> Pkg.develop(url="https://github.com/OWNER/REPO")
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
