_stringify(v::AbstractString) = String(v)
_stringify(v::Symbol) = String(v)
_stringify(v) = string(v)

function _build_properties(bootstrap_servers::AbstractString;
                           group_id::Union{Nothing,AbstractString}=nothing,
                           config::AbstractDict=Dict{String,String}())
    bs = String(bootstrap_servers)
    isempty(bs) && throw(ArgumentError("bootstrap_servers must be non-empty."))
    props_id = _B.create_properties()
    _B.properties_put(props_id, BOOTSTRAP_SERVERS, bs)
    if group_id !== nothing
        gid = String(group_id)
        isempty(gid) || _B.properties_put(props_id, GROUP_ID, gid)
    end
    for (k, v) in config
        _B.properties_put(props_id, _stringify(k), _stringify(v))
    end
    return props_id
end
