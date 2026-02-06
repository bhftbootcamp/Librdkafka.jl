_stringify(v::AbstractString) = String(v)
_stringify(v::Symbol) = String(v)
_stringify(v) = string(v)

function _build_properties(bootstrap_servers::AbstractString;
                           group_id::Union{Nothing,AbstractString}=nothing,
                           config::AbstractDict=Dict())
    props_id = create_properties()
    properties_put(props_id, BOOTSTRAP_SERVERS, String(bootstrap_servers))
    if group_id !== nothing && !isempty(String(group_id))
        properties_put(props_id, GROUP_ID, String(group_id))
    end
    for (k, v) in config
        properties_put(props_id, _stringify(k), _stringify(v))
    end
    return props_id
end
