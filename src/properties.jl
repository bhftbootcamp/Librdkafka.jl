_stringify(v::AbstractString) = String(v)
_stringify(v::Symbol) = String(v)
_stringify(v) = string(v)

function _build_properties(bootstrap_servers::AbstractString;
    group_id::Union{Nothing,AbstractString} = nothing,
    config::AbstractDict = Dict{String,String}(),
)
    bs = String(bootstrap_servers)
    isempty(bs) && throw(ArgumentError("bootstrap_servers must be non-empty."))
    props_id = _B.create_properties()
    props_id == 0 && _throw_error(:state, :create_properties, "Failed to create native properties handle.")
    try
        _B.properties_put(props_id, BOOTSTRAP_SERVERS, bs) ||
            _throw_error(:state, :properties_put, "Failed to set required bootstrap servers.",
                details = _details(:property => BOOTSTRAP_SERVERS, :value => bs))
        if group_id !== nothing
            gid = String(group_id)
            isempty(gid) || (
                _B.properties_put(props_id, GROUP_ID, gid) ||
                _throw_error(:state, :properties_put, "Failed to set consumer group id.",
                    details = _details(:property => GROUP_ID, :value => gid))
            )
        end
        for (k, v) in config
            key = _stringify(k)
            val = _stringify(v)
            _B.properties_put(props_id, key, val) ||
                _throw_error(:state, :properties_put, "Failed to set configuration property.",
                    details = _details(:property => key, :value => val))
        end
        return props_id
    catch
        # Caller never received the props_id, so the entry would leak in the
        # C++ properties_store. Free it.
        _B.properties_destroy(props_id)
        rethrow()
    end
end
