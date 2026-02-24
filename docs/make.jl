using Documenter
using Librdkafka

makedocs(
    sitename = "Librdkafka.jl",
    format = Documenter.HTML(),
    pages = [
        "Home" => "index.md",
    ],
    modules = [Librdkafka],
)

deploydocs(
    repo = "github.com/bhftbootcamp/Librdkafka.jl.git",
    devurl = "dev",
    devbranch = "master",
    push_preview = true,
)
