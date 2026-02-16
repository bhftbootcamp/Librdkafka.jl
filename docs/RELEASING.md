# Releasing and Green Registry

This repository registers package versions in `bhftbootcamp/Green` via `.github/workflows/registry.yml`.

## Rules

- Registration runs from `master` only.
- Release tags must match `Project.toml` version (`vX.Y.Z`).
- Do not create release tags from feature branches.

## Recovery: Tag Created on Wrong Branch (example: `v0.1.3`)

Run from a local clone with push access:

```bash
git fetch origin --tags
git checkout master
git pull --ff-only origin master

# Remove wrong remote/local tag
git tag -d v0.1.3 || true
git push origin :refs/tags/v0.1.3

# Recreate tag on master HEAD
git tag -a v0.1.3 -m v0.1.3
git push origin v0.1.3
```

Then in GitHub Actions:

1. Open workflow `Registry (Green) and Tag`.
2. Click `Run workflow` on branch `master`.
3. Confirm `Update Green registry entry` succeeds.

## Safer Alternative (recommended if `v0.1.3` was already used)

Release a new patch version from `master`:

```bash
git checkout master
git pull --ff-only origin master

# bump Project.toml version to 0.1.4
git commit -am "Release 0.1.4"
git push origin master
```

This triggers `registry.yml`, which registers in Green and creates `v0.1.4`.
