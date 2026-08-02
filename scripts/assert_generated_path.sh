#!/usr/bin/env bash

# Source this helper before recursively deleting a repository-local path.
lockdc_assert_generated_path() {
    local repo_root=$1
    local candidate=$2
    local home_root=${HOME:-}

    case "$candidate" in
        ""|/|"$repo_root")
            printf 'refusing unsafe generated path: %s\n' "${candidate:-<empty>}" >&2
            return 1
            ;;
    esac
    if [[ -n "$home_root" && "$candidate" == "$home_root" ]]; then
        printf 'refusing home directory as generated path: %s\n' "$candidate" >&2
        return 1
    fi
    case "$candidate" in
        "$repo_root"/build/*|"$repo_root"/dist/*|"$repo_root"/.cache/*|"$repo_root"/.luarocks-build/*)
            return 0
            ;;
        *)
            printf 'refusing path outside repository-generated roots: %s\n' "$candidate" >&2
            return 1
            ;;
    esac
}
