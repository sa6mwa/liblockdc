#!/usr/bin/env bash

# Source this helper before recursively deleting a repository-local path.
lockdc_assert_generated_path() {
    local repo_root=$1
    local candidate=$2
    local home_root=${HOME:-}
    local canonical_repo_root
    local canonical_candidate
    local candidate_parent
    local candidate_leaf
    local missing_suffix

    case "$candidate" in
        ""|/)
            printf 'refusing unsafe generated path: %s\n' "${candidate:-<empty>}" >&2
            return 1
            ;;
    esac
    if [ ! -d "$repo_root" ]; then
        printf 'repository root does not exist: %s\n' "$repo_root" >&2
        return 1
    fi

    canonical_repo_root=$(CDPATH= cd -P -- "$repo_root" && pwd -P)
    if [ -d "$candidate" ]; then
        canonical_candidate=$(CDPATH= cd -P -- "$candidate" && pwd -P) || {
            printf 'cannot resolve generated path: %s\n' "$candidate" >&2
            return 1
        }
    else
        candidate_parent=$(dirname -- "$candidate")
        candidate_leaf=$(basename -- "$candidate")
        missing_suffix="/$candidate_leaf"
        while [ ! -d "$candidate_parent" ]; do
            if [ "$candidate_parent" = / ] || [ "$candidate_parent" = . ]; then
                printf 'cannot resolve generated path parent: %s\n' "$candidate" >&2
                return 1
            fi
            candidate_leaf=$(basename -- "$candidate_parent")
            missing_suffix="/$candidate_leaf$missing_suffix"
            candidate_parent=$(dirname -- "$candidate_parent")
        done
        canonical_candidate=$(CDPATH= cd -P -- "$candidate_parent" && pwd -P) || {
            printf 'cannot resolve generated path parent: %s\n' "$candidate_parent" >&2
            return 1
        }
        canonical_candidate="$canonical_candidate$missing_suffix"
    fi

    if [ "$canonical_candidate" = "$canonical_repo_root" ]; then
        printf 'refusing unsafe generated path: %s\n' "$candidate" >&2
        return 1
    fi
    if [ -n "$home_root" ] && [ "$canonical_candidate" = "$home_root" ]; then
        printf 'refusing home directory as generated path: %s\n' "$candidate" >&2
        return 1
    fi
    case "$canonical_candidate" in
        "$canonical_repo_root"/build|"$canonical_repo_root"/build/*|"$canonical_repo_root"/dist|"$canonical_repo_root"/dist/*|"$canonical_repo_root"/.cache|"$canonical_repo_root"/.cache/*|"$canonical_repo_root"/.luarocks-build|"$canonical_repo_root"/.luarocks-build/*|"$canonical_repo_root"/devenv/volumes|"$canonical_repo_root"/devenv/volumes/*)
            return 0
            ;;
        *)
            printf 'refusing path outside repository-generated roots: %s\n' "$candidate" >&2
            return 1
            ;;
    esac
}
