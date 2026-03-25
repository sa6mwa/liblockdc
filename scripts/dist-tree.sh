#!/usr/bin/env bash
set -euo pipefail

declare -A ARCHIVE_NODE_TYPE=()

usage() {
  printf 'usage: %s [dist-dir]\n' "$0" >&2
  exit 2
}

build_archive_tree() {
  local archive_path="$1"
  local raw_path=""

  ARCHIVE_NODE_TYPE=()

  while IFS= read -r raw_path; do
    local normalized_path="${raw_path#./}"
    normalized_path="${normalized_path%/}"

    if [[ -z "${normalized_path}" ]]; then
      continue
    fi

    local current_path=""
    local segment=""
    local -a segments=()
    IFS='/' read -r -a segments <<< "${normalized_path}"

    for ((i = 0; i < ${#segments[@]}; ++i)); do
      segment="${segments[i]}"
      if [[ -z "${segment}" ]]; then
        continue
      fi

      if [[ -n "${current_path}" ]]; then
        current_path+="/"
      fi
      current_path+="${segment}"

      if ((i < ${#segments[@]} - 1)) || [[ "${raw_path}" == */ ]]; then
        ARCHIVE_NODE_TYPE["${current_path}"]="d"
      elif [[ -z "${ARCHIVE_NODE_TYPE[${current_path}]+x}" ]]; then
        ARCHIVE_NODE_TYPE["${current_path}"]="f"
      fi
    done
  done < <(tar -tzf "${archive_path}")
}

render_archive_subtree() {
  local parent_path="$1"
  local prefix="$2"
  local -A seen_children=()
  local -a children=()
  local node_path=""

  for node_path in "${!ARCHIVE_NODE_TYPE[@]}"; do
    local remainder=""
    if [[ -z "${parent_path}" ]]; then
      remainder="${node_path}"
    else
      if [[ "${node_path}" != "${parent_path}/"* ]]; then
        continue
      fi
      remainder="${node_path#${parent_path}/}"
    fi

    if [[ -z "${remainder}" ]]; then
      continue
    fi

    local child_name="${remainder%%/*}"
    if [[ -n "${seen_children[${child_name}]+x}" ]]; then
      continue
    fi

    seen_children["${child_name}"]=1
    children+=("${child_name}")
  done

  if ((${#children[@]} == 0)); then
    return
  fi

  mapfile -t children < <(printf '%s\n' "${children[@]}" | LC_ALL=C sort)

  local index=0
  local child_path=""
  local connector=""
  local next_prefix=""
  for child_name in "${children[@]}"; do
    child_path="${parent_path:+${parent_path}/}${child_name}"
    connector="|--"
    next_prefix="${prefix}|   "

    if ((index == ${#children[@]} - 1)); then
      connector="\`--"
      next_prefix="${prefix}    "
    fi

    if [[ "${ARCHIVE_NODE_TYPE[${child_path}]}" == "d" ]]; then
      printf '%s%s %s/\n' "${prefix}" "${connector}" "${child_name}"
      render_archive_subtree "${child_path}" "${next_prefix}"
    else
      printf '%s%s %s\n' "${prefix}" "${connector}" "${child_name}"
    fi

    ((index += 1))
  done
}

render_filesystem_tree() {
  local directory_path="$1"
  local prefix="$2"
  local -a children=()
  local entry_name=""

  while IFS= read -r -d '' entry_name; do
    children+=("${entry_name}")
  done < <(find "${directory_path}" -mindepth 1 -maxdepth 1 -printf '%f\0' | sort -z)

  if ((${#children[@]} == 0)); then
    return
  fi

  local index=0
  local entry_path=""
  local connector=""
  local next_prefix=""
  for entry_name in "${children[@]}"; do
    entry_path="${directory_path}/${entry_name}"
    connector="|--"
    next_prefix="${prefix}|   "

    if ((index == ${#children[@]} - 1)); then
      connector="\`--"
      next_prefix="${prefix}    "
    fi

    if [[ -d "${entry_path}" ]]; then
      printf '%s%s %s/\n' "${prefix}" "${connector}" "${entry_name}"
      render_filesystem_tree "${entry_path}" "${next_prefix}"
    elif [[ "${entry_name}" == *.tar.gz ]]; then
      printf '%s%s %s/\n' "${prefix}" "${connector}" "${entry_name}"
      build_archive_tree "${entry_path}"
      render_archive_subtree "" "${next_prefix}"
    else
      printf '%s%s %s\n' "${prefix}" "${connector}" "${entry_name}"
    fi

    ((index += 1))
  done
}

main() {
  local root_path="${1:-dist}"

  if (($# > 1)); then
    usage
  fi
  if [[ ! -d "${root_path}" ]]; then
    printf 'dist-tree: directory not found: %s\n' "${root_path}" >&2
    exit 1
  fi

  root_path="${root_path%/}"
  if [[ -z "${root_path}" ]]; then
    root_path="."
  fi

  printf '%s/\n' "${root_path}"
  render_filesystem_tree "${root_path}" ""
}

main "$@"
