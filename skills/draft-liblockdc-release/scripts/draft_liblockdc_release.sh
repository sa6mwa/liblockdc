#!/usr/bin/env bash
set -euo pipefail

STATE_DIR=".cache/draft-liblockdc-release"
STATE_FILE="${STATE_DIR}/state.env"

usage() {
  cat <<'EOF'
Usage:
  draft_liblockdc_release.sh draft [--allow-nonbump-tag] [--dry-run]
  draft_liblockdc_release.sh resume-draft [--dry-run]
  draft_liblockdc_release.sh publish [--dry-run]

Commands:
  draft         Validate the tagged release locally, run tests, run make world,
                verify dist/, then check origin and create a GitHub draft release.
  resume-draft  Continue from a completed local validation after the tagged commit
                and tag have been pushed to origin.
  publish       Publish an existing draft release for the vX.Y.Z tag on HEAD.

Options:
  --allow-nonbump-tag  Continue even if the HEAD tag is not newer than the
                       previous release tag on main. Use only after explicit
                       user confirmation.
  --dry-run            Print planned actions after preflight without running
                       tests, make world, or gh release mutations.
  -h, --help           Show this help.
EOF
}

die() {
  printf 'error: %s\n' "$1" >&2
  exit 1
}

info() {
  printf '[release] %s\n' "$1"
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || die "required tool '$1' is not available"
}

semver_core() {
  printf '%s\n' "${1#v}"
}

semver_gt() {
  local a b
  local a_major a_minor a_patch
  local b_major b_minor b_patch
  a="$(semver_core "$1")"
  b="$(semver_core "$2")"
  IFS=. read -r a_major a_minor a_patch <<<"$a"
  IFS=. read -r b_major b_minor b_patch <<<"$b"
  if (( a_major > b_major )); then
    return 0
  fi
  if (( a_major < b_major )); then
    return 1
  fi
  if (( a_minor > b_minor )); then
    return 0
  fi
  if (( a_minor < b_minor )); then
    return 1
  fi
  if (( a_patch > b_patch )); then
    return 0
  fi
  return 1
}

parse_origin_repo() {
  local origin_url stripped
  origin_url="$(git remote get-url origin 2>/dev/null)" || die "failed to resolve git origin remote"
  case "$origin_url" in
    git@github.com:*)
      stripped="${origin_url#git@github.com:}"
      ;;
    https://github.com/*)
      stripped="${origin_url#https://github.com/}"
      ;;
    ssh://git@github.com/*)
      stripped="${origin_url#ssh://git@github.com/}"
      ;;
    *)
      die "origin remote must point at GitHub, got: $origin_url"
      ;;
  esac
  stripped="${stripped%.git}"
  [[ "$stripped" == */* ]] || die "could not parse owner/repo from origin remote: $origin_url"
  printf '%s\n' "$stripped"
}

head_release_tag() {
  local tags
  mapfile -t tags < <(git tag --points-at HEAD | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' || true)
  if [ "${#tags[@]}" -eq 0 ]; then
    die "HEAD must have a vX.Y.Z tag. Fix the tag and re-run this skill."
  fi
  if [ "${#tags[@]}" -gt 1 ]; then
    die "HEAD has multiple release tags: ${tags[*]}. Keep exactly one vX.Y.Z tag on HEAD and re-run."
  fi
  printf '%s\n' "${tags[0]}"
}

previous_main_release_tag() {
  local current_tag="${1}"
  git tag --merged main --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-version:refname \
    | grep -vx "$current_tag" \
    | head -n1
}

ensure_github_access() {
  local repo="$1"
  info "checking gh authentication and repository access"
  gh auth status >/dev/null 2>&1 || die "gh is not authenticated. Authenticate with gh and re-run."
  gh repo view "$repo" >/dev/null 2>&1 || die "gh cannot access repository '$repo'. Check permissions and re-run."
  gh api "repos/$repo" >/dev/null 2>&1 || die "gh cannot access GitHub API for '$repo'. Check permissions and re-run."
}

assert_nonbump_policy() {
  local current_tag="$1"
  local previous_tag="$2"
  local allow_nonbump="$3"
  if [ -z "$previous_tag" ]; then
    info "no previous main release tag found; treating $current_tag as the first release"
    return 0
  fi
  if semver_gt "$current_tag" "$previous_tag"; then
    info "release tag $current_tag is newer than previous main tag $previous_tag"
    return 0
  fi
  if [ "$allow_nonbump" -eq 1 ]; then
    info "continuing with non-bump tag $current_tag after explicit override (previous tag: $previous_tag)"
    return 0
  fi
  die "release tag $current_tag is not newer than previous main tag $previous_tag. Confirm the tag with the user, then re-run with --allow-nonbump-tag if it is intentional."
}

collect_release_assets() {
  local checksum_file="$1"
  local asset
  RELEASE_ASSETS=()
  while read -r _ _ asset; do
    [ -n "$asset" ] || continue
    [ -f "dist/$asset" ] || die "checksum file references missing asset dist/$asset"
    RELEASE_ASSETS+=("dist/$asset")
  done <"$checksum_file"
  RELEASE_ASSETS+=("$checksum_file")
}

validate_dist() {
  local version="$1"
  local checksum_file="dist/liblockdc-${version}-CHECKSUMS"
  local line asset tarball_count

  [ -d dist ] || die "dist/ is missing; run make world successfully before drafting the release"
  [ -f "$checksum_file" ] || die "missing checksum file $checksum_file"
  [ -f "dist/lockdc-${version}-1.rockspec" ] || die "missing dist/lockdc-${version}-1.rockspec"
  [ -f "dist/lockdc-${version}-1.src.rock" ] || die "missing dist/lockdc-${version}-1.src.rock"

  tarball_count="$(find dist -maxdepth 1 -type f -name "liblockdc-${version}-*.tar.gz" | wc -l | tr -d ' ')"
  [ "$tarball_count" -gt 0 ] || die "no liblockdc-${version}-*.tar.gz artifacts found in dist/"

  while read -r line; do
    [ -n "$line" ] || continue
    asset="${line##* }"
    case "$asset" in
      liblockdc-"${version}"-*.tar.gz|lockdc-"${version}"-1.rockspec|lockdc-"${version}"-1.src.rock)
        ;;
      *)
        die "unexpected asset in $checksum_file: $asset"
        ;;
    esac
  done <"$checksum_file"

  info "verifying checksums in $checksum_file"
  (
    cd dist
    sha256sum -c "$(basename "$checksum_file")"
  ) || die "checksum verification failed for $(basename "$checksum_file")"

  collect_release_assets "$checksum_file"
}

assert_release_package_tests() {
  local release_dir="build/x86_64-linux-gnu-release"
  [ -d "$release_dir" ] || die "missing $release_dir; run make world successfully before drafting the release"
  info "rerunning release package tests"
  ctest --test-dir "$release_dir" --output-on-failure -R '^(package_archives_test|release_tarball_sdk_test|lua_release_package_test)$'
}

write_release_notes() {
  local notes_file="$1"
  git log -1 --format='%s%n%n%b' >"$notes_file"
  [ -s "$notes_file" ] || die "failed to derive release notes from HEAD commit message"
}

create_draft_release() {
  local repo="$1"
  local tag="$2"
  local notes_file="$3"

  gh release view "$tag" --repo "$repo" >/dev/null 2>&1 && die "release '$tag' already exists on GitHub. Delete or update it manually before re-running."

  info "creating GitHub draft release $tag"
  gh release create "$tag" "${RELEASE_ASSETS[@]}" \
    --repo "$repo" \
    --title "$tag" \
    --notes-file "$notes_file" \
    --draft \
    --verify-tag
}

publish_release() {
  local repo="$1"
  local tag="$2"
  info "publishing GitHub draft release $tag"
  gh release edit "$tag" --repo "$repo" --draft=false >/dev/null
}

ensure_origin_ready() {
  local tag="$1"
  local head_sha="$2"
  local remote_main_sha remote_tag_sha

  info "checking whether tagged commit and tag are present on origin"
  git fetch origin main --tags --quiet || die "failed to fetch origin/main and tags"

  remote_main_sha="$(git rev-parse origin/main 2>/dev/null)" || die "could not resolve origin/main after fetch"
  if [ "$remote_main_sha" != "$head_sha" ]; then
    die "origin/main does not point at HEAD ($head_sha). Push the commit to origin and then run resume-draft."
  fi

  remote_tag_sha="$(
    git ls-remote origin "refs/tags/${tag}^{}" "refs/tags/${tag}" \
      | awk 'NR==1 {print $1}'
  )"
  [ -n "$remote_tag_sha" ] || die "tag '$tag' is not present on origin. Push the tag and then run resume-draft."
  if [ "$remote_tag_sha" != "$head_sha" ]; then
    die "origin tag '$tag' does not resolve to HEAD ($head_sha). Push the correct tag and then run resume-draft."
  fi
}

write_state() {
  local repo="$1"
  local tag="$2"
  local version="$3"
  local head_sha="$4"
  mkdir -p "$STATE_DIR"
  cat >"$STATE_FILE" <<EOF
RELEASE_REPO='$repo'
RELEASE_TAG='$tag'
RELEASE_VERSION='$version'
RELEASE_HEAD_SHA='$head_sha'
EOF
}

load_state() {
  [ -f "$STATE_FILE" ] || die "no saved local validation state found. Run the draft command first."
  # shellcheck disable=SC1090
  . "$STATE_FILE"
  [ -n "${RELEASE_REPO:-}" ] || die "saved release state is invalid: missing repository"
  [ -n "${RELEASE_TAG:-}" ] || die "saved release state is invalid: missing tag"
  [ -n "${RELEASE_VERSION:-}" ] || die "saved release state is invalid: missing version"
  [ -n "${RELEASE_HEAD_SHA:-}" ] || die "saved release state is invalid: missing head sha"
}

clear_state() {
  rm -f "$STATE_FILE"
}

assert_state_matches_head() {
  local current_tag="$1"
  local current_sha="$2"
  [ "$RELEASE_TAG" = "$current_tag" ] || die "saved state tag '$RELEASE_TAG' does not match current HEAD tag '$current_tag'. Re-run draft."
  [ "$RELEASE_HEAD_SHA" = "$current_sha" ] || die "saved state head '$RELEASE_HEAD_SHA' does not match current HEAD '$current_sha'. Re-run draft."
}

main() {
  local command=""
  local allow_nonbump=0
  local dry_run=0
  local repo_root repo current_tag current_version previous_tag notes_file head_sha

  [ $# -gt 0 ] || {
    usage
    exit 1
  }

  command="$1"
  shift

  while [ $# -gt 0 ]; do
    case "$1" in
      --allow-nonbump-tag)
        allow_nonbump=1
        ;;
      --dry-run)
        dry_run=1
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
    shift
  done

  case "$command" in
    draft|resume-draft|publish)
      ;;
    *)
      usage
      die "unknown command: $command"
      ;;
  esac

  require_tool git
  require_tool gh
  require_tool make
  require_tool ctest
  require_tool sha256sum
  require_tool tar

  repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || die "must run inside the liblockdc git repository"
  cd "$repo_root"

  repo="$(parse_origin_repo)"
  current_tag="$(head_release_tag)"
  current_version="${current_tag#v}"
  head_sha="$(git rev-parse HEAD)"

  ensure_github_access "$repo"

  if [ "$command" = "publish" ]; then
    if [ "$dry_run" -eq 1 ]; then
      info "dry run: would publish GitHub release $current_tag in $repo"
      exit 0
    fi
    ensure_origin_ready "$current_tag" "$head_sha"
    publish_release "$repo" "$current_tag"
    info "release $current_tag is published"
    exit 0
  fi

  if [ "$command" = "resume-draft" ]; then
    load_state
    assert_state_matches_head "$current_tag" "$head_sha"
    if [ "$dry_run" -eq 1 ]; then
      info "dry run: would verify origin/main and origin tag $RELEASE_TAG, then create draft release"
      exit 0
    fi
    validate_dist "$RELEASE_VERSION"
    ensure_origin_ready "$RELEASE_TAG" "$RELEASE_HEAD_SHA"
    notes_file="$(mktemp)"
    trap 'rm -f "$notes_file"' EXIT
    write_release_notes "$notes_file"
    create_draft_release "$RELEASE_REPO" "$RELEASE_TAG" "$notes_file"
    clear_state
    info "draft release $RELEASE_TAG created successfully"
    exit 0
  fi

  previous_tag="$(previous_main_release_tag "$current_tag" || true)"
  assert_nonbump_policy "$current_tag" "$previous_tag" "$allow_nonbump"

  if [ "$dry_run" -eq 1 ]; then
    info "dry run: would run make clean"
    info "dry run: would run make test-all"
    info "dry run: would run make test-e2e"
    info "dry run: would run make world"
    info "dry run: would rerun release package tests"
    info "dry run: would validate dist/liblockdc-${current_version}-CHECKSUMS"
    info "dry run: would then require origin/main and tag $current_tag before creating a draft release"
    exit 0
  fi

  info "cleaning repository-generated state for a blank-repo validation start"
  make clean

  info "running full non-e2e test matrix via Makefile"
  make test-all

  info "running e2e tests via Makefile"
  make test-e2e

  info "running make world in the foreground"
  make world

  assert_release_package_tests
  validate_dist "$current_version"
  write_state "$repo" "$current_tag" "$current_version" "$head_sha"

  ensure_origin_ready "$current_tag" "$head_sha"

  notes_file="$(mktemp)"
  trap 'rm -f "$notes_file"' EXIT
  write_release_notes "$notes_file"
  create_draft_release "$repo" "$current_tag" "$notes_file"
  clear_state
  info "draft release $current_tag created successfully"
}

main "$@"
