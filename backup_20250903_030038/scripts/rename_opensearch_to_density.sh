#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 [TARGET_DIR] [--apply]

This script will rename the OpenSearch vendored copy from the given TARGET_DIR
by replacing occurrences of 'opensearch' -> 'density' (and case variants) in
file contents and in file/directory names.

Defaults:
  TARGET_DIR=third_party/opensearch
  Mode = dry-run (no changes). Use --apply to perform changes.

Examples:
  $0                      # dry-run for third_party/opensearch
  $0 third_party/opensearch --apply   # apply changes
EOF
}

TARGET=${1:-third_party/opensearch}
APPLY=false
if [ "${2:-}" = "--apply" ] || [ "${1:-}" = "--apply" ]; then
  APPLY=true
fi

if [ "${TARGET}" = "--apply" ]; then
  TARGET=third_party/opensearch
  APPLY=true
fi

if [ ! -d "$TARGET" ]; then
  echo "ERROR: target directory '$TARGET' not found"
  usage
  exit 2
fi

echo "Target: $TARGET"
echo "Mode: $( [ "$APPLY" = true ] && echo "APPLY" || echo "DRY-RUN" )"

# Safety: create a backup when applying changes
BACKUP_DIR="${TARGET}_backup_$(date +%Y%m%d%H%M%S)"
if [ "$APPLY" = true ]; then
  echo "Creating backup copy at: $BACKUP_DIR"
  mkdir -p "$(dirname "$BACKUP_DIR")"
  rsync -a --exclude='.git' "$TARGET/" "$BACKUP_DIR/"
fi

echo "Searching for files that contain 'opensearch' or 'OpenSearch'..."
mapfile -t MATCHING_FILES < <(grep -RIl --binary-files=without-match -E "opensearch|OpenSearch|OPENSEARCH" "$TARGET" || true)

echo "Found ${#MATCHING_FILES[@]} files with matches (first 200 shown):"
for i in "${MATCHING_FILES[@]:0:200}"; do
  echo "  $i"
done

if [ "$APPLY" = false ]; then
  echo "\nDRY-RUN: no changes will be made. To apply, re-run with --apply."
  exit 0
fi

echo "Applying content replacements in text files..."
for f in "${MATCHING_FILES[@]}"; do
  # make sure it's a regular file and writable
  if [ -f "$f" ] && [ -w "$f" ]; then
    # perl in-place replace the three case variants
    perl -0777 -pe 's/OpenSearch/Density/g; s/opensearch/density/g; s/OPENSEARCH/DENSITY/g' -i.bak "$f" && rm -f "${f}.bak"
    echo "patched: $f"
  fi
done

echo "Renaming files and directories containing the target strings (bottom-up)..."
# Perform depth-first rename so children are renamed before parents
cd "$(dirname "$TARGET")"
base=$(basename "$TARGET")
root="$(pwd)/$base"

# handle several case variants in a safe order
rename_pattern() {
  local from=$1
  local to=$2
  # find and rename files/dirs
  while IFS= read -r -d '' p; do
    newp="${p//$from/$to}"
    if [ "$p" != "$newp" ]; then
      echo "mv: '$p' -> '$newp'"
      mv -v "$p" "$newp"
    fi
  done < <(find "$root" -depth -name "*$from*" -print0)
}

# Order: OpenSearch -> Density, OPENSEARCH -> DENSITY, opensearch -> density
rename_pattern 'OpenSearch' 'Density'
rename_pattern 'OPENSEARCH' 'DENSITY'
rename_pattern 'opensearch' 'density'

echo "Rename complete. Review changes. Backup is at: $BACKUP_DIR"
echo "If you want to commit these changes, test builds, then commit the backup and updated tree as needed."
