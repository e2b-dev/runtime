#!/bin/bash

set -o pipefail

targetPath="{{ .TargetPath }}"
sourcePath="{{ .SourcePath }}"
owner="{{ .Owner }}"
permissions="{{ .Permissions }}"

workdir="{{ .Workdir }}"
user="{{ .User }}"

# Fill the workdir with user home directory if empty
if [ -z "${workdir}" ]; then
    # Use the user's home directory
    workdir=$(getent passwd "$user" | cut -d: -f6)
fi
# A user without a passwd entry leaves workdir empty; stay in the current
# directory then, as bash did implicitly before 5.3 made `cd ""` an error.
cd "${workdir:-.}" || exit 1

# Get the parent folder of the source file/folder
sourceFolder="$(dirname "$sourcePath")"

# Set targetPath relative to current working directory if not absolute
inputPath="$targetPath"
if [[ "$inputPath" = /* ]]; then
    targetPath="$inputPath"
else
    targetPath="$(pwd)/$inputPath"
fi

cd "$sourceFolder" || exit 1

# Get the entry (file, directory, or symlink) named by the source path
entry="$(basename "$sourcePath")"

if [ ! -e "$entry" ] && [ ! -L "$entry" ]; then
    echo "Error: source path does not exist: $sourcePath"
    exit 1
fi

# Check type BEFORE applying ownership/permissions to avoid dereferencing symlinks
if [ -L "$entry" ]; then
    # It's a symlink – create parent folders and move+rename it to the exact path
    if [[ "$targetPath" == */ ]]; then
        # Docker COPY semantics: a trailing slash means the target is a
        # directory — create it and move the entry inside under its own name
        mkdir -p "$targetPath"
    else
        mkdir -p "$(dirname "$targetPath")"
    fi
    # Change ownership of the symlink itself (not the target)
    chown -h "$owner" "$entry"
    # Note: chmod on symlinks affects the target, not the link itself in most systems
    # We skip chmod for symlinks as it's typically not meaningful
    mv "$entry" "$targetPath"
elif [ -f "$entry" ]; then
    # It's a file – create parent folders and move+rename it to the exact path
    chown "$owner" "$entry"
    if [ -n "$permissions" ]; then
        chmod "$permissions" "$entry"
    fi
    if [[ "$targetPath" == */ ]]; then
        # Docker COPY semantics: a trailing slash means the target is a
        # directory — create it and move the entry inside under its own name
        mkdir -p "$targetPath"
    else
        mkdir -p "$(dirname "$targetPath")"
    fi
    mv "$entry" "$targetPath"
elif [ -d "$entry" ]; then
    # It's a directory – apply ownership/permissions recursively, then merge
    # its contents into the target (Docker COPY semantics: existing directories
    # are merged into, existing files overwritten – mv can't merge into
    # non-empty directories, so copy and remove the source instead)
    chown -R "$owner" "$entry"
    if [ -n "$permissions" ]; then
        chmod -R "$permissions" "$entry"
    fi
    mkdir -p "$targetPath"
    # Merge via tar, matching Docker's tar-based COPY: unlike cp, it replaces
    # destination file symlinks instead of writing through them, follows
    # destination directory symlinks (usrmerge, e.g. /lib -> usr/lib), and
    # keeps the metadata of the target directory and other existing dirs
    (cd "$entry" && tar -cf - .) | tar -xf - -C "$targetPath" --keep-directory-symlink --no-overwrite-dir || exit 1
    # Restore write permissions so cleanup works even when a read-only
    # permissions argument was applied and we are not running as root
    chmod -R u+rwx "$entry"
    rm -rf "$entry"
else
    echo "Error: entry is neither file, directory, nor symlink"
    exit 1
fi
