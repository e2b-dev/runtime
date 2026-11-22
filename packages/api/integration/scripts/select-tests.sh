#!/usr/bin/env bash
# Print the `go test -run` regex for the tests an allow-list selects.
#
#   select-tests.sh --only <allow-list> <package>...
#
# The allow-list holds "package<TAB>test" lines, or "package<TAB>*" for a whole
# package — see compression-tests.tsv. Tests are enumerated from the source tree
# at run time, and *every* entry must match at least one of them: a named test
# that no longer exists and a wildcard whose package was renamed or emptied are
# both hard errors, so a stale list fails loudly instead of quietly shrinking
# coverage. `make check-tests-allowlist` proves that guard still bites.
set -euo pipefail

if [ "${1:-}" != "--only" ] || [ "$#" -lt 3 ]; then
	echo "usage: $0 --only <allow-list> <package>..." >&2
	exit 2
fi

only=$2
shift 2

if [ ! -r "$only" ]; then
	echo "$0: cannot read allow-list: $only" >&2
	exit 2
fi

dirs=()
for pkg in "$@"; do
	pkg=${pkg#./}
	case "$pkg" in
	*/...)
		while IFS= read -r dir; do
			dirs+=("${dir#./}")
		done < <(find "${pkg%/...}" -type d | sort)
		;;
	*) dirs+=("$pkg") ;;
	esac
done

names=$(
	for dir in "${dirs[@]}"; do
		compgen -G "${dir}/*_test.go" >/dev/null || continue
		sed -n 's/^func \(Test[A-Za-z0-9_]*\)(.*/\1/p' "${dir}"/*_test.go |
			sort -u | sed "s|^|${dir}\t|"
	done
)

if [ -z "$names" ]; then
	echo "no tests found in: $*" >&2
	exit 1
fi

printf '%s\n' "$names" | awk -F'\t' -v list="$only" '
	BEGIN {
		while ((getline line < list) > 0) {
			if (line ~ /^#/ || line == "") continue
			split(line, f, "\t")
			# Both kinds of entry start unmatched and must be hit below.
			if (f[2] == "*") wildcard[f[1]] = 0; else named[f[1] SUBSEP f[2]] = 0
		}
	}
	($1 in wildcard) { wildcard[$1]++; out = out (out ? "|" : "") $2; next }
	(($1 SUBSEP $2) in named) { named[$1 SUBSEP $2]++; out = out (out ? "|" : "") $2 }
	END {
		for (k in wildcard) if (!wildcard[k]) { print list ": no tests in package: " k > "/dev/stderr"; bad = 1 }
		for (k in named) if (!named[k]) { split(k, f, SUBSEP); print list ": no such test: " f[1] " " f[2] > "/dev/stderr"; bad = 1 }
		if (bad) exit 1
		if (out == "") { print list ": selected no tests" > "/dev/stderr"; exit 1 }
		print "^(" out ")$"
	}
'
