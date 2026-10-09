#!/usr/bin/env bats

# Checks on the hub, the five install guides and the reference: the text they
# quote, the links and pictures they point at, and the snippets they share.
# No Docker needed.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
}

# try_it_snippet <guide>: the body of the first fenced python block under that
# guide's `## Try it` heading. The search stops at the next `## ` heading, so a
# guide whose Try-it section lost its snippet fails here rather than quietly
# comparing a block from somewhere else on the page.
try_it_snippet() {
  python3 - "$1" <<'PY'
import sys

path = sys.argv[1]
lines = open(path).read().splitlines()
start = next((i for i, l in enumerate(lines) if l.strip() == "## Try it"), None)
if start is None:
    sys.exit("%s has no '## Try it' heading" % path)
end = next((i for i in range(start + 1, len(lines))
            if lines[i].startswith("## ")), len(lines))
open_at = next((i for i in range(start + 1, end)
                if lines[i].strip() == "```python"), None)
if open_at is None:
    sys.exit("%s has no python block under '## Try it'" % path)
body = []
for line in lines[open_at + 1:end]:
    if line.strip() == "```":
        break
    body.append(line)
else:
    sys.exit("%s: the python block under '## Try it' never closes" % path)
sys.stdout.write("\n".join(body) + "\n")
PY
}

# The hub's drawing is one PNG per colour scheme behind a <picture> element,
# the way the banner above it is. The link test below reads Markdown links
# only; the picture's src and srcset are HTML attributes, so a renamed export
# would leave the hub showing a broken image with nothing to say so.
@test "the hub's drawing exists for both colour schemes" {
  grep -q '<source media="(prefers-color-scheme: dark)" srcset="docs/overview-dark.png">' README.md
  grep -qE '<img [^>]*src="docs/overview-light.png"' README.md
  [ -s docs/overview-dark.png ]
  [ -s docs/overview-light.png ]
}

# The guides quote FIX lines their reader will meet in a log, so what is on the
# page can be matched against what the terminal printed. A quote that drifts
# from the script sends that reader hunting for a string nothing prints, which
# is exactly what happened when the host scripts' hints were made shape-neutral
# and the compose guide kept the old wording. Nothing else compares the two.
#
# Inline code spans are unwrapped first: markdown soft-wraps a long span across
# source lines, so a span's text is its source lines joined with single spaces.
# `die` in the shell scripts prints "FIX: $2" and carries only the remedy in
# its source, so a quote counts as found either whole or with the "FIX: "
# prefix removed. compose.yaml is in the search set for the hints it holds
# itself.
@test "every FIX line a guide quotes is printed by a script" {
  local quoted checked=0 line
  quoted="$(python3 - README.md compose/README.md terraform/gcp/README.md \
    terraform/aws/README.md terraform/azure/README.md kubernetes/README.md <<'PY'
import re, sys

for path in sys.argv[1:]:
    fenced, prose, in_fence = [], [], False
    for raw in open(path).read().splitlines():
        if raw.lstrip().startswith("```"):
            in_fence = not in_fence
            prose.append("")          # a fence ends the paragraph around it
            continue
        (fenced if in_fence else prose).append(raw)
    for raw in fenced:
        if raw.strip().startswith("FIX:"):
            print(raw.strip())
    for para in re.split(r"\n\s*\n", "\n".join(prose)):
        joined = " ".join(para.split())
        for span in re.findall(r"`([^`]+)`", joined):
            # A bare `FIX:` names the line, it does not quote one.
            if span.startswith("FIX:") and span != "FIX:":
                print(span)
PY
)"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    if ! grep -rqF "$line" compose/scripts compose/compose.yaml &&
       ! grep -rqF "${line#FIX: }" compose/scripts compose/compose.yaml; then
      echo "a guide quotes \"$line\", which no script prints" >&2
      return 1
    fi
    checked=$((checked + 1))
  done <<<"$quoted"
  # An extractor that stopped matching would pass every guide vacuously.
  [ "$checked" -gt 0 ]
}

# The hub and the five install guides cross-link each other, the diagrams,
# the manifests and the tests. A relative link that does not resolve is a dead
# end for whoever follows it, and nothing else checks them. URLs and bare
# in-page anchors are somebody else's business; every other target has to
# exist on disk, relative to the README that names it. Fenced code blocks are
# dropped first: a `](` inside one is not a link.
@test "every relative Markdown link in the six READMEs and the reference resolves" {
  local checked=0 readme dir target
  for readme in README.md compose/README.md terraform/gcp/README.md \
                terraform/aws/README.md terraform/azure/README.md \
                kubernetes/README.md docs/REFERENCE.md; do
    [ -f "$readme" ] || { echo "$readme is missing" >&2; return 1; }
    dir="$(dirname "$readme")"
    while IFS= read -r target; do
      target="${target%%#*}"
      [ -n "$target" ] || continue
      if [ ! -e "$dir/$target" ]; then
        echo "$readme links to $target, which does not exist" >&2
        return 1
      fi
      checked=$((checked + 1))
    done < <(awk '/^```/ { fenced = !fenced; next } !fenced' "$readme" |
      grep -oE '\]\([^)]+\)' | sed -E 's/^\]\(//; s/\)$//' |
      grep -vE '^[a-z][a-z0-9+.-]*:' || true)
  done
  # A regex that stopped matching would pass every README vacuously.
  [ "$checked" -gt 0 ]
}

# The five install guides each end in the same Try-it snippet: the shapes
# differ in how the reader gets the SDK variables into their shell, but the
# sandbox they then create is deliberately the same few lines, so someone who
# has run one shape recognises it in the next. Kept byte-identical here
# because nothing else compares them, and a snippet edited in one guide alone
# reads as a difference between the shapes that does not exist.
@test "the five guides' Try-it snippets are one snippet" {
  local guide out first=""
  for guide in compose/README.md terraform/gcp/README.md terraform/aws/README.md \
               terraform/azure/README.md kubernetes/README.md; do
    out="$BATS_TEST_TMPDIR/${guide//\//_}"
    try_it_snippet "$guide" > "$out" || return 1
    [ -s "$out" ] || { echo "$guide: the Try-it snippet came out empty" >&2; return 1; }
    if [ -z "$first" ]; then first="$out"; continue; fi
    run diff -u "$first" "$out"
    [ "$status" -eq 0 ] || {
      echo "compose/README.md's snippet (-) against $guide's (+):" >&2
      echo "$output" >&2
      return 1
    }
  done
}

# The hub's What-you-get section ends with one install snippet per way to run,
# each linking its guide, and its How-to-run table names the same ways. A
# guide added without its snippet and link, or a link whose guide went away,
# is how the hub drifts from the package it introduces.
@test "the hub's install snippets link exactly the install guides on disk" {
  local links guides rows
  links="$(awk '/^## What you get/ { f = 1; next } f && /^## / { exit } f' README.md |
    grep -oE '\]\([a-z/]+/README\.md#install\)' | sed -E 's/^\]\(//; s/#install\)$//' | sort)"
  guides="$(printf '%s\n' compose/README.md terraform/*/README.md kubernetes/README.md | sort)"
  # A section the extractor stopped reading would pass vacuously.
  [ "$(printf '%s\n' "$links" | grep -c .)" -gt 1 ]
  diff <(echo "$guides") <(echo "$links")
  # and the table's first column names the same ways as the Install headings
  rows="$(awk '/^\| Run it with \|/ { f = 1; next }
               f && /^\|-/ { next }
               f && /^\|/ { print; next }
               f { exit }' README.md | awk -F' *[|] *' '{ print $2 }' | sort)"
  diff <(grep -E '^### ' README.md | sed 's/^### //' | sort) <(echo "$rows")
}
