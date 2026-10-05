#!/usr/bin/env bats

# What the modules say, not whether they parse: `make lint` runs fmt, init
# without a backend and validate over both modules and their examples, and is
# the single lint path for them. These tests read the files, so they need no
# terraform on PATH.
#
# Every test name starts with the module it reads, and setup() runs the test
# from that module's directory: "gcp: ..." in terraform/gcp, "aws: ..." in
# terraform/aws, and "gcp+aws: ..." in terraform/ for the tests that compare
# the two. A check both modules must pass is a function below with one test
# per module, so a failure names the module it failed in.
#
# Assertions stand one per line. Under errexit, and so under bats, a failing
# command in an `a && b` list fails the test only when it is the list's last
# command, so a chained guard such as `[ -n "$a" ] && [ -n "$b" ]` cannot fail
# on `a`.

setup() {
  local dir
  case "$BATS_TEST_DESCRIPTION" in
    gcp+aws:*) dir=. ;;
    gcp:*) dir=gcp ;;
    aws:*) dir=aws ;;
    *)
      echo "name the test gcp:, aws: or gcp+aws: so setup() knows its module" >&2
      return 1
      ;;
  esac
  cd "$BATS_TEST_DIRNAME/../terraform/$dir" || return 1
}

# The three secrets reach the instance in its .env, and the startup script's
# output is the serial console and the cloud's log service, both readable
# with far less than the access the state file needs. `compose config`
# renders the interpolated file, a shell's xtrace traces every assignment
# however it is turned on (set -x, set -eux, set -o xtrace, sh -x), a verbose
# curl prints the headers it sends, the AWS session token among them, and a
# plain cat of the .env prints all three, so none of them may appear either.
secret_printing='docker compose logs|docker compose config|set[[:space:]]+-[A-Za-z]*x|set[[:space:]]+-o[[:space:]]+xtrace|sh[[:space:]]+-[A-Za-z]*x|curl[^|]*[[:space:]](-[A-Za-z]*v|--verbose|--trace)|cat[[:space:]]+\.env'
check_no_secret_printing() {
  # The pattern catches each form it is there for, so a clean scan below is
  # not a pattern that matches nothing.
  local form
  for form in 'docker compose logs api' 'docker compose config' 'set -x' 'set -eux' \
              'set -o xtrace' 'sh -x /x.sh' 'bash -ex /x.sh' 'curl -fsSv https://x' \
              'curl --verbose https://x' 'curl --trace-ascii - https://x' 'cat .env'; do
    grep -qE "$secret_printing" <<<"$form" || { echo "the pattern misses $form"; return 1; }
  done
  run grep -nE "$secret_printing" startup.sh.tftpl
  [ "$status" -eq 1 ] || { echo "the startup script prints what it must not: $output"; return 1; }
}

# The startup script deletes seven keys from the shipped .env and appends its
# own, which is what puts a Terraform install on the secrets in its state and
# the sizing and telemetry its variables ask for. compose.yaml reads each of
# them but one as ${KEY:-...}, so renaming one there and not here would
# silently drop the appended line: the file still parses, the stack still
# starts, and the install runs on what compose does when nothing is set -- its
# own generated team key and api secrets, which the state does not know, or
# the hugepage default rather than the requested one. The one is
# COMPOSE_PROFILES, which Compose itself reads from the project's .env and
# compose.yaml never interpolates; what has to exist for it is the profile the
# module writes into it.
check_env_keys() {
  keys="$(awk '/^  cat >> \.env <<EOF$/ { f = 1; next } f && /^EOF$/ { exit } f' \
    startup.sh.tftpl | sed -n 's/^\([A-Z_][A-Z0-9_]*\)=.*/\1/p')"
  # An anchor that stopped matching would pass the test vacuously.
  [ -n "$keys" ]

  # The sed just above the heredoc strips the shipped value of each key before
  # the override is appended. The two lists have to be the same: a key appended
  # but not stripped leaves the shipped default sitting above the override in
  # the same file, and a key stripped but not appended drops it altogether.
  deleted="$(grep -F 'sed -i -E' startup.sh.tftpl |
    grep -oE '\([A-Z0-9_|]+\)' | tr -d '()' | tr '|' '\n' | sort)"
  [ -n "$deleted" ]
  diff <(printf '%s\n' "$deleted") <(printf '%s\n' "$keys" | sort) || {
    echo "the sed delete list (-) and the appended keys (+) differ"
    return 1
  }

  while read -r key; do
    if [ "$key" = COMPOSE_PROFILES ]; then
      grep -qF 'var.otel_collector ? "otel" : ""' main.tf || {
        echo "main.tf no longer writes COMPOSE_PROFILES as otel or nothing"
        return 1
      }
      grep -qxF '    profiles: [otel]' ../../compose/compose.yaml || {
        echo "the startup script can turn on the otel profile, which no compose.yaml service has"
        return 1
      }
      continue
    fi
    # -F: the literal contains `$`, which macOS grep reads as an anchor.
    grep -qF "\${$key" ../../compose/compose.yaml || {
      echo "the startup script appends $key, which compose.yaml never reads"
      return 1
    }
  done <<<"$keys"
}

# Two variables, one setting: an endpoint of the operator's own wins, and the
# built-in collector implies its own loopback address when none is given.
check_collector_vars() {
  # shellcheck disable=SC2016  # the ${...} are the template's, matched literally with -F
  grep -qxF 'E2B_OTEL_COLLECTOR_GRPC_ENDPOINT=${otel_endpoint}' startup.sh.tftpl
  # shellcheck disable=SC2016
  grep -qxF 'COMPOSE_PROFILES=${compose_profiles}' startup.sh.tftpl
  # Squeezed, so the alignment `terraform fmt` picks does not matter.
  tr -s ' ' < main.tf | grep -qF 'otel_endpoint = var.otel_collector_grpc_endpoint != "" ? var.otel_collector_grpc_endpoint : (var.otel_collector ? "127.0.0.1:4317" : "")'
  tr -s ' ' < main.tf | grep -qF 'compose_profiles = var.otel_collector ? "otel" : ""'
  for var in otel_collector_grpc_endpoint otel_collector; do
    grep -qx "variable \"$var\" {" variables.tf || { echo "variables.tf has no $var"; return 1; }
    grep -qF "| \`$var\` |" README.md || { echo "the README's variables table has no $var"; return 1; }
  done
  # The address the built-in collector implies is the one its receiver binds.
  grep -qx '        endpoint: 127.0.0.1:4317' ../../compose/config/otel/otel-collector.yaml
}

# The services take host:port and nothing else, and the value lands unquoted
# in the instance's .env at first boot, so the variable refuses anything else
# at plan time rather than after a replace. Checked with the rule's own
# regex, which ERE reads the way Terraform's RE2 does.
check_endpoint_regex() {
  re="$(sed -n 's/.*can(regex("\(.*\)", var\.otel_collector_grpc_endpoint)).*/\1/p' variables.tf |
    sed 's/\\\\/\\/g')"
  # A pattern that stopped matching would pass the refusals vacuously.
  [ -n "$re" ]
  grep -qF 'condition     = var.otel_collector_grpc_endpoint == "" ||' variables.tf
  for ok in collector:4317 10.0.0.5:4317 '[::1]:4317' 127.0.0.1:4317; do
    grep -qE "$re" <<<"$ok" || { echo "refuses $ok"; return 1; }
  done
  # shellcheck disable=SC2016  # a literal $(...), which the heredoc would run
  for bad in http://collector:4317 collector collector:4317/ 'a b:1' '$(id):1'; do
    run grep -qE "$re" <<<"$bad"
    [ "$status" -eq 1 ] || { echo "accepts $bad"; return 1; }
  done
}

# The comment at the top of firewall.tf is the whole reasoning for the one rule
# an operator's CIDRs reach: it lists every port the stack binds and then names
# the few that are opened. Nothing else ties that prose to the HCL under it, so
# a port added to the rule and not the sentence -- or dropped from the sentence
# and left in the rule -- leaves the file arguing with itself, and the next
# reader deciding what the module exposes believes the sentence.
# check_firewall_header <opened ports, one per line>: each module finds the
# ports its own rule opens and passes them in.
check_firewall_header() {
  local opened="$1" header documented
  header="$(sed -e '/^[^#]/,$d' -e 's/^#[[:space:]]*//' firewall.tf | tr '\n' ' ')"
  documented="$(printf '%s\n' "$header" |
    sed -n 's/.*[[:space:]]only \(.*\) are opened.*/\1/p' |
    grep -oE '[0-9]+' | sort -n)"
  # An anchor that stopped matching would pass the test vacuously.
  [ -n "$documented" ]
  [ -n "$opened" ]

  diff <(printf '%s\n' "$documented") <(printf '%s\n' "$opened") || {
    echo "the header comment (-) and the clients rule (+) name different ports"
    return 1
  }
}

@test "gcp: the two shipped files are embedded from this directory, not fetched by default" {
  # -F: the literals contain `$`, which macOS grep reads as an anchor.
  # shellcheck disable=SC2016  # the ${path.module} is Terraform's, matched literally with -F
  grep -qF 'file("${path.module}/../../compose/compose.yaml")' main.tf
  # shellcheck disable=SC2016
  grep -qF 'file("${path.module}/../../compose/.env")' main.tf
  # The template assembles the URL from $MD, so match its parts.
  grep -q 'computeMetadata/v1/instance/attributes' startup.sh.tftpl
  grep -q 'docker compose up -d --wait' startup.sh.tftpl
  # Each metadata key is written in main.tf and read in the template, so a
  # rename in one file alone leaves the boot fetching a key nothing wrote.
  for key in e2b-compose-yaml e2b-dot-env; do
    grep -qF "$key" main.tf || { echo "$key is not in main.tf"; return 1; }
    grep -qF "$key" startup.sh.tftpl || { echo "$key is not in startup.sh.tftpl"; return 1; }
  done
}

@test "gcp: the startup script never prints the secrets it writes" {
  check_no_secret_printing
}

@test "gcp: the startup script writes only .env keys the compose project reads" {
  check_env_keys
}

@test "gcp: the collector variables reach the instance's .env" {
  check_collector_vars
}

@test "gcp: the endpoint variable takes host:port and refuses a URL" {
  check_endpoint_regex
}

@test "gcp: the firewall header names the ports the clients rule opens" {
  # The clients rule's own allow block, the first one in the file.
  opened="$(awk '/"google_compute_firewall" "clients"/ { f = 1 }
                 f && /ports +=/ { print; exit }' firewall.tf |
    grep -oE '[0-9]+' | sort -n)"
  check_firewall_header "$opened"
}

@test "aws: the firewall header names the ports the clients rule opens" {
  # The rules iterate this one list, so it is what they open.
  opened="$(sed -n 's/^  client_ports = \[\(.*\)\]$/\1/p' firewall.tf |
    grep -oE '[0-9]+' | sort -n)"
  check_firewall_header "$opened"
}

# A security group's inline ingress blocks and separate rule resources fight
# each other, and a second rule resource, a legacy aws_security_group_rule
# included, is a second policy nobody reads next to the header. So: one
# ingress resource, over the client ports and the operator's CIDRs, TCP, one
# port per rule; one egress rule for everything. Squeezed, every line of a
# block is one space and its text, so the settings match as whole lines.
@test "aws: the security group lets in the client ports from client_cidrs and nothing else" {
  [ "$(cat ./*.tf | grep -c '^resource "aws_vpc_security_group_ingress_rule"')" -eq 1 ] || {
    echo "expected exactly one aws_vpc_security_group_ingress_rule resource"
    return 1
  }
  run grep -nE '^resource "aws_security_group_rule"' ./*.tf
  [ "$status" -eq 1 ] || { echo "a legacy aws_security_group_rule: $output"; return 1; }
  run grep -nE '^[[:space:]]*(ingress|egress)[[:space:]]*(\{|=)' ./*.tf
  [ "$status" -eq 1 ] || { echo "inline ingress/egress rules: $output"; return 1; }
  rule="$(awk '/^resource "aws_vpc_security_group_ingress_rule" "clients"/ { f = 1 }
               f { print } f && /^}/ { exit }' firewall.tf | tr -s ' ')"
  [ -n "$rule" ]
  grep -qF 'setproduct(local.client_ports, distinct(var.client_cidrs))' <<<"$rule"
  grep -qxF ' ip_protocol = "tcp"' <<<"$rule"
  grep -qxF ' from_port = each.value.port' <<<"$rule"
  grep -qxF ' to_port = each.value.port' <<<"$rule"
  grep -qxF ' cidr_ipv4 = each.value.cidr' <<<"$rule"
  [ "$(cat ./*.tf | grep -c '^resource "aws_vpc_security_group_egress_rule"')" -eq 1 ]
  egress="$(awk '/^resource "aws_vpc_security_group_egress_rule" "all"/ { f = 1 }
                 f { print } f && /^}/ { exit }' firewall.tf | tr -s ' ')"
  [ -n "$egress" ]
  grep -qxF ' ip_protocol = "-1"' <<<"$egress"
  grep -qxF ' cidr_ipv4 = "0.0.0.0/0"' <<<"$egress"
}

# Sandboxes are Firecracker microVMs, so the instance needs /dev/kvm: a
# virtual instance only from the families AWS offers nested virtualization
# on, or any x86-64 metal type. A type outside the list launches, boots and
# then fails preflight on the instance; the variable refuses it at plan time.
# The arm64 types (Graviton, a1 among them) and the Mac types, which run
# only on dedicated hosts, are refused even as metal. Checked with the
# rules' own regexes, which ERE reads the way RE2 does.
@test "aws: instance_type takes the nested-virtualization types and refuses Graviton" {
  allow="$(sed -n 's/^    condition     = can(regex("\(.*\)", var\.instance_type))$/\1/p' variables.tf |
    sed 's/\\\\/\\/g')"
  deny="$(sed -n 's/^    condition     = !can(regex("\(.*\)", var\.instance_type))$/\1/p' variables.tf |
    sed 's/\\\\/\\/g')"
  # A pattern that stopped matching would pass the refusals vacuously.
  [ -n "$allow" ]
  [ -n "$deny" ]
  grep -qx '  default     = "m8i.xlarge"' variables.tf
  for ok in m8i.xlarge c8i.large r8i.2xlarge m7i.4xlarge c7i-flex.xlarge \
            m7i-flex.large r7i.48xlarge m8i.metal-48xl c7i.metal-24xl i4i.metal; do
    grep -qE "$allow" <<<"$ok" || { echo "refuses $ok"; return 1; }
    run grep -qE "$deny" <<<"$ok"
    [ "$status" -eq 1 ] || { echo "calls $ok Graviton"; return 1; }
  done
  for bad in m6i.xlarge m5.large t3.xlarge c6a.large m8i M8I.XLARGE ''; do
    run grep -qE "$allow" <<<"$bad"
    [ "$status" -eq 1 ] || { echo "accepts $bad"; return 1; }
  done
  for refused in c7g.xlarge m7g.large m8g.xlarge r8g.2xlarge c7gn.large c7g.metal m7g.metal \
                 a1.metal mac1.metal mac2.metal mac2-m2pro.metal mac-m4.metal; do
    grep -qE "$deny" <<<"$refused" || { echo "does not refuse $refused"; return 1; }
  done
  # An explicit zone must offer the type too, or the group's launches fail
  # only after apply has waited out its capacity timeout.
  tr -s ' ' < main.tf |
    grep -qF 'condition = var.availability_zone == "" ? length(self.locations) > 0 : contains(self.locations, var.availability_zone)' || {
    echo "main.tf does not check an explicit availability_zone against the zones that offer instance_type"
    return 1
  }
  # So must the zone the subnet was created in, which ignore_changes keeps
  # when instance_type changes later.
  tr -s ' ' < network.tf |
    grep -qF 'condition = contains(data.aws_ec2_instance_type_offerings.this.locations, self.availability_zone)' || {
    echo "network.tf does not check the subnet's zone against the zones that offer instance_type"
    return 1
  }
  # Those zones are the region's own only: a Local or Wavelength Zone an
  # account opted in to sorts first, and an instance there cannot take the
  # region's Elastic IP.
  tr -s ' ' < main.tf | grep -qF 'values = ["availability-zone"]' || {
    echo "main.tf does not list the region's own zones apart from Local and Wavelength Zones"
    return 1
  }
  tr -s ' ' < main.tf | grep -qF 'values = data.aws_availability_zones.regional.names' || {
    echo "main.tf does not limit the zones that offer instance_type to the region's own"
    return 1
  }
}

# The VPC is IPv4 only, so an IPv6 client CIDR would plan and then fail at
# apply; cidrnetmask() accepts only IPv4 prefixes, which is what the rule
# relies on. It accepts host bits (203.0.113.5/24) too, which the security
# group rule refuses at plan time, so the validation also compares each entry
# with its network address; and a CIDR listed twice must not become a
# duplicate key in the rules' for_each. vpc_cidr is held to the same network
# address, within its /16 to /20, and kept off the orchestrator's two sandbox
# networks on the instance.
@test "aws: client_cidrs and vpc_cidr take IPv4 network addresses only" {
  tr -s ' ' < variables.tf |
    grep -qF 'condition = alltrue([for c in var.client_cidrs : can(cidrnetmask(c)) && try(cidrsubnet(c, 0, 0) == c, false)])'
  grep -qF 'condition     = length(var.client_cidrs) > 0' variables.tf
  grep -qF 'setproduct(local.client_ports, distinct(var.client_cidrs))' firewall.tf
  tr -s ' ' < variables.tf |
    grep -qF 'condition = can(cidrnetmask(var.vpc_cidr)) && try(cidrsubnet(var.vpc_cidr, 0, 0) == var.vpc_cidr && tonumber(split("/", var.vpc_cidr)[1]) >= 16 && tonumber(split("/", var.vpc_cidr)[1]) <= 20, false)'
  tr -s ' ' < variables.tf |
    grep -qF 'condition = !contains(["10.11", "10.12"], try(join(".", slice(split(".", cidrhost(var.vpc_cidr, 0)), 0, 2)), ""))'
}

# The two shipped files are uploaded from this repository, the way the GCP
# module embeds them in metadata: a module source that is not the repository
# fails at plan time rather than booting an instance on files from elsewhere.
# The etag makes a changed file a changed object on the next apply.
@test "aws: the two shipped files are uploaded from this directory to a private bucket" {
  squeezed="$(tr -s ' ' < storage.tf)"
  for f in compose.yaml .env; do
    # The object keyed $f, so its source and etag are checked against the
    # same file: a swap between the two objects would otherwise pass.
    object="$(awk -v key="key = \"$f\"" '
      /^resource "aws_s3_object" / { block = ""; in_block = 1 }
      in_block { block = block $0 "\n" }
      in_block && /^}/ { in_block = 0; if (index(block, key)) printf "%s", block }
    ' <<<"$squeezed")"
    [ -n "$object" ] || { echo "no object is keyed $f"; return 1; }
    grep -qF "source = \"\${path.module}/../../compose/$f\"" <<<"$object" ||
      { echo "the object keyed $f does not upload compose/$f"; return 1; }
    grep -qF "etag = filemd5(\"\${path.module}/../../compose/$f\")" <<<"$object" ||
      { echo "the object keyed $f has no filemd5 etag of compose/$f"; return 1; }
    # S3 allows an object only 10 tags, so var.tags with 10 would fail the
    # apply here; the bucket carries them instead.
    run grep -qE '^ *tags *=' <<<"$object"
    [ "$status" -eq 1 ] || { echo "the object keyed $f is tagged"; return 1; }
  done
  for setting in block_public_acls block_public_policy ignore_public_acls restrict_public_buckets; do
    grep -qF "$setting = true" <<<"$squeezed" || { echo "$setting is not true"; return 1; }
  done
  grep -qF 'status = "Enabled"' <<<"$squeezed"
  grep -qF 'sse_algorithm = "AES256"' <<<"$squeezed"
  grep -qF 'force_destroy = true' <<<"$squeezed"
  run grep -nE '^resource "aws_s3_bucket_(policy|acl)"' ./*.tf
  [ "$status" -eq 1 ] || { echo "the bucket has a policy or an ACL: $output"; return 1; }
}

# The bucket name is name and a hyphen, then a suffix Terraform picks. S3
# refuses a bucket name that starts with a prefix it reserves only when
# CreateBucket runs, at apply; the variable refuses it at plan time, and it
# reads name with that hyphen, since a name of sthree makes a bucket sthree-...
# Checked with the rules' own regexes, which ERE reads the way RE2 does.
@test "aws: name refuses the bucket name prefixes S3 reserves" {
  # shellcheck disable=SC2016  # the ${var.name} is Terraform's, matched literally
  grep -qF 'bucket_prefix = "${var.name}-"' storage.tf
  allow="$(sed -n 's/^    condition     = can(regex("\(.*\)", var\.name))$/\1/p' variables.tf)"
  # shellcheck disable=SC2016
  deny="$(sed -n 's/^    condition     = !can(regex("\(.*\)", "\${var\.name}-"))$/\1/p' variables.tf)"
  # A pattern that stopped matching would pass the refusals vacuously.
  [ -n "$allow" ]
  [ -n "$deny" ]
  grep -qx '  default     = "e2b-embed"' variables.tf
  # The last three hold a reserved string past the start, which only a rule
  # anchored at the start lets through.
  for ok in e2b-embed embed-01 xn-embed sthreeembed amzn-s3-demox \
            embed-xn--x embed-sthree-x embed-amzn-s3-demo-x; do
    grep -qE "$allow" <<<"$ok" || { echo "refuses $ok"; return 1; }
    run grep -qE "$deny" <<<"$ok-"
    [ "$status" -eq 1 ] || { echo "calls $ok reserved"; return 1; }
  done
  # Each passes the first rule, so only the reserved-prefix rule refuses it.
  for reserved in xn--embed sthree-embed sthree amzn-s3-demo-x amzn-s3-demo; do
    grep -qE "$allow" <<<"$reserved" || { echo "the first rule already refuses $reserved"; return 1; }
    grep -qE "$deny" <<<"$reserved-" || { echo "does not refuse $reserved"; return 1; }
  done
}

# Anyone with a shell on the instance, and any process on the host, holds its
# role, so the role is the blast radius of a compromised install. Its one
# policy reads the two objects, moves this install's Elastic IP to itself,
# reports itself to its own group and runs the SSM agent for Session Manager
# and Run Command. It attaches no managed policy: AWS's one for the agent,
# AmazonSSMManagedInstanceCore, also reads any Parameter Store parameter in
# the account and region by name.
@test "aws: the instance role reads the two objects, claims its address, reports its health, runs the ssm agent and nothing more" {
  # The SSM agent's heartbeat, Session Manager's channels and Run Command's
  # messages, and no more of what the managed policy grants.
  agent_actions="$(printf '%s\n' ssm:UpdateInstanceInformation \
    ssmmessages:CreateControlChannel ssmmessages:CreateDataChannel \
    ssmmessages:OpenControlChannel ssmmessages:OpenDataChannel \
    ec2messages:AcknowledgeMessage ec2messages:DeleteMessage ec2messages:FailMessage \
    ec2messages:GetEndpoint ec2messages:GetMessages ec2messages:SendReply | sort)"
  # Every quoted service:action and service:key, for any service. Each ARN
  # holds a ${ and the principal has no colon, so only the actions and the
  # condition keys match.
  # IAM matches the service prefix and the action name without regard to case
  # and takes ? as a wildcard, so the scan does too, keeping the case it finds.
  actions="$(grep -oiE '"[a-z0-9*?-]+:[a-z0-9*?]+"' iam.tf | tr -d '"' | sort -u)"
  expected="$(printf '%s\n' autoscaling:SetInstanceHealth ec2:AssociateAddress ec2:InstanceProfile ec2:Vpc s3:GetObject sts:AssumeRole "$agent_actions" | sort)"
  diff <(printf '%s\n' "$expected") <(printf '%s\n' "$actions") || {
    echo "iam.tf names actions and condition keys (+) other than the ones it should (-)"
    return 1
  }
  # Not even in a comment: the parameter reads are what the agent's
  # statement leaves out of the managed policy.
  run grep -niF 'ssm:getparameter' iam.tf
  [ "$status" -eq 1 ] || { echo "iam.tf names a parameter read: $output"; return 1; }
  # One policy and no attachment in the whole module, so no second grant sits
  # in another file where the scan above does not look, and no managed
  # policy grants what the scan cannot see.
  [ "$(cat ./*.tf | grep -cE '^resource "aws_iam_(role_policy|policy)"')" -eq 1 ] || {
    echo "expected exactly one IAM policy resource in the module"
    return 1
  }
  run grep -nE '^resource "aws_iam_(role_)?policy_attachment' ./*.tf
  [ "$status" -eq 1 ] || { echo "a policy attachment: $output"; return 1; }
  run grep -nF ':iam::aws:policy/' ./*.tf
  [ "$status" -eq 1 ] || { echo "an AWS managed policy: $output"; return 1; }
  # Every ARN iam.tf names, each once: a resource added beside the right ones,
  # a second statement on one of them, or a wildcard spelled as an ARN shows
  # up in this diff.
  # shellcheck disable=SC2016  # Terraform's ${...}, literal in single quotes
  arns="$(printf '%s\n' \
    'arn:${data.aws_partition.current.partition}' \
    '${aws_s3_bucket.this.arn}/${aws_s3_object.compose_yaml.key}' \
    '${aws_s3_bucket.this.arn}/${aws_s3_object.dot_env.key}' \
    '${local.arn_prefix}:ec2:${local.region}:${local.account_id}:elastic-ip/${aws_eip.this.allocation_id}' \
    '${local.arn_prefix}:ec2:${local.region}:${local.account_id}:instance/*' \
    '${local.arn_prefix}:ec2:${local.region}:${local.account_id}:network-interface/*' \
    '${local.arn_prefix}:autoscaling:${local.region}:${local.account_id}:autoScalingGroup:*:autoScalingGroupName/${var.name}' |
    sort)"
  diff <(printf '%s\n' "$arns") <(grep -oE '"(arn:|[$][{])[^"]*"' iam.tf | tr -d '"' | sort) || {
    echo "iam.tf names ARNs (+) other than the ones it should (-)"
    return 1
  }
  # Both diffs read quoted strings only, so every Action and Resource value is
  # quoted strings and nothing else: an unquoted reference, such as a local or
  # a resource's arn, would grant what neither diff sees. Each value is read
  # from its key to the end of its line, or to its list's closing bracket.
  unquoted="$(awk '
    { s = $0 }
    !f && sub(/^[[:space:]]*"?(Action|Resource)"?[[:space:]]*=/, "", s) {
      f = 1
      n++
      list = (s ~ /^[[:space:]]*\[/)
    }
    f {
      gsub(/"[^"]*"/, "", s)
      rest = s
      gsub(/[][,[:space:]]/, "", rest)
      if (rest != "") print NR ": " $0
      if (!list || s ~ /\]/) f = 0
    }
    END { if (!n) print "no Action or Resource at all" }' iam.tf)"
  [ -z "$unquoted" ] || { echo "an Action or Resource that is not only quoted strings: $unquoted"; return 1; }
  # The instance and interface ARNs match any in the account and region, so
  # each statement's own condition is what holds it to this install.
  grep -A2 -F ':instance/*"]' iam.tf | grep -qF 'ArnEquals = { "ec2:InstanceProfile" = aws_iam_instance_profile.this.arn }'
  grep -A2 -F ':network-interface/*"]' iam.tf | grep -qF 'ArnEquals = { "ec2:Vpc" = aws_vpc.this.arn }'
  # An Allow with NotAction or NotResource grants everything but what it
  # names, and a role argument or an _exclusive resource attaches a policy
  # the counts above do not see.
  run grep -nE '(NotAction|NotResource|NotPrincipal)[[:space:]]*=|managed_policy_arns|inline_policy|_exclusive"' ./*.tf
  [ "$status" -eq 1 ] || { echo "a grant the scans above do not read: $output"; return 1; }
  # The agent's actions name no narrower resource, so its statement holds
  # the module's one bare wildcard, and holds those actions alone. A bare
  # wildcard, as an action or a resource in the string or the list form, is
  # a quoted star anywhere in the module; one written as an ARN shows in the
  # ARN diff.
  agent="$(awk '/Sid[[:space:]]*=[[:space:]]*"RunTheSsmAgent"/ { f = 1 } f { print } f && /^[[:space:]]*},?$/ { exit }' iam.tf | tr -s ' ')"
  [ -n "$agent" ]
  diff <(printf '%s\n' "$agent_actions") <(grep -oiE '"[a-z0-9*?-]+:[a-z0-9*?]+"' <<<"$agent" | tr -d '"' | sort) || {
    echo "the agent's statement names actions (+) other than the agent's (-)"
    return 1
  }
  grep -qxF ' Resource = ["*"]' <<<"$agent"
  [ "$(cat ./*.tf | grep -cF '"*"')" -eq 1 ] || {
    echo "a bare wildcard outside the agent's statement: $(grep -nF '"*"' ./*.tf)"
    return 1
  }
}

@test "aws: the startup script never prints the secrets it writes" {
  check_no_secret_printing
}

@test "aws: the startup script writes only .env keys the compose project reads" {
  check_env_keys
}

@test "aws: the endpoint variable takes host:port and refuses a URL" {
  check_endpoint_regex
}

# env_block <template>: the lines from the sed that strips the shipped keys
# through the chmod that closes the .env, inclusive.
env_block() {
  awk '/^  sed -i -E / { f = 1 } f { print } f && /^  chmod 0600 \.env$/ { exit }' "$1"
}

# The two modules put the same install on the same Compose files, so they
# write the same .env overrides; this is the block check_env_keys reasons
# about. Kept byte-identical so a key added on one cloud reaches the other.
@test "gcp+aws: both startup scripts write the same .env block" {
  gcp="$(env_block gcp/startup.sh.tftpl)"
  aws="$(env_block aws/startup.sh.tftpl)"
  # An anchor that stopped matching would compare two empty strings.
  [ -n "$gcp" ]
  [ -n "$aws" ]
  [ "$(printf '%s\n' "$gcp" | wc -l)" -gt 5 ]
  diff <(printf '%s\n' "$gcp") <(printf '%s\n' "$aws") || {
    echo "the gcp (-) and aws (+) .env blocks differ"
    return 1
  }
}

# variable_block <file> <name>: one variable's declaration, braces included.
variable_block() {
  awk -v want="variable \"$2\" {" '$0 == want { f = 1 } f { print } f && /^}$/ { exit }' "$1"
}

# The inputs that mean the same on both clouds are declared the same, with
# the same validation, so an install moves between them unchanged.
@test "gcp+aws: the variables both modules take are declared the same" {
  local var gcp aws
  for var in compose_base_url team_api_key hugepages otel_collector_grpc_endpoint otel_collector; do
    gcp="$(variable_block gcp/variables.tf "$var")"
    aws="$(variable_block aws/variables.tf "$var")"
    [ -n "$gcp" ] || { echo "gcp/variables.tf has no $var"; return 1; }
    [ -n "$aws" ] || { echo "aws/variables.tf has no $var"; return 1; }
    diff <(printf '%s\n' "$gcp") <(printf '%s\n' "$aws") || {
      echo "$var is declared differently in gcp (-) and aws (+)"
      return 1
    }
  done
}

# Every ${...} in the template is Terraform's (startup.sh.tftpl's header says
# the shell side never uses braces), so the placeholders and the keys main.tf
# passes to templatefile() must be the same set: a placeholder with no key
# fails the plan, and a key no placeholder uses is a value that never reaches
# the instance.
@test "aws: every template placeholder is a templatefile key, and every key is used" {
  # shellcheck disable=SC2016  # tr deletes the three characters, nothing expands
  used="$(grep -oE '\$\{[a-z_0-9]+\}' startup.sh.tftpl | tr -d '${}' | sort -u)"
  passed="$(awk '/templatefile\("\$\{path\.module\}\/startup\.sh\.tftpl", \{/ { f = 1; next }
                 f && /^  \}\)$/ { exit }
                 f' main.tf |
    sed -n 's/^[[:space:]]*\([a-z_0-9]*\)[[:space:]]*=.*/\1/p' | sort -u)"
  [ -n "$used" ]
  [ -n "$passed" ]
  diff <(printf '%s\n' "$passed") <(printf '%s\n' "$used") || {
    echo "main.tf passes (-) and the template uses (+) different names"
    return 1
  }
  run grep -nF '%{' startup.sh.tftpl
  [ "$status" -eq 1 ] || { echo "a template directive: $output"; return 1; }
}

# EC2 takes at most 16 KB of user data, and the startup script is all of it.
# Rendered here without Terraform: the template's own bytes, with every
# placeholder counted at 256 bytes, more than any value the module computes
# (the longest are the 64-character secrets and a bucket host under 100) and
# than any sane operator value. The launch template's precondition is the
# exact guard at plan time; this keeps the template far enough below it.
@test "aws: the startup script fits EC2's 16 KB user-data limit with room to spare" {
  bytes="$(wc -c < startup.sh.tftpl | tr -d ' ')"
  placeholders="$(grep -oE '\$\{[a-z_0-9]+\}' startup.sh.tftpl | wc -l | tr -d ' ')"
  budget=$((bytes + placeholders * 256))
  echo "template $bytes bytes, $placeholders placeholders, worst case $budget bytes"
  [ "$placeholders" -gt 0 ]
  [ "$budget" -lt 16384 ]
}

# render_template <file>: the template with each placeholder replaced by a
# value of its own shape, so the shell side can be checked as shell.
render_template() {
  sed -E -e 's/\$\{(admin_token|sandbox_access_token_hash_seed|compose_sha256|env_sha256)\}/0123456789abcdef/g' \
         -e 's/\$\{dashboard_host\}/203.0.113.10/g' \
         -e 's/\$\{hugepages\}/2048/g' \
         -e 's/\$\{[a-z_0-9]+\}/placeholder/g' "$1"
}

# cloud-init runs the script with its shebang, and /bin/sh on Ubuntu is dash:
# no bash. The two scripts it writes are checked the same way, since
# nothing else runs them before an instance does.
@test "aws: the startup script and the scripts it writes are POSIX sh" {
  [ "$(head -n 1 startup.sh.tftpl)" = '#!/bin/sh' ]
  run grep -nE '\[\[[[:space:]]|pipefail|<<<|^[[:space:]]*function[[:space:]]|\$'"'"'|^[[:space:]]*source[[:space:]]' startup.sh.tftpl
  [ "$status" -eq 1 ] || { echo "bash-only syntax: $output"; return 1; }
  render_template startup.sh.tftpl > "$BATS_TEST_TMPDIR/startup.sh"
  sed -n "/^cat > \/usr\/local\/lib\/e2b-embed\/aws.sh <<'EOF'$/,/^EOF$/p" "$BATS_TEST_TMPDIR/startup.sh" |
    sed '1d;$d' > "$BATS_TEST_TMPDIR/aws.sh"
  sed -n "/^cat > \/usr\/local\/sbin\/e2b-embed-watchdog <<'EOF'$/,/^EOF$/p" "$BATS_TEST_TMPDIR/startup.sh" |
    sed '1d;$d' > "$BATS_TEST_TMPDIR/watchdog.sh"
  local f
  for f in startup.sh aws.sh watchdog.sh; do
    [ -s "$BATS_TEST_TMPDIR/$f" ] || { echo "$f came out empty"; return 1; }
    if command -v dash >/dev/null 2>&1; then
      dash -n "$BATS_TEST_TMPDIR/$f" || { echo "$f is not valid sh"; return 1; }
    else
      sh -n "$BATS_TEST_TMPDIR/$f" || { echo "$f is not valid sh"; return 1; }
    fi
  done
  # SC2157: after rendering, compose_base_url is a constant, as Terraform
  # makes it on the instance.
  if command -v shellcheck >/dev/null 2>&1; then
    shellcheck -s sh -e SC2157 "$BATS_TEST_TMPDIR/startup.sh" "$BATS_TEST_TMPDIR/aws.sh" "$BATS_TEST_TMPDIR/watchdog.sh"
  fi
}

# The role's credentials sign three kinds of call. On a command line they
# would be in every process listing on the instance for the length of the
# call, so curl reads them from its standard input as a config file (-K -),
# and nothing else prints them.
@test "aws: the role's credentials reach curl on its standard input only" {
  run grep -nE -- '--user|[[:space:]]-u[[:space:]]|-H[[:space:]]+.x-amz-security-token' startup.sh.tftpl
  [ "$status" -eq 1 ] || { echo "credentials on a command line: $output"; return 1; }
  # Apart from being set and checked for emptiness, the secret key and the
  # session token appear on one line, which pipes into curl -K -.
  local var uses line
  for var in AWS_SK AWS_ST; do
    # shellcheck disable=SC2016  # the template's own shell text, matched literally
    uses="$(grep -n "$var" startup.sh.tftpl | grep -vE "^[0-9]+:[[:space:]]*$var=" |
      grep -vF '[ -n "$AWS_AK" ] && [ -n "$AWS_SK" ] && [ -n "$AWS_ST" ]')"
    [ "$(printf '%s\n' "$uses" | wc -l)" -eq 1 ] || { echo "$var is used on more than one line: $uses"; return 1; }
    line="${uses%%:*}"
    sed -n "${line}p" startup.sh.tftpl | grep -qE '\|$' || { echo "line $line does not pipe"; return 1; }
    sed -n "$((line + 1))p" startup.sh.tftpl | grep -qF 'curl -sS -m 60 -K - --aws-sigv4' ||
      { echo "line $line does not feed curl -K -"; return 1; }
  done
}

# The first boot calls aws_env through retry, in an `until` condition, where
# errexit does not apply: a metadata read that fails leaves its variable
# empty and the function goes on. So aws_env itself fails unless every value
# it reads came back, and retry tries again rather than letting
# AssociateAddress run with no instance or region. Run against a stub
# metadata service, one path failing at a time.
@test "aws: aws_env fails when the metadata service leaves any value it reads empty" {
  sed -n "/^cat > \/usr\/local\/lib\/e2b-embed\/aws.sh <<'EOF'$/,/^EOF$/p" startup.sh.tftpl |
    sed '1d;$d' > "$BATS_TEST_TMPDIR/aws.sh"
  [ -s "$BATS_TEST_TMPDIR/aws.sh" ]
  cat > "$BATS_TEST_TMPDIR/check.sh" <<'EOF'
# check.sh <aws.sh> <metadata path that fails, or none>
. "$1"
fail=$2
curl() {
  for arg; do url=$arg; done
  path=${url#http://169.254.169.254/latest/}
  [ "$path" != "meta-data/$fail" ] || return 22
  case $path in
    api/token) echo token ;;
    meta-data/instance-id) echo i-0123456789abcdef0 ;;
    meta-data/placement/region) echo us-east-1 ;;
    meta-data/iam/security-credentials/) echo role ;;
    meta-data/iam/security-credentials/role)
      printf '{\n  "Code" : "Success",\n  "AccessKeyId" : "AKIDEXAMPLE",\n  "SecretAccessKey" : "sk",\n  "Token" : "st"\n}\n' ;;
    *) return 22 ;;
  esac
}
aws_env
rc=$?
echo "$INSTANCE_ID $REGION $AWS_AK"
exit "$rc"
EOF
  local shell=sh missing
  if command -v dash >/dev/null 2>&1; then
    shell=dash
  fi
  # The stub answers every path, so the refusals below are not vacuous.
  run "$shell" "$BATS_TEST_TMPDIR/check.sh" "$BATS_TEST_TMPDIR/aws.sh" none
  [ "$status" -eq 0 ] || { echo "fails with every value present: $output"; return 1; }
  [ "$output" = "i-0123456789abcdef0 us-east-1 AKIDEXAMPLE" ]
  for missing in instance-id placement/region iam/security-credentials/ iam/security-credentials/role; do
    run "$shell" "$BATS_TEST_TMPDIR/check.sh" "$BATS_TEST_TMPDIR/aws.sh" "$missing"
    [ "$status" -ne 0 ] || { echo "succeeds when $missing fails: $output"; return 1; }
  done
}

# The instance claims the address before it fetches anything, and waits until
# it answers on it, so the files, the images and E2B_DASHBOARD_HOST all
# agree on one address.
@test "aws: the instance claims its Elastic IP before it fetches the files" {
  associate="$(grep -n 'Action=AssociateAddress' startup.sh.tftpl | cut -d: -f1)"
  # shellcheck disable=SC2016  # the ${...} is the template's
  wait_for="$(grep -n 'retry 20 has_address "${dashboard_host}"' startup.sh.tftpl | cut -d: -f1)"
  fetch="$(grep -n 'aws_curl s3 ' startup.sh.tftpl | head -n 1 | cut -d: -f1)"
  [ -n "$associate" ]
  [ -n "$wait_for" ]
  [ -n "$fetch" ]
  [ "$associate" -lt "$wait_for" ]
  [ "$wait_for" -lt "$fetch" ]
  # shellcheck disable=SC2016  # the ${...} are the template's
  grep -qF 'AllocationId=${eip_allocation_id}&InstanceId=$INSTANCE_ID&AllowReassociation=true' startup.sh.tftpl
  tr -s ' ' < main.tf | grep -qF 'eip_allocation_id = aws_eip.this.allocation_id'
  tr -s ' ' < main.tf | grep -qF 'dashboard_host = aws_eip.this.public_ip'
}

# The files come from the two objects the module wrote, checked against the
# hashes of the files it read, unless compose_base_url points elsewhere. The
# keys are the objects' own, so an object renamed in storage.tf is renamed
# here too, and referencing them orders the upload before any launch.
@test "aws: the compose files come from the two objects unless compose_base_url is set" {
  # shellcheck disable=SC2016
  grep -qF 'retry 10 aws_curl s3 "https://${s3_host}/${compose_key}" -f -o compose.yaml' startup.sh.tftpl
  # shellcheck disable=SC2016
  grep -qF 'retry 10 aws_curl s3 "https://${s3_host}/${env_key}" -f -o .env' startup.sh.tftpl
  # shellcheck disable=SC2016
  grep -qF 'echo "${compose_sha256}  compose.yaml" | sha256sum -c --quiet' startup.sh.tftpl
  # shellcheck disable=SC2016
  grep -qF 'echo "${env_sha256}  .env" | sha256sum -c --quiet' startup.sh.tftpl
  # shellcheck disable=SC2016
  [ "$(grep -cF 'if [ -n "${compose_base_url}" ]; then' startup.sh.tftpl)" -eq 2 ]
  squeezed="$(tr -s ' ' < main.tf)"
  grep -qF 'compose_key = aws_s3_object.compose_yaml.key' <<<"$squeezed"
  grep -qF 'env_key = aws_s3_object.dot_env.key' <<<"$squeezed"
  grep -qF 's3_host = aws_s3_bucket.this.bucket_regional_domain_name' <<<"$squeezed"
  # shellcheck disable=SC2016
  grep -qF 'compose_sha256 = filesha256("${path.module}/../../compose/compose.yaml")' <<<"$squeezed"
  # shellcheck disable=SC2016
  grep -qF 'env_sha256 = filesha256("${path.module}/../../compose/.env")' <<<"$squeezed"
}

# The downloads run just after the association changes the instance's public
# address, and under set -e one that fails once aborts the first boot, so
# each retries: from the bucket and from compose_base_url alike.
@test "aws: every download after the address moves retries" {
  fetches="$(sed -n '/^retry 20 has_address /,/^systemctl enable e2b-embed\.service$/p' startup.sh.tftpl |
    grep -E '(^|[[:space:]])(aws_)?curl[[:space:]]')"
  # Two files, each from the bucket or from compose_base_url.
  [ "$(printf '%s\n' "$fetches" | wc -l)" -eq 4 ]
  run grep -vE '^[[:space:]]*retry 10 ' <<<"$fetches"
  [ "$status" -eq 1 ] || { echo "a download that does not retry: $output"; return 1; }
}

# cloud-init runs user data once per instance, so the every-boot `up` the GCP
# module gets from its startup script is a systemd unit here: without it a
# reboot brings the containers back without the MSS clamp.
@test "aws: e2b-embed.service runs up -d --wait on every boot" {
  unit="$(awk "/^cat > \/etc\/systemd\/system\/e2b-embed.service <<'EOF'$/ { f = 1; next }
               f && /^EOF$/ { exit } f" startup.sh.tftpl)"
  [ -n "$unit" ]
  for line in 'Type=oneshot' 'RemainAfterExit=yes' 'WorkingDirectory=/opt/e2b' \
              'ExecStart=/usr/bin/docker compose up -d --wait' 'TimeoutStartSec=20min' \
              'Requires=docker.service' 'After=docker.service network-online.target' \
              'WantedBy=multi-user.target'; do
    grep -qxF "$line" <<<"$unit" || { echo "e2b-embed.service has no $line"; return 1; }
  done
  grep -qxF 'systemctl enable e2b-embed.service' startup.sh.tftpl
  grep -qxF 'systemctl start e2b-embed.service' startup.sh.tftpl
}

# The watchdog is what heals the group: ten minutes of failed /health, then
# SetInstanceHealth. It is enabled before Docker is installed, so a first
# boot that fails anywhere after it is replaced, as a managed instance group
# replaces one that never answers its health check.
@test "aws: the watchdog reports the instance Unhealthy after ten minutes without /health" {
  watchdog="$(awk "/^cat > \/usr\/local\/sbin\/e2b-embed-watchdog <<'EOF'$/ { f = 1; next }
                   f && /^EOF$/ { exit } f" startup.sh.tftpl)"
  [ -n "$watchdog" ]
  grep -qF 'curl -fsS -m 10 -o /dev/null http://127.0.0.1:3000/health' <<<"$watchdog"
  # shellcheck disable=SC2016  # the watchdog's own shell text, matched literally
  grep -qxF '[ "$failing" -ge 600 ] || exit 0' <<<"$watchdog"
  # shellcheck disable=SC2016
  grep -qF 'Action=SetInstanceHealth&Version=2011-01-01&InstanceId=$INSTANCE_ID&HealthStatus=Unhealthy' <<<"$watchdog"
  # The timer is what runs it: OnUnitActiveSec alone never fires, since the
  # service has not yet run when the timer starts, so OnBootSec gives the
  # first run; WantedBy starts the timer on every later boot.
  timer_unit="$(awk "/^cat > \/etc\/systemd\/system\/e2b-embed-watchdog.timer <<'EOF'$/ { f = 1; next }
                     f && /^EOF$/ { exit } f" startup.sh.tftpl)"
  [ -n "$timer_unit" ]
  for line in 'OnBootSec=1min' 'OnUnitActiveSec=1min' 'WantedBy=timers.target'; do
    grep -qxF "$line" <<<"$timer_unit" || { echo "e2b-embed-watchdog.timer has no $line"; return 1; }
  done
  service_unit="$(awk "/^cat > \/etc\/systemd\/system\/e2b-embed-watchdog.service <<'EOF'$/ { f = 1; next }
                       f && /^EOF$/ { exit } f" startup.sh.tftpl)"
  [ -n "$service_unit" ]
  grep -qxF 'Type=oneshot' <<<"$service_unit"
  grep -qxF 'ExecStart=/usr/local/sbin/e2b-embed-watchdog' <<<"$service_unit"
  timer="$(grep -nxF 'systemctl enable --now e2b-embed-watchdog.timer' startup.sh.tftpl | cut -d: -f1)"
  docker="$(grep -nF 'install -y -qq docker-ce' startup.sh.tftpl | cut -d: -f1)"
  [ -n "$timer" ]
  [ -n "$docker" ]
  [ "$timer" -lt "$docker" ]
}

# The watchdog's clock, run rather than read: the script as the instance gets
# it, with its clock and aws.sh moved into the test's directory, a stub curl
# that answers /health from a file, and a stub aws.sh that logs the report
# instead of sending it. Every good answer restarts the clock, a failure
# starts it only when none is set, and only a failure on a clock ten minutes
# old or more reports the instance.
@test "aws: the watchdog's clock restarts on every good answer and reports after ten minutes" {
  local dir=$BATS_TEST_TMPDIR shell=sh
  if command -v dash >/dev/null 2>&1; then
    shell=dash
  fi
  sed -n "/^cat > \/usr\/local\/sbin\/e2b-embed-watchdog <<'EOF'$/,/^EOF$/p" startup.sh.tftpl |
    sed -e '1d;$d' \
        -e "s|^stamp=/run/e2b-embed-watchdog\.last-ok\$|stamp=$dir/last-ok|" \
        -e "s|^\. /usr/local/lib/e2b-embed/aws\.sh\$|. $dir/aws.sh|" > "$dir/watchdog"
  # Both paths were moved, or the runs below would use the instance's own.
  grep -qxF "stamp=$dir/last-ok" "$dir/watchdog"
  grep -qxF ". $dir/aws.sh" "$dir/watchdog"
  cat > "$dir/aws.sh" <<'EOF'
aws_env() {
  echo aws_env >> "$WATCHDOG_DIR/calls"
  INSTANCE_ID=i-0123456789abcdef0
}
aws_query() {
  echo "$1 $2" >> "$WATCHDOG_DIR/calls"
}
EOF
  mkdir -p "$dir/bin"
  cat > "$dir/bin/curl" <<'EOF'
#!/bin/sh
for arg; do url=$arg; done
[ "$url" = http://127.0.0.1:3000/health ] && [ "$(cat "$WATCHDOG_DIR/health")" = up ]
EOF
  chmod +x "$dir/bin/curl"
  # The watchdog reads its clock with GNU stat, as Ubuntu has it; a BSD stat,
  # as on macOS, answers the same question with -f %m.
  if ! stat -c %Y "$dir" >/dev/null 2>&1; then
    cat > "$dir/bin/stat" <<'EOF'
#!/bin/sh
[ "$1" = -c ] && [ "$2" = %Y ] && exec /usr/bin/stat -f %m "$3"
exit 2
EOF
    chmod +x "$dir/bin/stat"
  fi
  export WATCHDOG_DIR=$dir
  # watchdog <up|down>: one run of the timer's service, /health answering or not.
  watchdog() {
    echo "$1" > "$dir/health"
    PATH="$dir/bin:$PATH" "$shell" "$dir/watchdog"
  }
  # age <seconds>: put the clock's last good answer that many seconds back,
  # in UTC so no daylight-saving hour moves it. GNU date takes -d @seconds,
  # BSD date -r seconds.
  age() {
    local t stamp
    t=$(($(date +%s) - $1))
    stamp="$(TZ=UTC date -d "@$t" +%Y%m%d%H%M.%S 2>/dev/null || TZ=UTC date -r "$t" +%Y%m%d%H%M.%S)"
    TZ=UTC touch -t "$stamp" "$dir/last-ok"
  }
  # since: the seconds since the clock's last good answer.
  since() {
    echo $(($(date +%s) - $(stat -c %Y "$dir/last-ok" 2>/dev/null || stat -f %m "$dir/last-ok")))
  }

  # A good answer with no clock sets one and reports nothing.
  watchdog up
  [ -f "$dir/last-ok" ]
  [ ! -e "$dir/calls" ]
  # A failure with no clock starts one there and reports nothing.
  rm "$dir/last-ok"
  watchdog down
  [ -f "$dir/last-ok" ]
  [ "$(since)" -lt 60 ]
  [ ! -e "$dir/calls" ]
  # Five minutes of failures: nothing yet, and the clock stays where it was.
  age 300
  watchdog down
  [ ! -e "$dir/calls" ]
  [ "$(since)" -ge 300 ]
  # A good answer restarts a clock eleven minutes old, so the failure after
  # it reports nothing.
  age 660
  watchdog up
  [ "$(since)" -lt 60 ]
  watchdog down
  [ ! -e "$dir/calls" ]
  # Eleven minutes of failures: one report, for this instance.
  age 660
  watchdog down
  diff <(printf '%s\n' aws_env 'autoscaling Action=SetInstanceHealth&Version=2011-01-01&InstanceId=i-0123456789abcdef0&HealthStatus=Unhealthy') "$dir/calls"
}

# lt_block: the launch template resource, squeezed so fmt's alignment does not
# matter. Every line is then one space and its text, so the tests match whole
# lines: a substring match would take a hop limit of 10 for 1.
lt_block() {
  awk '/^resource "aws_launch_template" "this"/ { f = 1 } f { print } f && /^}$/ { exit }' main.tf | tr -s ' '
}

# IMDSv2 only, one hop: a token is required for every read, and the PUT that
# issues one cannot cross Docker's bridge, so only the host and its
# host-network services reach the instance's role credentials.
@test "aws: the metadata service takes IMDSv2 tokens only, one hop" {
  lt="$(lt_block)"
  [ -n "$lt" ]
  grep -qxF ' http_endpoint = "enabled"' <<<"$lt"
  grep -qxF ' http_tokens = "required"' <<<"$lt"
  grep -qxF ' http_put_response_hop_limit = 1' <<<"$lt"
}

# The launch-time public address carries the first boot's downloads and its
# AssociateAddress call until the Elastic IP replaces it, and the root volume,
# which holds the .env and its three secrets, is encrypted.
@test "aws: the instance launches with a public address and an encrypted root volume" {
  lt="$(lt_block)"
  [ -n "$lt" ]
  grep -qxF ' associate_public_ip_address = true' <<<"$lt"
  grep -qxF ' device_name = "/dev/sda1"' <<<"$lt"
  grep -qxF ' encrypted = true' <<<"$lt"
}

# The instance must expose /dev/kvm: nested virtualization on every virtual
# type; a metal type has it without the flag.
@test "aws: the launch template turns on nested virtualization unless the type is metal" {
  lt="$(lt_block)"
  grep -qxF ' dynamic "cpu_options" {' <<<"$lt"
  grep -qxF ' for_each = local.metal ? [] : [1]' <<<"$lt"
  grep -qxF ' nested_virtualization = "enabled"' <<<"$lt"
  # shellcheck disable=SC2016  # Terraform's regex, matched literally with -F
  tr -s ' ' < main.tf | grep -qxF ' metal = can(regex("\\.metal", var.instance_type))'
}

# The rendered script is the whole of the user data. The textual budget test
# above keeps the template small; this is the exact check at plan time, for
# an operator value long enough to overflow anyway.
@test "aws: the launch template refuses user data over 16 KB" {
  lt="$(lt_block)"
  grep -qxF ' user_data = base64encode(local.startup_script)' <<<"$lt"
  grep -qxF ' condition = length(local.startup_script) < 16384' <<<"$lt"
}

# The group's own tags stay off what it launches (propagate_at_launch =
# false), so the module's tags reach the instance, its volume and its network
# interface through the launch template, one tag_specifications block each.
@test "aws: the launch template tags the instance, its volume and its network interface" {
  lt="$(lt_block)"
  [ -n "$lt" ]
  types="$(grep -A1 -xF ' tag_specifications {' <<<"$lt" |
    sed -n 's/^ resource_type = "\(.*\)"$/\1/p' | LC_ALL=C sort)"
  diff <(printf '%s\n' instance network-interface volume) <(printf '%s\n' "$types") || {
    echo "the launch template tags (+) other than the instance, its volume and its interface (-)"
    return 1
  }
  [ "$(grep -A2 -xF ' tag_specifications {' <<<"$lt" | grep -cxF ' tags = local.tags')" -eq 3 ]
}

# Canonical's parameter is read only when ami is empty, so an account that may
# not read the public parameters still plans with an ami of its own.
@test "aws: an explicit ami skips the Ubuntu parameter" {
  ssm="$(awk '/^data "aws_ssm_parameter" "ubuntu"/ { f = 1 } f { print } f && /^}$/ { exit }' main.tf | tr -s ' ')"
  [ -n "$ssm" ]
  grep -qxF ' count = var.ami == "" ? 1 : 0' <<<"$ssm"
  tr -s ' ' < main.tf |
    grep -qxF ' ami = var.ami != "" ? var.ami : one(data.aws_ssm_parameter.ubuntu[*].insecure_value)'
}

# One instance holding one Elastic IP: the group keeps exactly one, heals it
# on the watchdog's report after a grace period that covers a first boot, and
# replaces it delete-before-create. Its launch template version follows the
# template, so the next replacement runs the newest files. A replaced subnet
# or security group replaces the group first, since neither can be deleted
# under a running instance, and the group waits for everything its first boot
# needs that it does not reference: the role's grants, the route, the egress
# rule and the wait for the instance profile to propagate.
@test "aws: the group is one instance, healed by its watchdog and replaced delete-before-create" {
  asg="$(awk '/^resource "aws_autoscaling_group" "this"/ { f = 1 } f { print } f && /^}$/ { exit }' main.tf | tr -s ' ')"
  [ -n "$asg" ]
  for line in 'min_size = 1' 'max_size = 1' 'desired_capacity = 1' \
              'health_check_type = "EC2"' 'health_check_grace_period = 900' \
              'min_healthy_percentage = 0' 'max_healthy_percentage = 100' \
              'version = aws_launch_template.this.latest_version' \
              'replace_triggered_by = [aws_subnet.this.id, aws_security_group.this.id]' \
              'aws_iam_role_policy.this,' \
              'aws_route.internet,' 'aws_route_table_association.this,' \
              'aws_vpc_security_group_egress_rule.all,' \
              'time_sleep.iam_propagation,'; do
    grep -qxF " $line" <<<"$asg" || { echo "the group has no $line"; return 1; }
  done
  run grep -nE 'instance_refresh|load_balancers|target_group_arns' <<<"$asg"
  [ "$status" -eq 1 ] || { echo "$output"; return 1; }
  # The policy names the group by var.name, so the group must be named so.
  grep -qxF ' name = var.name' <<<"$asg"
}

# IAM is eventually consistent: a launch that names a new instance profile
# before EC2 accepts it fails with "Authentication Failure". The launch
# template waits on a sleep that starts once the profile and the policy
# exist, and a profile or policy replaced under a new name starts it again.
@test "aws: the launch waits 30 seconds for a new instance profile to propagate" {
  delay="$(awk '/^resource "time_sleep" "iam_propagation"/ { f = 1 } f { print } f && /^}$/ { exit }' iam.tf | tr -s ' ')"
  [ -n "$delay" ]
  grep -qxF ' create_duration = "30s"' <<<"$delay"
  grep -qxF ' profile = aws_iam_instance_profile.this.arn' <<<"$delay"
  grep -qxF ' policy = aws_iam_role_policy.this.id' <<<"$delay"
  grep -qxF ' depends_on = [time_sleep.iam_propagation]' <<<"$(lt_block)"
  tr -s ' ' < versions.tf | grep -qxF ' source = "hashicorp/time"'
}

# out_value <name>: the value line of output <name> in outputs.tf, squeezed
# as lt_block's lines are, so a test checks each value in the output that
# carries it.
out_value() {
  awk -v want="output \"$1\" {" '$0 == want { f = 1 } f && /^  value/ { print; exit }' outputs.tf | tr -s ' '
}

# Every address a reader is handed -- the three URL outputs and the browser's
# sandbox host -- is the Elastic IP, which exists before any instance does.
# Each port is checked in its own output: an api_url on 3001 would send the
# SDK to the dashboard.
@test "aws: every address the module hands out is the Elastic IP" {
  # shellcheck disable=SC2016  # Terraform's ${...}, compared literally
  [ "$(out_value api_url)" = ' value = "http://${aws_eip.this.public_ip}:3000"' ]
  # shellcheck disable=SC2016
  [ "$(out_value sandbox_url)" = ' value = "http://${aws_eip.this.public_ip}:3002"' ]
  # shellcheck disable=SC2016
  [ "$(out_value dashboard_url)" = ' value = "http://${aws_eip.this.public_ip}:3001"' ]
  tr -s ' ' < main.tf | grep -qF 'dashboard_host = aws_eip.this.public_ip'
}

# While the group replaces or refreshes its instance it lists the one going
# away, or only one still pending, so the session goes to the instance in
# service. The snippet runs under eval against a stub aws, which records the
# arguments as the AWS CLI would receive them.
@test "aws: session_command opens a session on the group's in-service instance" {
  local value
  value="$(out_value session_command)"
  value=${value#' value = "'}
  value=${value%'"'}
  # shellcheck disable=SC2016  # Terraform's ${...}, replaced literally
  value=${value//'${local.region}'/eu-west-1}
  # shellcheck disable=SC2016
  value=${value//'${aws_autoscaling_group.this.name}'/e2b-embed}
  aws() {
    printf '%s\n' "$@" > "$BATS_TEST_TMPDIR/$1"
    [ "$1" = ssm ] || echo i-0123456789abcdef0
  }
  eval "$value"
  # shellcheck disable=SC2016  # JMESPath's backticks, compared literally
  diff <(printf '%s\n' autoscaling describe-auto-scaling-groups --region eu-west-1 \
           --auto-scaling-group-names e2b-embed \
           --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`] | [0].InstanceId' \
           --output text) "$BATS_TEST_TMPDIR/autoscaling"
  diff <(printf '%s\n' ssm start-session --region eu-west-1 --target i-0123456789abcdef0) \
       "$BATS_TEST_TMPDIR/ssm"
}

# outputs <file>: the output names a .tf file declares, sorted.
outputs() {
  sed -n 's/^output "\([a-z0-9_]*\)".*/\1/p' "$1" | LC_ALL=C sort
}

# install_block <readme>: the first hcl fence under the guide's ## Install.
install_block() {
  awk '/^## Install$/ { s = 1; next }
       s && /^## / { exit }
       s && /^```hcl$/ { f = 1; next }
       f && /^```$/ { exit }
       f' "$1"
}

# The same four outputs for the SDK and the browser on both clouds; each
# names its own group and its own way onto the instance, and each example
# forwards all but the group. Each guide's Install block is what a reader
# copies, so it forwards what its example does.
@test "gcp+aws: both modules give the SDK and the browser the same outputs" {
  diff <(printf '%s\n' api_url dashboard_url e2b_api_key instance_group sandbox_url ssh_command) \
       <(outputs gcp/outputs.tf)
  diff <(printf '%s\n' api_url autoscaling_group dashboard_url e2b_api_key sandbox_url session_command) \
       <(outputs aws/outputs.tf)
  diff <(printf '%s\n' api_url dashboard_url e2b_api_key sandbox_url ssh_command) \
       <(outputs gcp/examples/basic/main.tf)
  diff <(printf '%s\n' api_url dashboard_url e2b_api_key sandbox_url session_command) \
       <(outputs aws/examples/basic/main.tf)
  local cloud
  for cloud in gcp aws; do
    diff <(outputs "$cloud/examples/basic/main.tf") <(outputs <(install_block "$cloud/README.md")) || {
      echo "$cloud: the example (-) and the README's Install block (+) forward different outputs"
      return 1
    }
  done
}

@test "aws: the collector variables reach the instance's .env" {
  check_collector_vars
}
