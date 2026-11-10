# E2B Embed on one EC2 instance with Terraform

Creates one Ubuntu 24.04 instance with nested virtualization, in an Auto
Scaling group of one, which installs Docker, writes the two files Embed ships
and runs `docker compose up -d --wait`. It is a single-machine package
rather than a deployment pattern; the hub is
[`../../README.md`](../../README.md).

## Requirements

- Terraform 1.7.5 or newer.
- An AWS account and credentials the AWS provider can read (an SSO profile
  in `AWS_PROFILE`, or any other source it supports), allowed to create a
  VPC and its network, an Elastic IP, a security group, an IAM role and
  instance profile, an S3 bucket, a launch template and an Auto Scaling
  group; to pass the instance role to EC2 (`iam:PassRole`); to launch from
  the template (`ec2:RunInstances` and `ec2:CreateTags`, which Auto Scaling
  checks against the caller when it creates the group); to read Canonical's
  public Ubuntu SSM parameter (`ssm:GetParameter`) when `ami` is empty; and,
  in an account that has never had an Auto Scaling group, to create Auto
  Scaling's service-linked role (`iam:CreateServiceLinkedRole`).
- The root volume is encrypted with the account's default EBS key. If that
  default is a customer-managed KMS key, its key policy has to let Auto
  Scaling's service-linked role use it, or every launch fails with
  `Client.InternalError`.
- A region that offers a nested-virtualization instance type, `m8i.xlarge`
  by default. The module puts the instance in the first of the region's own
  zones (not a Local or Wavelength Zone) that offers the type.
- The AWS CLI v2 and its Session Manager plugin on your own machine, for the
  `session_command` output, the port forwarding under Limitations and the
  instance refresh under Upgrading. `terraform apply` needs neither.
- No machine of your own. The module creates it.
- The module source has to be the repository, not a copy of this directory:
  it reads `../../compose/compose.yaml` and `../../compose/.env` relative to
  itself, which the `github.com/...//` source form below provides by cloning.
  A copy of this directory alone fails at plan time, and so does an absolute
  local path, which Terraform copies the same way. A checkout of your own
  works through a relative `source`, such as `../runtime/embed/terraform/aws`.

## Install

```hcl
provider "aws" {
  region = "us-east-1"
}

module "e2b" {
  source       = "github.com/e2b-dev/runtime//embed/terraform/aws?ref=<commit>"
  client_cidrs = ["203.0.113.0/24"]   # where your SDK clients connect from
}

output "api_url" { value = module.e2b.api_url }
output "sandbox_url" { value = module.e2b.sandbox_url }
output "dashboard_url" { value = module.e2b.dashboard_url }
output "session_command" { value = module.e2b.session_command }
output "e2b_api_key" {
  value     = module.e2b.e2b_api_key
  sensitive = true
}
```

```bash
terraform init && terraform apply
```

A module's outputs are not the root module's, so the five `output` blocks are
what makes `terraform output` see them; without them the Try it commands below
print nothing.

`?ref=<commit>` pins the source to a commit of the public repository's `main`
branch, so an `init` on another day fetches the same `compose.yaml` and `.env`;
upgrading is a deliberate change of that ref. Use `main` itself for the newest
code. The instance answers `GET /health` about three minutes after `apply`
returns, the Docker install and the image pulls behind it. The `base`
template build runs on past that, so the first sandbox can be created about
four minutes after `apply` returns.

## Try it

Put this install's three SDK variables in your shell:

```bash
export E2B_API_URL="$(terraform output -raw api_url)"
export E2B_SANDBOX_URL="$(terraform output -raw sandbox_url)"
export E2B_API_KEY="$(terraform output -raw e2b_api_key)"
```

Open `terraform output -raw dashboard_url` in a browser and paste the key
(`terraform output -raw e2b_api_key`) into its key form. The instance's `.env`
carries `E2B_DASHBOARD_HOST=<its Elastic IP>`, so the browser reaches sandbox
traffic at the same address, no tunnel needed.

Install the SDK in a virtual environment, which is what keeps it off the
system Python that Ubuntu's `pip` refuses to write to. This runs on your own
machine, not on the instance; on Ubuntu install `python3-venv` first with
`sudo apt-get update && sudo apt-get install -y python3-venv`, or it fails
with `ensurepip is not available`.

```bash
python3 -m venv .venv && .venv/bin/pip install e2b==2.46.0
```

Then create a sandbox. Save this and run it with `.venv/bin/python`:

```python
from e2b import Sandbox
sbx = Sandbox.create("base")
print(sbx.commands.run("echo hello from the sandbox").stdout)
sbx.kill()
```

The first attempt can fail with `404: tag 'default' does not exist for
template 'local-dev-team/base'`. That is the `base` build still finishing,
not a broken install: wait a minute and run the snippet again.

`curl -s -H "X-API-Key: $E2B_API_KEY" $E2B_API_URL/v2/templates` lists
`base` from the moment its build starts rather than when it finishes, so a
listing is not the go-ahead for the snippet above. Or run the packaged smoke
test on the instance itself: it does the same with the JavaScript SDK in a
container, then reaches a port inside the sandbox through client-proxy:

```bash
eval "$(terraform output -raw session_command)"
cd /opt/e2b && sudo docker compose --profile test run --rm smoke
```

## Remove

```bash
terraform destroy   # the instance, the bucket with its objects, and every other resource the module made
```

## Reference

### What it creates

Twenty-seven resources with one client CIDR, three more for each further
one: a VPC with one public subnet, an internet gateway, a route table and
its default route; an Elastic IP; a security group, a rule per client port
and CIDR, and one egress rule; an IAM role with its instance profile and
its one inline policy, and a 30-second `time_sleep` that gives the new
profile time to reach EC2 (Timings); a private, versioned, encrypted S3
bucket holding the two files; a launch template (`m8i.xlarge`, a 50 GB
encrypted gp3 root volume, nested virtualization on, IMDSv2 only, the
startup script as user data); an Auto Scaling group of one; and the three
secrets under Secrets below. The providers are aws, random and time.

The group replaces the instance when the instance's own watchdog reports it
unhealthy: `GET :3000/health` has failed for ten minutes. A replacement
starts from a fresh volume, so it rebuilds the `base` template and the
templates you built are gone.

### Security group

Three ports, and nothing else reaches the instance from outside: 3000, 3001
and 3002 from `client_cidrs`. There is no port 22 and no health-checker
range: Session Manager reaches the instance through its agent's outbound
connection, and the group hears about the instance's health from the
instance itself. The orchestrator's unauthenticated control port 5008 in
particular stays inside the instance.

### Timings

| Event | Time |
|-------|------|
| a new install's `terraform apply`, between the instance profile and the Auto Scaling group | 30 seconds |
| first boot, until `GET /health` answers | about 3 minutes |
| first boot, until the first `Sandbox.create` succeeds | about 4 minutes |
| a replacement after `/health` starts failing | about 14 minutes: 11 of failing, then a first boot |
| an instance refresh (see Upgrading), until `GET /health` answers on the new instance | about 3 minutes |
| a reboot of the instance | about 1 minute |

Expected on the default `m8i.xlarge` in `us-east-1`, but for the first row,
which is a fixed wait: IAM takes some seconds to make a new instance profile
usable by EC2, and a launch that names it sooner fails with
`Authentication Failure. Launching EC2 instance failed.` If the first launch
still fails that way, run `terraform apply` again.

To follow a boot:

```bash
eval "$(terraform output -raw session_command)"
sudo tail -f /var/log/cloud-init-output.log   # the first boot
sudo journalctl -u e2b-embed -f               # the stack's up, on every boot
```

The `session_command` output is a shell snippet that looks the current
instance id up, which is why it is used through `eval`. From outside,
`aws ec2 get-console-output --region <region> --instance-id <id> --latest --query Output --output text`
prints the same first-boot log (`<id>` as in the `id=` line under
Limitations).

### Secrets

`ADMIN_TOKEN`, `SANDBOX_ACCESS_TOKEN_HASH_SEED` and the team API key are all
generated per install, here by Terraform itself: the startup script writes
all three into the instance's `.env` before the first `up`, so the pair the
`api-secrets` one-shot generates on the instance is never read. To rotate
either api secret, replace its resource and then replace the instance as
Upgrading describes: `terraform apply -replace=module.e2b.random_bytes.admin_token`
or `-replace=module.e2b.random_bytes.sandbox_access_token_hash_seed` makes a
new launch-template version; a new hash seed ends the tokens of running
`secure` sandboxes. They live in the Terraform state, so treat the state file
and `terraform show` as secret, and they reach the instance through its user
data, which every version of the launch template keeps. Anyone in the
account allowed `ec2:DescribeLaunchTemplateVersions` can read it: that
action cannot be limited to one launch template, and read-only access such
as AWS's `ReadOnlyAccess` managed policy (`ec2:Describe*`) includes it. So
can anyone allowed `ec2:DescribeInstanceAttribute` or
`ec2:GetLaunchTemplateData`, which read it from the instance, and any
process on the host, root or not, through the metadata service. The
metadata service takes IMDSv2 tokens only, one network hop away, so
containers on Docker's bridge network cannot reach it; the host, its
host-network services and a Session Manager shell can. The startup script
does not print them; read the key with `terraform output -raw e2b_api_key`.
Set `team_api_key` to choose the key instead; change it and replace the
instance to rotate it. Keeping the three out of user data, in SSM Parameter
Store parameters only the instance's role can read, would narrow who can see
them; this module does not do that.

The instance's role may read the two files in the bucket, move this
install's Elastic IP to itself, mark its own instance unhealthy, and make
the calls the SSM agent needs for Session Manager and Run Command:
`ssm:UpdateInstanceInformation`, its heartbeat, and the `ssmmessages` and
`ec2messages` actions that carry sessions and commands. Those are on every
resource: the two message services have no resource to name, and the
heartbeat's instance does not exist when the policy is written. The role
reads no Parameter Store parameter and no other S3 object, and it does not
carry AWS's managed `AmazonSSMManagedInstanceCore` policy, which would let
it read any parameter in the account and region by name. Systems Manager
features beyond sessions and Run Command, such as State Manager
associations, Inventory and Patch Manager, are not granted. Anyone with a
shell on the instance, and any process on the host, holds that role.

The dashboard keeps the key you paste in an httpOnly browser cookie for a
year, not marked Secure because the instance serves plain http; sign-out
clears it.

### Upgrading

A newer ref points at newer files. `terraform apply` uploads them to the
bucket and records a new launch-template version, which the group uses for
the next instance it launches, but it does not replace the running one, and
that instance wrote its `compose.yaml` and `.env` once, on first boot.
Replace it deliberately:

```bash
aws autoscaling start-instance-refresh --region <region> --auto-scaling-group-name <name> --preferences '{"MinHealthyPercentage":0,"MaxHealthyPercentage":100,"InstanceWarmup":300}'
```

`<name>` is the module's `name`, `e2b-embed` unless you set it. The two
percentages matter: the instance holds the Elastic IP, so the refresh must
terminate it before it launches the new one (the group's own maintenance
policy says the same for every replacement). That is a fresh instance, so
the `base` template is rebuilt and the templates you built are gone. The
warm-up only shortens the wait for the refresh's own status, which
otherwise sits out the group's 900-second grace period. Expected:
`aws autoscaling describe-instance-refreshes --region <region> --auto-scaling-group-name <name> --query 'InstanceRefreshes[0].Status'`
reads `Successful` about 5 minutes after the refresh starts, a couple of
minutes after the stack answers on the new instance.

Unlike the GCP module's group, a replacement this group makes on its own,
after the watchdog reports the instance unhealthy, also boots the newest
launch-template version, so it comes up on the newest files. The AMI
follows Canonical's parameter the same way: an apply after Canonical
publishes a new Ubuntu 24.04 image records it in a new launch-template
version, and the next replacement boots it. Set `ami` to pin one.

`compose_base_url` points the first boot at the Compose files of a specific
commit instead of the files the module ships, for example
`https://raw.githubusercontent.com/e2b-dev/runtime/<commit>/embed/compose`.

### Telemetry

`otel_collector = true` runs the built-in OpenTelemetry collector on the
instance, keeping the sandbox and team metrics behind the dashboard's
monitoring charts in its own ClickHouse, and `otel_collector_grpc_endpoint`
sends the services' metrics, traces and logs to a collector of your own. Set
one or the other: with both, the services export to your endpoint and the
built-in collector receives nothing, so the dashboard's charts stay empty.
Both reach the instance's `.env` at first boot only, so changing either later
takes the replacement under Upgrading; the reference's
[Observability](../../docs/REFERENCE.md#observability) has the rest.

### Limitations

- No persistent data volume. Every replacement starts from a fresh volume,
  rebuilds the `base` template and loses the templates you built.
- No load-balancer health check. The instance's own watchdog is what reports
  it unhealthy, and the group's EC2 status checks cover an instance that
  stops responding altogether. Stopping the stack for more than ten minutes,
  with `docker compose down` for example, therefore gets the instance
  replaced: stop the watchdog first with
  `sudo systemctl stop e2b-embed-watchdog.timer`, and start it again only
  once `GET /health` answers, since it counts from the last answer it saw.
- Template builds with `copy()` steps upload through the orchestrator's port
  5008, which the security group keeps closed. Run them on the instance, or
  forward the four ports through Session Manager and point the SDK at
  `http://127.0.0.1:3000` and `http://127.0.0.1:3002`. One session per port:

  ```bash
  for port in 3000 3001 3002 5008; do
    eval "$(terraform output -raw session_command) --document-name AWS-StartPortForwardingSession --parameters portNumber=$port,localPortNumber=$port" &
  done
  ```

  The four sessions are jobs of your shell and run until you stop them:
  `kill %1 %2 %3 %4`, or the numbers `jobs` shows. Or one `ssh` through
  Session Manager forwards all four, in the foreground until Ctrl-C. Ubuntu's
  image ships EC2 Instance Connect, which accepts a key of your own for 60
  seconds, so nothing is installed on the instance and no port 22 is opened;
  `ssh` has to connect within that minute, and your credentials need
  `ec2-instance-connect:SendSSHPublicKey`:

  ```bash
  id="$(aws autoscaling describe-auto-scaling-groups --region <region> --auto-scaling-group-names <name> --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`] | [0].InstanceId' --output text)"
  aws ec2-instance-connect send-ssh-public-key --region <region> --instance-id "$id" --instance-os-user ubuntu --ssh-public-key file://~/.ssh/id_ed25519.pub
  ssh -i ~/.ssh/id_ed25519 -N -L 3000:127.0.0.1:3000 -L 3001:127.0.0.1:3001 -L 3002:127.0.0.1:3002 -L 5008:127.0.0.1:5008 \
    -o IdentitiesOnly=yes -o ProxyCommand="aws ssm start-session --region <region> --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p" ubuntu@"$id"
  ```
- x86-64 only. Graviton instances offer no nested virtualization. A Graviton
  `.metal` instance has a bare `/dev/kvm` and would run the stack on kernel
  6.10 or newer (the Compose guide's Requirements say why), but this module
  does not build an arm64 instance.
- A public subnet only. The instance needs a public address for its
  downloads and for its Elastic IP. In a private subnet it would need a NAT
  gateway, and a restricted egress would have to allow the hosts the Compose
  guide's Requirements list, `download.docker.com` and the Ubuntu apt mirrors
  for the first boot, the EC2, Auto Scaling and S3 APIs the instance calls,
  Session Manager's `ssm`, `ssmmessages` and `ec2messages` endpoints, and
  DNS to 8.8.8.8 for the sandboxes.
- AWS GovCloud (US) is untested. The module takes the partition of every ARN
  from the provider and Canonical publishes the same AMI parameter there, so
  it should work with the provider pointed at a GovCloud region.

### Variables

| Input | Default | What it is |
|-------|---------|------------|
| `client_cidrs` | required | the IPv4 CIDRs allowed to reach 3000, 3001 and 3002, as network addresses (`203.0.113.0/24`, `198.51.100.7/32`); at least one, and a browser's has to be its public IPv4 address; each CIDR takes three of the security group's inbound rules, 60 by default, so up to 20 CIDRs unless the account's quota is raised |
| `name` | `e2b-embed` | name prefix for every resource; the IAM role and instance profile, the security group and the Auto Scaling group take it as is, and IAM names are account-wide, so a second install in the same account needs its own `name`, in any region |
| `instance_type` | `m8i.xlarge` | a nested-virtualization type or an x86-64 `.metal` type: AWS supports nested virtualization on the 7th- and 8th-generation Intel families (`m7i`, `c7i` and `r7i`, `m8i`, `c8i` and `r8i`, and the `-flex` variants AWS offers of them), and this module was validated on `m8i.xlarge`; no arm64 or Mac type; 12 GiB RAM recommended, the default has 16 |
| `availability_zone` | the first that offers `instance_type` | the zone for the subnet and the instance, one of the region's own (not a Local or Wavelength Zone), read when the subnet is created; it has to offer `instance_type` |
| `vpc_cidr` | `10.10.0.0/16` | the dedicated VPC, as a network address from /16 to /20; it must not overlap `10.11.0.0/16` or `10.12.0.0/16`, the orchestrator's sandbox networks; read when the VPC is created: changing it on an existing install fails at apply, so destroy the install first |
| `ami` | Ubuntu 24.04 LTS | an AMI id; empty reads Canonical's current Ubuntu 24.04 amd64 image at each plan, and a set id skips that read; it must be known at plan time (a literal, a variable or a data source read at plan), because it decides whether the parameter is read, so an id from a resource created in the same apply fails the plan with `Invalid count argument`; the module wants Ubuntu 24.04 on x86-64 (the startup script installs Docker from Docker's `noble` repository) with its root device at `/dev/sda1`, where the launch template's root volume settings apply; the stack itself needs apt, a writable `/etc` and glibc 2.34 or newer |
| `root_volume_size_gb` | `50` | 20 GiB has to stay free after the OS and Docker |
| `root_volume_type` | `gp3` | the root volume's EBS type |
| `hugepages` | `2048` | 2 MiB hugepages reserved for sandboxes; 2048 is 4 GiB |
| `team_api_key` | generated | `e2b_` plus an even number of lowercase hex characters, at least 32 |
| `compose_base_url` | the shipped files | a directory URL to fetch the two files from at first boot |
| `otel_collector_grpc_endpoint` | empty | host:port of an OTLP/gRPC collector the services export metrics, traces and logs to, with no scheme; empty disables export unless `otel_collector` is on |
| `otel_collector` | `false` | run the built-in collector on the instance, writing sandbox and team metrics into its own ClickHouse; implies `127.0.0.1:4317` when the endpoint is empty |
| `tags` | `{}` | tags for every resource that takes them except the two bucket objects (S3 allows an object only 10), the instance, its volume and its network interface included |

| Output | What it is |
|--------|------------|
| `api_url` | `E2B_API_URL` for the SDK |
| `sandbox_url` | `E2B_SANDBOX_URL` for the SDK |
| `dashboard_url` | the dashboard, for a browser; sign in with `e2b_api_key` |
| `e2b_api_key` | `E2B_API_KEY`, the team API key the seed inserted (sensitive) |
| `autoscaling_group` | the name of the Auto Scaling group |
| `session_command` | a shell snippet for `eval`: a Session Manager shell on the group's in-service instance |

A worked call is in [`examples/basic/main.tf`](examples/basic/main.tf), which
`make lint` validates along with the module.
