# E2B Embed on one Azure instance with Terraform

Creates one Ubuntu 24.04 instance with nested virtualization, in a scale set
of one, which installs Docker, writes the two files Embed ships and runs
`docker compose up -d --wait`. It is a single-machine package rather than a
deployment pattern; the hub is [`../../README.md`](../../README.md).

## Requirements

- Terraform 1.7.5 or newer.
- An Azure subscription and credentials the azurerm provider can read
  (`az login`, or any other source it supports), with `ARM_SUBSCRIPTION_ID`
  set or `subscription_id` in the provider block.
- Permission to create a resource group and, in it, a virtual network, a
  security group, two public IP addresses, a NAT gateway, a user-assigned
  managed identity, a storage account and a scale set; **and** to write role
  assignments and one custom role definition, which Owner or the pair
  Contributor plus User Access Administrator grants. The module makes three
  role assignments and one role definition (Identity below).
- `storage_use_azuread = true` in the provider block. The storage account the
  module creates refuses shared keys, so Terraform writes the two files with
  the identity you ran it as rather than with an account key, which is what
  keeps a key and a SAS out of the state file.
- A region that offers the instance size, `Standard_D4ds_v7` by default, in
  the zone you ask for.
- The Azure CLI on your own machine, for the `run_command` output and the
  serial console under Reaching the instance. `terraform apply` needs neither.
- No machine of your own. The module creates it.
- The module source has to be the repository, not a copy of this directory:
  it reads `../../compose/compose.yaml` and `../../compose/.env` relative to
  itself, which the `github.com/...//` source form below provides by cloning.
  A copy of this directory alone fails at plan time, and so does an absolute
  local path, which Terraform copies the same way. A checkout of your own
  works through a relative `source`, such as `../runtime/embed/terraform/azure`.

## Install

```hcl
provider "azurerm" {
  features {}
  storage_use_azuread = true
}

module "e2b" {
  source       = "github.com/e2b-dev/runtime//embed/terraform/azure?ref=<commit>"
  client_cidrs = ["203.0.113.0/24"]   # where your SDK clients connect from
}

output "api_url" { value = module.e2b.api_url }
output "sandbox_url" { value = module.e2b.sandbox_url }
output "dashboard_url" { value = module.e2b.dashboard_url }
output "run_command" { value = module.e2b.run_command }
output "e2b_api_key" {
  value     = module.e2b.e2b_api_key
  sensitive = true
}
output "admin_password" {
  value     = module.e2b.admin_password
  sensitive = true
}
```

```bash
terraform init && terraform apply
```

A module's outputs are not the root module's, so the six `output` blocks are
what makes `terraform output` see them; without them the Try it commands below
print nothing.

`?ref=<commit>` pins the source to a commit of the public repository's `main`
branch, so an `init` on another day fetches the same `compose.yaml` and `.env`;
upgrading is a deliberate change of that ref. Use `main` itself for the newest
code. The instance answers `GET /health` about two minutes after `apply`
returns -- most of the Docker install and the image pulls happen while `apply`
is still waiting on the scale set. The `base` template build runs on past
that, so the first sandbox can be created about seven minutes after `apply`
returns; the Timings table below carries the measured numbers.

## Try it

Put this install's three SDK variables in your shell:

```bash
export E2B_API_URL="$(terraform output -raw api_url)"
export E2B_SANDBOX_URL="$(terraform output -raw sandbox_url)"
export E2B_API_KEY="$(terraform output -raw e2b_api_key)"
```

Open `terraform output -raw dashboard_url` in a browser and paste the key
(`terraform output -raw e2b_api_key`) into its key form. The instance's `.env`
carries `E2B_DASHBOARD_HOST=<its public IP>`, so the browser reaches sandbox
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
eval "$(terraform output -raw run_command) 'cd /opt/e2b && docker compose --profile test run --rm smoke'"
```

## Remove

```bash
terraform destroy   # the resource group and everything the module made in it
```

## Reference

### What it creates

One resource group and, in it: a virtual network with one subnet; a security
group with one inbound rule and its subnet association; a static Standard
public IP for clients, and a second one behind a NAT gateway for egress; a
user-assigned managed identity, a custom role definition and three role
assignments, and a 60-second `time_sleep` that gives them time to propagate
(Timings); a storage account that refuses shared keys, with one private
container holding the two files; a scale set of one
(`Standard_D4ds_v7`, a 50 GB Premium SSD OS disk, the Application Health
extension, the startup script as custom data); and the four secrets under
Secrets below. The providers are azurerm, random and time.

The scale set replaces the instance when the health extension reports
`GET :3000/health` dead past the grace period. The extension runs in Binary
Health States: a 200 is healthy, anything else -- another status, a timeout,
a refused connection -- is not. A replacement starts from a fresh disk, so it
rebuilds the `base` template and the templates you built are gone.

### Why there is a NAT gateway

Instances of a Flexible scale set get no outbound address of their own, and
the client address is not on the instance's network interface until the first
boot has already installed Docker and asked Resource Manager to attach it. The
NAT gateway is what carries that boot, and every later egress; it also gives
the install one stable outbound address, which an upstream allowlist can name.
It is the one resource the AWS and GCP modules have no counterpart for, and
the one line item on this shape's bill that those two do not have.

### Security group

Three ports, and nothing else reaches the instance from outside: 3000, 3001
and 3002 from `client_cidrs`. There is no port 22 and no probe range: the
serial console and Run Command reach the instance through the platform rather
than through the network, and the health extension asks `/health` from inside
the instance. The orchestrator's unauthenticated control port 5008 in
particular stays inside the instance.

### Identity

The instance's identity holds two grants, and anyone with a shell on the
instance holds them with it:

- **Storage Blob Data Reader on the container**, not on the account: it reads
  the two files the module uploaded and nothing else.
- **A custom role scoped to the resource group** with five actions:
  `Microsoft.Network/networkInterfaces/read` and `/write`,
  `Microsoft.Network/publicIPAddresses/read` and `/join/action`, and
  `Microsoft.Network/virtualNetworks/subnets/join/action`. That is the
  narrowest grant that attaches the address. The built-in role for the job is
  Network Contributor, which carries every other write in `Microsoft.Network`
  with it. The `join` on the subnet is there because a network interface is
  written whole: adding the address also re-states the subnet the interface is
  already in, and Azure authorizes that with a join. The resource group is the
  scope because the interface does not exist until the scale set launches an
  instance, so there is no narrower scope to name it at; the group holds this
  install and nothing else.

The third role assignment is not the instance's: it gives whoever runs
`terraform apply` Storage Blob Data Contributor on the same container, because
with shared keys refused, writing a blob is authorized the same way reading
one is.

Custom role definition names are unique per tenant, so a second install in the
same tenant needs its own `name`.

### Timings

| Event | Time |
|-------|------|
| `terraform apply` on a clean subscription, start to finish | about 7 minutes |
| of which, the wait for the role assignments to propagate | 60 seconds, fixed |
| after `apply` returns, until `GET /health` answers | about 2 minutes |
| after `apply` returns, until the first `Sandbox.create` succeeds | about 7 minutes |
| a replacement (Upgrading), until `GET /health` answers again | about 2 minutes |
| a replacement, until the rebuilt `base` takes a sandbox | about 4 minutes |
| `/health` dying, until the extension reports the instance unhealthy | about 2 minutes |
| a reboot of the instance | about 1 minute |

Measured on `Standard_D4ds_v7` in `eastus`. Most of the first boot happens
while `apply` is still waiting for the scale set, which is why `/health`
answers so soon after it returns. The 60-second row is a fixed wait: a role
assignment takes some time to reach the service that enforces it, and a call
inside that window is refused as if the assignment were never made. If an
apply still fails on a blob with `AuthorizationPermissionMismatch`, run
`terraform apply` again.

To follow a boot:

```bash
eval "$(terraform output -raw run_command) 'tail -n 100 /var/log/cloud-init-output.log'"
eval "$(terraform output -raw run_command) 'journalctl -u e2b-embed -n 100'"
```

From outside, `az vm boot-diagnostics get-boot-log --ids <id>` prints the
console log of an instance that never got far enough to run a command.

### Reaching the instance

`run_command` is a shell snippet that looks the current instance up and runs a
script on it through the guest agent, which is why it is used through `eval`;
put the script in quotes inside the `eval`, as the examples above do. It needs
no open port.

When the stack is too broken for that, the serial console is the way in:
`az serial-console connect --resource-group <name> --name <instance>`, user
`e2b`, password `terraform output -raw admin_password`. That is the only thing
the password is for; no port 22 is opened, on purpose.

### Upgrading

A newer ref points at newer files. `terraform apply` uploads them to the
container and records a new scale-set model, which the next instance uses, but
it does not replace the running one, and that instance wrote its
`compose.yaml` and `.env` once, on first boot. Replace it deliberately:

```bash
az vmss scale --resource-group <name> --name <name> --new-capacity 0
az vmss scale --resource-group <name> --name <name> --new-capacity 1
```

`<name>` is the module's `name`, `e2b-embed` unless you set it, for both the
resource group and the scale set. The two steps are the point: the instance
holds the public IP, so it has to go before the next one comes up, and
scaling to zero and back is what makes that explicit. The new instance boots
the newest model, attaches the same address and rebuilds base from a fresh
disk, so the templates you built are gone. A replacement the scale set makes
on its own, after the health extension reports the instance dead, uses the
newest model too.

The image follows the same rule: `version = "latest"` records a new model when
Canonical publishes a new Ubuntu 24.04 image, and the next replacement boots
it. Set `image.version` to pin one.

`compose_base_url` points the first boot at the Compose files of a specific
commit instead of the files the module ships, for example
`https://raw.githubusercontent.com/e2b-dev/runtime/<commit>/embed/compose`.

### Secrets

`ADMIN_TOKEN`, `SANDBOX_ACCESS_TOKEN_HASH_SEED` and the team API key are all
generated per install, here by Terraform itself: the startup script writes all
three into the instance's `.env` before the first `up`, so the pair the
`api-secrets` one-shot generates on the instance is never read. To rotate
either api secret, replace its resource and then replace the instance as
Upgrading describes: `terraform apply -replace=module.e2b.random_bytes.admin_token`
or `-replace=module.e2b.random_bytes.sandbox_access_token_hash_seed` records a
new model; a new hash seed ends the tokens of running `secure` sandboxes.

They live in the Terraform state, so treat the state file and `terraform show`
as secret, and they reach the instance in its custom data, which the scale-set
model keeps. Anyone allowed `Microsoft.Compute/virtualMachineScaleSets/read`
on the resource group can read it, and read-only access such as the built-in
Reader role includes that action. So can any process on the host, root or not,
through the instance metadata service. The startup script does not print them;
read the key with `terraform output -raw e2b_api_key`. Set `team_api_key` to
choose the key instead; change it and replace the instance to rotate it.
Keeping the three out of custom data, in a key vault only the instance's
identity can read, would narrow who can see them; this module does not do
that.

The fourth secret is the `e2b` user's password, for the serial console only
(Reaching the instance). It exists because Azure refuses a Linux instance with
neither a password nor an SSH key.

Azure encrypts the OS disk at rest with a platform key, so the `.env` and the
three secrets in it are encrypted without a setting here. The storage account
refuses shared keys, so no account key and no SAS exists for the container:
the instance reads the two files with its identity's token, and `terraform
apply` writes them with yours.

The dashboard keeps the key you paste in an httpOnly browser cookie for a
year, not marked Secure because the instance serves plain http; sign-out
clears it.

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

- No persistent data volume. Every replacement starts from a fresh disk,
  rebuilds the `base` template and loses the templates you built.
- Stopping the stack gets the instance replaced. The health extension reports
  it unhealthy about two minutes after `/health` stops answering, and the
  scale set then replaces it from a fresh disk. So `docker compose down` on
  the instance is not a pause: suspend repairs first with
  `az vmss update --resource-group <name> --name <name> --enable-automatic-repairs false`,
  and turn them back on once `/health` answers again with
  `az vmss update --resource-group <name> --name <name> --enable-automatic-repairs true --automatic-repairs-grace-period 15`.
  Re-enabling demands the grace period restated, and the flag takes bare
  minutes, not an ISO 8601 span -- the CLI wraps the value itself, so `PT15M`
  is refused as `PTPT15MM`. A later `terraform apply` also restores the
  policy, since the suspension lives outside the Terraform state.
- Nested virtualization cannot be checked before the apply. Azure decides it
  by the size alone, with no per-instance flag and no API to ask at plan time,
  so `instance_size` is held to the generations that have it rather than to
  what your region and subscription actually offer. A size either does not
  offer fails at apply with Azure's own message; a size without `/dev/kvm`
  would boot and then refuse every sandbox. Check with
  `az vm list-skus -l <region> --resource-type virtualMachines` before
  changing it: SKU availability varies by subscription offer as well as by
  region, and the default is not available in every one.
- Template builds with `copy()` steps upload through the orchestrator's port
  5008, which the security group keeps closed -- `client_cidrs` opens only
  3000, 3001 and 3002, and the upload URL the API hands back points at the
  instance's own loopback besides. Run such builds on the instance itself
  (the `run_command` output); building them remotely is not supported.
- x86-64 only. The Cobalt sizes are arm64 and the module's default image is
  x86-64 Ubuntu.
- One region, one zone. The instance is a scale set of one; `zone` moves it,
  and emptying `zone` places it regionally for a region that has no zones.
- Azure Government and the sovereign clouds are untested. Canonical publishes
  the same image there and the module names no endpoint of its own, so it
  should work with the provider pointed at one of them.

### Variables

| Input | Default | What it is |
|-------|---------|------------|
| `client_cidrs` | required | the IPv4 CIDRs allowed to reach 3000, 3001 and 3002, as network addresses (`203.0.113.0/24`, `198.51.100.7/32`); at least one, and a browser's has to be its public IPv4 address |
| `name` | `e2b-embed` | name prefix for the resource group and everything in it; the custom role definition takes it too, and role definition names are unique per tenant, so a second install in the same tenant needs its own `name` |
| `location` | `eastus` | the region for the resource group and everything in it |
| `zone` | `1` | the availability zone for the instance, as a bare number; empty places it regionally, for a region that has no zones |
| `instance_size` | `Standard_D4ds_v7` | a general-purpose D size with premium storage, generation 3 or newer (`Dsv3` and up, with or without the `a` and `d` letters), or an `Lsv3` and up; no burstable and no arm64 Cobalt size; 12 GiB RAM recommended, the default has 16. Which generations you can actually use depends on the region **and on your subscription's offer** — `az vm list-skus -l <region> --resource-type virtualMachines` is what answers it, and a size it marks `NotAvailableForSubscription` fails at apply |
| `image` | Ubuntu 24.04 LTS | the marketplace image, as publisher, offer, sku and version; the module wants Ubuntu 24.04 on x86-64 and a generation 2 image, and `latest` records a new model when Canonical publishes one |
| `os_disk_size_gb` | `50` | 20 GiB has to stay free after the OS and Docker |
| `os_disk_type` | `Premium_LRS` | the OS disk's storage type |
| `hugepages` | `2048` | 2 MiB hugepages reserved for sandboxes; 2048 is 4 GiB |
| `team_api_key` | generated | `e2b_` plus an even number of lowercase hex characters, at least 32 |
| `compose_base_url` | the shipped files | a directory URL to fetch the two files from at first boot |
| `otel_collector_grpc_endpoint` | empty | host:port of an OTLP/gRPC collector the services export metrics, traces and logs to, with no scheme; empty disables export unless `otel_collector` is on |
| `otel_collector` | `false` | run the built-in collector on the instance, writing sandbox and team metrics into its own ClickHouse; implies `127.0.0.1:4317` when the endpoint is empty |
| `tags` | `{}` | tags for every resource in the module's resource group |

| Output | What it is |
|--------|------------|
| `api_url` | `E2B_API_URL` for the SDK |
| `sandbox_url` | `E2B_SANDBOX_URL` for the SDK |
| `dashboard_url` | the dashboard, for a browser; sign in with `e2b_api_key` |
| `e2b_api_key` | `E2B_API_KEY`, the team API key the seed inserted (sensitive) |
| `scale_set` | the name of the scale set of one |
| `run_command` | a shell snippet for `eval`: run a script on the instance through the guest agent |
| `admin_password` | the `e2b` user's password, for the serial console (sensitive) |

A worked call is in [`examples/basic/main.tf`](examples/basic/main.tf), which
`make lint` validates along with the module.
