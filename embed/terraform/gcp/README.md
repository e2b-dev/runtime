# E2B Embed on one GCE instance with Terraform

Creates one Ubuntu 24.04 VM with nested virtualization, in a managed instance
group of one, which installs Docker, writes the two files Embed ships and runs
`docker compose up -d --wait`. It is a single-machine evaluation
package rather than a deployment pattern; the hub is
[`../../README.md`](../../README.md).

## Requirements

- Terraform 1.7.5 or newer.
- A GCP project with the Compute Engine API enabled, and credentials for it
  (`gcloud auth application-default login`, or a service account).
- No machine of your own. The module creates it.
- The module source has to be the repository, not a copy of this directory:
  it reads `../../compose/compose.yaml` and `../../compose/.env` relative to
  itself, which the `github.com/...//` source form below provides by cloning.
  A copy of this directory alone fails at plan time.

## Install

```hcl
module "e2b" {
  source       = "github.com/e2b-dev/runtime//embed/terraform/gcp?ref=<commit>"
  project_id   = "my-project"
  client_cidrs = ["203.0.113.0/24"]   # where your SDK clients connect from
}

output "api_url" { value = module.e2b.api_url }
output "sandbox_url" { value = module.e2b.sandbox_url }
output "ssh_command" { value = module.e2b.ssh_command }
output "e2b_api_key" {
  value     = module.e2b.e2b_api_key
  sensitive = true
}
```

```bash
terraform init && terraform apply
```

A module's outputs are not the root module's, so the four `output` blocks are
what makes `terraform output` see them; without them the Try it commands below
print nothing.

`?ref=<commit>` pins the source to a commit of the public repository's `main`
branch, so an `init` on another day fetches the same `compose.yaml` and `.env`;
upgrading is a deliberate change of that ref. Use `main` itself for the newest
code. The first instance answers `GET /health` about two
minutes after `apply` returns, the Docker install and the image pulls behind
it. The `base` template build runs on past that, so the first sandbox can be
created about four minutes after `apply` returns.

## Try it

Put this install's three SDK variables in your shell:

```bash
export E2B_API_URL="$(terraform output -raw api_url)"
export E2B_SANDBOX_URL="$(terraform output -raw sandbox_url)"
export E2B_API_KEY="$(terraform output -raw e2b_api_key)"
```

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
eval "$(terraform output -raw ssh_command)"
cd /opt/e2b && sudo docker compose --profile test run --rm smoke
```

## Remove

```bash
terraform destroy   # the instance and every other resource the module made
```

## Reference

### What it creates

Fourteen resources: a VPC and a subnet; a reserved external address; a
service account and its `roles/logging.logWriter` binding; an instance
template (`n4-standard-4`, a 50 GB Hyperdisk, nested virtualization on, the
two files in instance metadata); a health check and a zonal managed instance
group of one that auto-heals on `GET :3000/health`; three firewall rules; and
the three secrets under Secrets below.

The group recreates the instance when it fails its health check. A recreated
instance starts from a fresh disk, so it rebuilds the `base` template and the
templates you built are gone.

### Firewall

Three rules, and nothing else reaches the instance from outside: 3000 and
3002 from `client_cidrs`, 3000 from Google's health checkers, and 22 from
IAP. The orchestrator's unauthenticated control port 5008 in particular stays
inside the VM.

### Timings

| Event | Time |
|-------|------|
| first boot, until `GET /health` answers | about 2 minutes |
| first boot, until the first `Sandbox.create` succeeds | about 4 minutes |
| an auto-healing repair, or a replacement after an out-of-band delete | about 3 minutes |
| `rolling-action replace` (see Upgrading) | 5 to 6 minutes |
| a plain reset of the VM | about 1 minute |

Measured on the default `n4-standard-4`. A replacement takes longer than it
looks: the old instance keeps answering `/health` for the first 40 seconds or
so, so wait for the group's `currentAction` to read `NONE` before timing it.
To follow a boot:

```bash
eval "$(terraform output -raw ssh_command)"
sudo journalctl -u google-startup-scripts -f
```

The `ssh_command` output is a shell snippet that looks the current instance
name up, which is why it is used through `eval`.

### Secrets

`ADMIN_TOKEN`, `SANDBOX_ACCESS_TOKEN_HASH_SEED` and the team API key are all
generated per install, here by Terraform itself: the startup script writes
all three into the instance's `.env` before the first `up`, so the pair the
`api-secrets` one-shot generates on the instance is never read. To rotate
either api secret, replace its resource and then replace the instance as
Upgrading describes: `terraform apply -replace=module.e2b.random_bytes.admin_token`
or `-replace=module.e2b.random_bytes.sandbox_access_token_hash_seed` changes
the instance template; a new hash seed ends the tokens of running `secure`
sandboxes. They live in the Terraform state, so treat the state file and
`terraform show` as secret, and they reach the instance through its
startup-script metadata, which anyone with `compute.instances.get` on the
project can read and which any process on the instance can read from the
metadata server without authentication. The startup script does not print
them; read the key with `terraform output -raw e2b_api_key`. Set
`team_api_key` to choose the key instead; change it and recreate the instance
to rotate it.

### Upgrading

A newer ref points at newer files, which changes the instance template.
`terraform apply` records the new template but does not replace the running
instance, because the group's update policy is opportunistic, and an
auto-healing recreate in the meantime uses the template the instance was
created from rather than the new one. Replace it deliberately:

```bash
gcloud compute instance-groups managed rolling-action replace <name> --project <project> --zone <zone> --replacement-method=recreate --max-surge=0 --max-unavailable=1
```

The three flags matter: the instance holds a reserved address, so the
replacement must be delete-before-create. That is a fresh instance, so the
`base` template is rebuilt and the templates you built are gone. The command
also sets the group's update policy type to proactive; the next
`terraform apply` sets it back, which is the only change it will show.

`compose_base_url` points the first boot at the Compose files of a specific
commit instead of the files the module ships, for example
`https://raw.githubusercontent.com/e2b-dev/runtime/<commit>/embed/compose`.

### Limitations

- No persistent data disk. Every recreate or replace starts from a fresh
  disk, rebuilds the `base` template and loses the templates you built.
- Template builds with `copy()` steps upload through the orchestrator's port
  5008, which the firewall keeps closed. Run them on the instance, or tunnel
  the three ports and point the SDK at `http://127.0.0.1:3000` and
  `http://127.0.0.1:3002`:

  ```bash
  eval "$(terraform output -raw ssh_command) -- -N -L 3000:127.0.0.1:3000 -L 3002:127.0.0.1:3002 -L 5008:127.0.0.1:5008"
  ```

### Variables

| Input | Default | What it is |
|-------|---------|------------|
| `project_id` | required | the GCP project; the Compute Engine API must be enabled |
| `client_cidrs` | required | the CIDRs allowed to reach 3000 and 3002; at least one |
| `zone` | `us-west1-b` | the zone; the subnet's and the address's region follows from it |
| `name` | `e2b-embed` | name prefix for every resource |
| `machine_type` | `n4-standard-4` | 12 GiB RAM recommended; the default has 16 |
| `boot_disk_size_gb` | `50` | 20 GiB has to stay free after the OS and Docker |
| `boot_disk_type` | `hyperdisk-balanced` | n4 machine types support only Hyperdisk |
| `image` | Ubuntu 24.04 LTS | the stack needs apt, a writable `/etc` and glibc 2.34 or newer; x86-64 only (the startup script adds Docker's amd64 repository, and GCE's arm64 machine types have no nested virtualization) |
| `hugepages` | `2048` | 2 MiB hugepages reserved for sandboxes; 2048 is 4 GiB |
| `team_api_key` | generated | `e2b_` plus an even number of lowercase hex characters, at least 32 |
| `compose_base_url` | the shipped files | a directory URL to fetch the two files from at first boot |
| `labels` | `{}` | labels for the instance template and the address |

| Output | What it is |
|--------|------------|
| `api_url` | `E2B_API_URL` for the SDK |
| `sandbox_url` | `E2B_SANDBOX_URL` for the SDK |
| `e2b_api_key` | `E2B_API_KEY`, the team API key the seed inserted (sensitive) |
| `instance_group` | the self link of the managed instance group |
| `ssh_command` | a shell snippet for `eval`: SSH through IAP to the current instance |

A worked call is in [`examples/basic/main.tf`](examples/basic/main.tf), which
`make lint` validates along with the module.
