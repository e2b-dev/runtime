<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../.github/assets/e2b-embed-dark.png">
  <img alt="E2B Embed" src="../.github/assets/e2b-embed-light.png" width="100%">
</picture>

# E2B Embed

**E2B Embed runs the whole E2B stack, every feature included, on a single
machine in the environment of your choice.** Ship it inside your product, or
run it inside a customer's own environment, and bring E2B sandboxes to
customers whose data has to stay put, in government and in regulated
industries such as finance and healthcare. Same SDK, same API as E2B Cloud.
Open source, Apache-2.0.

[Docker Compose](#docker-compose)
| [Terraform on GCP](#terraform-on-gcp)
| [Terraform on AWS](#terraform-on-aws)
| [Kubernetes](#kubernetes)
| [Reference](docs/REFERENCE.md)

## How to run

Choose the setup that fits what you already have.

| Run it with | You need | The command | Install |
|-------------|----------|-------------|---------|
| Docker Compose | a Linux host you own | `docker compose up -d --wait` | [Docker Compose](#docker-compose) |
| Terraform on GCP | a Google Cloud project | `terraform apply` | [Terraform on GCP](#terraform-on-gcp) |
| Terraform on AWS | an AWS account | `terraform apply` | [Terraform on AWS](#terraform-on-aws) |
| Kubernetes | one node in your cluster | `kubectl apply -k` | [Kubernetes](#kubernetes) |

Every guide ends with your first sandbox, a few minutes after you start. If
you'd like to scale in production, please talk with us at
[e2b.dev/enterprise](https://e2b.dev/enterprise).

## What runs on the node

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/overview-dark.png">
  <img alt="E2B Embed: the complete E2B runtime on one node, and that node run with Docker Compose, on AWS or GCP, or on Kubernetes" src="docs/overview-light.png" width="100%">
</picture>

One node holds the complete E2B runtime. Your application talks to it
through the SDK, your team through the dashboard. The control plane is the
same API as E2B Cloud, the data plane runs the same E2B sandboxes, storage
keeps your templates and snapshots, and telemetry keeps your logs and
metrics. Nothing leaves the node. It is the same node wherever you run it,
on a Linux host you own with Docker Compose, on one VM on Google Cloud or
one instance on AWS with Terraform, or on one node in your Kubernetes
cluster.

## What you get

- **The same sandboxes as E2B Cloud.** Isolated, paused and resumed on
  demand.
- **The same SDK and API.** Moving your application over from E2B Cloud is a
  configuration change.
- **Everything stays on one single machine.** Templates, sandbox logs and data live
  on its disk and go nowhere else.
- **Private by design.** Only your application and your dashboard users need
  to reach the node, and the Terraform installs let in only the addresses you
  name. The [reference](docs/REFERENCE.md#ports-on-the-host-network) lists
  what to open.
- **Your own templates.** Build custom sandbox templates through the same API
  and start sandboxes from them right away.
- **The dashboard.** Your sandboxes, templates and logs in the browser, with a
  terminal and a file browser for every sandbox.
- **Open source.** Apache-2.0, in the public
  [E2B runtime repository](https://github.com/e2b-dev/runtime). No E2B
  account and no license key needed.

### Docker Compose

Run it on a Linux machine you already have. [Full guide](compose/README.md#install).

```bash
mkdir e2b && cd e2b
curl -fsSL --remote-name-all "https://raw.githubusercontent.com/e2b-dev/runtime/main/embed/compose/{compose.yaml,.env}"
docker compose up -d --wait
```

### Terraform on GCP

Create the machine in your Google Cloud project. [Full guide](terraform/gcp/README.md#install).

```hcl
module "e2b" {
  source       = "github.com/e2b-dev/runtime//embed/terraform/gcp?ref=main"
  project_id   = "my-project"
  client_cidrs = ["203.0.113.0/24"]   # where your SDK clients connect from
}
```

```bash
terraform init && terraform apply
```

### Terraform on AWS

Create the machine in your AWS account. [Full guide](terraform/aws/README.md#install).

```hcl
module "e2b" {
  source       = "github.com/e2b-dev/runtime//embed/terraform/aws?ref=main"
  client_cidrs = ["203.0.113.0/24"]   # where your SDK clients connect from
}
```

```bash
terraform init && terraform apply
```

### Kubernetes

Run it on a node in your existing cluster. [Full guide](kubernetes/README.md#install).

```bash
kubectl create namespace e2b
kubectl -n e2b create secret generic e2b-api --from-literal=ADMIN_TOKEN="$(openssl rand -hex 32)" --from-literal=SANDBOX_ACCESS_TOKEN_HASH_SEED="$(openssl rand -hex 32)"
kubectl apply -k "https://github.com/e2b-dev/runtime//embed/kubernetes?ref=main"
```

## When one node is not enough

Embed is one machine, by design. When you need more, E2B runs three other
ways, with the same SDK and API, so moving is a configuration change.

- **Private cloud**, in development. The whole platform inside your boundary,
  built for networks nothing may leave. Design partners welcome.
- **[Bring Your Own Cloud](https://e2b.dev/enterprise).** Your cloud account,
  operated by E2B.
- **[E2B Cloud](https://e2b.dev).** Managed by E2B, nothing to host.

[See E2B for enterprise](https://e2b.dev/enterprise) for private cloud and
Bring Your Own Cloud.

## Limitations

- **Runs on Linux.** Embed needs a Linux machine with hardware
  virtualization, and the Terraform installs create one for you. On a Mac, it
  runs inside a Linux virtual machine (Apple silicon M3 or newer, macOS 15 or
  newer).
- **Plain HTTP.** Embed answers over HTTP at the machine's own address, with
  no TLS of its own, which is why it belongs inside your network. A port
  inside a sandbox is reached at that same address. The
  [reference](docs/REFERENCE.md#reaching-a-port-inside-a-sandbox) shows how.

## Reference

For whoever operates the machine, [`docs/REFERENCE.md`](docs/REFERENCE.md)
has the detail on networking, logs and metrics, secrets, releases, building
templates and the developer tooling.
