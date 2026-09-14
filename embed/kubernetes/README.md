# E2B Embed on one Kubernetes node

One StatefulSet that translates [`../compose/compose.yaml`](../compose/compose.yaml):
the stores, Vector, the orchestrator, api and client-proxy run as sidecar init
containers in the compose start order, the one-shots as init containers
between them, and the pod is Ready once the `base` template exists. It is a
single-node evaluation package rather than a Kubernetes deployment pattern;
the hub is [`../README.md`](../README.md).

## Requirements

- Kubernetes 1.29 or newer, for sidecar init containers.
- One Linux x86-64 or arm64 node with KVM (`/dev/kvm` present, which on a VM
  means nested virtualization is on) and a 4 KiB-page kernel: Ubuntu 24.04, or
  another host with kernel 6.8 or newer, glibc 2.34 or newer, cgroup v2 and
  the `iptables`, `rsync`, `e2fsprogs` and `iproute2` tools; 12 GiB RAM and
  20 GiB free on `/`. No Container-Optimized OS. arm64 is not yet verified
  end to end: no arm64 node has run this stack. The seven pinned images are
  published for both architectures, and the `fetch-artifacts` init container
  verifies the arm64 orchestrator and envd against the `.sha256` their
  release writes beside the object. A pin with no such object still stops
  with a `FIX:` line naming it.
- 4 GiB of 2 MiB hugepages, reserved before the kubelet starts, because it
  advertises only what it saw then. Every sandbox needs them, and the
  orchestrator's request is what stops the kubelet capping the pod at zero.
- The node labelled `e2b.dev/single-node=true`: the databases live on its
  disk, so the pod is pinned to it.
- `git` on the machine you run `kubectl` from: the remote `apply -k` URL
  below and the `kubectl kustomize` against the same URL under Try it are git
  sources, and kustomize clones them with that binary.

## Install

On a fresh node, in this order (the reservation precedes the kubelet):

```bash
# the orchestrator execs the first four on the node, so they have to be there;
# python3-venv is for the SDK in Try it. Refresh the index first, or a package
# that is genuinely missing fails to fetch
sudo apt-get update && sudo apt-get install -y iptables rsync e2fsprogs iproute2 python3-venv
echo 'vm.nr_hugepages=2048' | sudo tee /etc/sysctl.d/90-e2b.conf && sudo sysctl -p /etc/sysctl.d/90-e2b.conf
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=v1.36.4+k3s1 INSTALL_K3S_EXEC="--node-label e2b.dev/single-node=true --write-kubeconfig-mode 644" sh -
```

Then `export KUBECONFIG=/etc/rancher/k3s/k3s.yaml`, which is what makes the
`kubectl` commands below work as your own user; k3s writes that file mode 600
and owned by root unless it is told otherwise. Mode 644 hands that file, a
cluster-admin credential, to every local user of the node, which is fine on a
machine that is yours alone; on a shared one drop the flag and run every
command as `sudo k3s kubectl` instead.

On a node that already runs Kubernetes the k3s line does not apply, and its
kubeconfig is whatever that cluster already gave you: install the same
packages, set the same sysctl, restart the kubelet
(`sudo systemctl restart k3s` on k3s) and run
`kubectl label node <node> e2b.dev/single-node=true`; the in-pod `host-setup`
writes that sysctl file and value too, so it changes nothing here. Either way
the node has to advertise `hugepages-2Mi: 4Gi` before you go on, or the pod
stays Pending.

Then install Embed:

```bash
kubectl create namespace e2b
kubectl -n e2b create secret generic e2b-api \
  --from-literal=ADMIN_TOKEN="$(openssl rand -hex 32)" \
  --from-literal=SANDBOX_ACCESS_TOKEN_HASH_SEED="$(openssl rand -hex 32)"
kubectl apply -k "https://github.com/e2b-dev/runtime//embed/kubernetes?ref=main"
kubectl -n e2b rollout status statefulset/e2b --timeout=15m
kubectl -n e2b logs e2b-0 -c ready
```

Ready takes about 3 minutes on `n4-standard-4`: 2 m 38 s from `apply -k` to
Ready in the reference run, 49 s of it the `base` template build. The
15-minute timeout above is a ceiling, not the expectation.

The namespace and the Secret come before `apply -k` so the api never starts
without them. Until the Secret exists the api container cannot be created:
the pod sits at `Init:CreateContainerConfigError` with the event
`secret "e2b-api" not found`, and the kubelet retries with a backoff of up to
five minutes. The kustomization carries the namespace too, so `apply -k`
reports `namespace/e2b configured` and warns that the one you created by hand
has no last-applied annotation; that is kubectl adding its annotation and
nothing else. It also brings a headless Service and the StatefulSet. From a
checkout: `kubectl apply -k embed/kubernetes`.

## Try it

Put this install's three SDK variables in your shell:

```bash
eval "$(kubectl -n e2b exec e2b-0 -c ready -- cat /run/e2b/sdk.env)"
```

Install the SDK in a virtual environment, which is what keeps it off the
system Python that Ubuntu's `pip` refuses to write to. On Ubuntu `venv`
comes from `python3-venv`: install it first, which the apt line in
[Install](#install) does, or this fails with `ensurepip is not available`.

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

There is no `smoke` service here. To run the same check with the JavaScript
SDK, which also reaches a port inside the sandbox through client-proxy, run
the image that carries it on the node. The image name comes out of the same
kustomization the install applied, so this needs no checkout:

```bash
IMG="$(kubectl kustomize "https://github.com/e2b-dev/runtime//embed/kubernetes?ref=main" | awk '/image: .*node-e2b/{print $2; exit}')"
kubectl -n e2b run smoke --rm -i --restart=Never --image="$IMG" --overrides='{"spec":{"hostNetwork":true}}' \
  --env E2B_API_URL="$E2B_API_URL" --env E2B_SANDBOX_URL="$E2B_SANDBOX_URL" --env E2B_API_KEY="$E2B_API_KEY" -- node /app/smoke.mjs
```

`--env E2B_API_KEY=...` puts the team key in the pod spec, so for as long as
the pod runs anyone who can read pods in the `e2b` namespace can read the key
out of `kubectl -n e2b get pod smoke -o yaml`, and, on a cluster whose audit
policy records request bodies, it lands in the API server's audit log. `--rm`
removes the pod when the test ends.

## Remove

Three tiers, matching compose's `down`, `down -v` and a purge:

```bash
# 1. the pod; databases, key and node state stay. Wait for it: the data
#    directory below must not go while the pod is still running.
kubectl -n e2b delete statefulset e2b && kubectl -n e2b wait --for=delete pod/e2b-0 --timeout=5m
# 2. on the node: the databases and the team key
sudo rm -rf /var/lib/e2b/data
# 3. the Secret and the namespace. kubectl delete -k "<the same URL>"
#    --ignore-not-found does the same; without that flag it exits 1 on the
#    StatefulSet step 1 already removed, having deleted the namespace anyway
kubectl delete namespace e2b
```

The data directory goes as one piece: the team API key lives in `seed-state`
beside the databases and must not outlive them, or the reverse. Past that, the
node keeps what `host-setup` wrote until it is recycled: the files under
`/etc`, the rest of `/var/lib/e2b` (its `storage` holds the built templates and
their cache, about 2 GiB after one install), `/fc-*`, the hugepage reservation
and the iptables rule.
There is no Kubernetes teardown one-shot; the compose shape's `host-teardown`
has no counterpart here.

## Reference

### What runs where

One pod on the labelled node. The stores, Vector, the orchestrator, api and
client-proxy are sidecar init containers started in the compose order, host
preparation first; the one-shots are ordinary init containers between them;
`ready` turns the pod Ready once the `base` template exists.

The pod uses the node's network and pid namespaces, runs three privileged
containers (`preflight`, `host-setup` and the orchestrator launcher, which
`nsenter` into the node), mounts the node's `/` for the artifact download and
`/sys/fs/cgroup` for the sandbox sweep, and prepares the node the way the
compose `host-setup` does: kernel modules, sysctls, hugepages, a udev rule, an
iptables rule and directories under `/`. Use a dedicated node. Sandboxes end
when the pod is deleted or restarted.

The databases and the team API key are `hostPath` directories under
`/var/lib/e2b/data` on that node, which is why the manifest pins the pod there
with a `nodeSelector`; a pod started anywhere else would begin with an empty
stack.

### Ports

The node exposes the same eleven ports as a compose host, and Postgres,
Redis, ClickHouse and Vector listen on loopback here too. The SDK variables
`ready` prints use the node's IP: reach 3000 and 3002 on it and firewall the
other nine (3003, 5007, 5008, 5009, 5109 and the sandbox egress proxies 5010,
5016, 5017 and 5018), since 5008 is an unauthenticated control API and the
egress proxies expect no external client.

k3s adds two ports of its own on top of those eleven: `*:6443`, the
Kubernetes API server, and `*:10250`, the kubelet. They belong to the
cluster rather than to the stack, and they want the same firewall treatment
as the nine, more urgently if anything: the install above writes the
cluster-admin kubeconfig mode 644, so 6443 is the port that hands out the
node.

Vector's log listener is the one port that differs. It binds
`127.0.0.1:20006` rather than compose's 30006, because 30000 to 32767 is
Kubernetes' default NodePort range and a NodePort allocated 30006 would DNAT
the pod's own loopback log traffic away from Vector.

### Secrets

The api's two secrets, `ADMIN_TOKEN` and `SANDBOX_ACCESS_TOKEN_HASH_SEED`,
come from the `e2b-api` Secret created during the install; nothing in the
manifest carries them. The compose shape generates them per install in an
`api-secrets` one-shot, which has no counterpart here: rotate them by
replacing the Secret and restarting the pod.

The team API key is different: the seed generates it on the first start and
keeps it in `/var/lib/e2b/data/seed-state/team-api-key` on the node, where
`ready` reads it. To rotate it, remove the file on the node and restart the
pod:

```bash
sudo rm /var/lib/e2b/data/seed-state/team-api-key   # on the node
kubectl -n e2b delete pod e2b-0
```

The seed generates a new key, revokes the ones it issued earlier, and `ready`
prints the new exports. To pin a key instead of generating one, patch the seed
init container's `SEED_TEAM_API_KEY` with a kustomize patch; the value is
`e2b_` followed by at least 32 hex characters, as compose's `TEAM_API_KEY`
documents.

### Upgrading

The generated ConfigMaps carry a content hash in their names, so changing a
pin in [`kustomization.yaml`](kustomization.yaml) or a file under
[`config/`](config/) and re-applying rolls the pod onto the new map. The
previous maps are left behind, because kustomize does not prune; delete them
by name if they bother you. A rolling update waits for the current pod to be
Ready, so a pod that is not (a crash-looping container, a Pending pod) adopts
nothing from a re-apply until you delete it: `kubectl -n e2b delete pod
e2b-0`, and the StatefulSet recreates it from the new template.

### Hugepages

`HUGEPAGES`, given to `host-setup` in
[`statefulset.yaml`](statefulset.yaml), and the orchestrator's `hugepages-2Mi`
request and limit are one number and have to move together: 2048 pages of
2 MiB is `4Gi`. [`../tests/kubernetes.bats`](../tests/kubernetes.bats) fails
when they disagree.

### Limitations

- Template builds with `copy()` steps upload through the orchestrator's port
  5008 (`LOCAL_UPLOAD_BASE_URL`), the same as compose from another machine.
  Build from the node (a `hostNetwork` pod, or `kubectl exec`), or tunnel 5008
  to your loopback (`ssh -N -L 5008:127.0.0.1:5008 <user>@<node>`) so the
  handed-out `http://127.0.0.1:5008/...` URL is valid unchanged. Do not open
  5008 to the network.
- `env/api.local.env` has no counterpart here. To add api environment, patch
  the api container with a kustomize patch.
- There is no `smoke` service. Try it above has the recipe that replaces it.
- A node that carries a taint needs a toleration, added with a kustomize
  patch; a commented example is in [`statefulset.yaml`](statefulset.yaml).

### Troubleshooting

- **The pod stays Pending with `Insufficient hugepages-2Mi`.** The node is not
  advertising the reservation. `kubectl get node -o
  jsonpath='{.items[0].status.allocatable.hugepages-2Mi}'` must print `4Gi`;
  the kubelet advertises only the hugepages it saw when it started, so set the
  sysctl and restart it.
- **The pod sits at `Init:CreateContainerConfigError`, with the event
  `secret "e2b-api" not found`.** The `e2b-api` Secret does not exist, so the
  kubelet cannot create the api container. Create it as the Install section
  shows; the kubelet retries with a backoff of up to five minutes, so the pod
  recovers on its own once the Secret is there.
- **The k3s install fails with `SSL certificate problem` or `Download failed`
  before anything E2B runs.** The unpinned installer asks k3s's channel
  server for the current `stable` release; when that service is down the
  installer treats the word `stable` as a version and fails. The install line
  above pins `INSTALL_K3S_VERSION`, which skips the channel lookup; keep the
  pin, or set it to the release you want.
- **`kubectl` says permission denied on `/etc/rancher/k3s/k3s.yaml`.** k3s
  wrote the kubeconfig mode 600 and owned by root, which is its default
  without `--write-kubeconfig-mode 644`. Either
  `sudo chmod 644 /etc/rancher/k3s/k3s.yaml` once and
  `export KUBECONFIG=/etc/rancher/k3s/k3s.yaml`, or run every command as
  `sudo k3s kubectl` instead.
