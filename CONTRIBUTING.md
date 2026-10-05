# Contributing to E2B Infrastructure

Thank you for contributing to E2B! Every contribution matters and we genuinely appreciate
the time and effort you put in.

## Before You Start: Open an Issue First

**Please open a GitHub issue before submitting a pull request** — especially for new features
or non-trivial changes. This lets our team confirm the direction, discuss scope, and avoid
situations where you put in significant work on something we can't merge.

Bug fixes are always welcome and can skip this step if the problem is clear.

We try to respond to issues within a few business days. If you don't hear back, feel free
to ping the issue.

## What We Welcome

- **Bug fixes** — always appreciated, no issue required for obvious bugs
- **Self-hosting improvements** — better docs, deployment UX
- **Documentation fixes** — typos, outdated instructions, missing steps
- **Test coverage** — especially integration tests for edge cases
- **Performance improvements** — with benchmarks to back them up

## Things Unlikely to Be Merged

To save your time, here's what we typically won't merge:

- Features that add significant maintenance burden without clear user demand
- Major refactors of core infrastructure components (Firecracker, networking, NBD) without
  prior discussion — these have high blast radius
- Changes that bypass existing safety mechanisms without a strong justification
- Large PRs without tests
- Stylistic changes (formatting, naming) that don't touch logic
- AI-generated code where the author can't explain what it does

## Sending a Pull Request

1. **Open an issue first** (unless it's a bug fix or docs change)
2. Wait for a team member to confirm the direction
3. Fork the repo and create a branch from `main`
4. Write tests for your changes
5. Make sure `make test` and `make lint` pass locally
6. Open a PR with a clear description of the problem and your solution
7. Link the related issue in the PR description

Keep PRs focused — one concern per PR. Smaller PRs get reviewed faster.

## Development Setup

See [CLAUDE.md](./CLAUDE.md) and [DEV-LOCAL.md](./DEV-LOCAL.md) for setup instructions.

## Code of Conduct

Be kind. We're all here to build something useful.
