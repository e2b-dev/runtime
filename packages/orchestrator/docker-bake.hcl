// The Makefile and the release workflow both build these targets. Run from
// this directory: the context is the parent, because the Dockerfile COPYs
// sibling modules (shared, clickhouse) relative to it.

variable "COMMIT_SHA" {
  default = ""
}

variable "VERSION" {
  default = ""
}

variable "PLATFORM" {
  default = "linux/amd64"
}

// A release pushes every target in this group, tagged with its version.
group "images" {
  targets = ["clean-nfs-cache"]
}

target "_common" {
  context    = ".."
  dockerfile = "orchestrator/Dockerfile"
  platforms  = [PLATFORM]
  args = {
    COMMIT_SHA = COMMIT_SHA
    VERSION    = VERSION
  }
}

target "binaries" {
  inherits = ["_common"]
  target   = "artifacts"
  output   = ["type=local,dest=bin"]
}

target "clean-nfs-cache" {
  inherits = ["_common"]
  target   = "clean-nfs-cache"
  tags     = ["clean-nfs-cache:local"]
}

target "orchestrator" {
  inherits = ["_common"]
  target   = "runtime"
  tags     = ["orchestrator:local"]
}
