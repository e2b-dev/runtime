locals {
  # The zone selects the region: "us-west1-b" → "us-west1". One knob, no mismatch.
  region       = join("-", slice(split("-", var.zone), 0, 2))
  team_api_key = var.team_api_key != "" ? var.team_api_key : "e2b_${random_bytes.team_api_key.hex}"
  startup_script = templatefile("${path.module}/startup.sh.tftpl", {
    compose_base_url               = var.compose_base_url
    admin_token                    = random_bytes.admin_token.hex
    sandbox_access_token_hash_seed = random_bytes.sandbox_access_token_hash_seed.hex
    team_api_key                   = local.team_api_key
    hugepages                      = var.hugepages
    dashboard_host                 = google_compute_address.this.address
    # The built-in collector listens on 127.0.0.1:4317, so turning it on
    # points the services there unless an endpoint of the operator's own wins.
    otel_endpoint    = var.otel_collector_grpc_endpoint != "" ? var.otel_collector_grpc_endpoint : (var.otel_collector ? "127.0.0.1:4317" : "")
    compose_profiles = var.otel_collector ? "otel" : ""
  })
}

# random_bytes, not random_id: its hex attribute is sensitive, so the values
# never appear in `terraform apply` or `terraform show` output. They are in
# the state and in instance metadata regardless (README, Secrets).
resource "random_bytes" "admin_token" {
  length = 32
}

resource "random_bytes" "sandbox_access_token_hash_seed" {
  length = 32
}

resource "random_bytes" "team_api_key" {
  length = 16
}

resource "google_compute_network" "this" {
  project                 = var.project_id
  name                    = var.name
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "this" {
  project       = var.project_id
  name          = var.name
  region        = local.region
  network       = google_compute_network.this.id
  ip_cidr_range = "10.10.0.0/24"
}

resource "google_compute_address" "this" {
  project = var.project_id
  name    = var.name
  region  = local.region
  labels  = var.labels
}

resource "google_service_account" "this" {
  project      = var.project_id
  account_id   = var.name
  display_name = "${var.name} instance"
}

resource "google_project_iam_member" "log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.this.email}"
}

resource "google_compute_instance_template" "this" {
  project      = var.project_id
  name_prefix  = "${var.name}-"
  region       = local.region
  machine_type = var.machine_type
  labels       = var.labels

  disk {
    source_image = var.image
    auto_delete  = true
    boot         = true
    disk_size_gb = var.boot_disk_size_gb
    disk_type    = var.boot_disk_type
  }

  network_interface {
    subnetwork = google_compute_subnetwork.this.id
    # A reserved address in a template only works while one instance holds
    # it: replacements must be delete-before-create (max_surge 0, RECREATE,
    # and the README's rolling-action flags). A surging rollout would fail
    # to create the second instance with the address in use.
    access_config {
      nat_ip = google_compute_address.this.address
    }
  }

  # Sandboxes are Firecracker microVMs; the instance itself must expose KVM.
  advanced_machine_features {
    enable_nested_virtualization = true
  }

  # The two shipped files ride in metadata, read back by the startup script.
  # Not templatefile(): compose.yaml has its own ${...} interpolations.
  metadata = {
    enable-oslogin   = "TRUE"
    e2b-compose-yaml = file("${path.module}/../../compose/compose.yaml")
    e2b-dot-env      = file("${path.module}/../../compose/.env")
  }
  metadata_startup_script = local.startup_script

  service_account {
    email  = google_service_account.this.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_compute_health_check" "this" {
  project             = var.project_id
  name                = var.name
  check_interval_sec  = 30
  timeout_sec         = 10
  healthy_threshold   = 1
  unhealthy_threshold = 10

  http_health_check {
    port         = 3000
    request_path = "/health"
  }
}

resource "google_compute_instance_group_manager" "this" {
  project            = var.project_id
  name               = var.name
  zone               = var.zone
  base_instance_name = var.name
  target_size        = 1

  version {
    instance_template = google_compute_instance_template.this.self_link
  }

  # The first boot installs Docker and runs the whole first `up` (about four
  # minutes with downloads); only after that is /health meaningful.
  auto_healing_policies {
    health_check      = google_compute_health_check.this.id
    initial_delay_sec = 900
  }

  # OPPORTUNISTIC: a template change (a new module version embeds new files)
  # never replaces the running instance by itself; the operator triggers it
  # (README, Upgrade). One instance holding one reserved address: replace in
  # place, never surge.
  update_policy {
    type                  = "OPPORTUNISTIC"
    minimal_action        = "REPLACE"
    replacement_method    = "RECREATE"
    max_surge_fixed       = 0
    max_unavailable_fixed = 1
  }

  # `rolling-action replace` stamps a generated version name; without this
  # every later plan would try to clear it. It also sets type=PROACTIVE,
  # which the next apply resets on purpose (README, Upgrade).
  lifecycle {
    ignore_changes = [version[0].name]
  }
}
