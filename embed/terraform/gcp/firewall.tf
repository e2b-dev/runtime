# The dedicated VPC denies ingress by default; these three rules are the
# whole policy. The stack binds thirteen ports on every interface (api 3000,
# 5009, 5109; dashboard 3001; client-proxy 3002, 3003; dashboard-api 3010;
# orchestrator 5007, 5008 and the sandbox egress proxies 5010, 5016, 5017,
# 5018); 5008 is an unauthenticated control API and the egress proxies and
# dashboard-api expect no external client, so only 3000, 3001 and 3002 are
# opened, and only to the operator's CIDRs.
resource "google_compute_firewall" "clients" {
  project                 = var.project_id
  name                    = "${var.name}-clients"
  network                 = google_compute_network.this.id
  direction               = "INGRESS"
  source_ranges           = var.client_cidrs
  target_service_accounts = [google_service_account.this.email]

  allow {
    protocol = "tcp"
    ports    = ["3000", "3001", "3002"]
  }
}

resource "google_compute_firewall" "health_check" {
  project                 = var.project_id
  name                    = "${var.name}-health-check"
  network                 = google_compute_network.this.id
  direction               = "INGRESS"
  source_ranges           = ["35.191.0.0/16", "130.211.0.0/22"]
  target_service_accounts = [google_service_account.this.email]

  allow {
    protocol = "tcp"
    ports    = ["3000"]
  }
}

resource "google_compute_firewall" "iap_ssh" {
  project                 = var.project_id
  name                    = "${var.name}-iap-ssh"
  network                 = google_compute_network.this.id
  direction               = "INGRESS"
  source_ranges           = ["35.235.240.0/20"]
  target_service_accounts = [google_service_account.this.email]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}
