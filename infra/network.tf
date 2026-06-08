resource "google_compute_address" "vm_spotify_ip" {
  name         = "vm-spotify-ip"
  region       = var.region
  network_tier = "PREMIUM"
}

resource "google_compute_firewall" "vm_spotify_ssh" {
  name    = "vm-spotify-allow-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  direction     = "INGRESS"
  source_ranges = var.allowed_ssh_cidrs
  target_tags   = ["vm-spotify"]
}

resource "google_compute_firewall" "vm_spotify_portainer" {
  name    = "vm-spotify-allow-portainer"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["9443"]
  }

  direction     = "INGRESS"
  source_ranges = var.allowed_ssh_cidrs
  target_tags   = ["vm-spotify"]
}
