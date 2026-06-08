resource "google_compute_instance" "vm_spotify" {
  name         = var.vm_name
  machine_type = var.machine_type
  zone         = var.zone

  tags = ["vm-spotify"]

  boot_disk {
    auto_delete = true
    initialize_params {
      image = var.disk_image
      size  = var.disk_size_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    network    = "default"
    subnetwork = "default"

    access_config {
      nat_ip       = google_compute_address.vm_spotify_ip.address
      network_tier = "PREMIUM"
    }
  }

  metadata = {
    enable-osconfig = "TRUE"
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
    provisioning_model  = "STANDARD"
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_secure_boot          = false
    enable_vtpm                 = true
  }

  service_account {
    email  = "126997656473-compute@developer.gserviceaccount.com"
    scopes = [
      "https://www.googleapis.com/auth/devstorage.read_only",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring.write",
      "https://www.googleapis.com/auth/service.management.readonly",
      "https://www.googleapis.com/auth/servicecontrol",
      "https://www.googleapis.com/auth/trace.append",
    ]
  }

  allow_stopping_for_update = true

  lifecycle {
    ignore_changes = [
      boot_disk[0].initialize_params[0].image,  # image version drifts with new releases
      metadata["ssh-keys"],                       # managed dynamically by gcloud/OS Login
    ]
  }
}
