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
    startup-script  = <<-EOF
      #!/bin/bash
      set -e
      exec > /var/log/startup-script.log 2>&1

      # Outils système
      apt-get update -q
      apt-get install -y -q git curl python3-pip python3-venv build-essential

      # uv (gestionnaire de paquets Python)
      export HOME=/root
      curl -LsSf https://astral.sh/uv/install.sh | sh
      export PATH="/root/.local/bin:$PATH"
      echo 'export PATH="/root/.local/bin:$PATH"' >> /root/.bashrc

      # Cloner le repo
      cd /opt
      if [ ! -d "Recommandation_musique" ]; then
        git clone https://github.com/Thomas-Brvn/Recommandation_musique.git
      fi
      cd Recommandation_musique

      # Installer les dépendances (sans Airflow ni Spark pour le démarrage)
      uv sync

      # Initialiser Airflow (Airflow 3 : pas de commande users create)
      export AIRFLOW_HOME=/opt/Recommandation_musique/.airflow
      uv sync --extra airflow
      uv run airflow db migrate

      # Service systemd pour Airflow
      cat > /etc/systemd/system/airflow.service <<'UNIT'
[Unit]
Description=Airflow Standalone
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/Recommandation_musique
Environment="AIRFLOW_HOME=/opt/Recommandation_musique/.airflow"
Environment="PATH=/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ExecStart=/root/.local/bin/uv run airflow standalone
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
UNIT

      # Service systemd pour l'API FastAPI
      cat > /etc/systemd/system/fastapi.service <<'UNIT'
[Unit]
Description=Music Recommendation FastAPI
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/Recommandation_musique
Environment="PATH=/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
EnvironmentFile=/opt/Recommandation_musique/.env
ExecStart=/root/.local/bin/uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
UNIT

      # Activer et démarrer les services
      systemctl daemon-reload
      systemctl enable airflow fastapi
      systemctl start airflow fastapi

      echo "✅ Setup terminé"
    EOF
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
      "https://www.googleapis.com/auth/devstorage.read_write",
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
