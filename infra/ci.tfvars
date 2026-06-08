project_id   = "projetetude-497218"
region       = "us-central1"
zone         = "us-central1-b"
vm_name      = "vm-spotify"
machine_type = "e2-medium"
disk_size_gb = 10
disk_image   = "debian-cloud/debian-12"
ssh_user     = "alphonsemarcay"

# In CI, SSH is restricted to known IPs (e.g. GitHub Actions runners).
# For now, keep open. Tighten later if needed.
allowed_ssh_cidrs = ["0.0.0.0/0"]
