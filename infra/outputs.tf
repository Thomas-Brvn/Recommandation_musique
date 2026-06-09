output "vm_external_ip" {
  description = "External IP of the VM"
  value       = google_compute_address.vm_spotify_ip.address
}

output "vm_internal_ip" {
  description = "Internal IP of the VM"
  value       = google_compute_instance.vm_spotify.network_interface[0].network_ip
}

output "ssh_command" {
  description = "SSH command to connect"
  value       = "ssh ${var.ssh_user}@${google_compute_address.vm_spotify_ip.address}"
}
