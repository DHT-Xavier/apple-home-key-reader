restart:
	sudo systemctl restart apple-home-key.service

log-f:
	journalctl -u apple-home-key.service -f

log:
	journalctl -u apple-home-key.service