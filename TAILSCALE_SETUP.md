# Tailscale VPN Setup — Secure Remote Access

## Overview

The bot dashboard on port `8087` is protected behind a Tailscale private VPN.  
Port `8087` is **closed to the public internet** (Azure NSG) and only accessible via Tailscale.

---

## Tailscale Network Devices

| Device | Tailscale IP | OS |
|---|---|---|
| arbscanner-dublin-vm (Azure VM) | `100.106.19.81` | Linux |
| pc1 (Windows laptop) | `100.68.9.78` | Windows |
| iphone-12 | `100.92.200.15` | iOS |

**Tailscale account:** `jlaurabi@gmail.com`

---

## Accessing the Bot Dashboard

From any authorized device (PC or iPhone), open:

```
http://100.106.19.81:8087
```

- Works from **any network** (home, mobile data, coffee shop, etc.)
- Only devices logged into the Tailscale account can connect
- Traffic is **end-to-end encrypted** (WireGuard)

---

## How It Works

Tailscale creates a private mesh VPN between your devices only.  
The `100.x.x.x` IP range is a private Tailscale-only address space — it does not exist on the public internet.  
Even if someone knows the IP, they cannot connect without being authenticated to your Tailscale account.

```
Your Phone  ──┐
              ├── Encrypted WireGuard tunnel ── Azure VM (port 8087)
Your Laptop ──┘

Public internet: port 8087 blocked, connection refused
```

---

## VM Setup Details

Tailscale was installed on `2026-04-27` on the Azure Dublin VM.

```bash
# Install
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up

# Auto-start on reboot (already enabled)
sudo systemctl enable tailscaled

# Check status
sudo tailscale status
```

Tailscale version: `1.96.4`  
Service: `tailscaled` (systemd, enabled, starts on reboot)

---

## Adding a New Device

1. Install Tailscale on the new device: https://tailscale.com/download
2. Log in with `jlaurabi@gmail.com` (Google)
3. The new device will appear in the Tailscale admin panel and can immediately access `http://100.106.19.81:8087`

---

## Tailscale Admin Panel

Manage devices, revoke access, check connection status:  
https://login.tailscale.com/admin/machines

---

## Azure NSG (Firewall) Status

| Port | Rule | Access |
|---|---|---|
| `22` (SSH) | Open | Via SSH key only |
| `8087` (Bot dashboard) | **Closed / Deny all** | Tailscale only |
| `8086` (Bot dashboard v6) | Open (restrict if needed) | Public |
