# multiscale
A Windows GUI orchestrator for multiplexing egress traffic across multiple isolated Tailscale networks. 

This tool leverages Tailscale's userspace networking mode (`gVisor`) to run infinite independent Tailscale daemons simultaneously. Each instance binds to a unique local SOCKS5 port and routes traffic through a specific remote exit node. Because it uses userspace networking, it **does not** create virtual network adapters or alter the Windows system routing table. Your host machine's normal network traffic remains entirely unaffected.

## Features
* **Total Isolation:** Each node uses its own state directory and named pipe (`\\.\pipe\ts_node...`).
* **Zero Host Interference:** Leaves the host OS routing table untouched.
* **Persistent Configuration:** Automatically saves Ports, Exit Node IPs, and AuthKeys.
* **Clean Shutdown:** Gracefully tracks and kills spawned background `tailscaled.exe` processes on exit.

## Prerequisites
To run or compile this application, you need:
1. **Tailscale:** Installed in its default Windows directory (`C:\Program Files\Tailscale\`).
2. **Go:** Version 1.20 or newer.
3. **C Compiler (For Windows GUI):** Fyne requires a C-compiler to build graphics libraries. We recommend [w64devkit](https://github.com/skeeto/w64devkit/releases).

---
<img width="2505" height="1086" alt="multiscale" src="https://github.com/user-attachments/assets/1150a453-7555-4db4-a7f3-12eb845393e2" />
