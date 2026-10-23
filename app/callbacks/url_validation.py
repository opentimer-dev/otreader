"""Callback URL validation (basic SSRF safeguards).

Rules:
* scheme must be http or https, a host is required;
* the host is resolved and *every* resolved address is checked;
* link-local (incl. 169.254.169.254 cloud metadata), multicast, unspecified and
  reserved addresses are always rejected;
* loopback / private / public ranges are allowed according to configuration
  (defaults allow all three, suitable for a private race LAN where the timing
  software may run on the same host or another LAN machine);
* an optional ``allowed_networks`` CIDR allow-list restricts targets further.

Limitation: validation happens when the URL is set (and at startup).  A DNS name
that later resolves elsewhere is not re-checked per request; use IP addresses or
``allowed_networks`` if the API is reachable from untrusted networks.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlsplit, urlunsplit

from app.config import CallbackConfig


class CallbackURLError(ValueError):
    pass


def redact_url(url: str) -> str:
    """Hide credentials embedded in a URL (``http://user:pass@host`` -> ``http://***@host``)."""
    parts = urlsplit(url)
    if parts.username or parts.password:
        host = parts.hostname or ""
        if parts.port:
            host = f"{host}:{parts.port}"
        return urlunsplit((parts.scheme, f"***@{host}", parts.path, parts.query, parts.fragment))
    return url


def _check_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address, cfg: CallbackConfig) -> None:
    # Checked before is_reserved: Python classifies ::1 as reserved (::/8).
    if not ip.is_loopback and (ip.is_link_local or ip.is_multicast or ip.is_unspecified or ip.is_reserved):
        raise CallbackURLError(f"address {ip} is not an allowed callback target")
    if cfg.allowed_networks:
        if not any(ip in ipaddress.ip_network(n, strict=False) for n in cfg.allowed_networks):
            raise CallbackURLError(f"address {ip} is outside callbacks.allowed_networks")
        return
    if ip.is_loopback:
        if not cfg.allow_loopback:
            raise CallbackURLError(f"loopback address {ip} not allowed (callbacks.allow_loopback=false)")
    elif ip.is_private:
        if not cfg.allow_private_networks:
            raise CallbackURLError(f"private address {ip} not allowed (callbacks.allow_private_networks=false)")
    elif not cfg.allow_public_networks:
        raise CallbackURLError(f"public address {ip} not allowed (callbacks.allow_public_networks=false)")


async def validate_callback_url(url: str, cfg: CallbackConfig) -> str:
    if len(url) > 2048:
        raise CallbackURLError("URL too long")
    parts = urlsplit(url.strip())
    if parts.scheme not in ("http", "https"):
        raise CallbackURLError("callback URL must use http or https")
    host = parts.hostname
    if not host:
        raise CallbackURLError("callback URL must include a host")
    try:
        port = parts.port or (443 if parts.scheme == "https" else 80)
    except ValueError:
        raise CallbackURLError("invalid port in callback URL") from None
    try:
        ip = ipaddress.ip_address(host)
        addrs = [ip]
    except ValueError:
        try:
            infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            raise CallbackURLError(f"cannot resolve callback host {host!r}: {exc}") from None
        addrs = [ipaddress.ip_address(i[4][0].split("%")[0]) for i in infos]
    for a in addrs:
        _check_ip(a, cfg)
    return url.strip()
