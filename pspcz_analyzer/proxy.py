"""Client-IP extraction and IP whitelist logic, proxy-aware.

The admin dashboard gates access on an IP whitelist. When the app runs
behind a reverse proxy the real client IP arrives in ``X-Forwarded-For``,
but that header is attacker-controlled unless the direct TCP peer is a
known, trusted proxy. Trusting it unconditionally lets any remote client
send ``X-Forwarded-For: 127.0.0.1`` and pass a loopback whitelist.

:func:`extract_client_ip` implements the rightmost-untrusted algorithm:
X-Forwarded-For is consulted only when the TCP peer itself is trusted,
and the client IP is the first (rightmost) entry that is NOT a trusted
proxy. Everything else falls back to the TCP peer.
"""

import ipaddress
from collections.abc import Sequence

from loguru import logger
from starlette.requests import Request

IPNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network
IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address


def parse_ip_networks(raw: str) -> list[IPNetwork]:
    """Parse a comma-separated list of IP/CIDR entries into network objects.

    Invalid entries are logged and skipped rather than raising, so one
    typo in the env var does not lock the admin out entirely.

    Args:
        raw: Comma-separated IP addresses or CIDR networks.

    Returns:
        Parsed networks, in input order.
    """
    networks: list[IPNetwork] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        try:
            networks.append(ipaddress.ip_network(entry, strict=False))
        except ValueError:
            logger.warning("[proxy] Invalid IP/CIDR entry ignored: {}", entry)
    return networks


def _parse_address(ip_str: str) -> IPAddress | None:
    """Parse an IP string, normalizing IPv4-mapped IPv6 to plain IPv4.

    Args:
        ip_str: IP address literal (possibly ``::ffff:x.x.x.x`` form).

    Returns:
        The parsed address, or None if unparseable.
    """
    try:
        addr: IPAddress = ipaddress.ip_address(ip_str.strip())
    except ValueError:
        return None
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
        return addr.ipv4_mapped
    return addr


def is_ip_allowed(ip_str: str, networks: Sequence[IPNetwork]) -> bool:
    """Check whether an IP literal falls within any of the given networks.

    Args:
        ip_str: IP address literal.
        networks: Allowed networks to match against.

    Returns:
        True if the IP is in at least one network, False otherwise
        (including unparseable input).
    """
    addr = _parse_address(ip_str)
    if addr is None:
        return False
    return any(addr in net for net in networks)


def extract_client_ip(request: Request, trusted_proxies: Sequence[IPNetwork]) -> str:
    """Determine the real client IP for a request, respecting proxies safely.

    X-Forwarded-For is only trusted when the direct TCP peer is itself a
    trusted proxy; otherwise an attacker could spoof the header. When the
    peer is trusted, the chain is walked right-to-left and the first
    untrusted entry (the one the trusted proxy appended) is returned.

    Args:
        request: Incoming request.
        trusted_proxies: Networks whose X-Forwarded-For headers may be
            trusted (typically loopback and/or the reverse proxy's IP).

    Returns:
        The client IP string. Falls back to the TCP peer (or ``0.0.0.0``
        if unknown) whenever the header is absent or fully trusted.
    """
    client = request.client
    peer = client.host if client else "0.0.0.0"

    if not is_ip_allowed(peer, trusted_proxies):
        # Untrusted peer: any X-Forwarded-For is attacker-controlled.
        return peer

    forwarded = request.headers.get("x-forwarded-for", "")
    entries = [entry.strip() for entry in forwarded.split(",") if entry.strip()]
    if not entries:
        return peer

    for entry in reversed(entries):
        if not is_ip_allowed(entry, trusted_proxies):
            return entry
    # Every entry is a trusted proxy — return the leftmost as best effort.
    return entries[0]
