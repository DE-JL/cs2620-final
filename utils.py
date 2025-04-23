import netifaces

from config import NETWOK, MACHINES_LOCAL, MACHINES_PUBLIC

def get_ipaddr() -> str | None:
    """
    Retrieve the IPv4 address of the specified network interface.

    :return: The IPv4 address as a string or None if not found.
    """
    try:
        addrs = netifaces.ifaddresses(NETWOK['interface'])

        # netifaces.AF_INET is the IPv4 family
        if netifaces.AF_INET in addrs:
            # Each entry looks like {'addr': '10.X.X.X', 'netmask': '255.255.0.0', 'broadcast': '10.250.255.255'}
            return addrs[netifaces.AF_INET][0]['addr']

    except ValueError:
        # NETWORK_INTERFACE might not exist on this machine
        return None
    
def get_id_to_addr_map() -> dict[int, str]:
    """
    Returns a mapping from IDs to config for either the local or public machines.
    """
    return MACHINES_PUBLIC if NETWOK['public_status'] else MACHINES_LOCAL
