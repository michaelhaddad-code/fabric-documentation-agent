import subprocess
import time
from .config import AZ_CLI

_cache: dict[str, tuple[str, float]] = {}  # resource → (token, expiry_unix_time)
_TOKEN_TTL = 50 * 60  # 50 min — refreshes before the 60-min Azure expiry


def get_token(resource: str, force_refresh: bool = False) -> str:
    now = time.time()
    if not force_refresh and resource in _cache:
        token, expiry = _cache[resource]
        if now < expiry:
            return token
    result = subprocess.run(
        [AZ_CLI, 'account', 'get-access-token', '--resource', resource,
         '--query', 'accessToken', '-o', 'tsv'],
        capture_output=True, text=True, shell=True, check=True,
    )
    token = result.stdout.strip()
    if not token:
        raise RuntimeError(f'No token returned for resource {resource}. Run: az login')
    _cache[resource] = (token, now + _TOKEN_TTL)
    return token
