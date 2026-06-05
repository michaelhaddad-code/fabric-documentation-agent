import subprocess
from .config import AZ_CLI

_cache: dict[str, str] = {}


def get_token(resource: str, force_refresh: bool = False) -> str:
    if not force_refresh and resource in _cache:
        return _cache[resource]
    result = subprocess.run(
        [AZ_CLI, 'account', 'get-access-token', '--resource', resource,
         '--query', 'accessToken', '-o', 'tsv'],
        capture_output=True, text=True, shell=True, check=True,
    )
    token = result.stdout.strip()
    if not token:
        raise RuntimeError(f'No token returned for resource {resource}. Run: az login')
    _cache[resource] = token
    return token
