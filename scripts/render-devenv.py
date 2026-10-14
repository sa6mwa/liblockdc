#!/usr/bin/env python3
"""Render the checkout's declared Podman service graph and shell endpoints."""
import hashlib
import json
import os
import re
from pathlib import Path
import shlex
import sys

if len(sys.argv) == 1:
    primary = 'a'
elif len(sys.argv) == 3 and sys.argv[1] == '--disk-primary' and sys.argv[2] in ('a', 'b'):
    primary = sys.argv[2]
else:
    sys.exit('usage: render-devenv.py [--disk-primary a|b]')
root = Path(__file__).resolve().parent.parent
workspace = root / 'build/devenv'
if workspace.is_symlink() or not workspace.resolve().is_relative_to(root / 'build'):
    sys.exit('build/devenv must not be a symlink')
workspace.mkdir(parents=True, exist_ok=True, mode=0o700)
identity = hashlib.sha256(os.fsencode(root)).hexdigest()
base = 20000 + (int(identity[:8], 16) % 4000) * 10
names = ['LOCKDC_DISK_A_PORT', 'LOCKDC_DISK_B_PORT', 'LOCKDC_S3_PORT',
         'LOCKDC_MINIO_API_PORT', 'LOCKDC_MINIO_CONSOLE_PORT']
path = workspace / 'devenv.yaml'
if path.is_symlink():
    sys.exit(f'generated file must not be a symlink: {path}')
saved_ports = {}
if path.exists():
    try:
        saved = json.loads(path.read_text())
        bindings = [('lockd-disk-a', 9341), ('lockd-disk-b', 9342),
                    ('lockd-s3', 9343), ('minio', 9000), ('minio', 9001)]
        for name, (container_name, container_port) in zip(names, bindings):
            ports = [port['hostPort'] for container in saved['spec']['containers']
                     if container['name'] == container_name
                     for port in container.get('ports', [])
                     if port['containerPort'] == container_port]
            if len(ports) != 1 or type(ports[0]) is not int:
                raise ValueError(f'invalid saved binding for {name}')
            saved_ports[name] = str(ports[0])
    except (OSError, ValueError, KeyError, TypeError) as error:
        sys.exit(f'cannot read saved devenv ports: {error}; run make dev-reset')
values = {'POD_NAME': 'liblockdc-' + identity[:12], 'DEVENV_ROOT': str(workspace)}
for offset, name in enumerate(names):
    raw = os.environ.get(name, saved_ports.get(name, str(base + offset)))
    if not raw.isascii() or not raw.isdecimal() or not 1024 <= int(raw) <= 65535:
        sys.exit(f'{name} must be an integer port between 1024 and 65535')
    values[name] = int(raw)
if len({values[name] for name in names}) != len(names):
    sys.exit('local service host ports must be distinct')

def expand(value):
    if isinstance(value, dict):
        return {key: expand(item) for key, item in value.items()}
    if isinstance(value, list):
        return [expand(item) for item in value]
    if isinstance(value, str):
        matches = list(re.finditer(r'@([A-Z0-9_]+)@', value))
        for match in matches:
            key = match.group(1)
            if key not in values:
                sys.exit(f'unknown manifest placeholder: {key}')
        if len(matches) == 1 and matches[0].group(0) == value:
            return values[matches[0].group(1)]
        value = re.sub(r'@([A-Z0-9_]+)@', lambda match: str(values[match.group(1)]), value)
    return value

template = (root / 'devenv.yaml.in').read_text()
pod = expand(json.loads('\n'.join(line for line in template.splitlines()
                                  if not line.startswith('#'))))
manifest = json.dumps(pod, indent=2) + '\n'
if path.exists() and path.read_text() != manifest:
    sys.exit('existing devenv configuration differs; run make dev-reset before changing ports')
for volume in pod['spec']['volumes']:
    directory = Path(volume['hostPath']['path'])
    if directory.is_symlink() or not directory.resolve().is_relative_to(workspace):
        sys.exit(f'generated service directory must not be a symlink: {directory}')
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
for name in ('tmp', 'logs'):
    directory = workspace / name
    if directory.is_symlink():
        sys.exit(f'generated directory must not be a symlink: {directory}')
    directory.mkdir(exist_ok=True, mode=0o700)
env = {name: str(values[name]) for name in names}
env.update(LOCKDC_DEVENV_POD=values['POD_NAME'],
           LOCKDC_E2E_DISK_ENDPOINT=f'https://localhost:{values[names[0 if primary == "a" else 1]]}',
           LOCKDC_E2E_DISK_B_ENDPOINT=f'https://localhost:{values[names[1]]}',
           LOCKDC_E2E_S3_ENDPOINT=f'https://localhost:{values[names[2]]}',
           LOCKDC_E2E_DISK_BUNDLE=str(workspace / 'credentials/disk/client.pem'),
           LOCKDC_E2E_S3_BUNDLE=str(workspace / 'credentials/s3/client.pem'),
           LOCKDC_E2E_MEM_SOCKET=str(workspace / 'state/mem-run/lockd.sock'),
           LOCKDC_E2E_TC_BUNDLE=str(workspace / 'credentials/disk/tc-client.pem'))
env.update(LOCKDC_URL=env['LOCKDC_E2E_DISK_ENDPOINT'],
           LOCKDC_E2E_DISK_A_ENDPOINT=env['LOCKDC_E2E_DISK_ENDPOINT'],
           LOCKDC_E2E_DISK_A_BUNDLE=env['LOCKDC_E2E_DISK_BUNDLE'],
           LOCKDC_CLIENT_PEM=env['LOCKDC_E2E_DISK_BUNDLE'],
           LOCKDC_FALLBACK_URL=f'https://localhost:{values[names[1 if primary == "a" else 0]]}')
for destination, content in ((path, manifest), (workspace / 'env.sh',
        ''.join(f'export {key}={shlex.quote(value)}\n' for key, value in env.items()))):
    if destination.is_symlink():
        sys.exit(f'generated file must not be a symlink: {destination}')
    fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write(content)
    destination.chmod(0o600)
print(path)
