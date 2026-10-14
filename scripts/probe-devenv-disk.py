#!/usr/bin/env python3
"""Select an active disk node using the declared image's bounded CLI probe."""
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

root = Path(__file__).resolve().parent.parent
workspace = root / 'build/devenv'
env = {key: value for key, value in os.environ.items()
       if not key.startswith('LOCKD_')}
for node, port in zip(('a', 'b'), sys.argv[1:]):
    command = [str(workspace / 'state/bootstrap/lockd'), 'client',
               '--config', str(root / 'devenv/client.yaml'),
               '--bundle', str(workspace / 'credentials/disk/client.pem'),
               '--server', f'https://localhost:{port}', '--timeout', '2s']
    key = 'devenv-readiness-' + uuid.uuid4().hex
    try:
        result = subprocess.run(command + ['acquire', '--namespace', 'default',
            '--key', key, '--owner', 'devenv-readiness', '--ttl', '10s',
            '--block=-1', '--retries=-1', '--output', 'json'],
            env=env, capture_output=True, text=True, timeout=5, check=True)
        lease = json.loads(result.stdout)
        subprocess.run(command + ['release', '--namespace', 'default',
            '--key', key, '--lease', lease['lease_id'], '--txn-id', lease['txn_id'],
            '--fencing-token', str(lease['fencing_token']), '--rollback'],
            env=env, capture_output=True, text=True, timeout=5, check=True)
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError) as error:
        print(f'disk-{node} API readiness probe failed: {error}', file=sys.stderr)
        continue
    print(node)
    sys.exit(0)
sys.exit(1)
