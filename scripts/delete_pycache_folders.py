import sys
import os
import shutil
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path

root = Path(os.path.dirname(__file__)).parent
pycache_folders = list(root.glob('*/__pycache__'))
print(f'Found {len(pycache_folders)} pycache folders')
for d in root.glob('*/__pycache__'):
    print(f'deleting {d}')
    shutil.rmtree(d)
    print(f'Deleted {d.parent.name}\\{d.name}')