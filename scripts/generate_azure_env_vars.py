
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()
import json
import pyperclip

while True:
    try:
        azure_envs = json.loads(pyperclip.paste())
        break
    except Exception:
        input('Could not parse clipboard. Copy the Azure Environment Variables JSON, then press Enter...')
    


current_env = []
with open('.env' , 'r') as f:
    current_env_file = f.read()


if current_env_file:
    current_env_line = current_env_file.split('\n')

for line in current_env_line:
    if len(line) > 0 and line[0] !='#':
        line_split = line.split('=')
        st =  line_split[1][0]
        end = line_split[1][-1]
        if line_split[1][0] == '"' and line_split[1][-1] == "'":
            test = f'{line_split[1][1:-1]}'
            line_split[1] = f'{line_split[1][1:-1]}'
            bp = 'here'
        elif line_split[1][0] == "'":
            line_split[1] = line_split[1].replace("'", '"') 
        entry = {
            'name': f'{line_split[0]}',
            'value': line_split[1],
            "slotSetting": False
        }
        current_env.append(entry)


bp = 'here'
azure_env_backup = azure_envs.copy()
print(f'Current number of env variables in Azure input: {len(azure_envs)}')
print(f'Current number of env variables in local .env file: {len(current_env)}')
for entry in current_env:
    if entry['name'] in [az['name'] for az in azure_envs]:
        bp = 'here'
    else:
        bp = 'here'
        azure_envs.append(entry)

pyperclip.copy(json.dumps(azure_envs, indent=2))
print(f'New env json ready for paste to azure! {len(azure_env_backup) - len(azure_envs)} variables added')

bp = 'here'
