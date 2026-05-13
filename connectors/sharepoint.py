
from config.settings import SHAREPOINT
import requests


class Sharepoint:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.server = 'https://journeyhl.sharepoint.com'
        self.directory_id = SHAREPOINT['directory_id'] or ''
        self.client_secret_id = SHAREPOINT['client_secret_id'] or ''
        self.client_secret_value = SHAREPOINT['client_secret_value'] or ''
        self.object_id = SHAREPOINT['object_id'] or ''
        self.app_id = SHAREPOINT['app_id'] or ''
        self._set_default_sites()
        self._auth()

    def _set_default_sites(self):
        self.marketing = f'{self.server}/sites/Marketing'

    def _auth(self):
        url = f'https://login.microsoftonline.com/{self.directory_id}/oauth2/v2.0/token'
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.app_id,
            'client_secret': self.client_secret_value,
            'scope': 'https://graph.microsoft.com/.default'
        }
        r = requests.post(url, data=data)
        r.raise_for_status()
        self.token = r.json()['access_token']
        r2 = requests.get('https://graph.microsoft.com/v1.0/sites/journeyhl.sharepoint.com:/sites/Marketing',
                          headers={'Authorization': f'Bearer {self.token}'})
        r2.raise_for_status()
        self.site_id = r2.json()['id']

    def get_file(self, server_relative_path: str) -> bytes:
        rel_path = server_relative_path.lstrip('/').replace('sites/Marketing/', '', 1).replace('Shared Documents/', '', 1)
        url = f'https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root:/{rel_path}:/content'
        r = requests.get(url, headers={'Authorization': f'Bearer {self.token}'})
        if not r.ok:
            print(r.text)
        r.raise_for_status()
        content = r.content
        return content
