import os
import tarfile
import tempfile
from datetime import datetime
from functools import lru_cache
import requests


class YaFile(object):
    URL = "https://cloud-api.yandex.net/v1/disk/resources"

    def __init__(self, token):
        self.token = token
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'OAuth {self.token}'
        }

    @lru_cache(maxsize=256)
    def get_info(self, path, sort=None):
        try:
            if sort is None:
                req_url = f'{self.URL}?path={path}'
            else:
                req_url = f'{self.URL}?path={path}&sort={sort}'
            req = requests.get(req_url, headers=self.headers).json()
        except KeyError:
            return ""
        return req if 'error' not in req else ""

    def exist(self,path):
        return 'path' in self.get_info(path)

    def is_dir(self,path):
        if self.exist(path):
            return 'dir' == self.get_info(path)['type']
        else:
            return False

    def is_file(self, path):
        if self.exist(path):
            return 'file' == self.get_info(path)['type']
        else:
            return False

    def list(self, path, sort=None):
        if self.is_dir(path):
            return self.get_info(path, sort)['_embedded']['items']
        else:
            return []

    def create(self, path):
        requests.put(f'{self.URL}?path={path}', headers=self.headers)

    def delete(self, path):
        requests.delete(f'{self.URL}?path={path}', headers=self.headers)

    def upload(self, loadfile, remote_path, replace=False):
        res = requests.get(f'{self.URL}/upload?path={remote_path}&overwrite={replace}', headers=self.headers).json()
        with open(loadfile, 'rb') as f:
            try:
                requests.put(res['href'], files={'file':f})
            except KeyError:
                print(f"{res} (path: {loadfile}, remote: {remote_path})")


class YaBackup(YaFile):
    def __init__(self, token, remote_dir="", date_template='%Y_%m_%d', prefix=""):
        YaFile.__init__(self, token)
        self.remote_dir = f"/{remote_dir.strip('/')}"
        self.date = datetime.now().strftime(date_template)
        self.prefix = prefix

    def full_path(self, path, suffix=""):
        res_path = self.remote_dir
        res_path += f"/{self.prefix}{os.path.basename(path)}"
        res_path += f"-{self.date}"
        res_path += suffix
        return res_path

    @staticmethod
    def archive(archive, path):
        with tarfile.open(archive, 'w:gz') as arch:
            if os.path.isdir(path):
                for dir_name, _, files in os.walk(path):
                    for f in files:
                        arch.add(os.path.join(dir_name, f))
            elif os.path.isfile(path):
                arch.add(path)
        return arch.name

    def backup(self, path, archive=True):
        if archive:
            with tempfile.TemporaryDirectory(prefix="ya_backup-") as tmpdir:
                arch = YaBackup.archive(os.path.join(tmpdir, os.path.basename(path)) + ".tgz", path)
                self.upload(arch, f"{self.full_path(path)}.tgz")
        else:
            if os.path.isdir(path):
                for d, _, files in os.walk(path):
                    remote_dir_path = self.full_path(path, suffix=f"/{d.replace(path, '').strip('/')}")
                    if not self.is_dir(remote_dir_path):
                        self.create(remote_dir_path)
                    for f in files:
                        self.upload(os.path.join(d, f), f"{remote_dir_path}/{f}", True)
            elif os.path.isfile(path):
                self.upload(path, self.full_path(path))

    def clear_old(self, how_many_to_store, path=None, prefix=None):
        if path is None or path == self.remote_dir:
            file_list = self.list(self.remote_dir, sort='modified')
        else:
            file_list = self.list(f"{self.remote_dir}/{path}", sort='modified')
        if len(file_list) == 0:
            return
        else:
            if not prefix is None:
                file_list = map(file_list, lambda x: x['name'].startswith(prefix))
        files_to_delete = file_list[how_many_to_store:]


if __name__ == '__main__':
    yb = YaBackup(os.environ['TOKEN'], os.environ['REMOTE_DIR'])
    yb.clear_old(2, prefix="tmp2-")
    yb.backup("/home/pavel/tmp", False)
