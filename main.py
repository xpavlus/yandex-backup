import os
import re
import tarfile
import tempfile
from datetime import datetime
from functools import lru_cache
import requests


class YaFile(object):
    URL = "https://cloud-api.yandex.net/v1/disk/resources"
    DELIMITER = "/"

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
            return {}
        return {} if 'error' in req else req

    def exist(self, path):
        return 'path' in self.get_info(path)

    def is_dir(self, path):
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
        req = requests.put(f'{self.URL}?path={path}', headers=self.headers)
        return req

    def delete(self, path):
        req = requests.delete(f'{self.URL}?path={path}', headers=self.headers)
        return req

    def upload(self, loadfile, remote_path, replace=False):
        res = requests.get(f'{self.URL}/upload?path={remote_path}&overwrite={replace}', headers=self.headers).json()
        with open(loadfile, 'rb') as f:
            try:
                requests.put(res['href'], files={'file': f})
            except KeyError:
                print(f"{res} (path: {loadfile}, remote: {remote_path})")


class YaBackup(YaFile):
    def __init__(self, token, remote_dir="", date_template='%Y_%m_%d', prefix=""):
        YaFile.__init__(self, token)
        self.remote_dir = f"/{remote_dir.strip(self.DELIMITER)}"
        self.date = datetime.now().strftime(date_template)
        self.prefix = prefix

    def join(self, *args):
        paths = [s.strip(self.DELIMITER) for s in args]
        result = self.DELIMITER.join(
            list(filter(lambda x: x, paths))
        )
        return self.DELIMITER + result

    def get_root(self):
        return self.remote_dir

    def backup_path(self, path, prefix="", suffix=""):
        result = f"{self.prefix}{prefix}{path.rstrip(self.DELIMITER).split(self.DELIMITER)[-1]}-{self.date}{suffix}"
        return result

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
                self.upload(arch, self.join(self.get_root(), self.backup_path(path, suffix=".tgz")))
        else:
            if os.path.isdir(path):
                backup_path = self.join(self.get_root(), self.backup_path(path))
                for d, _, files in os.walk(path):
                    path_diff = d.replace(path, '')
                    store_dir = self.join(backup_path, path_diff)
                    if not self.is_dir(store_dir):
                        self.create(store_dir)
                    for f in files:
                        self.upload(os.path.join(d, f), self.join(store_dir, f), True)
            elif os.path.isfile(path):
                ext = re.findall(r'\.\w+$', path)[-1]
                if ext:
                    path_trim = path[:path.rindex(ext)]
                    self.upload(path, self.join(self.get_root(), self.backup_path(path_trim, suffix=ext)))
                else:
                    self.upload(path, self.join(self.get_root(), self.backup_path(path)))


    def clear_old(self, how_many_to_store, path="", prefix=""):
        if path or path == self.remote_dir:
            file_list = self.list(self.remote_dir, sort='modified')
        else:
            file_list = self.list(f"{self.remote_dir}/{path}", sort='modified')
        if len(file_list) == 0:
            return
        else:
            if prefix:
                file_list = list(filter(lambda x: x['name'].startswith(prefix), file_list))
        for file in file_list[how_many_to_store:]:
            self.delete(self.join(self.get_root(), path, file['name']))


if __name__ == '__main__':
    from dotenv import load_dotenv
    from yaml import safe_load as load_yaml

    load_dotenv()

    yb = YaBackup(os.getenv('TOKEN'), os.getenv('REMOTE_DIR'))


    def do_backup(path, store_days, do_archive):
        yb.clear_old(
            store_days,
            prefix=path.rstrip('/').split('/')[-1] + "-"
        )
        yb.backup(path, do_archive)


    with open('./backup_list.yml', 'r') as f:
        config = load_yaml(f)

    for i, items in config.items():
        do_backup(i, items['days to store'], items['archive'])
