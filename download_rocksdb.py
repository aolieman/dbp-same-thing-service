import sys
from urllib.request import OpenerDirector, HTTPRedirectHandler, HTTPSHandler, urlretrieve

REPO_URL = 'https://github.com/facebook/rocksdb'

assert len(sys.argv) > 1, 'Please provide a download directory, e.g. /build'
assert len(sys.argv) < 3, f'Please omit the unexpected arguments: {sys.argv[2:]}'
download_dir = sys.argv[1]

od = OpenerDirector()
od.add_handler(HTTPSHandler())
od.add_handler(HTTPRedirectHandler())

resp = od.open(f'{REPO_URL}/releases/latest/download/')
tag_name = resp.headers['location'].split('/')[-1]

release_url = f'{REPO_URL}/archive/{tag_name}.tar.gz'
file_path, headers = urlretrieve(release_url, f'{download_dir}/latest.tar.gz')

print(f'RocksDB {tag_name} was downloaded to {file_path}', file=sys.stderr)
print(tag_name[1:])
