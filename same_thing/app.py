import os
import sys

from apistar import App, Route
from apistar.exceptions import NotFound


# ensure module is found in path
BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)
sys.path.insert(0, os.path.abspath(BASE_DIR))

from same_thing.db import split_values, get_connection_to_latest
from same_thing.sink import DBP_GLOBAL_MARKER, DBP_GLOBAL_PREFIX

db = get_connection_to_latest(max_retries=11, read_only=True)


def lookup(uri: str) -> dict:
    normalized_uri = uri.lstrip(DBP_GLOBAL_PREFIX)
    if normalized_uri.startswith(DBP_GLOBAL_MARKER):
        normalized_uri = normalized_uri.encode('utf8')
    else:
        normalized_uri = db.get(uri.encode('utf8'))
        if not normalized_uri:
            raise NotFound()

    value_bytes = db.get(normalized_uri)
    if not value_bytes:
        raise NotFound()

    return {
        'global': DBP_GLOBAL_PREFIX + normalized_uri.decode('utf8'),
        'locals': split_values(value_bytes),
    }


routes = [
    Route('/lookup/', method='GET', handler=lookup),
]

app = App(routes=routes)


if __name__ == '__main__':
    app.serve('0.0.0.0', 5000, debug=True, use_reloader=True)
