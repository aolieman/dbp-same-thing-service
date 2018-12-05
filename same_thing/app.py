import os
import sys

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse

# ensure module is found in path
BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)
sys.path.insert(0, os.path.abspath(BASE_DIR))

from same_thing.db import split_values, get_connection_to_latest
from same_thing.sink import DBP_GLOBAL_MARKER, DBP_GLOBAL_PREFIX

db = get_connection_to_latest(max_retries=12, read_only=True)
app = Starlette(debug='--debug' in sys.argv)


@app.route('/lookup/', methods=['GET'])
def lookup(request: Request) -> JSONResponse:
    uri = request.query_params.get('uri')
    if not uri:
        return JSONResponse({
            'uri': 'The `uri` parameter must be provided.'
        }, status_code=400)

    normalized_uri = uri.lstrip(DBP_GLOBAL_PREFIX)
    if normalized_uri.startswith(DBP_GLOBAL_MARKER):
        normalized_uri = normalized_uri[len(DBP_GLOBAL_MARKER):].encode('utf8')
    else:
        normalized_uri = db.get(uri.encode('utf8'))
        if not normalized_uri:
            return not_found(uri)

    value_bytes = db.get(normalized_uri)
    if not value_bytes:
        return not_found(uri)

    return JSONResponse({
        'global': f"{DBP_GLOBAL_PREFIX}{DBP_GLOBAL_MARKER}{normalized_uri.decode('utf8')}",
        'locals': split_values(value_bytes),
    })


def not_found(uri: str) -> JSONResponse:
    return JSONResponse({
        'uri': uri
    }, status_code=404)
