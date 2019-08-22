from __future__ import annotations

import logging
import sys
from typing import Dict, Optional, Any, List

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse

from same_thing.db import purge_data_dbs
from same_thing.exceptions import UriNotFound
from same_thing.query import get_cluster, UriCluster

debug = '--debug' in sys.argv
if debug:
    # assume this is run in a single process
    purge_data_dbs()

app = Starlette(debug=debug)


@app.on_event('startup')
def log_ready_message() -> None:
    logger = logging.getLogger('uvicorn')
    logger.info('Same Thing Service is ready for lookups.')


@app.route('/lookup/', methods=['GET'])
def lookup(request: Request) -> JSONResponse:
    single_uri = request.query_params.get('uri')
    uris = request.query_params.getlist('uris') or [single_uri]
    if not any(uris):
        return JSONResponse({
            'uri': 'The `uri` parameter must be provided.'
        }, status_code=400)

    fields_by_uri: Dict[str, Optional[UriCluster]] = {}
    for uri in uris:
        try:
            fields_by_uri[uri] = get_cluster(uri)
        except UriNotFound:
            fields_by_uri[uri] = None

    response_fields: Dict[str, Any] = {}
    if not any(fields_by_uri.values()):
        if single_uri:
            return single_uri_not_found(single_uri)
        else:
            return multiple_uris_not_found(uris)
    elif single_uri:
        response_fields = fields_by_uri[single_uri]
    else:
        response_fields['uris'] = fields_by_uri

    meta = request.query_params.get('meta')
    if not (meta and meta == 'off'):
        response_fields['meta'] = {
            'documentation': 'http://dev.dbpedia.org/Global%20IRI%20Resolution%20Service',
            'github': 'https://github.com/dbpedia/dbp-same-thing-service',
            'license': 'http://purl.org/NET/rdflicense/cc-by3.0',
            'license_comment': 'Free service provided by DBpedia. Usage and republication of data implies that you '
                               'attribute either http://dbpedia.org as the source or reference the latest general '
                               'DBpedia paper or the specific paper mentioned in the GitHub Readme.',
            'comment': """
                The service resolves any IRI to its cluster and displays the global IRI and its cluster members.
                Cluster members can change over time as the DBpedia community, 
                data providers and professional services curate the linking space. 
    
                Usage note: 
                1. Save the global ID AND the local IRI that seems most appropriate. 
                   It is recommended that you become a data provider, in which case the local IRI would be your IRI.  
                2. Use the global ID to access anything DBpedia.
                3. Use the stored local ID to update and re-validate linking and clusters.
            """,
        }

    return JSONResponse(response_fields)


def single_uri_not_found(uri: str) -> JSONResponse:
    return JSONResponse({
        'uri': uri
    }, status_code=404)


def multiple_uris_not_found(uris: List[str]) -> JSONResponse:
    return JSONResponse({
        'uris': uris
    }, status_code=404)
