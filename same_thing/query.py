import re
from urllib.parse import unquote

from same_thing.db import get_connection_to_latest, is_cluster_membership, sorted_cluster
from same_thing.exceptions import UriNotFound
from same_thing.sink import DBP_GLOBAL_PREFIX, DBP_GLOBAL_MARKER

db = get_connection_to_latest(max_retries=12, read_only=True)
wiki_article_re = re.compile(
    r'https?://(?P<locale>[a-z-]{2,}\.)wikipedia.org/wiki/(?P<slug>.+)$'
)


def get_uri(uri):
    normalized_uri = uri.lstrip(DBP_GLOBAL_PREFIX)
    if normalized_uri.startswith(DBP_GLOBAL_MARKER):
        normalized_uri = normalized_uri[len(DBP_GLOBAL_MARKER):].encode('utf8')
    else:
        uri = unquote(uri).replace(' ', '_').replace('"', '%22')
        if 'dbpedia.org' in uri:
            uri = uri.replace('dbpedia.org/page/', 'dbpedia.org/resource/')
        else:
            # todo: assigment expression candidate
            wiki_match = wiki_article_re.match(uri)
            if wiki_match:
                locale = wiki_match.group('locale').replace('en.', '')
                uri = f"http://{locale}dbpedia.org/resource/{wiki_match.group('slug')}"

        normalized_uri = db.get(uri.encode('utf8'))
        if not normalized_uri:
            raise UriNotFound()

    value_bytes = db.get(normalized_uri)
    if not value_bytes:
        raise UriNotFound()
    elif not is_cluster_membership(value_bytes):
        normalized_uri = value_bytes
        value_bytes = db.get(value_bytes)

    singletons, local_ids = sorted_cluster(value_bytes)

    return {
        'global': f"{DBP_GLOBAL_PREFIX}{DBP_GLOBAL_MARKER}{normalized_uri.decode('utf8')}",
        'locals': local_ids,
        'cluster': singletons,
    }
