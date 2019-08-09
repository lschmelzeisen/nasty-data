import sys
from logging import getLogger
from pathlib import Path
from typing import Dict

from elasticsearch import Elasticsearch
from elasticsearch_dsl.connections import create_connection

import opamin


def connect_elasticsearch(config: Dict) -> Elasticsearch:
    logger = getLogger(opamin.__name__)
    c = config['elasticsearch-secrets']  # Shortcut alias.

    if not Path(c['ca-crt-path']).exists():
        logger.error('Could not find CA-Certificate in '
                     '"{}".'.format(c['ca-crt-path']))
        sys.exit()

    logger.debug('Connecting to elasticsearch...')
    elasticsearch_connection = create_connection(
        hosts=[c['ip']],
        http_auth=(c['user'], c['password']),
        port=9200,

        # Use SSL
        scheme='https',
        use_ssl=True,
        ca_certs=c['ca-crt-path'],
        ssl_show_warn=True,
        ssl_assert_hostname=False,
        verify_certs=True,

        # Enable HTTP compression because we will probably insert a lot of large
        # documents and the documentation says it will help:
        # https://elasticsearch-py.readthedocs.io/en/master/#compression
        http_compress=True,

        # For development, so errors are seen comparatively fast.
        max_retries=2,
        timeout=3,
    )

    return elasticsearch_connection
