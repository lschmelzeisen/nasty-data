from pathlib import Path
from sys import exit

from elasticsearch import Elasticsearch

SECRETS_FOLDER = Path('secrets')
CA_CRT_PATH = SECRETS_FOLDER / 'ca.crt'
ELASTIC_IP_PATH = SECRETS_FOLDER / 'elastic-ip'
ELASTIC_PASSWORD_PATH = SECRETS_FOLDER / 'elastic-password'

if not CA_CRT_PATH.exists():
    print('Couldn\'t find CA-Certificate in in {}.'.format(CA_CRT_PATH))
    exit()

try:
    with ELASTIC_IP_PATH.open() as fin:
        ELASTIC_IP = fin.read()
except IOError:
    print('Couldn\'t access IP for Elasticsearch instance in {}.'
          .format(ELASTIC_IP_PATH))
    raise

try:
    with ELASTIC_PASSWORD_PATH.open() as fin:
        ELASTIC_PASSWORD = fin.read()
except IOError:
    print('Couldn\'t acess Password for Elastic user in {}.'
          .format(ELASTIC_PASSWORD_PATH))
    raise

es = Elasticsearch(
    [ELASTIC_IP],
    http_auth=('elastic', ELASTIC_PASSWORD),
    scheme='https',
    port=9200,
    use_ssl=True,
    verify_certs=True,
    ca_certs=CA_CRT_PATH,
    ssl_show_warn=True,
    ssl_assert_hostname=False,
)

print(es.cat.health())
