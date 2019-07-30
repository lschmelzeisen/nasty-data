import json
from collections import defaultdict
from json import JSONDecodeError
from pathlib import Path
from sys import exit, stderr

from elasticsearch import Elasticsearch
from tqdm import tqdm

from src.data.reddit import IncompletePostError, PromotedContentError, \
    RedditPost, RedditPostLoadingError
from src.util.compression import DecompressingTextIOWrapper

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

Elasticsearch()
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

files = [
    Path('programming_samples_small.jsonl'),
    # Path('programming_samples_large.jsonl'),
    # Path('RC_2007-05.bz2'),
    # Path('RS_2011-01.bz2'),
    # Path('RS_2015-01.zst'),
    # Path('RS_v2_2005-06.xz')
]
counts = defaultdict(int)
for file in files:
    with DecompressingTextIOWrapper(file, encoding='utf-8') as fin, \
            tqdm(desc=str(file), total=fin.size(),
                 unit='b', unit_scale=True) as progress_bar:
        for line_no, line in enumerate(fin):
            # For some reason, there is at least one line (specifically, line
            # 29876 in file RS_2011-01.bz2) that contains NUL characters at the
            # beginning of it, which we remove with the following.
            line = line.lstrip('\x00')

            try:
                obj = json.loads(line)

                if '_comment' in obj:
                    continue

                post = RedditPost.load_json(obj)
                counts['Success.'] += 1
            except (JSONDecodeError, RedditPostLoadingError) as e:
                error = str(e)
                if isinstance(e, JSONDecodeError):
                    error = 'Could not decode JSON.'
                counts[error] += 1

                non_logging_errors = [IncompletePostError, PromotedContentError]
                if type(e) not in non_logging_errors:
                    print('WARNING: {:s} From line {:d} in file "{:s}": {:s}'
                          .format(error, line_no, str(file), line.rstrip('\n')),
                          file=stderr)

            progress_bar.n = fin.tell()
            progress_bar.refresh()

print('Report of loaded Reddit objects:')
total = sum(counts.values())
for error in sorted(counts.keys()):
    count = counts[error]
    print('- {:s}: {:d} ({:.2%})'.format(error[:-1], count, count / total))
