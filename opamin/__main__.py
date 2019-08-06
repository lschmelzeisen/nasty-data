import json
import sys
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from json import JSONDecodeError
from logging import getLogger
from pathlib import Path
from typing import List, Tuple

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch_dsl.connections import create_connection
from tqdm import tqdm

import opamin
import opamin.commands
from opamin.commands import *  # Must be wildcard import so subcommands work.
from opamin.data.reddit import RedditPost, get_reddit_index, \
    reset_reddit_index
from opamin.util.compression import DecompressingTextIOWrapper
from opamin.util.logging import setup_logging

SECRETS_FOLDER = Path('secrets')
CA_CRT_PATH = SECRETS_FOLDER / 'ca.crt'
ELASTIC_IP_PATH = SECRETS_FOLDER / 'elastic-ip'
ELASTIC_PASSWORD_PATH = SECRETS_FOLDER / 'elastic-password'


def main(argv: List[str] = sys.argv[1:]):
    args, command = load_args(argv)

    setup_logging(args.log_level)
    logger = getLogger(opamin.__name__)
    logger.debug('Raw arguments: {}'.format(argv))
    logger.debug('Parsed arguments: {}'.format(vars(args)))
    logger.debug('Command class: {}.{}'.format(command.__module__,
                                               command.__name__))

    connection = connect_elasticsearch()
    reddit_index = get_reddit_index()
    reset_reddit_index()
    res = bulk(connection, (d.to_dict(include_meta=True, skip_empty=True)
                            for d in index_reddit()))


def load_args(argv: List[str]) -> Tuple[Namespace, Command]:
    argparser = ArgumentParser(prog='opamin', description='TODO')

    argparser.add_argument('-v', '--version', action='version',
                           version='%(prog)s development version')
    argparser.add_argument('--log-level', metavar='<level>', type=str,
                           choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                           default='INFO', dest='log_level',
                           help='set logging level (DEBUG, INFO, WARN, ERROR)')

    def add_subcommands(argparser, cls, depth=1):
        if len(cls.__subclasses__()):
            title = ('sub' * (depth - 1)) + 'command'
            subparsers = argparser.add_subparsers(title=title,
                                                  metavar='<{}>'.format(title))
            subparsers.required = True

            for subclass in cls.__subclasses__():
                subparser = subparsers.add_parser(
                    subclass.command,
                    aliases=subclass.aliases,
                    help=subclass.description,
                    description=subclass.description)
                subparser.set_defaults(command=subclass)
                subclass.config_argparser(subparser)
                add_subcommands(subparser, subclass, depth=depth + 1)

    add_subcommands(argparser, Command)

    args = argparser.parse_args(argv)

    return args, args.command


def connect_elasticsearch() -> Elasticsearch:
    logger = getLogger(opamin.__name__)

    if not CA_CRT_PATH.exists():
        logger.error('Could not find CA-Certificate in in '
                     '"{}".'.format(CA_CRT_PATH))
        exit()

    elastic_ip = None
    try:
        with ELASTIC_IP_PATH.open() as fin:
            elastic_ip = fin.read()
    except IOError:
        logger.exception('Could not access IP for Elasticsearch instance in '
                         '"{}".'.format(ELASTIC_IP_PATH))
        exit()

    elastic_password = None
    try:
        with ELASTIC_PASSWORD_PATH.open() as fin:
            elastic_password = fin.read()
    except IOError:
        logger.exception('Could not access Password for Elastic user in '
                         '"{}".'.format(ELASTIC_PASSWORD_PATH))
        exit()

    return create_connection(
        hosts=[elastic_ip],
        http_auth=('elastic', elastic_password),
        port=9200,

        # Use SSL
        scheme='https',
        use_ssl=True,
        ca_certs=CA_CRT_PATH,
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


def index_reddit() -> None:
    logger = getLogger(opamin.__name__)

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
                progress_bar.n = fin.tell()
                progress_bar.refresh()

                # For some reason, there is at least one line (specifically,
                # line 29876 in file RS_2011-01.bz2) that contains NUL
                # characters at the beginning of it, which we remove with the
                # following.
                line = line.lstrip('\x00')

                try:
                    obj = json.loads(line)

                    if '_comment' in obj:
                        continue

                    post = RedditPost.load_pushshift_json(obj)
                    counts['Success.'] += 1
                    yield post
                except (JSONDecodeError, RedditPost.LoadingError) as e:
                    error = str(e)
                    if isinstance(e, JSONDecodeError):
                        error = 'Could not decode JSON.'
                    counts[error] += 1

                    non_logging_errors = [RedditPost.IncompleteDataError,
                                          RedditPost.PromotedContentError]
                    if type(e) not in non_logging_errors:
                        logger.exception('{:s} From line {:d} in file "{}": '
                                         '{:s}'.format(error, line_no, file,
                                                       line.rstrip('\n')))

    logger.info('Report of loaded Reddit objects:')
    total = sum(counts.values())
    for error in sorted(counts.keys()):
        count = counts[error]
        logger.info('- {:s}: {:d} ({:.2%})'
                    .format(error[:-1], count, count / total))


if __name__ == '__main__':
    main()
