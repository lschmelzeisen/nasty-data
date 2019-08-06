import json
import sys
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from json import JSONDecodeError
from logging import getLogger
from pathlib import Path
from typing import Dict, Generator, List, Tuple

import toml
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch_dsl.connections import create_connection
from tqdm import tqdm

import opamin
import opamin.commands
from opamin.commands import Command
from opamin.data.reddit import RedditPost, get_reddit_index, \
    reset_reddit_index
from opamin.util.compression import DecompressingTextIOWrapper
from opamin.util.logging import setup_logging


def main(argv: List[str] = sys.argv[1:]):
    args, command = load_args(argv)

    setup_logging(args.log_level)
    logger = getLogger(opamin.__name__)
    logger.debug('Raw arguments: {}'.format(argv))
    logger.debug('Parsed arguments: {}'.format(vars(args)))
    logger.debug('Parsed command: {}.{}'.format(command.__module__,
                                                command.__name__))

    config = load_config()

    command(args, config).run()

    connection = connect_elasticsearch(config)
    reddit_index = get_reddit_index()
    reset_reddit_index()
    res = bulk(connection, (d.to_dict(include_meta=True, skip_empty=True)
                            for d in index_reddit()))


def load_args(argv: List[str]) -> Tuple[Namespace, Command.__class__]:
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


def load_config(path: Path = Path(__file__).parent.parent / 'config.toml') \
        -> Dict:
    logger = getLogger(opamin.__name__)

    if not path.exists():
        logger.error('Could not find config file in "{}". Make sure you copy '
                     'the example config file to this location and set your '
                     'personal settings/secrets.'.format(path))
        sys.exit()

    logger.debug('Loading config from "{}"...'.format(path))
    with path.open(encoding='UTF-8') as fin:
        config = toml.load(fin)

    def hide_secrets(value, hidden=False):
        if isinstance(value, dict):
            return {k: hide_secrets(v, hidden=(hidden or ('secret' in k)))
                    for k, v in value.items()}
        return '<hidden>' if hidden else value

    logger.debug('Loaded config:')
    for line in toml.dumps(hide_secrets(config)).splitlines():
        logger.debug('  ' + line)

    return config


def connect_elasticsearch(config: Dict) -> Elasticsearch:
    logger = getLogger(opamin.__name__)
    c = config['elasticsearch-secrets']  # Shortcut alias.

    if not Path(c['ca-crt-path']).exists():
        logger.error('Could not find CA-Certificate in '
                     '"{}".'.format(c['ca-crt-path']))
        sys.exit()

    return create_connection(
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


def index_reddit() -> Generator[RedditPost, None, None]:
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
