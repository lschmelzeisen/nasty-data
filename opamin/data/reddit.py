from datetime import datetime
from pprint import pformat
from typing import Any, Dict

from elasticsearch_dsl import Document, Index, MetaField, analyzer, field, \
    token_filter, tokenizer


# === Documentation of design decision
#
# - The Reddit Pushshift dataset contains Reddit links and comments from a large
#   time-span in which certain fields were added/deleted to the format multiple
#   times. Out dataformat attempts to normalize what fields are present and
#   what values they might contain. Specifically, fields which are not present
#   for the majority of entries are excluded and there might be some bias in
#   field selection to our intended use case: extracting arguments from Reddit
#   texts.
# - Field/class names were chosen to match the names in the original Reddit JSON
#   format. In case a field only originates from its Pushshift-nature the
#   Pushshift name was used. The goal was to still be aware of all fields in the
#   dataset. This is why we track what fields where accessed (in
#   load_pushshift_json) and throw an error if we encounter an unexpected field.
# - Most fields in the Pushshift dataset can be not present or contain empty
#   values, we try to substitute sensible defaults or make those fields
#   optional. There is one error in the dataset we chose not to recover from,
#   that is entries where the subreddit field is missing or empty (about 0.01%
#   of posts). We chose to ignore these entries. Additionally, we ignore
#   posts on reddit marked as promoted content, i.e. bought ads, because they
#   are extremely rare, contain a load of additional fields that other entries
#   don't have, and are arguably not part of the organic reddit community.
# - We only use field.Text for the title, selftext, and body string-fields
#   because those are probably the only fields we want to search on. Other
#   fields like author, subreddit, etc. are field.Keyword because we would only
#   be interested in performing exact-match queries. We never use the
#   "store"-attribute on text-Fields and don't add a keyword multi-field to them
#   because we don't anticipate the need to do exact-match queries on the
#   text-Fields and because the original value is already stored in the
#   _source-meta-field. Disabling _source and enabling "store" did not yield a
#   very noticeable change in opamin size but severely reduces stuff we can do
#   with the opamin. For the storage space / functionality trade-off again, we
#   set index_options='offsets' and don't enable 'index_phrases' (see experiment
#   below).
# - field.Integer was used for all numeric fields because it seemed large enough
#   to contain all actually occurring values, and for almost all fields there
#   are some entries in the dataset for which the next smaller type (short)
#   would be too small (besides gilded but we kept it Integer for consistency).
# - Post IDs are stored as Elasticsearch IDs so that we don't need to keep track
#   of two different IDs in code that uses this. One direct consequence of this
#   is, because the Pushshift dataset contains posts with the same IDs multiple
#   times in rare cases, that we can only store one version of each post with
#   the same ID.
# - For analyzers we were again guided by our intended use case of extracting
#   arguments: recall is much more important for us then precision and we
#   basically only care about English text. For this reason we perform
#   asciifolding and add a second analyzer that performs English stemming.
#   Additionally, since Reddit texts may contain URLs or Email addresses we used
#   the uax_url_email tokenizer instead of the standard one. Last, because we
#   don't anticipate building an actual user facing UI for our copy of the
#   Reddit dataset, we didn't pay any attention to stuff like autocompletion.
#   One note for the future: adding more analyzers does not seem to require that
#   much more storage space, because only the inverted opamin is enlarged and
#   analyzer values are not stored per entry.
# - Since the Reddit dataset is very large, we did enable "best_compression",
#   i.e. DEFLATE, for the opamin instead of the default, i.e. LZ4. While this
#   should increase access times a bit it considerably decreases storage space
#   requirements, and we don't anticipate frequent access anyhow.
#
# === Experiment on required storage space for different opamin configurations
#
# The following was done with a small sample of reddit posts (total 32158, 36mb
# uncompressed): the first 100 posts of each of 324 different Pushshift archives
# files from 2005-12 until 2019-02. We did note track down opamin times because
# this experiment was performed over a network connection and the random delay
# added by the network was far larger than any change in opamin times.
#
# +--------------------------+---------------+----------------+--------+
# | index_options            |               | compression    |        |
# +------+-------+-----+-----+ index_phrases +---------+------+ space  |
# | docs | freqs | pos | off |               | default | best |        |
# +------+-------+-----+-----+---------------+---------+------+--------+
# | x    |       |     |     |               | x       |      |        |
# |      |       |     |     |               |         |      | 18.2mb |
# +------+-------+-----+-----+---------------+---------+------+--------+
# |      | x     |     |     |               | x       |      | 18.5mb |
# +------+-------+-----+-----+---------------+---------+------+--------+
# |      |       | x   |     |               | x       |      | 19.7mb |
# +------+-------+-----+-----+---------------+---------+------+--------+
# |      |       |     | x   |               | x       |      | 21.4mb |
# +------+-------+-----+-----+---------------+---------+------+--------+
# |      |       | x   |     | x             | x       |      | 29.4mb |
# +------+-------+-----+-----+---------------+---------+------+--------+
# |      |       |     | x   | x             | x       |      | 33.8mb |
# +------+-------+-----+-----+---------------+---------+------+--------+
# | x    |       |     |     |               |         | x    | 15.2mb |
# +------+-------+-----+-----+---------------+---------+------+--------+
# |      | x     |     |     |               |         | x    | 15.5mb |
# +------+-------+-----+-----+---------------+---------+------+--------+
# |      |       | x   |     |               |         | x    | 16.7mb |
# +------+-------+-----+-----+---------------+---------+------+--------+
# |      |       |     | x   |               |         | x    | 18.6mb |
# +------+-------+-----+-----+---------------+---------+------+--------+
# |      |       | x   |     | x             |         | x    | 26.7mb |
# +------+-------+-----+-----+---------------+---------+------+--------+
# |      |       |     | x   | x             |         | x    | 30.9mb |
# +------+-------+-----+-----+---------------+---------+------+--------+
#
# With index_options="offsets", index_phrases=False, codec="best_compression":
# +--------------------+--------------+--------+
# | _source            | store        |        |
# +---------+----------+------+-------+ space  |
# | enabled | disabled | True | False |        |
# +---------+----------+------+-------+--------+
# | x       |          | x    |       | 19.0mb |
# +---------+----------+------+-------+--------+
# |         | x        | x    |       | 19.1mb |
# +---------+----------+------+-------+--------+
# | x       |          |      | x     | 18.5mb |
# +---------+----------+------+-------+--------+
# |         | x        |      | x     | 18.5mb |
# +---------+----------+------+-------+--------+
#
# === Helpful links
#
# - Field datatypes are documented under:
#   https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
# - Arguments for field datatypes are documented under:
#   https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html

# TODO: normalize/remove the markdown from Reddit posts?


class RedditPost(Document):
    archived = field.Boolean(required=True)
    author = field.Keyword(required=True)
    author_cakeday = field.Boolean(required=True)
    author_flair_text = field.Keyword()
    created = field.Date(required=True)
    # "moderator" or "admin" (maybe other values?).
    distinguished = field.Keyword()
    edited = field.Date()
    gilded = field.Integer(required=True)
    retrieved_on = field.Date()
    # "markdown" or "richtext".
    rte_mode = field.Keyword()
    score = field.Integer(required=True)
    stickied = field.Boolean(required=True)
    subreddit = field.Keyword(required=True)
    subreddit_id = field.Keyword(required=True)

    class Meta:
        # Disable dynamic addition of fields:
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic.html
        dynamic = MetaField('strict')

    class LoadingError(Exception):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    class IncompleteDataError(LoadingError):
        def __init__(self, msg):
            super().__init__(msg)

    class PromotedContentError(LoadingError):
        def __init__(self):
            super().__init__('Promoted content, ignoring.')

    class UnhandledAttributeError(LoadingError):
        def __init__(self, obj: Dict[str, Any]):
            super().__init__('Unhandled attribute(s) in post JSON: {:s}.'
                             .format(', '.join(sorted(obj.keys()))))

    @property
    def permalink(self):
        raise NotImplementedError()

    def __str__(self):
        d = self.to_dict()
        d['_id'] = self.meta.id
        d['permalink'] = self.permalink
        return pformat(d, indent=2)

    @classmethod
    def load_pushshift_json(cls, obj: Dict[str, Any]) -> 'RedditPost':
        if 'subreddit' not in obj:
            raise cls.IncompleteDataError(
                'Missing subreddit name in post JSON, which is required.')
        if 'promoted' in obj:
            raise cls.PromotedContentError()

        id = obj.pop('id')
        assert id
        obj.pop('name', None)

        archived = bool(obj.pop('archived', False))

        author = obj.pop('author')
        assert author
        obj.pop('author_created_utc', None)
        obj.pop('author_fullname', None)
        obj.pop('author_id', None)
        obj.pop('author_patreon_flair', None)

        author_cakeday = bool(obj.pop('author_cakeday', False))

        author_flair_text = obj.pop('author_flair_text')
        obj.pop('author_flair_background_color', None)
        obj.pop('author_flair_css_class', None)
        obj.pop('author_flair_richtext', None)
        obj.pop('author_flair_template_id', None)
        obj.pop('author_flair_text_color', None)
        obj.pop('author_flair_type', None)

        created = datetime.utcfromtimestamp(int(obj.pop('created_utc')))
        obj.pop('created', None)

        distinguished = obj.pop('distinguished', None)

        edited = obj.pop('edited') or None  # "or None" in case of empty string.
        if edited:
            edited = datetime.utcfromtimestamp(int(edited))

        gilded = int(obj.pop('gilded', 0))

        retrieved_on = None
        if 'retrieved_on' in obj:
            retrieved_on = datetime.utcfromtimestamp(
                int(obj.pop('retrieved_on')))

        rte_mode = obj.pop('rte_mode', None)

        score = int(obj.pop('score'))
        obj.pop('ups', None)
        obj.pop('downs', None)
        obj.pop('score_hidden', None)

        stickied = bool(obj.pop('stickied', False))

        subreddit = obj.pop('subreddit')
        assert subreddit
        subreddit_id = obj.pop('subreddit_id')
        assert subreddit_id
        obj.pop('subreddit_name_prefixed', None)
        obj.pop('subreddit_type', None)
        obj.pop('subreddit_subscribers', None)
        obj.pop('approved', None)
        obj.pop('approved_at_utc', None)
        obj.pop('approved_by', None)
        obj.pop('banned_at_utc', None)
        obj.pop('banned_by', None)
        obj.pop('ban_note', None)
        obj.pop('can_gild', None)
        obj.pop('can_mod_post', None)
        obj.pop('gildings', None)
        obj.pop('likes', None)
        obj.pop('no_follow', None)
        obj.pop('quarantine', None)
        obj.pop('quarantined', None)
        obj.pop('permalink', None)
        obj.pop('post_hint', None)
        obj.pop('preview', None)
        obj.pop('removal_reason', None)
        obj.pop('saved', None)
        obj.pop('send_replies', None)

        construction_kwargs = {
            '_id': id,
            'archived': archived,
            'author': author,
            'author_cakeday': author_cakeday,
            'author_flair_text': author_flair_text,
            'created': created,
            'distinguished': distinguished,
            'edited': edited,
            'gilded': gilded,
            'retrieved_on': retrieved_on,
            'rte_mode': rte_mode,
            'score': score,
            'stickied': stickied,
            'subreddit': subreddit,
            'subreddit_id': subreddit_id,
        }

        if 'title' in obj:
            result = RedditLink.load_pushshift_json_overwrite(
                obj, construction_kwargs)
        elif 'body' in obj:
            result = RedditComment.load_pushshift_json_overwrite(
                obj, construction_kwargs)
        else:
            raise cls.IncompleteDataError(
                'Could not determine whether given post is link or comment '
                '(both "title" and ''"body" attribute are absent).')

        if obj:
            raise cls.UnhandledAttributeError(obj)
        return result

    @classmethod
    def load_pushshift_json_overwrite(cls,
                                      obj: Dict[str, Any],
                                      post_kwargs: Dict[str, Any]) \
            -> 'RedditPost':
        raise NotImplementedError


class RedditLink(RedditPost):
    # Link ID of crosspost parent.
    crosspost_parent = field.Keyword()
    # Domain of URL or "self.<subreddit>".
    domain = field.Keyword(required=True)
    is_self = field.Boolean(required=True)
    link_flair_text = field.Keyword()
    locked = field.Boolean(required=True)
    # JSON object that is not further analyzed.
    media = field.Object(enabled=False)
    # JSON object that is not further analyzed.
    media_embed = field.Object(enabled=False)
    num_comments = field.Integer(required=True)
    over_18 = field.Boolean(required=True)
    selftext = field.Text(required=True,
                          index_options='offsets',
                          analyzer='standard_uax_url_email',
                          fields={'english_analyzed': field.Text(
                              analyzer='english_uax_url_email')})
    spoiler = field.Boolean(required=True)
    # A URL, "default", "self", or "nsfw" (maybe other values?).
    thumbnail = field.Keyword(required=True)
    title = field.Text(required=True,
                       index_options='offsets',
                       analyzer='standard_uax_url_email',
                       fields={'english_analyzed': field.Text(
                           analyzer='english_uax_url_email')})
    url = field.Keyword(required=True)

    @property
    def permalink(self):
        return 'https://reddit.com/r/{}/comments/{}/'.format(
            self.subreddit, self.meta.id[3:])

    @classmethod
    def load_pushshift_json_overwrite(cls,
                                      obj: Dict[str, Any],
                                      construction_kwargs: Dict[str, Any]) \
            -> 'RedditLink':
        crosspost_parent = obj.pop('crosspost_parent', None)
        obj.pop('crosspost_parent_list', None)

        domain = obj.pop('domain')
        is_self = bool(obj.pop('is_self'))

        link_flair_text = obj.pop('link_flair_text', None)
        obj.pop('link_flair_background_color', None)
        obj.pop('link_flair_css_class', None)
        obj.pop('link_flair_richtext', None)
        obj.pop('link_flair_template_id', None)
        obj.pop('link_flair_text_color', None)
        obj.pop('link_flair_type', None)

        locked = bool(obj.pop('locked', False))

        media = obj.pop('media', None)
        media_embed = obj.pop('media_embed', None)
        obj.pop('media_metadata', None)
        obj.pop('media_only', None)

        num_comments = int(obj.pop('num_comments'))
        obj.pop('num_crossposts', None)
        obj.pop('num_reports', None)

        over_18 = bool(obj.pop('over_18'))

        selftext = obj.pop('selftext')
        selftext.replace('&lt;', '<')
        selftext.replace('&gt;', '>')
        selftext.replace('&amp;', '&')
        obj.pop('selftext_html', None)

        spoiler = bool(obj.pop('spoiler', False))

        # "or" because empty str is possible.
        thumbnail = obj.pop('thumbnail', '') or 'default'
        obj.pop('thumbnail_height', None)
        obj.pop('thumbnail_width', None)

        title = obj.pop('title')
        assert title
        url = obj.pop('url')
        assert url

        obj.pop('brand_safe', None)
        obj.pop('category', None)
        obj.pop('clicked', None)
        obj.pop('collections', None)
        obj.pop('content_categories', None)
        obj.pop('contest_mode', None)
        obj.pop('event_end', None)
        obj.pop('event_is_live', None)
        obj.pop('event_start', None)
        obj.pop('from', None)
        obj.pop('from_id', None)
        obj.pop('from_kind', None)
        obj.pop('hidden', None)
        obj.pop('hide_score', None)
        obj.pop('ignore_reports', None)
        obj.pop('is_crosspostable', None)
        obj.pop('is_meta', None)
        obj.pop('is_original_content', None)
        obj.pop('is_reddit_media_domain', None)
        obj.pop('is_robot_indexable', None)
        obj.pop('is_video', None)
        obj.pop('mod_reports', None)
        obj.pop('parent_whitelist_status', None)
        obj.pop('pinned', None)
        obj.pop('post_categories', None)
        obj.pop('previous_visits', None)
        obj.pop('pwls', None)
        obj.pop('removed', None)
        obj.pop('report_reasons', None)
        obj.pop('secure_media', None)
        obj.pop('secure_media_embed', None)
        obj.pop('spam', None)
        obj.pop('suggested_sort', None)
        obj.pop('user_reports', None)
        obj.pop('view_count', None)
        obj.pop('visited', None)
        obj.pop('whitelist_status', None)
        obj.pop('wls', None)

        construction_kwargs.update({
            '_id': 't3_' + construction_kwargs['_id'],
            'crosspost_parent': crosspost_parent,
            'domain': domain,
            'is_self': is_self,
            'link_flair_text': link_flair_text,
            'locked': locked,
            'media': media,
            'media_embed': media_embed,
            'num_comments': num_comments,
            'over_18': over_18,
            'selftext': selftext,
            'spoiler': spoiler,
            'thumbnail': thumbnail,
            'title': title,
            'url': url,
        })

        result = cls(**construction_kwargs)
        return result


class RedditComment(RedditPost):
    body = field.Text(required=True,
                      index_options='offsets',
                      analyzer='standard_uax_url_email',
                      fields={'english_analyzed': field.Text(
                          analyzer='english_uax_url_email')})
    link_id = field.Keyword(required=True)
    parent_id = field.Keyword(required=True)

    @property
    def permalink(self):
        return 'https://reddit.com/r/{}/comments/{}/_/{}/'.format(
            self.subreddit, self.link_id[3:], self.meta.id[3:])

    @classmethod
    def load_pushshift_json_overwrite(cls,
                                      obj: Dict[str, Any],
                                      construction_kwargs: Dict[str, Any]) \
            -> 'RedditComment':
        body = obj.pop('body')

        link_id = obj.pop('link_id')
        assert link_id

        parent_id = obj.pop('parent_id')
        assert parent_id

        obj.pop('collapsed', None)
        obj.pop('collapsed_reason', None)
        obj.pop('controversiality')
        obj.pop('is_submitter', None)

        construction_kwargs.update({
            '_id': 't1_' + construction_kwargs['_id'],
            'body': body,
            'link_id': link_id,
            'parent_id': parent_id
        })

        result = cls(**construction_kwargs)
        return result


def get_reddit_index():
    return Index('reddit')


def reset_reddit_index():
    reddit_index = get_reddit_index()

    reddit_index.delete(ignore=404)

    reddit_index.settings(
        number_of_shards=1, number_of_replicas=0, codec='best_compression')

    reddit_index.analyzer(analyzer(
        'standard_uax_url_email',
        tokenizer=tokenizer('uax_url_email'),
        filter=[token_filter('asciifolding'),
                token_filter('lowercase')]))
    reddit_index.analyzer(analyzer(
        'english_uax_url_email',
        tokenizer=tokenizer('uax_url_email'),
        filter=[token_filter('asciifolding'),
                token_filter('english_possessive_stemmer',
                             type='stemmer',
                             language='possessive_english'),
                token_filter('lowercase'),
                token_filter('english_stop',
                             type='stop',
                             stopwords='_english_'),
                token_filter('english_stemmer',
                             type='stemmer',
                             language='english')]))

    reddit_index.document(RedditLink)
    reddit_index.document(RedditComment)

    reddit_index.create()
