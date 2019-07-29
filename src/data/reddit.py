from datetime import datetime
from pprint import pformat
from typing import Any, Dict, List, Optional


class RedditPostLoadingError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class IncompletePostError(RedditPostLoadingError):
    def __init__(self, msg):
        super().__init__(msg)


class PromotedContentError(RedditPostLoadingError):
    def __init__(self):
        super().__init__('Promoted content, ignoring.')


class UnhandledAttributeError(RedditPostLoadingError):
    def __init__(self, obj: Dict[str, Any]):
        super().__init__('Unhandled attribute(s) in post JSON: {:s}.'
                         .format(', '.join(sorted(obj.keys()))))


class RedditPost:
    def __init__(self,
                 id: str,
                 archived: bool,
                 author: str,
                 author_cakeday: bool,
                 author_flair_text: Optional[str],
                 created: datetime,
                 distinguished: Optional[str],
                 edited: Optional[datetime],
                 gilded: int,
                 retrieved_on: Optional[datetime],
                 rte_mode: str,
                 score: int,
                 stickied: bool,
                 subreddit: str,
                 subreddit_id: str):
        assert id
        assert archived is not None
        assert author
        assert author_cakeday is not None
        assert created
        assert gilded is not None
        assert score is not None
        assert stickied is not None
        assert subreddit
        assert subreddit_id

        self.id = id
        self.archived = archived
        self.author = author
        self.author_cakeday = author_cakeday
        self.author_flair_text = author_flair_text
        self.created = created
        self.distinguished = distinguished
        self.edited = edited
        self.gilded = gilded
        self.retrieved_on = retrieved_on
        self.rte_mode = rte_mode
        self.score = score
        self.stickied = stickied
        self.subreddit = subreddit
        self.subreddit_id = subreddit_id

    @property
    def permalink(self):
        raise NotImplementedError()

    def __str__(self):
        d = self.__dict__.copy()
        d['permalink'] = self.permalink
        return pformat(d, indent=2)

    @classmethod
    def load_json(cls, obj: Dict[str, Any]) -> 'RedditPost':
        if 'subreddit' not in obj:
            raise IncompletePostError('Missing subreddit name in post JSON, '
                                      'which is required.')
        if 'promoted' in obj:
            raise PromotedContentError()

        id = obj.pop('id')
        obj.pop('name', None)

        archived = bool(obj.pop('archived', False))

        author = obj.pop('author')
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

        distinguished = obj.pop('distinguished')

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
        subreddit_id = obj.pop('subreddit_id')
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

        base_args = [id, archived, author, author_cakeday, author_flair_text,
                     created, distinguished, edited, gilded, retrieved_on,
                     rte_mode, score, stickied, subreddit, subreddit_id]

        if 'title' in obj:
            result = RedditLink.load_json_helper(obj, base_args)
        elif 'body' in obj:
            result = RedditComment.load_json_helper(obj, base_args)
        else:
            raise IncompletePostError('Could not determine whether given post'
                                      'is link or comment (both "title" and '
                                      '"body" attribute are absent).')

        if obj:
            raise UnhandledAttributeError(obj)
        return result


class RedditLink(RedditPost):
    def __init__(self,
                 # RedditPost
                 id: str,
                 archived: bool,
                 author: str,
                 author_cakeday: bool,
                 author_flair_text: Optional[str],
                 created: datetime,
                 distinguished: Optional[str],
                 edited: Optional[datetime],
                 gilded: int,
                 retrieved_on: Optional[datetime],
                 rte_mode: str,
                 score: int,
                 stickied: bool,
                 subreddit: str,
                 subreddit_id: str,
                 # RedditLink
                 crosspost_parent: Optional[str],
                 domain: str,
                 is_self: bool,
                 link_flair_text: Optional[str],
                 locked: bool,
                 media,
                 media_embed,
                 num_comments,
                 over_18: bool,
                 selftext: str,
                 spoiler: bool,
                 thumbnail: str,
                 title: str,
                 url: str):
        super().__init__(id, archived, author, author_cakeday,
                         author_flair_text, created, distinguished, edited,
                         gilded, retrieved_on, rte_mode, score, stickied,
                         subreddit, subreddit_id)

        assert domain is not None
        assert is_self is not None
        assert locked is not None
        assert num_comments is not None
        assert over_18 is not None
        assert selftext is not None
        assert spoiler is not None
        assert thumbnail
        assert title
        assert url

        self.type = 'link'
        self.id = 't3_' + self.id

        self.crosspost_parent = crosspost_parent
        self.domain = domain
        self.is_self = is_self
        self.link_flair_text = link_flair_text
        self.locked = locked
        self.media = media
        self.media_embed = media_embed
        self.num_comments = num_comments
        self.over_18 = over_18
        self.selftext = selftext
        self.spoiler = spoiler
        self.thumbnail = thumbnail
        self.title = title
        self.url = url

    @property
    def permalink(self):
        return 'https://reddit.com/r/{}/comments/{}/'.format(
            self.subreddit, self.id[3:])

    @classmethod
    def load_json_helper(cls,
                         obj: Dict[str, Any],
                         base_args: List) -> 'RedditLink':
        crosspost_parent = obj.pop('crosspost_parent', None)
        obj.pop('crosspost_parent_list', None)

        domain = obj.pop('domain')
        is_self = bool(obj.pop('is_self'))

        link_flair_text = obj.pop('link_flair_text')
        obj.pop('link_flair_background_color', None)
        obj.pop('link_flair_css_class', None)
        obj.pop('link_flair_richtext', None)
        obj.pop('link_flair_template_id', None)
        obj.pop('link_flair_text_color', None)
        obj.pop('link_flair_type', None)

        locked = bool(obj.pop('locked', False))

        media = obj.pop('media')
        media_embed = obj.pop('media_embed')
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
        url = obj.pop('url')

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

        result = cls(*base_args, crosspost_parent, domain, is_self,
                     link_flair_text, locked, media, media_embed, num_comments,
                     over_18, selftext, spoiler, thumbnail, title, url)
        return result


class RedditComment(RedditPost):
    def __init__(self,
                 # RedditPost
                 id: str,
                 archived: bool,
                 author: str,
                 author_cakeday: bool,
                 author_flair_text: Optional[str],
                 created: datetime,
                 distinguished: Optional[str],
                 edited: Optional[datetime],
                 gilded: int,
                 retrieved_on: Optional[datetime],
                 rte_mode: str,
                 score: int,
                 stickied: bool,
                 subreddit: str,
                 subreddit_id: str,
                 # RedditComment
                 body: str,
                 link_id: str,
                 parent_id: str):
        super().__init__(id, archived, author, author_cakeday,
                         author_flair_text, created, distinguished, edited,
                         gilded, retrieved_on, rte_mode, score, stickied,
                         subreddit, subreddit_id)

        assert body is not None
        assert link_id
        assert parent_id

        self.type = 'comment'
        self.id = 't1_' + self.id

        self.body = body
        self.link_id = link_id
        self.parent_id = parent_id

    @property
    def permalink(self):
        return 'https://reddit.com/r/{}/comments/{}/_/{}/'.format(
            self.subreddit, self.link_id[3:], self.id[3:])

    @classmethod
    def load_json_helper(cls,
                         obj: Dict[str, Any],
                         base_args: List) -> 'RedditComment':
        body = obj.pop('body')
        link_id = obj.pop('link_id')
        parent_id = obj.pop('parent_id')

        obj.pop('collapsed', None)
        obj.pop('collapsed_reason', None)
        obj.pop('controversiality')
        obj.pop('is_submitter', None)

        result = cls(*base_args, body, link_id, parent_id)
        return result
