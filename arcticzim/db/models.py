"""
Database model definitions.

@var ARCTICZIM_USERNAME: a username used as author for some helper objects
@type ARCTICZIM_USERNAME: L{str}
"""
import datetime

from typing import List, Optional

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy import String, Unicode
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


ARCTICZIM_USERNAME= "_ArcticZim"


class Base(DeclarativeBase):
    """
    Sqlalchemy declarative base.
    """
    pass


class Subreddit(Base):
    """
    This model represents a subreddit in the database.
    """
    __tablename__ = "subreddit"
    name: Mapped[str] = mapped_column(Unicode(32), primary_key=True)
    subscribers: Mapped[int]

    posts: Mapped[List["Post"]] = relationship(
        back_populates="subreddit",
        cascade="all, delete-orphan",
    )
    comments: Mapped[List["Comment"]] = relationship(
        back_populates="subreddit",
        cascade="all, delete-orphan",
    )


class User(Base):
    """
    This model represents a user (aka author) in the database.
    """
    __tablename__ = "user"
    __table_args__ = (
        UniqueConstraint("name"),
    )

    # uid: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    # id: Mapped[str] = mapped_column(String(16))
    # name: Mapped[str] = mapped_column(String(32), index=True, unique=True)
    name: Mapped[str] = mapped_column(String(32), primary_key=True)
    created: Mapped[datetime.datetime]

    posts: Mapped[List["Post"]] = relationship(
        back_populates="author",
        cascade="all, delete-orphan",
        # primaryjoin="User.name == Post.author",
    )
    comments: Mapped[List["Comment"]] = relationship(
        back_populates="author",
        cascade="all, delete-orphan",
        # primaryjoin="User.name == Post.author",
    )


class Post(Base):
    """
    This model represents a post in the database.
    """
    __tablename__ = "post"

    __table_args__ = (
        UniqueConstraint("id"),
        UniqueConstraint("name"),
    )

    uid: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    id: Mapped[str] = mapped_column(String(8), index=True, unique=True)

    archived: Mapped[Optional[bool]]
    author_name: Mapped[str] = mapped_column(ForeignKey("user.name"), index=True)
    author_cakeday: Mapped[Optional[bool]]
    author_flair_background_color: Mapped[Optional[str]] = mapped_column(Unicode(8))
    author_flair_css_class: Mapped[Optional[str]] = mapped_column(Unicode(16))
    # author_flair_richtext: {'always_present': False, 'types': {<class 'list'>}, 'nullable': False, 'example': [], 'count': 55284, 'max_length': 0}
    author_flair_text: Mapped[Optional[str]] = mapped_column(Unicode(128))
    author_flair_text_color: Mapped[Optional[str]] = mapped_column(Unicode(16))
    author_flair_type: Mapped[Optional[str]] = mapped_column(Unicode(8))
    author_fullname: Mapped[Optional[str]] = mapped_column(Unicode(16))
    created_utc: Mapped[int]
    crosspost_parent: Mapped[Optional[str]] = mapped_column(String(16))
    distinguished: Mapped[Optional[str]] = mapped_column(String(16))
    domain: Mapped[Optional[str]] = mapped_column(Unicode(128))
    edited: Mapped[Optional[int]]
    # edited: Mapped[Optional[bool]]
    hide_score: Mapped[Optional[bool]]
    is_gallery: Mapped[Optional[bool]]
    is_meta: Mapped[Optional[bool]]
    is_original_content: Mapped[Optional[bool]]
    is_reddit_media_domain: Mapped[Optional[bool]]
    is_self: Mapped[bool]
    is_video: Mapped[Optional[bool]]
    link_flair_background_color: Mapped[Optional[str]] = mapped_column(Unicode(8))
    link_flair_css_class: Mapped[Optional[str]] = mapped_column(Unicode(16))
    # link_flair_richtext: {'always_present': False, 'types': {<class 'list'>}, 'nullable': False, 'example': [{'e': 'text', 't': 'Discussion'}], 'count': 60890, 'max_length': 1}
    link_flair_template_id: Mapped[Optional[str]] = mapped_column(Unicode(64))
    link_flair_text: Mapped[Optional[str]] = mapped_column(Unicode(128))
    link_flair_text_color: Mapped[Optional[str]] = mapped_column(Unicode(16))
    link_flair_type: Mapped[Optional[str]] = mapped_column(Unicode(8))
    locked: Mapped[Optional[bool]]
    media_metadata: Mapped[Optional[str]] = mapped_column(Unicode(4096))
    media_only: Mapped[Optional[bool]]
    name: Mapped[str] = mapped_column(String(16), index=True, unique=True)
    no_follow: Mapped[Optional[bool]]
    num_comments: Mapped[int]
    num_crossposts: Mapped[Optional[int]]
    num_reports: Mapped[Optional[int]]
    over_18: Mapped[bool]
    parent_whitelist_status: Mapped[Optional[str]] = mapped_column(Unicode(8))
    permalink: Mapped[str] = mapped_column(Unicode(128))
    pinned: Mapped[Optional[bool]]
    poll_data: Mapped[Optional[str]] = mapped_column(Unicode(4096))
    post_hint: Mapped[Optional[str]] = mapped_column(Unicode(16))
    previous_selftext: Mapped[Optional[str]] = mapped_column(Unicode(65536), deferred=True)
    quarantine: Mapped[Optional[bool]]
    removal_reason: Mapped[Optional[str]] = mapped_column(Unicode(8))
    removed_by_category: Mapped[Optional[str]] = mapped_column(Unicode(16))
    retrieved_on: Mapped[Optional[int]]
    retrieved_utc: Mapped[Optional[int]]
    rte_mode: Mapped[Optional[str]] = mapped_column(Unicode(8))
    score: Mapped[int]
    selftext: Mapped[str] = mapped_column(Unicode(65536), deferred=True)
    # selftext_html: Mapped[Optional[str]] = Unicode(4096)
    spoiler: Mapped[bool]
    stickied: Mapped[Optional[bool]]
    subreddit_name: Mapped[str] = mapped_column(ForeignKey("subreddit.name"), index=True)
    subreddit_id: Mapped[str] = mapped_column(String(12), index=True)
    subreddit_name_prefixed: Mapped[Optional[str]] = mapped_column(String(32))
    # subreddit_subscribers: Mapped[Optional[int]]
    # subreddit_type: Mapped[Optional[str]] = mapped_column(Unicode(8))
    thumbnail: Mapped[Optional[str]] = mapped_column(Unicode(256))
    thumbnail_height: Mapped[Optional[int]]
    thumbnail_width: Mapped[Optional[int]]
    title: Mapped[str] = mapped_column(Unicode(512), deferred=True)
    ups: Mapped[int]
    upvote_ratio: Mapped[int]
    url: Mapped[str] = mapped_column(Unicode(1024))
    url_overridden_by_dest: Mapped[Optional[str]] = mapped_column(Unicode(1024))

    author: Mapped["User"] = relationship(
        back_populates="posts",
        cascade="all",
        # primaryjoin="User.name == Post.author",
    )
    comments: Mapped[List["Comment"]] = relationship(
        back_populates="post",
        cascade="all,delete-orphan",
        # primaryjoin="User.name == Post.author",
    )
    root_comment: Mapped[Optional["Comment"]] = relationship(
        cascade="all",
        primaryjoin="Comment.id == foreign(Post.id)",
    )
    subreddit: Mapped["Subreddit"] = relationship(
        back_populates="posts",
        cascade="all",
    )

    def create_root_comment(self):
        """
        Create a placeholder root comment and return it.

        @return: the created root comment
        @rtype: L{Comment}
        """
        root_comment = Comment(
            author_name=ARCTICZIM_USERNAME,
            body="",
            controversiality=0,
            created_utc=self.created_utc,
            edited=0,
            gilded=0,
            id=self.id,
            parent_id=None,
            link_id=self.name,
            name=self.name,
            score=self.score,
            ups=0,
            permalink="",
            subreddit=self.subreddit,
        )
        # self.root_comment.post = self
        return root_comment

    @property
    def is_poll(self):
        """
        Check whether this post is a poll or not.

        @return: True if this post is a poll, False otherwise
        @rtype: L{bool}
        """
        return (self.poll_data is not None)

    @property
    def icon_name(self):
        """
        Return the name of the icon to use for this post type

        @return: the name of the icon to use
        @rtype: L{str}
        """
        if self.is_poll:
            return "poll"
        icons = {
            "rich:video": "video",
            "hosted:video": "video",
            "image": "img",
            "link": "link",
            "self": "text",
        }
        name = icons.get(self.post_hint, None)
        if name is None:
            if self.is_self:
                return "text"
            else:
                return "link"
        return name


class Comment(Base):
    """
    This model represents a comment in the database.
    """
    __tablename__ = "comment"
    __table_args__ = (
        UniqueConstraint("id"),
        UniqueConstraint("name"),
    )

    uid: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    # all_awardings: {'always_present': False, 'types': {<class 'list'>}, 'nullable': False, 'example': [{'award_sub_type': 'GLOBAL', 'award_type': 'global', 'coin_price': 500, 'coin_reward': 100, 'count': 1, 'days_of_drip_extension': 0, 'days_of_premium': 7, 'description': 'Gives the author a week of Reddit Premium, %{coin_symbol}100 Coins to do with as they please, and shows a Gold Award.', 'end_date': None, 'giver_coin_reward': None, 'icon_format': None, 'icon_height': 512, 'icon_url': 'https://www.redditstatic.com/gold/awards/icon/gold_512.png', 'icon_width': 512, 'id': 'gid_2', 'is_enabled': True, 'is_new': False, 'name': 'Gold', 'penny_donate': None, 'penny_price': None, 'resized_icons': [{'height': 16, 'url': 'https://www.redditstatic.com/gold/awards/icon/gold_16.png', 'width': 16}, {'height': 32, 'url': 'https://www.redditstatic.com/gold/awards/icon/gold_32.png', 'width': 32}, {'height': 48, 'url': 'https://www.redditstatic.com/gold/awards/icon/gold_48.png', 'width': 48}, {'height': 64, 'url': 'https://www.redditstatic.com/gold/awards/icon/gold_64.png', 'width': 64}, {'height': 128, 'url': 'https://www.redditstatic.com/gold/awards/icon/gold_128.png', 'width': 128}], 'start_date': None, 'subreddit_coin_reward': 0, 'subreddit_id': None}], 'count': 605752, 'max_length': 6}
    archived: Mapped[Optional[bool]]
    author_name: Mapped[str] = mapped_column(ForeignKey("user.name"), index=True)
    author_cakeday: Mapped[Optional[bool]]
    author_created_utc: Mapped[Optional[int]]
    author_flair_background_color: Mapped[Optional[str]] = mapped_column(Unicode(8))
    author_flair_css_class: Mapped[Optional[str]] = mapped_column(Unicode(16))
    # author_flair_richtext: {'always_present': False, 'types': {<class 'list'>}, 'nullable': False, 'example': [{'e': 'text', 't': 'SYSTEM'}], 'count': 607055, 'max_length': 1}
    author_flair_template_id: Mapped[Optional[str]] = mapped_column(Unicode(64))
    author_flair_text: Mapped[Optional[str]] = mapped_column(Unicode(32))
    author_flair_text_color: Mapped[Optional[str]] = mapped_column(Unicode(4))
    author_flair_type: Mapped[Optional[str]] = mapped_column(Unicode(8))
    author_fullname: Mapped[Optional[str]] = mapped_column(Unicode(16))
    author_is_blocked: Mapped[Optional[bool]]
    author_patreon_flair: Mapped[Optional[bool]]
    author_premium: Mapped[Optional[bool]]
    body: Mapped[str] = mapped_column(Unicode(16384), deferred=True)
    body_html: Mapped[Optional[str]] = mapped_column(Unicode(4096), deferred=True)
    body_sha1: Mapped[Optional[str]] = mapped_column(Unicode(64), deferred=True)
    can_gild: Mapped[Optional[bool]]
    can_mod_post: Mapped[Optional[bool]]
    collapsed: Mapped[Optional[bool]]
    collapsed_reason: Mapped[Optional[str]] = mapped_column(Unicode(32))
    collapsed_reason_code: Mapped[Optional[str]] = mapped_column(Unicode(16))
    controversiality: Mapped[int]
    created_utc: Mapped[int]
    distinguished: Mapped[Optional[str]] = mapped_column(Unicode(16))
    downs: Mapped[Optional[int]]
    edited: Mapped[int]
    # expression_asset_data: {'always_present': False, 'types': {<class 'dict'>}, 'nullable': False, 'example': {'avatar_exp|145108495|bravo': {'avatar': {'e': 'Image', 'm': 'image/png', 's': {'u': 'https://i.redd.it/snoovatar/avatars/nftv2_bmZ0X2VpcDE1NToxMzdfZWI5NTlhNzE1ZGZmZmU2ZjgyZjQ2MDU1MzM5ODJjNDg1OWNiMTRmZV8xNDk4NDM2MQ_rare_e90add21-f3d6-4bce-8c20-105673bacfd7.png', 'x': 380, 'y': 600}}, 'expression': [{'e': ' Image', 'l': 'FRONT', 'm': 'image/png', 'n': 'bravo', 's': {'u': 'https://i.redd.it/snoovatar/expressions/bravo_1x1_1x.png', 'x': 150, 'y': 150}}, {'e': ' Image', 'l': 'BACK', 'm': 'image/png', 'n': 'bravo', 's': {'u': 'https://i.redd.it/snoovatar/expressions/bravo_1x1_1x_bg.png', 'x': 150, 'y': 150}}, {'e': ' Image', 'l': 'FRONT', 'm': 'image/png', 'n': 'bravo', 's': {'u': 'https://i.redd.it/snoovatar/expressions/bravo_1x1_2x.png', 'x': 300, 'y': 300}}, {'e': ' Image', 'l': 'BACK', 'm': 'image/png', 'n': 'bravo', 's': {'u': 'https://i.redd.it/snoovatar/expressions/bravo_1x1_2x_bg.png', 'x': 300, 'y': 300}}, {'e': ' Image', 'l': 'FRONT', 'm': 'image/png', 'n': 'bravo', 's': {'u': 'https://i.redd.it/snoovatar/expressions/bravo_1x1_3x.png', 'x': 500, 'y': 500}}, {'e': ' Image', 'l': 'BACK', 'm': 'image/png', 'n': 'bravo', 's': {'u': 'https://i.redd.it/snoovatar/expressions/bravo_1x1_3x_bg.png', 'x': 500, 'y': 500}}], 'perspective': 'CROPPED', 'position': 'CENTER', 'size': 'SIZE_1_X_1'}}, 'count': 5}
    gilded: Mapped[int]
    # gildings: {'always_present': False, 'types': {<class 'dict'>}, 'nullable': False, 'example': {'gid_1': 0, 'gid_2': 0, 'gid_3': 0}, 'count': 621211}
    id: Mapped[str] = mapped_column(Unicode(8), unique=True, index=True)
    is_submitter: Mapped[Optional[bool]]
    link_id: Mapped[str] = mapped_column(ForeignKey("post.name"), index=True)
    locked: Mapped[Optional[bool]]
    name: Mapped[str] = mapped_column(Unicode(16), unique=True, index=True)
    nest_level: Mapped[Optional[int]]
    no_follow: Mapped[Optional[bool]]
    num_reports: Mapped[Optional[int]]
    parent_id: Mapped[Optional[str]] = mapped_column(ForeignKey("comment.name"), index=True)
    permalink: Mapped[str] = mapped_column(Unicode(128))
    quarantined: Mapped[Optional[bool]]
    removal_reason: Mapped[Optional[str]] = mapped_column(Unicode(8))
    replies: Mapped[Optional[str]] = mapped_column(Unicode(128))
    # report_reasons: {'always_present': False, 'types': {<class 'list'>, <class 'NoneType'>}, 'nullable': True, 'example': None, 'count': 239684, 'max_length': 0}
    retrieved_on: Mapped[Optional[int]]
    rte_mode: Mapped[Optional[str]] = mapped_column(Unicode(8))
    saved: Mapped[Optional[bool]]
    score: Mapped[int]
    score_hidden: Mapped[Optional[bool]]
    send_replies: Mapped[Optional[bool]]
    stickied: Mapped[Optional[bool]]
    subreddit_name: Mapped[str] = mapped_column(ForeignKey("subreddit.name"))
    total_awards_received: Mapped[Optional[int]]
    ups: Mapped[int]

    author: Mapped["User"] = relationship(
        back_populates="comments",
        cascade="all",
        # primaryjoin="User.name == Post.author",
    )
    post: Mapped["Post"] = relationship(
        back_populates="comments",
        cascade="all",
        # primaryjoin="User.name == Post.author",
    )
    children: Mapped[List["Comment"]] = relationship(
        back_populates="parent",
        cascade="all,delete-orphan",
    )
    parent: Mapped["Comment"] = relationship(
        back_populates="children",
        cascade="all",
        remote_side=[name],
    )
    subreddit: Mapped["Subreddit"] = relationship(
        back_populates="comments",
        cascade="all",
    )

    @property
    def distinguished_class(self):
        """
        Return a string that can be used to identify if this comment was made by someone important.

        @return: a string if the commenter is somehow remarkable or L{None}
        @rtype: L{str} or L{None}
        """
        if self.distinguished:
            return self.distinguished
        elif self.is_submitter:
            return "submmitter"
        else:
            return None


class MediaFile(Base):
    """
    This object tracks the status of media files.

    This class keeps track of both URLs and checksums. The first mediafile
    that downloads a file is considered the primary one.
    """
    __tablename__ = "mediafile"
    __table_args__ = (
        UniqueConstraint("url"),
    )
    uid: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(String(20248), unique=True, index=True)
    md5: Mapped[str] = mapped_column(String(32), index=True, nullable=True)
    mimetype: Mapped[str] = mapped_column(String(256), index=True, nullable=True)
    # extension: Mapped[str] = mapped_column(String(64))
    downloaded: Mapped[bool]
    size: Mapped[Optional[int]]
    primary_uid: Mapped[int] = mapped_column(ForeignKey("mediafile.uid"), nullable=True)

    primary: Mapped["MediaFile"] = relationship(
        back_populates="duplicates",
        cascade="all",
        remote_side=[uid],
    )
    duplicates: Mapped[List["MediaFile"]] = relationship(
        back_populates="primary",
        cascade="all,delete-orphan",
    )
