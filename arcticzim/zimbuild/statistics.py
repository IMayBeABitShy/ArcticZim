"""
This module handles the generation of statistics.
"""
from sqlalchemy import select, func, distinct

from ..db.models import Post, Comment


class PostListStatistics(object):
    """
    This class holds statistics about a list of posts.

    @ivar count: total amount of posts
    @type count: L{int}

    @ivar total_score: total combined score of all posts
    @type total_score: L{int}
    @ivar min_score: score of the lowest scored post
    @type min_score: L{int}
    @ivar max_score: score of the heighest scored post
    @type max_score: L{int}

    @ivar oldest_utc: creation time of the oldest post
    @type oldest_utc: L{int}
    @ivar newest_utc: creation time of the oldest post
    @type newest_utc: L{int}

    @ivar total_comments: total amount of comments across all posts
    @type total_comments: L{int}
    @ivar max_comments: lowest amount of comments in a post
    @type max_comments: L{int}
    @ivar min_comments: highest amount of comments in a post
    @type min_comments: L{int}

    @ivar num_posters: number of users who have submitted a post
    @type num_posters: L{int}
    @ivar num_commentors: number of users who have submitted a comment
    @type num_commentors: L{int}
    """
    def __init__(
        self,
        count,
        total_score,
        min_score,
        max_score,
        oldest_utc,
        newest_utc,
        total_comments,
        max_comments,
        min_comments,
        num_posters,
        num_commentors,
    ):
        """
        The default constructor.

        @param count: total amount of posts
        @type count: L{int}

        @param total_score: total combined score of all posts
        @type total_score: L{int}
        @param min_score: score of the lowest scored post
        @type min_score: L{int}
        @param max_score: score of the heighest scored post
        @type max_score: L{int}

        @param oldest_utc: creation time of the oldest post
        @type oldest_utc: L{int}
        @param newest_utc: creation time of the oldest post
        @type newest_utc: L{int}

        @param total_comments: total amount of comments across all posts
        @type total_comments: L{int}
        @param max_comments: lowest amount of comments in a post
        @type max_comments: L{int}
        @param min_comments: highest amount of comments in a post
        @type min_comments: L{int}

        @param num_posters: number of users who have submitted a post
        @type num_posters: L{int}
        @param num_commentors: number of users who have submitted a comment
        @type num_commentors: L{int}
        """
        self.count = count
        self.total_score = total_score
        self.min_score = min_score
        self.max_score = max_score
        self.oldest_utc = oldest_utc
        self.newest_utc = newest_utc
        self.total_comments = total_comments
        self.max_comments = max_comments
        self.min_comments = min_comments
        self.num_posters = num_posters
        self.num_commentors = num_commentors

    @property
    def average_score(self):
        """
        Calculate the average score of the posts.

        @return: the average score of the posts
        @rtype: L{float} or L{None}
        """
        if self.count == 0:
            return None
        return self.total_score / self.count

    @property
    def average_comments(self):
        """
        Calculate the average amount of comments of the posts.

        @return: the average amount of comments of the posts
        @rtype: L{float} or L{None}
        """
        if self.count == 0:
            return None
        return self.total_comments / self.count

    @property
    def average_number_of_posts_per_poster(self):
        """
        Calculate the average amount of posts made by users who have submitted at least one post.

        @return: the average amount of posts made by users who have submitted at least one post.
        @rtype: L{float}
        """
        if self.num_posters == 0:
            return None
        return self.count / self.num_posters

    @property
    def average_number_of_comments_per_commentor(self):
        """
        Calculate the average amount of comments made by users who have submitted at least one comment.

        @return: the average amount of comment made by users who have submitted at least one comment.
        @rtype: L{float}
        """
        if self.num_commentors == 0:
            return None
        return self.total_comments / self.num_commentors


def query_post_stats(session, post_filter, comment_filter):
    """
    Query the statistics.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param post_filter: filter to apply on posts
    @type post_filter: sqlalchemy filter condition
    @param comment_filter: filter to apply on comments
    @type comment_filter: sqlalchemy filter condition
    @return: the collected statistics
    @rtype: L{PostListStatistics}
    """
    post_stat_stmt = select(
        func.count(Post.uid).label("count"),
        func.sum(Post.score).label("total_score"),
        func.min(Post.score).label("min_score"),
        func.max(Post.score).label("max_score"),
        func.min(Post.created_utc).label("oldest_utc"),
        func.max(Post.created_utc).label("newest_utc"),
        func.count(distinct(Post.author_name)).label("num_posters"),
    ).where(
        post_filter
    )
    post_result = session.execute(post_stat_stmt).one()
    comment_group_subq = select(
        Comment.link_id,
        func.count(Comment.uid).label("count"),
    ).where(
        comment_filter
    ).group_by(
        Comment.link_id,
    ).subquery()
    comment_stat_stmt = select(
        func.sum(comment_group_subq.c.count).label("total_comments"),
        func.min(comment_group_subq.c.count).label("min_comments"),
        func.max(comment_group_subq.c.count).label("max_comments"),
    )
    comment_result = session.execute(comment_stat_stmt).one()
    commentor_stmt = select(
        func.count(distinct(Comment.author_name)).label("num_commentors"),
    )
    commentor_result = session.execute(commentor_stmt).one()
    stats = PostListStatistics(
        count=post_result.count,
        total_score=post_result.total_score,
        min_score=post_result.min_score,
        max_score=post_result.max_score,
        oldest_utc=post_result.oldest_utc,
        newest_utc=post_result.newest_utc,
        total_comments=(comment_result.total_comments if comment_result.total_comments is not None else 0),
        min_comments=comment_result.min_comments,
        max_comments=comment_result.max_comments,
        num_posters=post_result.num_posters,
        num_commentors=commentor_result.num_commentors,
    )
    return stats
