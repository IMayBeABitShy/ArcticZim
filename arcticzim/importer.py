"""
Import logic.

@var POST_COLUMNS: keys from dataset to add to post model
@type POST_COLUMNS: L{list} of L{str}
@var POST_FILTERS: a dictionary mapping post columns to transformer to apply
@type POST_FILTERS: L{dict} of L{str} -> callable
@var COMMENT_COLUMNS: keys from dataset to add to comment model
@type COMMENT_COLUMNS: L{list} of L{str}
@var COMMENT_FILTERS: a dictionary mapping comment columns to transformer to apply
@type COMMENT_FILTERS: L{dict} of L{str} -> callable
"""
import json
import datetime

from sqlalchemy import select

from .jsonl import process_jsonl
from .db.models import Post, User, Comment, Subreddit, ARCTICZIM_USERNAME
from .util import chunked


POST_COLUMNS = [c.key for c in Post.__table__.columns]
POST_FILTERS = {
    "poll_data": json.dumps,
    "media_metadata": json.dumps,
    "edited": lambda x: {False: 0, True: -1}.get(x, x),
}
COMMENT_COLUMNS = [c.key for c in Comment.__table__.columns]
COMMENT_FILTERS = {
    "edited": lambda x: {False: 0, True: -1}.get(x, x)
}


def prepare_db(session):
    """
    Prepare a database.

    This inserts some default objects into the database.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    """
    # first, check if the user already exists
    if session.execute(select(User).where(User.name == ARCTICZIM_USERNAME)).one_or_none() is None:
        user = User(
            name=ARCTICZIM_USERNAME,
            created=datetime.datetime.now(),
        )
        session.add(user)
        session.commit()


def import_posts(session, posts):
    """
    Create a database post from a dataset post and insert it into the db.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}

    @param posts: list of dictionaries from dataset containing post data
    @type posts: L{list} of L{dict}
    """
    # get or create authors
    authors = {}
    for d in posts:
        author_name = d["author"]
        if author_name not in authors:
            author = session.get(User, author_name)
            if author is None:
                author = User(
                    # id=d["author_fullname"],
                    name=author_name,
                    created=datetime.datetime.fromtimestamp(
                        d["author_created_utc"]
                        if d.get("author_created_utc", None) is not None
                        else 0
                    ),
                )
                session.add(author)
            authors[author_name] = author
    # get or create subreddit
    subreddits = {}
    for d in posts:
        subreddit_name = d["subreddit"]
        if subreddit_name not in subreddits:
            subreddit = session.get(Subreddit, subreddit_name)
            if subreddit is None:
                subreddit = Subreddit(
                    # id=d["author_fullname"],
                    name=subreddit_name,
                    subscribers=d.get("subreddit_subscribers", 0),
                )
                session.add(subreddit)
            if d.get("subreddit_subscribers", 0) > subreddit.subscribers:
                subreddit.subscribers = d.get("subreddit_subscribers", 0)
            subreddits[subreddit_name] = subreddit
    # create posts
    post_objs = []
    for d in posts:
        # create kwargs
        kwargs = {}
        author_name = d["author"]
        subreddit_name = d["subreddit"]
        kwargs["author_name"] = author_name
        kwargs["author"] = authors[author_name]
        kwargs["subreddit_name"] = subreddit_name
        kwargs["subreddit"] = subreddits[subreddit_name]
        # fill in remaining kwargs
        for key in d.keys():
            if key in POST_COLUMNS:
                value = d[key]
                if key == "edited" and isinstance(value, bool):
                    if value:
                        value = -1
                    else:
                        value = 0
                kwargs[key] = POST_FILTERS.get(key, lambda x: x)(value)
        post = Post(**kwargs)
        rc = post.create_root_comment()
        rc.post = post
        post_objs.append(post)
        # generate a root comment
        session.add(post)
    session.commit()


def import_posts_from_file(session, path, batch_size=1000):
    """
    Import posts from a arcticshift dataset, adding them to the session.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param path: path to file to read
    @type path: L{str}
    @param batch_size: how many posts to import at once
    @type batch_size: L{int}
    """
    n = 0
    for post_batch in chunked(process_jsonl(path, desc="Importing posts"), batch_size):
        import_posts(session, post_batch)
        n += len(post_batch)
    print("Imported {} posts.".format(n))


def import_comments(session, comments):
    """
    Create database comments from dataset comments and insert them into the db.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param comments: list of dictionaries from dataset containing comment data
    @type comments: L{list} of L{dict}
    @return: the amount of failed imports
    @rtype: L{int}
    """
    n_fails = 0
    # get or create authors
    authors = {}
    for d in comments:
        author_name = d["author"]
        if author_name not in authors:
            author = session.get(User, author_name)
            if author is None:
                author = User(
                    # id=d["author_fullname"],
                    name=author_name,
                    created=datetime.datetime.fromtimestamp(
                        d["author_created_utc"]
                        if d.get("author_created_utc", None) is not None
                        else 0
                    ),
                )
                session.add(author)
            authors[author_name] = author
    # get or create subreddits
    subreddits = {}
    for d in comments:
        subreddit_name = d["subreddit"]
        if subreddit_name not in subreddits:
            subreddit = session.get(Subreddit, subreddit_name)
            if subreddit is None:
                subreddit = Subreddit(
                    # id=d["author_fullname"],
                    name=subreddit_name,
                    subscribers=d.get("subreddit_subscribers", 0),
                )
                session.add(subreddit)
            if d.get("subreddit_subscribers", 0) > subreddit.subscribers:
                subreddit.subscribers = d.get("subreddit_subscribers", 0)
            subreddits[subreddit_name] = subreddit
    # get posts
    posts = {}
    for d in comments:
        post_name = d["link_id"]
        if post_name not in posts:
            post = session.execute(select(Post).where(Post.name == post_name)).one_or_none()
            if post is not None:
                posts[post_name] = post[0]
    # create comments:
    parents = {}
    for d in comments:
        kwargs = {}
        author_name = d["author"]
        author = authors[author_name]
        kwargs["author"] = author
        kwargs["author_name"] = author_name
        subreddit_name = d["subreddit"]
        subreddit = subreddits[subreddit_name]
        kwargs["subreddit"] = subreddit
        kwargs["subreddit_name"] = subreddit_name
        post = posts.get(d["link_id"])
        if post is None:
            n_fails += 1
            continue
        kwargs["post"] = post
        parent_id = d["parent_id"]
        if parent_id not in parents:
            parent = session.execute(select(Comment).where(Comment.name == parent_id)).one_or_none()
            if parent is None:
                # can't insert comment before parent
                n_fails += 1
                continue
            parent = parent[0]
            parents[parent_id] = parent
        else:
            parent = parents[parent_id]
        kwargs["parent"] = parent
        # fill in remaining kwargs
        for key in d.keys():
            if key in COMMENT_COLUMNS:
                value = d[key]
                kwargs[key] = COMMENT_FILTERS.get(key, lambda x: x)(value)
        comment = Comment(**kwargs)
        session.add(comment)
        parents[comment.name] = comment
    session.commit()
    return n_fails


def import_comments_from_file(session, path, batch_size=1000):
    """
    Import comments from a arcticshift dataset, adding them to the session.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param path: path to file to read
    @type path: L{str}
    @param batch_size: how many comments to import at once
    @type batch_size: L{int}
    """
    n = 0
    n_fails = 0
    for comment_batch in chunked(process_jsonl(path, desc="Importing comments"), batch_size):
        cur_fails = import_comments(session, comment_batch)
        n += len(comment_batch) - cur_fails
        n_fails += cur_fails
    print("Imported {} comments, failed to import {} comments..".format(n, n_fails))
