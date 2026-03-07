"""
This module contains functionality for automatically downloading post and comment data from arcticshift.
"""
import datetime
import time

import requests
import tqdm


def retrieve_posts(subreddit=None, author=None, after=None, before=None, sleep=0.1):
    """
    Retrieve posts from arctic shift.

    Either subreddit or author must be specified, but not both.

    @param subreddit: subreddit to download posts from
    @type subreddit: L{str} or L{None}
    @param author: author to download posts from
    @type author: L{str} or L{None}
    @param after: only retrieve posts after this timestamp
    @type after: L{int} or L{None}
    @param before: only retrieve posts before this timestamp
    @type before: L{int} or L{None}
    @param sleep: seconds to sleep between requests
    @type sleep: L{bool}
    @yields: the post data
    @ytype: L{dict}
    """
    params = {
        "sort": "asc",
        "limit": "auto",
    }
    if subreddit is not None:
        if author is not None:
            raise ValueError("Only one of author or subreddit may be specified")
        params["subreddit"] = subreddit
    elif author is not None:
        params["author"] = author
    else:
        raise ValueError("At least one of author or subreddit needs to be specified")
    if after is None:
        after = 0
    if before is not None:
        params["before"] = datetime.datetime.fromtimestamp(before).isoformat()

    # retrieve posts
    bar = tqdm.tqdm(desc="Retrieving posts", unit="posts")
    n_requests = 0
    n_posts = 0
    while True:
        params["after"] = datetime.datetime.fromtimestamp(after).isoformat()
        n_requests += 1
        r = requests.get(
            "https://arctic-shift.photon-reddit.com/api/posts/search",
            params=params,
        )
        r.raise_for_status()
        content = r.json()["data"]
        if not content:
            # end of data reached
            break
        for post in content:
            yield post
            bar.update(1)
        after = post["created_utc"] + 1
        bar.set_postfix({"Time": datetime.datetime.fromtimestamp(after).isoformat(), "requests": n_requests})
        time.sleep(sleep)


def retrieve_comments(subreddit=None, author=None, after=None, before=None, sleep=0.1):
    """
    Retrieve comments from arctic shift.

    Either subreddit or author must be specified, but not both.

    @param subreddit: subreddit to download posts from
    @type subreddit: L{str} or L{None}
    @param author: author to download posts from
    @type author: L{str} or L{None}
    @param after: only retrieve posts after this timestamp
    @type after: L{int} or L{None}
    @param before: only retrieve posts before this timestamp
    @type before: L{int} or L{None}
    @param sleep: seconds to sleep between requests
    @type sleep: L{bool}
    @yields: the comment data
    @ytype: L{dict}
    """
    params = {
        "sort": "asc",
        "limit": "auto",
    }
    if subreddit is not None:
        if author is not None:
            raise ValueError("Only one of author or subreddit may be specified")
        params["subreddit"] = subreddit
    elif author is not None:
        params["author"] = author
    else:
        raise ValueError("At least one of author or subreddit needs to be specified")
    if after is None:
        after = 0
    if before is not None:
        params["before"] = datetime.datetime.fromtimestamp(before).isoformat()

    # retrieve comments
    bar = tqdm.tqdm(desc="Retrieving comments", unit="comments")
    n_requests = 0
    n_comments = 0
    while True:
        params["after"] = datetime.datetime.fromtimestamp(after).isoformat()
        n_requests += 1
        r = requests.get(
            "https://arctic-shift.photon-reddit.com/api/comments/search",
            params=params,
        )
        r.raise_for_status()
        content = r.json()["data"]
        if not content:
            # end of data reached
            break
        for comment in content:
            yield comment
            bar.update(1)
        after = comment["created_utc"] + 1
        bar.set_postfix({"Time": datetime.datetime.fromtimestamp(after).isoformat(), "requests": n_requests})
        time.sleep(sleep)

