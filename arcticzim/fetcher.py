"""
This module handles the fetching of additional data like wiki pages.
"""
import time
import datetime

import requests
from sqlalchemy import select, func
from sqlalchemy.orm import undefer
import tqdm

from .db.models import Subreddit, Post, WikiPage, SubredditRule
from .importer import import_posts, import_comments
from .util import get_urls_from_string, parse_reddit_url


def reddit_reference_to_url(reference, to_root):
    """
    Generate a ZIM-internal URL for the reference.

    @param reference: reference to turn into a URL
    @type reference: L{dict}
    @param to_root: rootification prefix, just like in templates
    @type to_root: L{str}
    @return: the generated URL
    @rtype: L{str}
    """
    if reference["type"] == "subreddit":
        postfix = "/r/{}/".format(reference["subreddit"])
    elif reference["type"] == "post":
        postfix = "/r/{}/{}/".format(reference["subreddit"], reference["post"])
    elif reference["type"] == "comment":
        postfix = "/r/{}/{}/#comment-{}".format(reference["subreddit"], reference["post"], reference["comment"])
    else:
        raise ValueError("Unknown reference: {}".format(reference))
    return "//" + to_root + postfix


def get_reddit_references_from_post(post):
    """
    Return all reddit references contained in a post.

    @param post: post to get references from
    @type post: L{arcticzim.db.models.Post}
    @return: all references in the post, like L{parse_reddit_url}
    @rtype: L{list} of L{dict}
    """
    # get all URLs
    urls = [post.url]
    urls += get_urls_from_string(post.selftext)
    # find references
    references = [parse_reddit_url(url) for url in urls]
    references = [r for r in references if r is not None]
    # ignore references to this specific post
    references = [r for r in references if (r["type"] != "post") or (r["post"] != post.id)]
    return references


def get_reddit_references_from_text(s):
    """
    Return all reddit references contained in a text.

    @param s: text to get references from
    @type s: L{str}
    @return: all references in the post, like L{parse_reddit_url}
    @rtype: L{list} of L{dict}
    """
    # get all URLs
    urls = get_urls_from_string(s)
    # find references
    references = [parse_reddit_url(url) for url in urls]
    references = [r for r in references if r is not None]
    return references


def has_post_locally(session, postid):
    """
    Check if a post exists locally.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param postid: id of post to check for
    @type postid: L{str}
    """
    stmt = select(Post.id).where(Post.id == postid)
    post = session.execute(stmt).one_or_none()
    return (post is not None)


def has_subreddit_locally(session, subreddit_name):
    """
    Check if a subreddit exists locally.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param subreddit_name: name of subreddit to check for
    @type subreddit_name: L{str}
    """
    stmt = select(Subreddit.name).where(Subreddit.name == subreddit_name)
    subreddit = session.execute(stmt).one_or_none()
    return (subreddit is not None)


def fetch_all_references(session, sleep=1):
    """
    Fetch all referenced objects, be it from crossposts or wiki references.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param sleep: how many seconds to wait between requests
    @type sleep: L{str}
    @return: whether anything new has been fetched
    @rtype: L{bool}
    """
    did_fetch_something_new = False
    # posts
    n = session.execute(select(func.count(Post.uid))).one()[0]
    stmt = select(Post).options(
        undefer(Post.url),
        undefer(Post.selftext),
    ).execution_options(
        yield_per=1000,
    )
    for post in tqdm.tqdm(session.execute(stmt).scalars(), desc="Searching in posts and fetching results...", total=n, unit="posts"):
        references = get_reddit_references_from_post(
            post,
        )
        references = [
            e
            for e in references
            if (e["type"] in ("post", "comment")) and not has_post_locally(session=session, postid=e["post"])
        ]
        if not references:
            continue
        for reference in tqdm.tqdm(references, desc="Fetching referenced objects for {}".format(post.id), total=len(references), unit="obj"):
            if reference["type"] in ("post", "comment"):
                if not has_post_locally(session, postid=reference["post"]):
                    # we need a second check in case the same post referenced the same post multiple times
                    fetch_post(session=session, postid=reference["post"], sleep=sleep)
                    did_fetch_something_new = True
            time.sleep(sleep)
    # wikipages
    n = session.execute(select(func.count(WikiPage.uid))).one()[0]
    stmt = select(WikiPage).options(
        undefer(WikiPage.content),
    ).execution_options(
        yield_per=1000,
    )
    for wikipage in tqdm.tqdm(session.execute(stmt).scalars(), desc="Searching in wikipages and fetching results...", total=n, unit="posts"):
        references = get_reddit_references_from_text(
            wikipage.content,
        )
        references = [
            e
            for e in references
            if (e["type"] in ("post", "comment")) and not has_post_locally(session=session, postid=e["post"])
        ]
        if not references:
            continue
        for reference in tqdm.tqdm(references, desc="Fetching referenced objects for {}/wiki/{}".format(wikipage.subreddit_name, wikipage.basepath), total=len(references), unit="obj"):
            if reference["type"] in ("post", "comment"):
                if not has_post_locally(session, postid=reference["post"]):
                    # we need a second check in case the same post referenced the same post multiple times
                    fetch_post(session=session, postid=reference["post"], sleep=sleep)
                    did_fetch_something_new = True
            time.sleep(sleep)
    return did_fetch_something_new


def fetch_post(session, postid, sleep=1):
    """
    Fetch a post from Arctic Shift and insert it into the databse.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param postid: id of post to check for
    @type postid: L{str}
    @param sleep: time to sleep between each request, in seconds
    @type sleep: L{int} or L{float}
    """
    # fetch the post
    url = "https://arctic-shift.photon-reddit.com/api/posts/ids?ids={}".format(postid)
    r = requests.get(url)
    r.raise_for_status()
    json = r.json()
    if ("data" not in json) or (len(json["data"]) == 0):
        return
    import_posts(session, json["data"])
    # fetch the comments
    start = int(json["data"][0].get("created_utc", 0))
    end = int(time.time())
    timestamp = start
    if timestamp > 0:
        timestamp - 100  # just to be sure we fetch all data
    raw_comments = []
    with tqdm.tqdm(
        desc="Fetching comments for {}".format(postid),
        total=(end - start),
        unit="seconds",
    ) as bar:
        while True:
            time.sleep(sleep)
            comment_url = "https://arctic-shift.photon-reddit.com/api/comments/search?link_id={}&sort=asc&after={}&limit=auto".format(
                postid,
                datetime.datetime.fromtimestamp(timestamp).isoformat(),
            )
            r = requests.get(comment_url)
            r.raise_for_status()
            json = r.json()
            if ("data" not in json) or (len(json["data"]) == 0):
                break
            raw_comments += json["data"]
            bar.set_postfix({"comments": len(raw_comments)})
            new_timestamp = timestamp
            for comment in json["data"]:
                if comment.get("created_utc", 0) > new_timestamp:
                    new_timestamp = int(comment["created_utc"])
            if new_timestamp == timestamp:
                new_timestamp += 1
            timestamp = new_timestamp
            bar.n = int(timestamp - start)
        bar.n = int(end - start)
        import_comments(session, raw_comments)


def get_wikipages_for_subreddit(subreddit_name):
    """
    Fetch wiki pages for a specific subreddit and return them.

    @param subreddit_name: name of subreddit to fetch wikipages for
    @type subreddit_name: L{str}
    @return: a list of wiki pages
    @rtype: L{list} of L{arcticzim.db.models.WikiPage}
    """
    url = "https://arctic-shift.photon-reddit.com/api/subreddits/wikis?subreddit={}&limit=100".format(subreddit_name)
    r = requests.get(url)
    r.raise_for_status()
    rawpages = r.json()["data"]
    pages = []
    for rawpage in rawpages:
        page = WikiPage(
            subreddit_name=subreddit_name,
            path=rawpage["path"],
            content=rawpage.get("content", "[page empty or content not available]"),
            revision_date=rawpage.get("revision_date", 0),
            revision_author=rawpage.get("revision_author", None) or "[Author unknown]",
            revision_reason=rawpage.get("revision_reason", None),
            retrieved_on=rawpage.get("retrieved_on", 0),
        )
        pages.append(page)
    return pages


def fetch_wiki_for_subreddit(session, subreddit_name):
    """
    Fetch wiki pages for a specific subreddit and insert them into the database.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param subreddit_name: name of subreddit to fetch wikipages for
    @type subreddit_name: L{str}
    @return: whether anything new has been fetched
    @rtype: L{bool}
    """
    did_fetch_something = False
    pages = get_wikipages_for_subreddit(subreddit_name)
    for page in pages:
        session.merge(page)
        did_fetch_something = True
    session.commit()
    return did_fetch_something


def fetch_all_wikis(session, sleep=1):
    """
    Fetch all wiki pages and insert them into the database.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param sleep: how many seconds to wait between each request
    @type sleep: L{int}
    @return: whether anything new has been fetched
    @rtype: L{bool}
    """
    did_fetch_something_new = False
    stmt = select(Subreddit).where(~Subreddit.wikipages.any())
    for subreddit in session.execute(stmt).scalars():
        print("Fetching wikipages for: {}".format(subreddit.name))
        if fetch_wiki_for_subreddit(session, subreddit.name):
            did_fetch_something_new = True
        time.sleep(sleep)
    return did_fetch_something_new


def get_rules_for_subreddit(subreddit_name):
    """
    Fetch the rules for a specific subreddit and return them.

    @param subreddit_name: name of subreddit to fetch rules for
    @type subreddit_name: L{str}
    @return: a list of subreddit rules
    @rtype: L{list} of L{arcticzim.db.models.SubredditRule}
    """
    url = "https://arctic-shift.photon-reddit.com/api/subreddits/rules?subreddits={}".format(subreddit_name)
    r = requests.get(url)
    r.raise_for_status()
    all_rawrules = r.json()["data"]
    if not all_rawrules:
        # no rules for subreddits (found)
        return []
    rawrules = all_rawrules[0]["rules"]
    rules = []
    for rawrule in rawrules:
        rule = SubredditRule(
            subreddit_name=subreddit_name,
            kind=rawrule["kind"],
            priority=rawrule["priority"],
            short_name=rawrule["short_name"],
            created_utc=rawrule["created_utc"],
            description=rawrule["description"],
            violation_reason=rawrule.get("violation_reason", ""),
        )
        rules.append(rule)
    return rules


def fetch_rules_for_subreddit(session, subreddit_name):
    """
    Fetch the rules for a specific subreddit and insert them into the database.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param subreddit_name: name of subreddit to fetch rules for
    @type subreddit_name: L{str}
    @return: whether anything new has been fetched
    @rtype: L{bool}
    """
    did_fetch_something = False
    rules = get_rules_for_subreddit(subreddit_name)
    for rule in rules:
        session.merge(rule)
        did_fetch_something = True
    session.commit()
    return did_fetch_something


def fetch_all_rules(session, sleep=1):
    """
    Fetch all rules and insert them into the database.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param sleep: how many seconds to wait between each request
    @type sleep: L{int}
    @return: whether anything new has been fetched
    @rtype: L{bool}
    """
    did_fetch_something_new = False
    stmt = select(Subreddit).where(~Subreddit.rules.any())
    for subreddit in session.execute(stmt).scalars():
        print("Fetching rules for: {}".format(subreddit.name))
        if fetch_rules_for_subreddit(session, subreddit.name):
            did_fetch_something_new = True
        time.sleep(sleep)
    return did_fetch_something_new


def fetch_all(session, sleep=1, with_references=True):
    """
    Run all fetch operations.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param sleep: how many seconds to wait between each request
    @type sleep: L{int}
    @param with_references: wether referenced reddit pages (e.g. posts) should also be fetched
    @type with_references: L{bool}
    @return: whether anything new has been fetched
    @rtype: L{bool}
    """
    did_fetch_wiki = fetch_all_wikis(session, sleep=sleep)
    did_fetch_rule = fetch_all_rules(session, sleep=sleep)
    if with_references:
        did_fetch_post = fetch_all_references(session, sleep=sleep)
    return any((did_fetch_wiki, did_fetch_rule, did_fetch_post))


class ReferenceUrlRewriter(object):
    """
    This class handles the rewriting of URLs referencing reddit pages.

    @ivar session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    """
    def __init__(self, session):
        """
        The default constructor.

        @param session: sqlalchemy session to use
        @type session: L{sqlalchemy.orm.Session}
        """
        self.session = session

    def should_rewrite(self, reference):
        """
        Check if a reference should be rewritten.

        @param reference: reference to check
        @type reference: L{dict}
        @return: whether the reference should be rewritten or not
        @rtype: L{bool}
        """
        if reference["type"] in ("post", "comment"):
            return has_post_locally(self.session, reference["post"])
        elif reference["type"] == "subreddit":
            return has_subreddit_locally(self.session, reference["subreddit"])
        else:
            return False

    def rewrite_url(self, url, to_root):
        """
        Rewrites an external URL to an internal one if the referenced object exists locally.

        @param url: url to rewrite
        @type url: L{str}
        @param to_root: like with the renderer, a prefix (e.g. C{../..}) that indicates how to navigate to the root directory
        @type to_root: L{str}
        @return: the url to use, which may still be an external one
        @rtype: L{str}
        """
        reference = parse_reddit_url(url)
        if reference is None:
            # not a reddit url
            return url
        if self.should_rewrite(reference):
            return reddit_reference_to_url(reference, to_root=to_root)
        else:
            return url

    def rewrite_urls_in_text(self, text, to_root):
        """
        Rewrite all external URLs found in the text with internal ones if the files exist locally.

        @param text: text to rewrite
        @type text: L{str}
        @param to_root: like with the renderer, a prefix (e.g. C{../..}) that indicates how to navigate to the root directory
        @type to_root: L{str}
        @return: the rewritten text
        @rtype: L{str}
        """
        urls = get_urls_from_string(text)
        for url in urls:
            new_url = self.rewrite_url(url, to_root=to_root)
            if new_url != url:
                text = text.replace(url, new_url)
        return text
