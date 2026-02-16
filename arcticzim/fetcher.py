"""
This module handles the fetching of additional data like wiki pages.
"""
import time

import requests
from sqlalchemy import select

from .db.models import Subreddit, WikiPage


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
            content=rawpage["content"],
            revision_date=rawpage["revision_date"],
            revision_author=rawpage["revision_author"],
            revision_reason=rawpage.get("revision_reason", None),
            retrieved_on=rawpage["retrieved_on"],
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
    """
    pages = get_wikipages_for_subreddit(subreddit_name)
    for page in pages:
        session.merge(page)
    session.commit()


def fetch_all_wikis(session, sleep=1):
    """
    Fetch all wiki pages and insert them into the database.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param sleep: how many seconds to wait between each request
    @type sleep: L{int}
    """
    stmt = select(Subreddit).where(~Subreddit.wikipages.any())
    for subreddit in session.execute(stmt).scalars():
        print("Fetching wikipages for: {}".format(subreddit.name))
        fetch_wiki_for_subreddit(session, subreddit.name)
        time.sleep(sleep)


def fetch_all(session, sleep=1):
    """
    Run all fetch operations.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param sleep: how many seconds to wait between each request
    @type sleep: L{int}
    """
    fetch_all_wikis(session)
