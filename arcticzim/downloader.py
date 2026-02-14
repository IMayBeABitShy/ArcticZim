"""
This module contains the code for downloading media files.
"""
import os
import hashlib
import time
from mimetypes import guess_type
from urllib.parse import urlparse, parse_qsl, unquote_plus, urlunparse, urlencode

from sqlalchemy import select, func
from sqlalchemy.orm import undefer
import requests
import tqdm

from .db.models import MediaFile, Post
from .imgutils import minimize_image, mimetype_is_image


class DownloadFailed(Exception):
    """
    Exception raised when a download failed.
    """
    pass


def unify_url(url):
    """
    Transform an URL into a "unified" URL used to identify similiar URLs.

    The resulting URL may not be valid and is only supposed to be used
    for identifying if two URLs are the same.

    @param url: URL to unify
    @type url: L{str}
    @return: a unified URL
    @rtype: L{str}
    """
    # modified version of https://stackoverflow.com/a/9468284
    if isinstance(url, bytes):
        url = url.decode("utf-8")
    parts = urlparse(url)
    _query = urlencode(
        list(sorted(frozenset(parse_qsl(parts.query)))),
    )
    _path = unquote_plus(parts.path)
    parts = parts._replace(
        query=_query,
        path=_path,
        fragment="",
        scheme="http",
    )
    return urlunparse(parts)


def hash_url(url):
    """
    Hash a url, returning the hexdigest.

    @param url: URL to hash
    @type url: L{str}
    @return: the hashed url
    @rtype: L{str}
    """
    hasher = hashlib.md5(unify_url(url).encode("utf-8"))
    return hasher.hexdigest()


def has_downloaded(session, url, any_status=True):
    """
    Check if the URL has already been downloaded.

    @param session: sqlalchemy session
    @type session: L{sqlalchemy.orm.Session}
    @param url: url to check
    @type url: L{str}
    @param any_status: if nonzero, return True as long as the download has been attempted
    @type any_status: L{bool}
    @return: whether the file has been downloaded or not
    @rtype: L{bool}
    """
    unified_url = unify_url(url)
    mf = session.execute(
        select(MediaFile).where(
            (MediaFile.url == unified_url),
            ((MediaFile.downloaded == True) if not any_status else True),
        )
    ).one_or_none()
    return (mf is not None)


def download(session, url, mediadir, enable_post_processing=True):
    """
    Check if the URL has already been downloaded.

    @param session: sqlalchemy session
    @type session: L{sqlalchemy.orm.Session}
    @param url: url to download
    @type url: L{str}
    @param mediadir: directory where files should be downloaded too
    @type mediadir: L{str}
    @param enable_post_processing: whether post-processing should be applied
    @type enable_post_processing: L{bool}
    """
    url_hash = hash_url(url)
    outpath = os.path.join(mediadir, url_hash)
    try:
        if is_ytdlp(url):
            do_ytldp_download(url=url, mediadir=mediadir)
        else:
            headers = {
                "user-agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
            }
            r = requests.get(url, headers=headers, stream=True)
            try:
                r.raise_for_status()
            except Exception as e:
                raise DownloadFailed() from e
            mimetype = r.headers.get("content-type", None)
            if mimetype is None:
                mimetype = guess_type(url)
            elif ";" in mimetype:
                mimetype = mimetype[:mimetype.find(";")]
            hasher = hashlib.md5()
            size = 0
            with open(outpath, "wb") as fout:
                for chunk in r.iter_content(chunk_size=4096):
                    fout.write(chunk)
                    hasher.update(chunk)
                    size += len(chunk)
            md5 = hasher.hexdigest()
            existing_mf = session.execute(
                select(MediaFile).where(
                    MediaFile.md5 == md5,
                    MediaFile.downloaded == True,
                    MediaFile.primary_uid is None,
                )
            ).one_or_none()
            if existing_mf is not None:
                # file already downloaded
                os.remove(os.path.join(mediadir, url_hash))
                mf = MediaFile(
                    url=unify_url(url),
                    downloaded=True,
                    md5=md5,
                    mimetype=mimetype,
                    primary_uid=existing_mf[0].uid,
                )
                session.add(mf)
                session.commit()
                return
            else:
                mf = MediaFile(
                    url=unify_url(url),
                    downloaded=True,
                    md5=md5,
                    mimetype=mimetype,
                    size=size,
                )
                if enable_post_processing:
                    post_process(mediadir, mf)
                session.add(mf)
                session.commit()

    except DownloadFailed:
        mo = MediaFile(
            url=unify_url(url),
            downloaded=False,
        )
        session.add(mo)
        session.commit()


def is_ytdlp(url):
    """
    Check if the target url should be downloaded via yt-dlp.

    @param url: url to check
    @type url: L{bool}
    @return: whether yt-dlp should be used to download target url
    @rtype: L{bool}
    """
    return False  # not implemented


def do_ytldp_download(url, mediadir):
    """
    Download a URL with yt-dlp.

    @param url: url to download
    @type url: L{str}
    @param mediadir: directory where files should be downloaded too
    @type mediadir: L{str}
    """
    raise NotImplementedError("yt-dlp support is not implemented!")


def post_process(mediadir, mediafile):
    """
    Post process an image file.

    Don't forget to add and commit the mediafile afterwards!

    @param mediadir: directory containing the files
    @type mediadir: L{str}
    @param mediafile: the mediafile object
    @type mediafile: L{arcticzim.db.models.MediaFile}
    """
    path = os.path.join(mediadir, hash_url(mediafile.url))
    if mimetype_is_image(mediafile.mimetype):
        mimetype, size = minimize_image(path)
        mediafile.size = size
        mediafile.mimetype = mimetype



def get_urls_from_post(post):
    """
    Return all URLs used in a post

    @param post: post to get URLs from
    @type post: L{arcticzim.db.models.Post}
    @return: a list of posts
    @rtype: L{list} of L{str}
    """
    urls = []
    if post.post_hint in ("rich:video", "hosted:video", "image"):
        urls.append(post.url)
    if post.selftext:
        for url in get_urls_in_string(post.selftext):
            urls.append(url)
    return urls


def get_urls_in_string(s):
    """
    Find all URLs in a string and return them

    @param s: string to search for URLs
    @type s: L{str}
    @return: a list of URLs found
    @rtype: L{list} of L{str}
    """
    return []  # not implemented


def download_all(session, mediadir, sleep=0.5, enable_post_processing=True):
    """
    Download all files of posts.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param mediadir: directory to store media in
    @type mediadir: L{str}
    @param sleep: sleep time between downloads in seconds
    @type sleep: L{int} or L{float}
    @param enable_post_processing: whether post-processing should be applied
    @type enable_post_processing: L{bool}
    """
    n = session.execute(select(func.count(Post.uid))).one()[0]
    stmt = select(Post).options(
        undefer(Post.url),
        undefer(Post.selftext),
    ).execution_options(
        yield_per=1000,
    )
    for post in tqdm.tqdm(session.execute(stmt).scalars(), desc="Searching posts", total=n, unit="posts"):
        urls = get_urls_from_post(post)
        urls = [url for url in urls if not has_downloaded(session=session, url=url)]
        if not urls:
            continue
        for url in tqdm.tqdm(urls, desc="downloading files for {}".format(post.id), total=len(urls), unit="files"):
            download(
                session=session,
                url=url,
                mediadir=mediadir,
                enable_post_processing=enable_post_processing,
            )
            time.sleep(sleep)


class MediaFileManager(object):
    """
    This class managers the conversion from external to internal links.

    @ivar session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @ivar enabled: if the rewrite is enabled in the first place
    @type enabled: L{bool}
    @ivar referenced_files: a list of files referenced by the rewritten URLs
    @type referenced_files: L{list} of L{int}
    """
    def __init__(self, session, enabled=True):
        """
        The default constructor.

        @param session: sqlalchemy session to use
        @type session: L{sqlalchemy.orm.Session}
        @param enabled: if the rewrite is enabled in the first place
        @type enabled: L{bool}
        """
        self.session = session
        self.enabled = enabled
        self.referenced_files = []

    def reset(self):
        """
        Reset internal states.
        """
        self.referenced_files = []

    def should_rewrite(self, mediafile):
        """
        Check whether an URL should be rewritten.

        This method does not check if the local file exists. That is
        presumed to have already been done. Instead, this method checks
        whether we want to include the file.

        @param mediafile: mediafile to check
        @type mediafile: L{arcticzim.db.models.MediaFile}
        @return: whether the link should be rewritten
        @rtype: L{str}
        """
        return self.enabled  # not implemented

    def rewrite_url(self, url, to_root):
        """
        Rewrites an external URL to an internal one if the file exists locally.

        @param url: url to rewrite
        @type url: L{str}
        @param to_root: like with the renderer, a prefix (e.g. C{../..}) that indicates how to navigate to the root directory
        @type to_root: L{str}
        @return: the url to use, which may still be an external one
        @rtype: L{str}
        """
        unified_url = unify_url(url)
        mf = self.session.execute(select(MediaFile).where(MediaFile.url == unified_url)).one_or_none()
        if mf is None:
            # file not downloaded, can't rewrite
            return url
        mf = mf[0]
        if mf.primary_uid is not None:
            mf = mf.primary
        if not mf.downloaded:
            # file not downloaded, can't rewrite
            return url
        if not self.should_rewrite(mf):
            # we should not use this media file
            return url
        new_url = "{}/media/{}".format(to_root, mf.uid)
        self.referenced_files.append(mf.uid)
        return new_url

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
        urls = get_urls_in_string(text)
        for url in urls:
            new_url = rewrite_url(url, to_root=to_root)
            if new_url != url:
                text = text.replace(url, new_url)
        return text
