"""
This module contains the code for downloading media files.
"""
import os
import hashlib
import time
import json
from mimetypes import guess_type
from urllib.parse import urlparse, parse_qsl, unquote_plus, urlunparse, urlencode

from sqlalchemy import select, func
from sqlalchemy.orm import undefer, selectinload
import requests
import tqdm
from yt_dlp import YoutubeDL
from redvid import Downloader as RedvidDL

from .db.models import MediaFile, Post, Comment
from .imgutils import minimize_image, mimetype_is_image, mimetype_is_video
from .util import get_urls_from_string


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
    if url == "":
        return ""
    parts = urlparse(url)
    _query = urlencode(
        list(sorted(frozenset(parse_qsl(parts.query)))),
    )
    _path = parts.path
    while True:
        # using a loop here as a URL may be urlencoded multiple times
        # and we need the output here to be stable
        unquoted = unquote_plus(_path)
        if unquoted == _path:
            break
        _path = unquoted
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


def download(session, url, mediadir, enable_post_processing=True, download_videos=True, max_image_dimension=512):
    """
    Download the media at the specified URL.

    @param session: sqlalchemy session
    @type session: L{sqlalchemy.orm.Session}
    @param url: url to download
    @type url: L{str}
    @param mediadir: directory where files should be downloaded too
    @type mediadir: L{str}
    @param enable_post_processing: whether post-processing should be applied
    @type enable_post_processing: L{bool}
    @param download_videos: whether videos should be downloaded
    @type download_videos: L{bool}
    @param max_image_dimension: how many pixels the wider side of an image may have at most
    @type max_image_dimension: L{int}
    """
    url_hash = hash_url(url)
    outpath = os.path.join(mediadir, url_hash)
    guessed_mimetype = guess_type(urlparse(url).path)[0]
    is_probably_image = (guessed_mimetype is not None) and (guessed_mimetype.startswith("image/"))
    try:
        if (is_ytdlp(url) or is_redvid(url)) and not is_probably_image:
            if not download_videos:
                return
            if is_redvid(url):
                mimetype = do_redvid_download(url=url, mediadir=mediadir, outpath=outpath)
            else:
                mimetype = do_ytldp_download(url=url, mediadir=mediadir, outpath=outpath)
            hasher = hashlib.md5()
            size = 0
            with open(outpath, "rb") as fin:
                chunk = True
                while chunk:
                    chunk = fin.read(4096)
                    size += len(chunk)
                    hasher.update(chunk)
            md5 = hasher.hexdigest()
        else:
            headers = {
                "user-agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
            }
            try:
                r = requests.get(url, headers=headers, stream=True)
                r.raise_for_status()
            except Exception as e:
                raise DownloadFailed() from e
            mimetype = r.headers.get("content-type", None)
            if mimetype is None:
                mimetype = guess_type(urlparse(url).path)[0]
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
    except DownloadFailed:
        mo = MediaFile(
            url=unify_url(url),
            downloaded=False,
        )
        session.add(mo)
        session.commit()
        return

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
            post_process(mediadir, mf, max_image_dimension=max_image_dimension)
        session.add(mf)
        session.commit()


def is_ytdlp(url):
    """
    Check if the target url should be downloaded via yt-dlp.

    @param url: url to check
    @type url: L{bool}
    @return: whether yt-dlp should be used to download target url
    @rtype: L{bool}
    """
    with YoutubeDL(params={"allowed_extractors": ["default", "-generic"]}) as yt:
        for ie in yt._ies.values():  # TODO: private attribute access is probably a bad idea
            if ie.suitable(url) and ie.working():
                return True
    return False


def do_ytldp_download(url, mediadir, outpath):
    """
    Download a URL with yt-dlp.

    @param url: url to download
    @type url: L{str}
    @param mediadir: directory where files should be downloaded too
    @type mediadir: L{str}
    @param outpath: path the file should be written to
    @type outpath: L{str}
    @return: the mimetype of the downloaded video
    @rtype: L{str}
    """
    hashed_url = hash_url(url)
    params = {
        # TODO: change "worst" to smallest filesize
        "format": "worst/worstvideo+worstaudio",
        "outtmpl": "{}.%(ext)s".format(outpath),
    }
    with YoutubeDL(params=params) as yt:
        try:
            yt.download(url)
        except Exception as e:
            raise DownloadFailed("yt-dlp raised an exception when attempting to download video") from e
    # todo: the following is probably inefficient for large file collections
    for fname in os.listdir(mediadir):
        if fname.startswith(hashed_url):
            break
    mimetype = guess_type(fname)[0] or "video/mp4"
    os.rename(os.path.join(mediadir, fname), outpath)
    return mimetype


def is_redvid(url):
    """
    Check if the target url should be downloaded via redvid.

    @param url: url to check
    @type url: L{bool}
    @return: whether redvid should be used to download target url
    @rtype: L{bool}
    """
    return ("://v.redd.it" in url)


def do_redvid_download(url, mediadir, outpath):
    """
    Download a URL with redvid.

    @param url: url to download
    @type url: L{str}
    @param mediadir: directory where files should be downloaded too
    @type mediadir: L{str}
    @param outpath: path the file should be written to
    @type outpath: L{str}
    @return: the mimetype of the downloaded video
    @rtype: L{str}
    """
    downloader = RedvidDL(url=url, path=mediadir, min_q=True)
    try:
        path = downloader.download()
    except BaseException as e:
        raise DownloadFailed("redvid raised an exception when attempting to download video") from e
    mimetype = guess_type(path)[0] or "video/mp4"
    os.rename(path, outpath)
    return mimetype


def post_process(mediadir, mediafile, max_image_dimension=512):
    """
    Post process an image file.

    Don't forget to add and commit the mediafile afterwards!

    @param mediadir: directory containing the files
    @type mediadir: L{str}
    @param mediafile: the mediafile object
    @type mediafile: L{arcticzim.db.models.MediaFile}
    @param max_image_dimension: how many pixels the wider side of an image may have at most
    @type max_image_dimension: L{int}
    """
    path = os.path.join(mediadir, hash_url(mediafile.url))
    if mimetype_is_image(mediafile.mimetype):
        mimetype, size = minimize_image(
            path,
            max_w=max_image_dimension,
            max_h=max_image_dimension,
        )
        mediafile.size = size
        mediafile.mimetype = mimetype


def get_urls_from_post(post, include_reddit_videos=True, include_external_videos=False, include_comments=False):
    """
    Return all URLs used in a post

    @param post: post to get URLs from
    @type post: L{arcticzim.db.models.Post}
    @param include_reddit_videos: whether reddit videos should be included
    @type include_reddit_videos: L{bool}
    @param include_external_videos: whether non-reddit videos should be included
    @type include_external_videos: L{bool}
    @param include_comments: if nonzero, download media in comments too
    @type include_comments: L{bool}
    @return: a list of posts
    @rtype: L{list} of L{str}
    """
    urls = []
    if post.post_hint in ("rich:video", ) and include_external_videos:
        urls.append(post.url)
    if post.post_hint in ("hosted:video", ) and include_reddit_videos:
        urls.append(post.url)
    if post.post_hint in ("image", ):
        urls.append(post.url)
    if post.selftext:
        for url in get_media_urls_from_string(
            post.selftext,
            include_reddit_videos=include_reddit_videos,
            include_external_videos=include_external_videos,
        ):
            urls.append(url)
    if post.is_gallery:
        media_metadata = json.loads(post.media_metadata)
        if media_metadata:
            for img_data in media_metadata.values():
                if ("s" in img_data) and ("u" in img_data["s"]):
                    urls.append(img_data["s"]["u"])
                elif ("p" in img_data) and (len(img_data["p"]) > 0) and ("u" in img_data["p"][-1]):
                    urls.append(img_data["p"][-1]["u"])
    if include_comments:
        for comment in post.comments:
            urls += get_media_urls_from_string(comment.body)
    return urls


def get_media_urls_from_string(s, include_reddit_videos=True, include_external_videos=False):
    """
    Find all media URLs matching certain conditions in a string and return them

    @param s: string to search for URLs
    @type s: L{str}
    @param include_reddit_videos: whether reddit videos should be included
    @type include_reddit_videos: L{bool}
    @param include_external_videos: whether non-reddit videos should be included
    @type include_external_videos: L{bool}
    @return: a list of URLs found
    @rtype: L{list} of L{str}
    """
    urls = get_urls_from_string(s)
    found_urls = []
    for url in urls:
        mimetype = guess_type(urlparse(url).path)[0]
        if mimetype is None:
            # no guess
            continue
        if mimetype.startswith("video/"):
            if is_redvid(url):
                if include_reddit_videos:
                    found_urls.append(url)
            elif is_ytdlp(url):
                if include_external_videos:
                    found_urls.append(url)
        elif mimetype.startswith("image/"):
            found_urls.append(url)
    return found_urls


def download_all(
    session,
    mediadir,
    sleep=0.5,
    enable_post_processing=True,
    download_reddit_videos=True,
    download_external_videos=False,
    include_comments=False,
    max_image_dimension=512,
):
    """
    Download all files of posts.

    @param session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @param mediadir: directory to store media in
    @type mediadir: L{str}
    @param sleep: sleep time between downloads in seconds
    @type sleep: L{int} or L{float}
    @param download_reddit_videos: whether reddit videos should be downloaded
    @type download_reddit_videos: L{bool}
    @param download_external_videos: whether non-reddit videos should be downloaded
    @type download_external_videos: L{bool}
    @param enable_post_processing: whether post-processing should be applied
    @type enable_post_processing: L{bool}
    @param include_comments: if nonzero, download media in comments too
    @type include_comments: L{bool}
    @param max_image_dimension: how many pixels the wider side of an image may have at most
    @type max_image_dimension: L{int}
    """
    n = session.execute(select(func.count(Post.uid))).one()[0]
    stmt = select(Post).options(
        undefer(Post.url),
        undefer(Post.selftext),
        selectinload(Post.comments),
        undefer(Post.comments, Comment.body),
    ).execution_options(
        yield_per=1000,
    )
    for post in tqdm.tqdm(session.execute(stmt).scalars(), desc="Searching posts", total=n, unit="posts"):
        urls = get_urls_from_post(
            post,
            include_reddit_videos=download_reddit_videos,
            include_external_videos=download_external_videos,
            include_comments=include_comments,
        )
        urls = set([url for url in urls if not has_downloaded(session=session, url=url)])
        if not urls:
            continue
        for url in tqdm.tqdm(urls, desc="downloading files for {}".format(post.id), total=len(urls), unit="files"):
            download(
                session=session,
                url=url,
                mediadir=mediadir,
                enable_post_processing=enable_post_processing,
                download_videos=(download_external_videos or download_reddit_videos),
                max_image_dimension=max_image_dimension,
            )
            time.sleep(sleep)


class MediaFileManager(object):
    """
    This class managers the conversion from external to internal links.

    @ivar session: sqlalchemy session to use
    @type session: L{sqlalchemy.orm.Session}
    @ivar enabled: if the rewrite is enabled in the first place
    @type enabled: L{bool}
    @ivar images_enabled: if media links should be rewritten
    @type images_enabled: L{bool}
    @ivar videos_enabled: if video links should be rewritten
    @type videos_enabled: L{bool}
    @ivar referenced_files: a list of files referenced by the rewritten URLs
    @type referenced_files: L{list} of L{int}
    """
    def __init__(self, session, enabled=True, images_enabled=True, videos_enabled=True):
        """
        The default constructor.

        @param session: sqlalchemy session to use
        @type session: L{sqlalchemy.orm.Session}
        @param enabled: if the rewrite is enabled in the first place
        @type enabled: L{bool}
        @param images_enabled: if media links should be rewritten
        @type images_enabled: L{bool}
        @param videos_enabled: if video links should be rewritten
        @type videos_enabled: L{bool}
        """
        self.session = session
        self.enabled = enabled
        self.images_enabled = images_enabled
        self.videos_enabled = videos_enabled
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
        if not self.enabled:
            return False
        if mimetype_is_image(mediafile.mimetype) and self.images_enabled:
            return True
        if mimetype_is_video(mediafile.mimetype) and self.videos_enabled:
            return True
        return False

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
        if not unified_url:
            # we should not rewrite empty URLs
            return url
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
        urls = get_media_urls_from_string(text)
        for url in urls:
            new_url = self.rewrite_url(url, to_root=to_root)
            if new_url != url:
                text = text.replace(url, new_url)
        return text
