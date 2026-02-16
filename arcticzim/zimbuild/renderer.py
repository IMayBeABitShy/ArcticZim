"""
The renderer generates HTML pages.

@var POSTS_PER_PAGE: how many posts are max allowed on a single page of a subreddit, ...
@type POSTS_PER_PAGE: L{int}
@var MAX_ITEMS_PER_RESULT: a subresult is submitted after this many items
@type MAX_ITEMS_PER_RESULT: L{int}
@var SUBREDDITS_ON_INDEX_PAGE: number of subreddits that should appear on the index page
@type SUBREDDITS_ON_INDEX_PAGE: L{int}
@var SUBREDDITS_PER_PAGE: how many subreddits should be listed on a page
@type SUBREDDITS_PER_PAGE: L{int}
"""
import json
import datetime
import math
import pprint

import htmlmin
import mistune
from jinja2 import Environment, PackageLoader, select_autoescape, Undefined

# optional optimization dependencies
try:
    import minify_html
except ImportError:
    minify_html = None

from ..util import format_size, format_number, get_resource_file_path, parse_reddit_url
from ..downloader import MediaFileManager
from .buckets import BucketMaker


POSTS_PER_PAGE = 20
MAX_ITEMS_PER_RESULT = 200
SUBREDDITS_ON_INDEX_PAGE = 20
SUBREDDITS_PER_PAGE = 40


class RenderedObject(object):
    """
    Base class for render results.
    """
    pass


class HtmlPage(RenderedObject):
    """
    This class holds the representation of a single rendered page.

    @ivar path: absolute path of the rendered page
    @type path: L{str}
    @ivar title: title of the page
    @type title: L{str}
    @ivar content: the HTML code of the page
    @type content: L{str}
    @ivar is_front: True if this is a front article
    @type is_front: L{bool}
    """
    def __init__(self, path, title, content, is_front=True):
        """
        The default constructor.

        @param path: absolute path of the rendered page
        @type path: L{str}
        @param title: title of the page
        @type title: L{str}
        @param content: the HTML code of the page
        @type content: L{str}
        @param is_front: True if this is a front article
        @type is_front: L{bool}
        """
        assert isinstance(path, str)
        assert isinstance(title, str)
        assert isinstance(content, str)
        assert isinstance(is_front, bool)
        self.path = path
        self.title = title
        self.content = content
        self.is_front = is_front

    def __str__(self):
        return "{}(path={}, title={}, content={} bytes)".format(self.__class__.__name__, self.path, self.title, len(self.content))


class JsonObject(RenderedObject):
    """
    This class holds a rendered json object.

    @ivar path: absolute path the object should be stored at
    @type path: L{str}
    @ivar title: title of the object
    @type title: L{str}
    @ivar content: the serialized json object to store
    @type content: any or L{str}
    """
    def __init__(self, path, title, content):
        """
        The default constructor.

        @ivar path: absolute path the object should be stored at
        @type path: L{str}
        @ivar title: title of the object
        @type title: L{str}
        @ivar content: the json object to store
        @type content: json-serializable
        """
        assert isinstance(path, str)
        assert isinstance(title, str)
        self.path = path
        self.title = title
        if isinstance(content, str):
            self.content = content
        else:
            self.content = json.dumps(content, separators=(",", ":"))

    def __str__(self):
        return "{}(path={}, title={}, content={} bytes)".format(self.__class__.__name__, self.path, self.title, len(self.content))


class Script(RenderedObject):
    """
    This class holds a rendered js script.

    @ivar path: absolute path the script should be stored at
    @type path: L{str}
    @ivar title: title of the object
    @type title: L{str}
    @ivar content: the script itself
    @type content: L{str}
    """
    def __init__(self, path, title, content):
        """
        The default constructor.

        @ivar path: absolute path the object should be stored at
        @type path: L{str}
        @ivar title: title of the script
        @type title: L{str}
        @ivar content: the script to store
        @type content: L{str}
        """
        assert isinstance(path, str)
        assert isinstance(title, str)
        self.path = path
        self.title = title
        self.content = content

    def __str__(self):
        return "{}(path={}, title={}, content={} bytes)".format(self.__class__.__name__, self.path, self.title, len(self.content))


class Redirect(RenderedObject):
    """
    This class holds redirect information.

    @ivar source: source path
    @type source: L{str}
    @ivar target: target path to redirect to
    @type target: L{str}
    @ivar title: title of the redirect
    @type title: L{str}
    @ivar is_front: True if this is a front article
    @type is_front: L{bool}
    """
    def __init__(self, source, target, title, is_front=False):
        """
        The default constructor.

        @param source: source path
        @type source: L{str}
        @param target: target path to redirect to
        @type target: L{str}
        @param title: title of the redirect
        @type title: L{str}
        @param is_front: True if this is a front article
        @type is_front: L{bool}
        """
        self.source = source
        self.target = target
        self.title = title
        self.is_front = is_front

    def __str__(self):
        return "{}(path={}, target={}, title={})".format(self.__class__.__name__, self.source, self.target, self.title)


class RenderResult(object):
    """
    This class encapsulates a list of  multiple L{RenderedObject},
    which together make up a rendered result.

    @ivar _objects: list of the rendered objects
    @type _objects: L{list} of L{RenderedObject}
    """
    def __init__(self, objects=None):
        """
        The default constructor.

        @param objects: list of the rendered objects or a rendered object, if any
        @type objects: L{None} or L{RenderedObject} or L{list} of L{RenderedObject}
        """
        if objects is None:
            self._objects = []
        elif isinstance(objects, RenderedObject):
            self._objects = [objects]
        elif isinstance(objects, (tuple, list)):
            self._objects = list(objects)
        else:
            raise TypeError("Expected None, RenderedObject or list/tuple of RenderedObject, got {} instead!".format(repr(objects)))

    def __str__(self):
        return "{}({})".format(self.__class__.__name__, pprint.pformat([str(o) for o in self.iter_objects()]))

    def add(self, obj):
        """
        Add an object to the result.

        @param obj: object to add
        @type obj: L{RenderedObject}
        """
        self._objects.append(obj)

    def iter_objects(self):
        """
        Iterate over the objects in this result.

        @yields: the objects in this result
        @ytype: L{RenderedObject}
        """
        for obj in self._objects:
            yield obj


class FileReferences(RenderedObject):
    """
    A list of MediaFile uids that were referenced in the post.

    @ivar uids: list of referenced uids
    @type uids: L{list} of L{int}
    """
    def __init__(self, uids):
        """
        The default constructor.

        @param uids: list of referenced uids
        @type uids: L{list} of L{int}
        """
        self.uids = uids


class SubredditInfo(object):
    """
    A helper class that holds details about a subreddit.

    @ivar name: name of this subreddit
    @type name: L{str}
    @ivar posts: number of posts in this subreddit
    @type posts: L{int}
    """
    def __init__(self, name, posts):
        """
        The default constructor.

        @param name: name of this subreddit
        @type name: L{str}
        @param posts: number of posts in this subreddit
        @type posts: L{int}
        """
        assert isinstance(name, str)
        assert isinstance(posts, int)
        self.name = name
        self.posts = posts


class RenderOptions(object):
    """
    Options for the renderer.

    @ivar with_stats: if nonzero, include statistics
    @type with_stats: L{bool}
    @ivar with_users: if nonzero, include user pages<
    @type with_users: L{bool}
    @ivar with_videos: if nonzero, include videos
    @type with_videos: L{bool}
    """
    def __init__(self, with_stats=True, with_users=True, with_videos=False):
        """
        The default constructor.

        @param with_stats: if nonzero, include statistics
        @type with_stats: L{bool}
        @param with_users: if nonzero, include user pages<
        @type with_users: L{bool}
        @param with_videos: if nonzero, include videos
        @type with_videos: L{bool}
        """
        self.with_stats = with_stats
        self.with_users = with_users
        self.with_videos = with_videos


class HtmlRenderer(object):
    """
    The HTML renderer renders HTML pages for various objects.

    @ivar worker: worker this renderer is for
    @type worker: L{arcticzim.zimbuild.worker.Worker}
    @ivar environment: the jinja2 environment used to render templates
    @type environment: L{jinja2.Environment}
    @ivar options: render options
    @type options: L{RenderOptions}
    @ivar filemanager: the media file manager
    @type filemanager: L{arcticzim.downloader.MediaFileManager}
    @ivar reference_rewrite: the URL rewriter for reddit references
    @type reference_rewriter: L{arcticzim.fetcher.ReferenceUrlRewriter}
    """
    def __init__(self, worker, options, filemanager, reference_rewriter):
        """
        The default constructor.

        @param worker: worker this renderer is for
        @type worker: L{arcticzim.zimbuild.worker.Worker}
        @param options: render options
        @type options: L{RenderOptions}
        @param filemanager: the media file manager
        @type filemanager: L{arcticzim.downloader.MediaFileManager}
        @param reference_rewrite: the URL rewriter for reddit references
        @type reference_rewriter: L{arcticzim.fetcher.ReferenceUrlRewriter}
        """
        assert isinstance(options, RenderOptions)
        assert isinstance(filemanager, MediaFileManager)
        self.worker = worker
        self.options = options
        self.filemanager = filemanager
        self.reference_rewriter = reference_rewriter

        # setup jinja environment
        self.environment = Environment(
            loader=PackageLoader("arcticzim.zimbuild"),
            auto_reload=False,
            autoescape=select_autoescape(),
        )

        # configure environment globals
        self.environment.globals["options"] = self.options

        # configure filters
        self.environment.filters["render_comment_text"] = self._render_comment_text_filter
        self.environment.filters["render_post_text"] = self._render_comment_text_filter
        self.environment.filters["render_wiki_text"] = self._render_comment_text_filter
        self.environment.filters["render_license"] = self._render_license_text_filter
        self.environment.filters["format_number"] = format_number
        self.environment.filters["format_size"] = format_size
        self.environment.filters["format_date"] = self._format_date
        self.environment.filters["format_timestamp"] = self._format_timestamp
        self.environment.filters["first_elements"] = self._first_elements
        self.environment.filters["default_index"] = self._default_index
        self.environment.filters["rewrite_url"] = self._rewrite_url_filter
        self.environment.filters["load_json"] = json.loads
        self.environment.filters["first_nonzero"] = self._first_nonzero_filter
        self.environment.filters["render_postsummary_by_url"] = self._render_postsummary_by_url
        self.environment.filters["debug"] = print

        # configure tests
        self.environment.tests["date"] = self._is_date
        self.environment.tests["local_post_url"] = self._is_local_post_url

    @staticmethod
    def minify_html(s):
        """
        Minify html code.

        @param s: html code to minify
        @type s: L{str}
        @return: the minified html
        @rtype: L{str}
        """
        if minify_html is None:
            # fall back to htmlmin
            return htmlmin.minify(
                s,
                remove_comments=True,
                remove_empty_space=True,
                reduce_boolean_attributes=True,
                # remove_optional_attribute_quotes=True,
                remove_optional_attribute_quotes=False,  # firefox complains for some tags
            )
        else:
            return minify_html.minify(
                s,
            )

    def _add_file_references(self, result):
        """
        Add file references for the observed files into the render results.

        @param result: result to add file references to
        @type result: L{RenderResult}
        """
        fro = FileReferences(list(set(self.filemanager.referenced_files)))
        result.add(fro)

    def render_post(self, post):
        """
        Render a post.

        @param post: post to render
        @type post: L{arcticzim.db.models.Post}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        self.filemanager.reset()
        result = RenderResult()
        post_template = self.environment.get_template("postpage.html.jinja")
        post_page = post_template.render(
            post=post,
            to_root="../../..",
        )
        result.add(
            HtmlPage(
                path="r/{}/{}/".format(post.subreddit.name, post.id),
                title=post.title,
                content=self.minify_html(post_page),
                is_front=True,
            ),
        )
        # add redirect from id -> post
        result.add(
            Redirect(
                post.id,
                "r/{}/{}/".format(post.subreddit.name, post.id),
                title=post.title,
                is_front=False,
            ),
        )
        # add poll data if necessary
        if post.is_poll:
            raw_poll_data = json.loads(post.poll_data)
            if not raw_poll_data:
                # for some reason, post.poll_data can be false
                poll_data = False
            elif ("vote_count" not in raw_poll_data["options"][0]):
                # if the poll data is for some reason not available, this happens
                poll_data = False
            else:
                poll_data = {
                    "labels": [option["text"] for option in raw_poll_data["options"]],
                    "datasets": [
                        {
                            "data": [option["vote_count"] for option in raw_poll_data["options"]],
                        }
                    ],
                }
            result.add(
                JsonObject(
                    path="r/{}/{}/poll.json".format(post.subreddit.name, post.id),
                    title="Poll data for {}".format(post.id),
                    content=poll_data,
                )
            )
        # add referenced files
        self._add_file_references(result)
        return result

    def directly_render_post_summary(self, post, to_root):
        """
        Render a post summary, directly returning the HTML.

        @param post: post to render
        @type post: L{arcticzim.db.models.Post}
        @param to_root: rootification prefix (see templates)
        @type to_root: L{str}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        summary_template = self.environment.get_template("postsummary.html.jinja")
        post_page = summary_template.render(
            post=post,
            with_subreddit=True,
            with_util_links=True,
            to_root=to_root,
        )
        return post_page

    def render_subreddit(self, subreddit, sort="top", posts=None, num_posts=None):
        """
        Render a subreddit.

        If posts is specified, it should be an iterable yielding lists
        of posts sorted by the required order. If it is
        not specified, it will be generated from subreddit.posts

        @param subreddit: subreddit to render
        @type subreddit: L{arcticzim.db.models.Subreddit}
        @param sort: by which metric posts should be sorted
        @type sort: L{str}
        @param posts: an iterable yielding the posts in the subreddit in a sorted order as described above
        @type posts: iterable yielding L{arcticzim.db.models.Post} or L{None}
        @param num_posts: number of posts in this subreddit
        @type num_posts: L{int} or L{None}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        # general preparations
        if posts is None:
            num_posts = len(subreddit.posts)
            if sort == "top":
                posts = sorted(subreddit.posts, key=lambda x: x.score, reverse=True)
            elif sort == "new":
                posts = sorted(subreddit.posts, key=lambda x: x.created_utc, reverse=True)
            else:
                raise Exception("Invalid subreddit post sort order!")
        else:
            assert num_posts is not None
        has_wiki = (len(subreddit.wikipages) > 0)
        result = RenderResult()
        items_in_result = 0
        if num_posts == 0:
            # subreddit has no posts
            # we do not return any page in this case
            yield result
            return
        if sort == "top":
            # by default, redirect to top page 1
            result.add(
                Redirect(
                    "r/{}/".format(subreddit.name),
                    "r/{}/top_page_1/".format(subreddit.name),
                    title="r/{}".format(subreddit.name),
                    is_front=True,
                ),
            )
            items_in_result += 1
        # prepare rendering the subreddit pages
        list_page_template = self.environment.get_template("subredditpage.html.jinja")
        num_pages = math.ceil(num_posts / POSTS_PER_PAGE)
        bucketmaker = BucketMaker(POSTS_PER_PAGE)
        # render the subreddit post list pages
        page_index = 1
        for post in posts:
            bucket = bucketmaker.feed(post)
            if bucket is not None:
                items_in_result += self._render_subreddit_postlist_page(
                    subreddit=subreddit,
                    posts=bucket,
                    page_index=page_index,
                    num_pages=num_pages,
                    template=list_page_template,
                    sort=sort,
                    has_wiki=has_wiki,
                    result=result,
                )
                if items_in_result >= MAX_ITEMS_PER_RESULT:
                    yield result
                    result = RenderResult()
                    items_in_result = 0
                page_index += 1
        bucket = bucketmaker.finish()
        if bucket is not None:
            items_in_result += self._render_subreddit_postlist_page(
                subreddit=subreddit,
                posts=bucket,
                page_index=page_index,
                num_pages=num_pages,
                template=list_page_template,
                has_wiki=has_wiki,
                sort=sort,
                result=result,
            )
            if items_in_result >= MAX_ITEMS_PER_RESULT:
                yield result
                result = RenderResult()
                items_in_result = 0
        yield result

    def _render_subreddit_postlist_page(self, subreddit, posts, page_index, num_pages, template, sort, has_wiki, result):
        """
        Helper function for rendering a list page of posts in a subreddit.

        This function renders a page of posts in the subreddit and adds the
        rendered page to the result.

        @param subreddit: subreddit this page is for
        @type subreddit: L{arcticzim.db.models.Subreddit}
        @param posts: list of posts that should be listed on this page
        @type posts: L{list} of L{arcticzim.db.models.Post}
        @param page_index: index of current page (1-based)
        @type page_index: L{int}
        @param num_pages: total number of pages
        @type num_pages: L{int}
        @param template: template that should be rendered
        @type template: L{jinja2.Template}
        @param sort: sort order this page is for
        @type sort: L{str}
        @param has_wiki: whether the subreddit has a wiki or not
        @type has_wiki: L{bool}
        @param result: result the rendered page should be added to
        @type result: L{RenderResult}
        @return: the number of items added to the render result
        @rtype: L{int}
        """
        page = template.render(
            to_root="../../..",
            subreddit=subreddit,
            posts=posts,
            num_pages=num_pages,
            cur_page=page_index,
            has_wiki=has_wiki,
            sort=sort,
        )
        result.add(
            HtmlPage(
                path="r/{}/{}_page_{}/".format(subreddit.name, sort, page_index),
                content=self.minify_html(page),
                title="r/{} - {} - Page {}".format(subreddit.name, sort, page_index),
                is_front=False,
            ),
        )
        return 1  # 1 item added

    def render_subreddit_stats(self, subreddit, stats):
        """
        Render the statistics page of a subreddit.

        @param subreddit: subreddit to render
        @type subreddit: L{arcticzim.db.models.Subreddit}
        @param stats: stats to render
        @type stats: L{arcticzim.zimbuild.statistics.PostListStatistics}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        result = RenderResult()
        template = self.environment.get_template("subredditstatspage.html.jinja")
        page = template.render(
            to_root="../../..",
            subreddit=subreddit,
            stats=stats,
            has_wiki=(len(subreddit.wikipages) > 0),
        )
        result.add(
            HtmlPage(
                path="r/{}/statistics/".format(subreddit.name),
                content=self.minify_html(page),
                title="r/{} - Staticstics".format(subreddit.name),
                is_front=True,
            ),
        )
        return result

    def render_subreddit_wiki(self, subreddit):
        """
        Render the statistics page of a subreddit.

        @param subreddit: subreddit to render
        @type subreddit: L{arcticzim.db.models.Subreddit}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        result = RenderResult()
        template = self.environment.get_template("subredditwikipage.html.jinja")
        for wikipage in subreddit.wikipages:
            basepath = wikipage.basepath
            to_root = "../../.." + ("/.." * basepath.count("/"))
            page = template.render(
                to_root=to_root,
                subreddit=subreddit,
                wikipage=wikipage,
                has_wiki=True,
            )
            result.add(
                HtmlPage(
                    path="r/{}/wiki/{}".format(subreddit.name, wikipage.basepath),
                    content=self.minify_html(page),
                    title="r/{} - Wiki - {}".format(subreddit.name, wikipage.basepath),
                    is_front=True,
                ),
            )
        index_template = self.environment.get_template("subredditwikiindexpage.html.jinja")
        page = index_template.render(
            to_root="../../..",
            subreddit=subreddit,
            has_wiki=True,
        )
        result.add(
            HtmlPage(
                path="r/{}/wiki/_pageindex".format(subreddit.name),
                content=self.minify_html(page),
                title="r/{} - Wiki - Index".format(subreddit.name),
                is_front=True,
            ),
        )
        # default wiki redirect
        result.add(
            Redirect(
                source="r/{}/wiki/".format(subreddit.name),
                target="r/{}/wiki/_pageindex".format(subreddit.name),
                title="r/{} - Wiki".format(subreddit.name),
                is_front=True,
            )
        )
        return result

    def render_subreddit_rules(self, subreddit):
        """
        Render the rules page of a subreddit.

        @param subreddit: subreddit to render
        @type subreddit: L{arcticzim.db.models.Subreddit}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        result = RenderResult()
        template = self.environment.get_template("subredditrulespage.html.jinja")
        page = template.render(
            to_root="../../..",
            subreddit=subreddit,
            has_wiki=(len(subreddit.wikipages) > 0),
        )
        result.add(
            HtmlPage(
                path="r/{}/rules/".format(subreddit.name),
                content=self.minify_html(page),
                title="r/{} - Rules".format(subreddit.name),
                is_front=True,
            ),
        )
        return result


    def render_user_posts(self, user, sort="top", posts=None, num_posts=None):
        """
        Render the posts of a user.

        If posts is specified, it should be an iterable yielding lists
        of posts sorted by the required order. If it is
        not specified, it will be generated from user.posts

        @param user: user whose posts to render
        @type user: L{arcticzim.db.models.User}
        @param sort: by which metric posts should be sorted
        @type sort: L{str}
        @param posts: an iterable yielding the posts made by this user in a sorted order as described above
        @type posts: iterable yielding L{arcticzim.db.models.Post} or L{None}
        @param num_posts: number of posts made by this user
        @type num_posts: L{int} or L{None}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """

        # general preparations
        if posts is None:
            num_posts = len(user.posts)
            if sort == "top":
                posts = sorted(user.posts, key=lambda x: x.score, reverse=True)
            elif sort == "new":
                posts = sorted(user.posts, key=lambda x: x.created_utc, reverse=True)
            else:
                raise Exception("Invalid subreddit post sort order!")
        else:
            assert num_posts is not None
        result = RenderResult()
        items_in_result = 0
        if sort == "top":
            # by default, redirect to top posts page 1
            result.add(
                Redirect(
                    "u/{}/".format(user.name),
                    "u/{}/posts_top_page_1/".format(user.name),
                    title="u/{}".format(user.name),
                    is_front=True,
                ),
            )
            items_in_result += 1
        if num_posts == 0:
            # user has no posts
            empty_template = self.environment.get_template("useremptypage.html.jinja")
            page = empty_template.render(
                to_root="../../..",
                user=user,
                part="posts",
            )
            result.add(
                HtmlPage(
                    path="u/{}/posts_{}_page_1/".format(user.name, sort),
                    content=self.minify_html(page),
                    title="u/{} - Posts".format(user.name),
                    is_front=False,
                ),
            )
            yield result
            return
        # prepare rendering the user pages
        list_page_template = self.environment.get_template("userpostspage.html.jinja")
        num_pages = math.ceil(num_posts / POSTS_PER_PAGE)
        bucketmaker = BucketMaker(POSTS_PER_PAGE)
        # render the user post list pages
        page_index = 1
        for post in posts:
            bucket = bucketmaker.feed(post)
            if bucket is not None:
                items_in_result += self._render_user_postlist_page(
                    user=user,
                    posts=bucket,
                    page_index=page_index,
                    num_pages=num_pages,
                    template=list_page_template,
                    sort=sort,
                    result=result,
                )
                if items_in_result >= MAX_ITEMS_PER_RESULT:
                    yield result
                    result = RenderResult()
                    items_in_result = 0
                page_index += 1
        bucket = bucketmaker.finish()
        if bucket is not None:
            items_in_result += self._render_user_postlist_page(
                user=user,
                posts=bucket,
                page_index=page_index,
                num_pages=num_pages,
                template=list_page_template,
                sort=sort,
                result=result,
            )
            if items_in_result >= MAX_ITEMS_PER_RESULT:
                yield result
                result = RenderResult()
                items_in_result = 0
        yield result

    def _render_user_postlist_page(self, user, posts, page_index, num_pages, template, sort, result):
        """
        Helper function for rendering a list page of posts made by a user.

        This function renders a page of posts made by the user and adds the
        rendered page to the result.

        @param user: user this page is for
        @type user: L{arcticzim.db.models.User}
        @param posts: list of posts that should be listed on this page
        @type posts: L{list} of L{arcticzim.db.models.Post}
        @param page_index: index of current page (1-based)
        @type page_index: L{int}
        @param num_pages: total number of pages
        @type num_pages: L{int}
        @param template: template that should be rendered
        @type template: L{jinja2.Template}
        @param sort: sort order this page is for
        @type sort: L{str}
        @param result: result the rendered page should be added to
        @type result: L{RenderResult}
        @return: the number of items added to the render result
        @rtype: L{int}
        """
        page = template.render(
            to_root="../../..",
            user=user,
            posts=posts,
            num_pages=num_pages,
            cur_page=page_index,
            sort=sort,
        )
        result.add(
            HtmlPage(
                path="u/{}/posts_{}_page_{}/".format(user.name, sort, page_index),
                content=self.minify_html(page),
                title="u/{} - {} Posts - Page {}".format(user.name, sort, page_index),
                is_front=False,
            ),
        )
        return 1  # 1 item added

    def render_user_comments(self, user, sort="top", comments=None, num_comments=None):
        """
        Render the comments of a user.

        If comments is specified, it should be an iterable yielding lists
        of comments sorted by the required order. If it is
        not specified, it will be generated from user.comments

        @param user: user whose comments to render
        @type user: L{arcticzim.db.models.User}
        @param sort: by which metric posts should be sorted
        @type sort: L{str}
        @param comments: an iterable yielding the comments made by the user in a sorted order as described above
        @type comments: iterable yielding L{arcticzim.db.models.Comment} or L{None}
        @param num_comments: number of comments made by this user
        @type num_comments: L{int} or L{None}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """

        # general preparations
        if comments is None:
            num_comments = len(user.comments)
            if sort == "top":
                comments = sorted(user.comments, key=lambda x: x.score, reverse=True)
            elif sort == "new":
                comments = sorted(user.comments, key=lambda x: x.created_utc, reverse=True)
            else:
                raise Exception("Invalid subreddit comment sort order!")
        else:
            assert num_comments is not None
        result = RenderResult()
        items_in_result = 0
        if num_comments == 0:
            # user has no comments
            empty_template = self.environment.get_template("useremptypage.html.jinja")
            page = empty_template.render(
                to_root="../../..",
                user=user,
                part="comments",
            )
            result.add(
                HtmlPage(
                    path="u/{}/comments_{}_page_1/".format(user.name, sort),
                    content=self.minify_html(page),
                    title="u/{} - Comments".format(user.name),
                    is_front=False,
                ),
            )
            yield result
            return
        # prepare rendering the user pages
        list_page_template = self.environment.get_template("usercommentspage.html.jinja")
        num_pages = math.ceil(num_comments / POSTS_PER_PAGE)
        bucketmaker = BucketMaker(POSTS_PER_PAGE)
        # render the user post list pages
        page_index = 1
        for comment in comments:
            bucket = bucketmaker.feed(comment)
            if bucket is not None:
                items_in_result += self._render_user_commentslist_page(
                    user=user,
                    comments=bucket,
                    page_index=page_index,
                    num_pages=num_pages,
                    template=list_page_template,
                    sort=sort,
                    result=result,
                )
                if items_in_result >= MAX_ITEMS_PER_RESULT:
                    yield result
                    result = RenderResult()
                    items_in_result = 0
                page_index += 1
        bucket = bucketmaker.finish()
        if bucket is not None:
            items_in_result += self._render_user_commentslist_page(
                user=user,
                comments=bucket,
                page_index=page_index,
                num_pages=num_pages,
                template=list_page_template,
                sort=sort,
                result=result,
            )
            if items_in_result >= MAX_ITEMS_PER_RESULT:
                yield result
                result = RenderResult()
                items_in_result = 0
        yield result

    def _render_user_commentslist_page(self, user, comments, page_index, num_pages, template, sort, result):
        """
        Helper function for rendering a list page of comments made by a user.

        This function renders a page of comments made by the user and adds the
        rendered page to the result.

        @param user: user this page is for
        @type user: L{arcticzim.db.models.User}
        @param comments: list of comments that should be listed on this page
        @type comments: L{list} of L{arcticzim.db.models.Comment}
        @param page_index: index of current page (1-based)
        @type page_index: L{int}
        @param num_pages: total number of pages
        @type num_pages: L{int}
        @param template: template that should be rendered
        @type template: L{jinja2.Template}
        @param sort: sort order this page is for
        @type sort: L{str}
        @param result: result the rendered page should be added to
        @type result: L{RenderResult}
        @return: the number of items added to the render result
        @rtype: L{int}
        """
        page = template.render(
            to_root="../../..",
            user=user,
            comments=comments,
            num_pages=num_pages,
            cur_page=page_index,
            sort=sort,
        )
        result.add(
            HtmlPage(
                path="u/{}/comments_{}_page_{}/".format(user.name, sort, page_index),
                content=self.minify_html(page),
                title="u/{} - {} Comments - Page {}".format(user.name, sort, page_index),
                is_front=False,
            ),
        )
        return 1  # 1 item added

    def render_user_stats(self, user, stats):
        """
        Render the statistics page of a user.

        @param user: user to render
        @type user: L{arcticzim.db.models.User}
        @param stats: stats to render
        @type stats: L{arcticzim.zimbuild.statistics.PostListStatistics}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        result = RenderResult()
        template = self.environment.get_template("userstatspage.html.jinja")
        page = template.render(
            to_root="../../..",
            user=user,
            stats=stats,
        )
        result.add(
            HtmlPage(
                path="u/{}/statistics/".format(user.name),
                content=self.minify_html(page),
                title="u/{} - Staticstics".format(user.name),
                is_front=True,
            ),
        )
        return result

    def render_index(self, subreddit_infos):
        """
        Render the index page.

        @param subreddit_infos: a list of subreddits that should be listed on the index page
        @type subreddit_infos: L{list} of L{SubredditInfo}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        result = RenderResult()
        template = self.environment.get_template("indexpage.html.jinja")
        page = template.render(
            to_root=".",
            subreddit_infos=subreddit_infos,
        )
        result.add(
            HtmlPage(
                path="index.html",
                title="Welcome to ArcticZim!",
                content=self.minify_html(page),
                is_front=True,
            ),
        )
        return result

    def render_subreddit_list(self, subreddit_infos):
        """
        Render the list of subreddits.

        @param subreddit_infos: a list of all subreddits, sorted.
        @type subreddit_infos: L{list} of L{SubredditInfo}
        @yields: the generated pages and redirects
        @rtype: L{RenderResult}
        """
        result = RenderResult()
        result.add(
            Redirect(
                "subreddits/",
                "subreddits/subreddits_page_1/",
                title="All Subreddits",
                is_front=True,
            ),
        )
        list_page_template = self.environment.get_template("subredditlistpage.html.jinja")
        num_subreddits = len(subreddit_infos)
        num_pages = math.ceil(num_subreddits / SUBREDDITS_PER_PAGE)
        bucketmaker = BucketMaker(SUBREDDITS_PER_PAGE)
        # render the user post list pages
        page_index = 1
        items_in_result = 0
        for subreddit_info in subreddit_infos:
            bucket = bucketmaker.feed(subreddit_info)
            if bucket is not None:
                items_in_result += self._render_subreddit_list_page(
                    subreddit_infos=bucket,
                    page_index=page_index,
                    num_pages=num_pages,
                    template=list_page_template,
                    result=result,
                )
                if items_in_result >= MAX_ITEMS_PER_RESULT:
                    yield result
                    result = RenderResult()
                    items_in_result = 0
                page_index += 1
        bucket = bucketmaker.finish()
        if bucket is not None:
            items_in_result += self._render_subreddit_list_page(
                subreddit_infos=bucket,
                page_index=page_index,
                num_pages=num_pages,
                template=list_page_template,
                result=result,
            )
            if items_in_result >= MAX_ITEMS_PER_RESULT:
                yield result
                result = RenderResult()
                items_in_result = 0
        yield result

    def _render_subreddit_list_page(self, subreddit_infos, page_index, num_pages, template, result):
        """
        Helper function for rendering a list page of the subreddit list.

        This function renders a page of subreddit infos and adds the
        rendered page to the result.

        @param subreddit_infos: list of subreddit_infos that should be listed on this page
        @type subreddit_infos: L{list} of L{SubredditInfo}
        @param page_index: index of current page (1-based)
        @type page_index: L{int}
        @param num_pages: total number of pages
        @type num_pages: L{int}
        @param template: template that should be rendered
        @type template: L{jinja2.Template}
        @param result: result the rendered page should be added to
        @type result: L{RenderResult}
        @return: the number of items added to the render result
        @rtype: L{int}
        """
        page = template.render(
            to_root="../..",
            subreddit_infos=subreddit_infos,
            num_pages=num_pages,
            cur_page=page_index,
        )
        result.add(
            HtmlPage(
                path="subreddits/subreddits_page_{}/".format(page_index),
                content=self.minify_html(page),
                title="All Subreddits - Page {}".format(page_index),
                is_front=False,
            ),
        )
        return 1  # 1 item added

    def render_scripts(self):
        """
        Generate the js scripts.

        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        result = RenderResult()
        # jQuery
        path = get_resource_file_path("jquery", "jquery.js")
        with open(path, "r", encoding="utf-8") as fin:
            script = fin.read()
        result.add(
            Script(
                path="scripts/jquery.js",
                content=script,
                title="jquery.js",
            ),
        )
        # chartjs
        path = get_resource_file_path("chartjs", "chart.js")
        with open(path, "r", encoding="utf-8") as fin:
            script = fin.read()
        result.add(
            Script(
                path="scripts/chart.js",
                content=script,
                title="Chart.js",
            ),
        )
        # polls
        path = get_resource_file_path("poll.js")
        with open(path, "r", encoding="utf-8") as fin:
            script = fin.read()
        result.add(
            Script(
                path="scripts/poll.js",
                content=script,
                title="Poll.js",
            ),
        )
        # collapser
        path = get_resource_file_path("collapser.js")
        with open(path, "r", encoding="utf-8") as fin:
            script = fin.read()
        result.add(
            Script(
                path="scripts/collapser.js",
                content=script,
                title="Collapser.js",
            ),
        )
        # preview
        path = get_resource_file_path("preview.js")
        with open(path, "r", encoding="utf-8") as fin:
            script = fin.read()
        result.add(
            Script(
                path="scripts/preview.js",
                content=script,
                title="preview.js",
            ),
        )
        return result

    def render_info_pages(self):
        """
        Render the various information pages.

        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        result = RenderResult()
        template = self.environment.get_template("infopage.html.jinja")
        info_page = template.render(
            to_root="..",
        )
        result.add(
            HtmlPage(
                path="info/",
                title="ArcticZim - Informations",
                content=self.minify_html(info_page),
                is_front=True,
            ),
        )
        license_template = self.environment.get_template("licensepage.html.jinja")
        license_paths = {
            "jQuery": ("jquery", "LICENSE.txt"),
            "Chart.js": ("chartjs", "LICENSE.md"),
        }
        licenses = {
            k: open(get_resource_file_path(*v)).read()
            for k, v in license_paths.items()
        }
        licenses["Icons"] = 'Several icons have been taken (or use assets from) [iconsDB.com](https://iconsdb.com).'
        license_page = license_template.render(
            to_root="../..",
            licenses=licenses,
        )
        result.add(
            HtmlPage(
                path="info/licenses/",
                title="ArcticZim - Licenses",
                content=self.minify_html(license_page),
                is_front=True,
            ),
        )
        return result

    def render_global_stats(self, stats):
        """
        Render the global statistics.

        @param stats: stats to render
        @type stats: L{arcticzim.zimbuild.statistics.PostListStatistics}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        result = RenderResult()
        template = self.environment.get_template("globalstatspage.html.jinja")
        page = template.render(
            to_root="..",
            stats=stats,
        )
        result.add(
            HtmlPage(
                path="statistics/",
                content=self.minify_html(page),
                title="ArcticZim - Staticstics",
                is_front=True,
            ),
        )
        return result

    # =========== filters ===============

    def _rewrite_url_filter(self, value, to_root):
        """
        Rewrite a URL.

        @param value: URL to rewrite
        @type value: L{str}
        @param to_root: same as in the templates
        @type to_root: L{str}
        """
        value = self.filemanager.rewrite_url(value, to_root=to_root)
        value = self.reference_rewriter.rewrite_url(value, to_root=to_root)
        return value

    def _render_comment_text_filter(self, value, to_root):
        """
        Render a comment body, returning the rendered html.

        @param value: comment text to render
        @type value: L{str}
        @param to_root: same as in the templates
        @type to_root: L{str}
        @return: the rendered HTML of the comment text
        @rtype: L{str}
        """
        rewritten = self.filemanager.rewrite_urls_in_text(
            value,
            to_root=to_root,
        )
        rewritten = self.reference_rewriter.rewrite_urls_in_text(
            rewritten,
            to_root=to_root,
        )
        rendered = mistune.html(
            rewritten,
        )
        return rendered

    def _render_license_text_filter(self, value):
        """
        Render a license text, returning the rendered html.

        @param value: license text to render
        @type value: L{str}
        @return: the rendered HTML of the license text
        @rtype: L{str}
        """
        rendered = mistune.html(value)
        return rendered

    def _format_date(self, value):
        """
        Format a date.

        @param value: date to format
        @type value: L{datetime.datetime}
        @return: the formated date
        @rtype: L{str}
        """
        return value.strftime("%Y-%m-%d")

    def _format_timestamp(self, value, allow_none=False):
        """
        Format a timestamp to a human readbale date.

        @param value: timestamp to format
        @type value: L{int}
        @param allow_none: if nonzero, allow passing None as a value
        @type allow_none: L{bool}
        @return: the formated date and time
        @rtype: L{str}
        """
        if value is None:
            if allow_none:
                return "-"
            else:
                raise TypeError("Got None as value with allow_none=False!")
        return datetime.datetime.fromtimestamp(value).isoformat()

    def _first_elements(self, value, n):
        """
        Return the first n elements in a value

        @param value: list whose first elements should be returned
        @type value: L{list} or L{tuple}
        @param n: number of elements to return
        @type n: L{int}
        @return: the first n elements in value
        @rtype: L{list}
        """
        return list(value)[:n]

    def _default_index(self, value, i, default):
        """
        Return value[i] if value is defined and value[i] exists, otherwise default.

        @param value: element to get i-th element of
        @type value: any indexable or L{jinja2.Undefined}
        @param i: index/key to get
        @type i: any
        @param default: default value to return
        @type default: any
        @return: value[i] if value is defined and value[i] exists, otherwise default
        @rtype: type of value[i] or default
        """
        if isinstance(value, Undefined):
            return default
        try:
            return value[i]
        except (KeyError, IndexError):
            return default

    def _first_nonzero_filter(self, value):
        """
        Returns the first nonzero element in a list or the last one if none of them are nonzero.

        @param value: value to process
        @type value: L{list}
        @return: the first nonzero element in the list or the last element
        @rtype: as value[x]
        """
        for v in value:
            if v:
                return v
        return v

    def _render_postsummary_by_url(self, url, to_root):
        """
        Render a postsummary for a post with a specific url.

        @param url: (reddit) url to post
        @type url: L{str}
        @param to_root: same as in templates
        @type to_root: L{str}
        @return: the rendered post summary
        @rtype: L{str}
        """
        reference = parse_reddit_url(url)
        if (reference is None) or (reference["type"] != "post"):
            raise ValueError("Error: not a reddit reference")
        post_id = reference["post"]
        return self.worker.directly_render_postsummary(post_id, to_root=to_root)

    # =========== tests ===============

    def _is_date(self, obj):
        """
        Return True if obj is a date or datetime.

        @param obj: object to check
        @type obj: any
        @return: True if obj is a date or datetime
        @rtype: L{bool}
        """
        return isinstance(obj, (datetime.date, datetime.datetime))

    def _is_local_post_url(self, url):
        """
        Return True if url is a url referencing an post that is locally available.

        @param url: url to check
        @type url: L{str}
        @return: whether the url points to a post that is locally available
        @rtype: L{bool}
        """
        if not isinstance(url, str):
            return False
        parsed = parse_reddit_url(url)
        if parsed is None:
            return False
        if parsed["type"] != "post":
            return False
        return self.reference_rewriter.should_rewrite(parsed)

