"""
This module manages the build of the ZIM files.

It handles the ZIM creator, add basic content, instantiates the workers,
issues them their tasks and adds the result to the creator.

@var MAX_OUTSTANDING_TASKS: max size of the task queue
@type MAX_OUTSTANDING_TASKS: L{int}
@var MAX_RESULT_BACKLOG: max size of the task result queue
@type MAX_RESULT_BACKLOG: L{int}
@var POSTS_PER_TASK: number of post IDs to send per worker tasks
@type POSTS_PER_TASK: L{int}
"""
import multiprocessing
import threading
import queue
import datetime
import time
import os
import contextlib
import math
import pdb
import signal
import pathlib

from scss.compiler import Compiler as ScssCompiler
from scss.namespace import Namespace as ScssNamespace
from scss.types import String as ScssString
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from libzim.writer import Creator, Item, StringProvider, FileProvider, Hint
import tqdm

try:
    import psutil
except ImportError:
    psutil = None
try:
    import setproctitle
except ImportError:
    setproctitle = None

from ..util import get_package_dir, get_resource_file_path, set_or_increment
from ..util import format_timedelta, format_size, format_number
from ..downloader import hash_url
from ..imgutils import mimetype_is_image
from ..db.models import Post, Subreddit, User, MediaFile, ARCTICZIM_USERNAME
from .renderer import HtmlPage, Redirect, JsonObject, Script, FileReferences, RenderOptions
from .worker import Worker, WorkerOptions
from .worker import StopTask, PostRenderTask, EtcRenderTask, SubredditRenderTask, UserRenderTask
from .worker import MARKER_TASK_COMPLETED, MARKER_WORKER_STOPPED
from .buckets import BucketMaker


MAX_OUTSTANDING_TASKS = 1024 * 8
MAX_RESULT_BACKLOG = 1024
POSTS_PER_TASK = 64


# =============== HELPER FUNCTIONS ================


def get_n_cores():
    """
    Return the number of cores to use.
    If multiprocessing is available, this is the number of cores available.
    Otherwise, this will be 1.

    @return: the number of cores to use.
    @rtype: L{int}
    """
    if multiprocessing is not None:
        return multiprocessing.cpu_count()
    else:
        return 1


def config_process(name, nice=0, ionice=0):
    """
    Configure the current OS process.

    This function expects the linux values and will try to guess the
    approximate windows values.

    @param name: name for the current process
    @type name: L{str}
    @param nice: new nice value for current process (-21->19 (lowest))
    @type nice: L{int}
    @param ionice: new io nice value for the process (0->17 (lowest))
    @type ionice: L{int}
    """
    # name
    if setproctitle is not None:
        setproctitle.setproctitle(name)
    # nice and ionice
    if psutil is not None:
        p = psutil.Process()
        if psutil.LINUX:
            p.nice(nice)
            p.ionice(psutil.IOPRIO_CLASS_BE, ionice)
        else:
            if nice > 0:
                nv = psutil.ABOVE_NORMAL_PRIORITY_CLASS
            elif nice < 0:
                nv = psutil.BELOW_NORMAL_PRIORITY_CLASS
            else:
                nv = psutil.NORMAL_PRIORITY_CLASS
            p.nice(nv)
            if ionice < 4:
                iv = psutil.IOPRIO_HIGH
            elif ionice > 4:
                iv = psutil.IOPRIO_LOW
            else:
                iv = psutil.IOPRIO_NORMAL
            p.ionice(iv)


def config_thread(name):
    """
    Configure the current OS thread.

    @param name: new name of the thread
    @type name: L{str}
    """
    if setproctitle is not None:
        setproctitle.setthreadtitle(name)


# ================ DEBUG HELPER ============


def on_pdb_interrupt(sig, frame):
    """
    Called on an SIGUSR1 interrupt to start pdb debugging.
    """
    pdb.Pdb().set_trace(frame)


try:
    signal.signal(signal.SIGUSR1, on_pdb_interrupt)
except Exception:
    pass


# =============== ITEM DEFINITIONS ================


class HtmlPageItem(Item):
    """
    A L{libzim.writer.Item} for HTML pages.
    """
    def __init__(self, path, title, content, is_front=True):
        """
        The default constructor.

        @param path: path of the page in the ZIM file
        @type path: L{str}
        @param title: title of the page
        @type title: L{str}
        @param content: the content of the page
        @type content: L{str}
        @param is_front: if this is nonzero, set this as a front article
        @type is_front: L{bool}
        """
        super().__init__()
        self._path = path
        self._title = title
        self._content = content
        self._is_front = is_front

    def get_path(self):
        return self._path

    def get_title(self):
        return self._title

    def get_mimetype(self):
        return "text/html"

    def get_contentprovider(self):
        return StringProvider(self._content)

    def get_hints(self):
        return {
            Hint.FRONT_ARTICLE: self._is_front,
            Hint.COMPRESS: True,
        }


class JsonItem(Item):
    """
    A L{libzim.writer.Item} for json.
    """
    def __init__(self, path, title, content):
        """
        The default constructor.

        @param path: path of to store item in ZIM file
        @type path: L{str}
        @param title: title of the json file
        @type title: L{str}
        @param content: the content of the json file
        @type content: L{str}
        """
        super().__init__()
        self._path = path
        self._title = title
        self._content = content

    def get_path(self):
        return self._path

    def get_title(self):
        return self._title

    def get_mimetype(self):
        return "application/json"

    def get_contentprovider(self):
        return StringProvider(self._content)

    def get_hints(self):
        return {
            Hint.FRONT_ARTICLE: False,
            Hint.COMPRESS: True,
        }


class ScriptItem(Item):
    """
    A L{libzim.writer.Item} for a js script.
    """
    def __init__(self, path, title, content):
        """
        The default constructor.

        @param path: path of to store item in ZIM file
        @type path: L{str}
        @param title: title of the js file
        @type title: L{str}
        @param content: the content of the js file
        @type content: L{str}
        """
        super().__init__()
        self._path = path
        self._title = title
        self._content = content

    def get_path(self):
        return self._path

    def get_title(self):
        return self._title

    def get_mimetype(self):
        return "text/javascript"

    def get_contentprovider(self):
        return StringProvider(self._content)

    def get_hints(self):
        return {
            Hint.FRONT_ARTICLE: False,
            Hint.COMPRESS: True,
        }


class StylesheetItem(Item):
    """
    A L{libzim.writer.Item} for the CSS stylesheet.
    """
    def __init__(self, theme="light"):
        """
        The default constructor.

        @param theme: theme to render
        @type theme: L{str}
        """
        super().__init__()
        self._theme = theme

    def get_path(self):
        return "style_{}.css".format(self._theme)

    def get_title(self):
        return "CSS Stylesheet"

    def get_mimetype(self):
        return "text/css"

    def get_contentprovider(self):
        path = get_resource_file_path("style.scss")
        with open(path, "r") as fin:
            scss = fin.read()
        namespace = ScssNamespace()
        namespace.set_variable("$theme", ScssString(self._theme))
        compiler = ScssCompiler(
            root=pathlib.Path(get_package_dir()),
            search_path=["resources"],
            live_errors=False,  # raise exception when compilation fails
            namespace=namespace,
        )
        css = compiler.compile_string(scss)
        return StringProvider(css)

    def get_hints(self):
        return {
            Hint.FRONT_ARTICLE: False,
            Hint.COMPRESS: True,
        }


class FaviconItem(Item):
    """
    A L{libzim.writer.Item} for the favicon.
    """
    def __init__(self):
        """
        The default constructor.
        """
        super().__init__()

    def get_path(self):
        return "favicon.png"

    def get_title(self):
        return "Favicon (PNG)"

    def get_mimetype(self):
        return "image/png"

    def get_contentprovider(self):
        return FileProvider(get_resource_file_path("icons", "arcticzim_highres.png"))

    def get_hints(self):
        return {
            Hint.FRONT_ARTICLE: False,
            Hint.COMPRESS: True,
        }


class IconItem(Item):
    """
    A L{libzim.writer.Item} for various icons.
    """
    def __init__(self, icon_name):
        """
        The default constructor.

        @param icon_name: name of the icon to include
        @type icon_name: L{str}
        """
        super().__init__()
        self.icon_name = icon_name

    def get_path(self):
        return "icons/icon_{}.png".format(self.icon_name)

    def get_title(self):
        return "Icon for {}".format(self.icon_name)

    def get_mimetype(self):
        return "image/png"

    def get_contentprovider(self):
        return FileProvider(get_resource_file_path("icons", "icon_{}.png".format(self.icon_name)))

    def get_hints(self):
        return {
            Hint.FRONT_ARTICLE: False,
            Hint.COMPRESS: True,
        }


class MediaItem(Item):
    """
    A L{libzim.writer.Item} for media files.
    """
    def __init__(self, mediadir, mediafile):
        """
        The default constructor.

        @param mediadir: path to the media directory
        @type mediadir: L{str}
        @param mediafile: the media file this is for
        @type mediafile: L{arcticzim.db.models.MediaFile}
        """
        self._mediadir = mediadir
        self._uid = mediafile.uid
        self._mimetype = mediafile.mimetype
        self._url = mediafile.url

    def get_path(self):
        return "media/{}".format(self._uid)

    def get_title(self):
        return self._url

    def get_mimetype(self):
        return self._mimetype

    def get_contentprovider(self):
        return FileProvider(os.path.join(self._mediadir, hash_url(self._url)))

    def get_hints(self):
        return {
            Hint.FRONT_ARTICLE: False,
            Hint.COMPRESS: True,
        }


# =============== BUILD LOGIC =================


class BuildOptions(object):
    """
    A class containing the build options for the ZIM.

    @ivar name: human-readable identifier of the resource
    @type name: L{str}
    @ivar title: title of ZIM file
    @type title: L{str}
    @ivar creator: creator of the ZIM file content
    @type creator: L{str}
    @ivar publisher: publisher of the ZIM file
    @type publisher: L{str}
    @ivar description: description of the ZIM file
    @type description: L{str}
    @ivar language: language to use (e.g. "eng")
    @type language: L{str}
    @ivar indexing: whether indexing should be enabled or not
    @type indexing: L{bool}

    @ivar with_stats: if nonzero, include statistics
    @type with_stats: L{bool}
    @ivar with_users: if nonzero, include user pages
    @type with_users: L{bool}
    @ivar with_media: if nonzero, include media files
    @type with_media: L{str}

    @ivar use_threads: if nonzero, use threads instead of processes
    @type use_threads: L{bool}
    @ivar num_workers: number of (non-zim) workers to use
    @type num_workers: L{int}
    @ivar log_directory: if not None, enable logging and write logs into this directory
    @type log_directory: L{str} or L{None}

    @ivar eager: if nonzero, eager load objects from database
    @type eager: L{bool}
    @ivar memprofile_directory: if not None, enable memory profiling and write files into this directory
    @type memprpofile_directory: L{str} or L{None}

    @ivar skip_posts: debug option to not render posts
    @type skip_posts: L{bool}
    """
    def __init__(
        self,

        # ZIM options
        name="arcticzim_eng",
        title="ArcticZim",
        creator="Reddit and ArcticShift",
        publisher="ArcticZim",
        description="ZIM file containing a part of reddit",
        language="eng",
        indexing=True,

        # content options
        with_stats=True,
        with_users=True,
        with_media=True,

        # genral build_options
        log_directory=None,

        # worker management options
        use_threads=False,
        num_workers=None,

        # worker options
        eager=True,
        memprofile_directory=None,

        # debug options
        skip_posts=False,
        ):
        """
        The default constructor.

        @param name: human-readable identifier of the resource
        @type name: L{str}
        @param title: title of ZIM file
        @type title: L{str}
        @param creator: creator of the ZIM file content
        @type creator: L{str}
        @param publisher: publisher of the ZIM file
        @type publisher: L{str}
        @param description: description of the ZIM file
        @type description: L{str}
        @param language: language to use (e.g. "eng")
        @type language: L{str}
        @param indexing: whether indexing should be enabled or not
        @type indexing: L{bool}

        @param use_threads: if nonzero, use threads instead of processes
        @type use_threads: L{bool}
        @param num_workers: number of (non-zim) workers to use (None -> auto)
        @type num_workers: L{int} or L{None}

        @param log_directory: if specified, enable logging and write logs into this directory
        @type log_directory: L{str} or L{None}

        @param eager: if nonzero, eager load objects from database
        @type eager: L{bool}
        @param memprofile_directory: if specified, enable memory profiling and write files into this directory
        @type memprofile_directory: L{str} or L{None}

        @param skip_posts: debug option to not render posts
        @type skip_posts: L{bool}
        @param with_stats: if nonzero, include statistics
        @type with_stats: L{bool}
        @param with_users: if nonzero, include user pages
        @type with_users: L{bool}
        @param with_media: if nonzero, include media files
        @type with_media: L{str}
        """
        self.name = name
        self.title = title
        self.creator = creator
        self.publisher = publisher
        self.description = description
        self.language = language
        self.indexing = indexing

        self.with_stats = with_stats
        self.with_users = with_users
        self.with_media = with_media

        self.use_threads = bool(use_threads)
        if num_workers is None:
            self.num_workers = get_n_cores()
        else:
            self.num_workers = int(num_workers)

        self.log_directory = log_directory

        self.eager = eager
        self.memprofile_directory = memprofile_directory

        self.skip_posts = skip_posts

    @staticmethod
    def add_argparse_options(parser):
        """
        Add all CLI options to the specified argparse parser.

        @param parser: argument parser to which to add the arguments
        @type parser: L{argparse.ArgumentParser}
        """
        parser.add_argument(
            "--name",
            action="store",
            dest="name",
            default="arcticzim_eng",
            help="a human readable identifier for the ZIM",
        )
        parser.add_argument(
            "--title",
            action="store",
            dest="title",
            default="ArcticZim",
            help="the title of the ZIM file",
        )
        parser.add_argument(
            "--creator",
            action="store",
            dest="creator",
            default="Reddit and Arctic Shift",
            help="creator(s) of the ZIM file content",
        )
        parser.add_argument(
            "--publisher",
            action="store",
            dest="publisher",
            default="ArcticZim",
            help="creator of the ZIM file itself",
        )
        parser.add_argument(
            "--description",
            action="store",
            dest="description",
            default="A ZIM file containing a part of reddit",
            help="a short description of the content",
        )
        parser.add_argument(
            "--language",
            action="store",
            dest="language",
            default="eng",
            help="ISO639-3 language identifier describing content language",
        )
        parser.add_argument(
            "--no-indexing",
            action="store_false",
            dest="indexing",
            help="disable indexing of ZIM",
        )

        parser.add_argument(
            "--threaded",
            action="store_true",
            help="use threads instead of processes for workers"
        )
        parser.add_argument(
            "--workers",
            action="store",
            type=int,
            default=None,
            help="use this many non-zim workers",
        )
        parser.add_argument(
            "--log-directory",
            action="store",
            default=None,
            help="enable logging and write logs into this directory",
        )
        parser.add_argument(
            "--no-stats",
            action="store_false",
            dest="with_stats",
            help="do not include statistics.",
        )
        parser.add_argument(
            "--no-media",
            action="store_false",
            dest="with_media",
            help="do not include media.",
        )
        parser.add_argument(
            "--no-users",
            action="store_false",
            dest="with_users",
            help="do not include user pages.",
        )
        parser.add_argument(
            "--lazy",
            action="store_false",
            dest="eager",
            help="Do not eager load related objects, ...",
        )
        parser.add_argument(
            "--memprofile-directory",
            action="store",
            default=None,
            help="enable memory profile and write into this directory",
        )
        parser.add_argument(
            "--debug-skip-posts",
            action="store_true",
            dest="skip_posts",
            help="do not include posts (debug option)",
        )

    @classmethod
    def from_ns(cls, ns):
        """
        Instantiate build options from a namespace returned by a parser.
        This method assumes that the parser was preprared using L{BuildOptions.add_argparse_options}.

        @param ns: namespace containg the arguments
        @type ns: L{argparse.Namespace}
        """
        bo = cls(
            name=ns.name,
            title=ns.title,
            creator=ns.creator,
            publisher=ns.publisher,
            description=ns.description,
            language=ns.language,
            indexing=ns.indexing,

            use_threads=ns.threaded,
            num_workers=ns.workers,
            log_directory=ns.log_directory,
            eager=ns.eager,
            memprofile_directory=ns.memprofile_directory,

            with_stats=ns.with_stats,
            with_users=ns.with_users,
            with_media=ns.with_media,
            skip_posts=ns.skip_posts,
        )
        return bo

    def get_metadata_dict(self):
        """
        Return a dictionary encoding the ZIM metadata described by this file.

        Additional metadata will likely be added.

        @return: a dictionary containing the metadata of this ZIM file.
        @rtype: L{bool}
        """
        tags = [
            "_sw:no",
            "_ftindex:" + ("yes" if self.indexing else "no"),
            "_pictures:" + ("yes" if self.with_media else "no"),
            "_videos:" + ("yes" if self.with_media else "no"),
            "_category:reddit",
        ]
        metadata = {
            "Name": self.name,
            "Title": self.title,
            "Creator": self.creator,
            "Date": datetime.date.today().isoformat(),
            "Publisher": self.publisher,
            "Description": self.description,
            "Language": self.language,
            "Tags": ";".join(tags),
            "Scraper": "arcticzim",
        }
        return metadata

    def get_worker_options(self):
        """
        Return the worker options the worker should use.

        @return: options for the worker.
        @rtype: L{arcticzim.zimbuild.worker.WorkerOptions}
        """
        options = WorkerOptions(
            eager=self.eager,
            memprofile_directory=self.memprofile_directory,
            log_directory=self.log_directory,
            with_stats=self.with_stats,
            with_media=self.with_media,
        )
        return options

    def get_render_options(self):
        """
        Return the render options the renderer should use.

        @return: options for the renderer
        @rtype: L{arcticzim.zimbuild.renderer.RenderOptions}
        """
        options = RenderOptions(
            with_stats=self.with_stats,
            with_users=self.with_users,
        )
        return options


class ZimBuilder(object):
    """
    The ZimBuilder manages the ZIM build process.

    @ivar inqueue: the queue where tasks will be put
    @type inqueue: L{multiprocessing.Queue} or L{queue.Queue}
    @ivar outqueue: the queue containing the task results
    @type outqueue: L{multiprocessing.Queue} or L{queue.Queue}
    @ivar connection_config: configuration for database connection
    @type connection_config: L{arcticzim.db.connection.ConnectionConfig}
    @ivar num_files_added: a dict mapping a filetype to the number of files of that type
    @type num_files_added: L{dict} of L{str} -> L{int}
    @ivar next_worker_id: ID to give next worker
    @type next_worker_id: L{int}
    @ivar mediadir: location where media files are stored
    @type mediadir: L{str}
    """
    def __init__(self, connection_config, mediadir):
        """
        The default constructor.

        @param connection_config: configuration for database connection
        @type connection_config: L{arcticzim.db.connection.ConnectionConfig}
        @param mediadir: location where media files are stored
        @type mediadir: L{str}
        """
        self.connection_config = connection_config

        self.inqueue = None
        self.outqueue = None
        self.num_files_added = {}
        self.media_file_references = set()
        self.next_worker_id = 0
        self.mediadir = mediadir

    def _init_queues(self, options):
        """
        Initialize the queues.

        @param options: build options
        @type options: L{BuildOptions}
        """
        if options.use_threads:
            self.inqueue = queue.Queue(maxsize=MAX_OUTSTANDING_TASKS)
            self.outqueue = queue.Queue(maxsize=MAX_RESULT_BACKLOG)
        else:
            self.inqueue = multiprocessing.Queue(maxsize=MAX_OUTSTANDING_TASKS)
            self.outqueue = multiprocessing.Queue(maxsize=MAX_RESULT_BACKLOG)

    def cleanup(self):
        """
        Perform clean up tasks.
        """
        pass
        # self.session.close()
        # self.engine.dispose()

    def log(self, msg, end="\n"):
        """
        Log a message.

        @param msg: message to log
        @type msg: L{str}
        @param end: suffix for the message, defaults to newline
        @type end: L{str}
        """
        # note: this is primarily a simple replacement for historical functionality.
        print(msg, end=end)

    def build(self, outpath, options):
        """
        Build a ZIM.

        @param outpath: path to write ZIM to
        @type outpath: L{str}
        @param options: build options for the ZIM
        @type options: L{BuildOptions}
        """
        # prepare build
        self.log("Preparing build...")

        start = time.time()

        # find and report options for the build
        self.log(" -> Generating ZIM creation config...")
        compression = "zstd"
        # clustersize = 8 * 1024 * 1024  # 8 MiB
        clustersize = 2 * 1024 * 1024  # 2 MiB
        verbose = True
        n_creator_workers = get_n_cores()
        n_render_workers = options.num_workers
        use_threads = options.use_threads
        self.log("        -> Path:             {}".format(outpath))
        self.log("        -> Verbose:          {}".format(verbose))
        self.log("        -> Compression:      {}".format(compression))
        self.log("        -> Cluster size:     {}".format(format_size(clustersize)))
        self.log("        -> Creator Workers:  {}".format(n_creator_workers))
        self.log("        -> Render Workers:   {}".format(n_render_workers))
        self.log("            -> using: {}".format("threads" if use_threads else "processes"))
        if not use_threads:
            self.log("            -> started using: {}".format(multiprocessing.get_start_method()))
        self.log("            -> eagerloading: {}".format("enabled" if options.eager else "disabled"))
        self.log("        -> Done.")

        # connect to database
        # initially, we did this in __init__ and set the engine+session
        # as attributes, but as the engine is not pickable, this prevents
        # the use of "forkserver" for multiprocessing
        self.log(" -> Establishing database connection... ", end="")
        engine = self.connection_config.connect()
        session = Session(engine)
        self.log("Done.")

        # initailize queues
        self.log(" -> Initiating queues...", end="")
        self._init_queues(options)
        self.log("Done.")

        # open the ZIM creator
        self.log("Opening ZIM creator, writing to path '{}'... ".format(outpath), end="")
        with Creator(outpath) \
         .config_indexing(options.indexing, options.language) \
         .config_clustersize(clustersize) \
         .config_verbose(verbose) \
         .config_nbworkers(n_creator_workers) as creator:
            self.log("Done.")

            # configurations
            self.log("Configuring ZIM... ", end="")
            creator.set_mainpath("index.html")
            self.log("Done.")

            # add metadata
            self.log("Adding metadata... ", end="")
            metadata = options.get_metadata_dict()
            for key, value in metadata.items():
                creator.add_metadata(key, value)
                set_or_increment(self.num_files_added, "metadata")
            self.log("Done.")

            # add illustrations
            self.log("Adding main illustration... ", end="")
            imagepath = get_resource_file_path("icons", "arcticzim.png")
            with open(imagepath, "rb") as fin:
                creator.add_illustration(48, fin.read())
            set_or_increment(self.num_files_added, "image")
            creator.add_item(FaviconItem())
            set_or_increment(self.num_files_added, "image")
            for icon_name in ("text", "poll", "img", "link", "video"):
                creator.add_item(IconItem(icon_name))
                set_or_increment(self.num_files_added, "image")
            self.log("Done.")

            # add general items
            self.log("Adding stylesheet... ", end="")
            creator.add_item(StylesheetItem(theme="light"))
            set_or_increment(self.num_files_added, "css")
            creator.add_item(StylesheetItem(theme="dark"))
            set_or_increment(self.num_files_added, "css")
            self.log("Done.")

            # add content
            self._add_content(creator, session, options=options)

            # finish up
            self.log("Finalizing ZIM...")
        self.log("Done.")
        self.log("Cleaning up... ", end="")
        self.cleanup()
        self.log("Done.")

        # We're done, find and report some stats about the build
        final_size = os.stat(outpath).st_size
        end = time.time()
        time_elapsed = end - start
        self.log("Finished ZIM creation in {}.".format(format_timedelta(time_elapsed)))
        self.log("Final size: {}".format(format_size(final_size)))
        self.log("Added files: ")
        for filetype, amount in self.num_files_added.items():
            if filetype != "total":
                # print total later
                self.log("    {}: {} ({})".format(filetype, amount, format_number(amount)))
        total_amount = self.num_files_added["total"]
        self.log("    total: {} ({})".format(total_amount, format_number(total_amount)))


    def _add_content(self, creator, session, options):
        """
        Add the content of the ZIM file.

        @param creator: the ZIM creator
        @type creator: L{libzim.writer.Creator}
        @param session: sqlalchemy session for data querying
        @type session: L{sqlalchemy.orm.Session}
        @param options: build options for the ZIM
        @type options: L{BuildOptions}
        """
        self.log("Adding content...")
        # --- miscelaneous pages ---
        self.log(" -> Adding miscelaneous pages...")
        n_misc_pages = 3
        if options.with_stats:
            # account for the stat task
            n_misc_pages += 1
        with self._run_stage(
            creator=creator,
            options=options,
            task_name="Adding miscelaneous pages",
            n_tasks=n_misc_pages,
            task_unit="pages",
        ):
            self._send_etc_tasks(session, with_stats=options.with_stats)
        # --- subreddits ---
        self.log(" -> Adding subreddits...")
        self.log("     -> Finding subreddits... ", end="")
        n_subreddits = session.execute(
            select(func.count(Subreddit.name))
        ).scalar_one()
        self.log("found {} subreddits.".format(n_subreddits))
        n_tasks_per_subreddit = 2
        if options.with_stats:
            # one extra task for the stats
            n_tasks_per_subreddit += 1
        with self._run_stage(
            creator=creator,
            options=options,
            task_name="Adding subreddits",
            n_tasks=n_subreddits*n_tasks_per_subreddit,
            task_multiplier=(1/n_tasks_per_subreddit),
            task_unit="subreddits",
        ):
            self._send_subreddit_tasks(session, with_stats=options.with_stats)
        # --- users ---
        if options.with_users:
            self.log(" -> Adding users...")
            self.log("     -> Finding users... ", end="")
            n_users = session.execute(
                select(func.count(User.name)).where(User.name != ARCTICZIM_USERNAME)
            ).scalar_one()
            self.log("found {} users.".format(n_users))
            n_tasks_per_user = 4
            if options.with_stats:
                # again, extra task for the stats
                n_tasks_per_user += 1
            with self._run_stage(
                creator=creator,
                options=options,
                task_name="Adding users",
                n_tasks=n_users*n_tasks_per_user,  # x tasks per user
                task_multiplier=(1/n_tasks_per_user),
                task_unit="users",
            ):
                self._send_user_tasks(session, with_stats=options.with_stats)
        else:
            self.log(" -> Skipping users!")
        # --- posts ---
        if not options.skip_posts:
            self.log(" -> Adding posts...")
            self.log("     -> Finding posts... ", end="")
            n_posts = session.execute(
                select(func.count(Post.uid))
            ).scalar_one()
            self.log("found {} posts.".format(n_posts))
            n_post_tasks = math.ceil(n_posts / POSTS_PER_TASK)
            with self._run_stage(
                creator=creator,
                options=options,
                task_name="Adding posts",
                n_tasks=n_post_tasks,
                task_unit="posts",
                task_multiplier=POSTS_PER_TASK,
            ):
                self._send_post_tasks(session)
        else:
            self.log(" -> Skipping posts!")
        # --- media ---
        if options.with_media:
            self.log(" -> Adding media...")
            n = len(self.media_file_references)
            with tqdm.tqdm(desc="Adding files", total=n, unit="files") as bar:
                while self.media_file_references:
                    uid = self.media_file_references.pop()
                    stmt = select(MediaFile).where(MediaFile.uid == uid)
                    mf = session.execute(stmt).one()[0]
                    item = MediaItem(self.mediadir, mf)
                    creator.add_item(item)
                    if mimetype_is_image(mf.mimetype):
                        set_or_increment(self.num_files_added, "image", 1)
                    else:
                        set_or_increment(self.num_files_added, "other media", 1)
                    bar.update(1)
        else:
            self.log(" -> Skipping media!")

    @contextlib.contextmanager
    def _run_stage(self, options, **kwargs):
        """
        Add the content of the ZIM file.

        @param options: zim build options
        @type options: L{BuildOptions}
        @param kwargs: keyword arguments passed to L{ZimBuilder._creator_thread}
        @type kwargs: L{dict}
        """

        if "options" not in kwargs:
            kwargs["options"] = options
        n_workers = options.num_workers

        worker_class = (threading.Thread if options.use_threads else multiprocessing.Process)
        worker_options = options.get_worker_options()
        render_options = options.get_render_options()

        # start workers
        self.log("     -> Starting workers... ", end="")
        workers = []
        for i in range(n_workers):
            worker_id = self.next_worker_id
            self.next_worker_id += 1
            worker = worker_class(
                name="Content worker {}".format(worker_id),
                target=self._worker_process,
                kwargs={
                    "id": worker_id,
                    "connection_config": self.connection_config,
                    "worker_options": worker_options,
                    "render_options": render_options,
                },
            )
            worker.daemon = True
            worker.start()
            workers.append(worker)
        self.log("Done.")

        # start the background creator thread
        # for the duration of this method, only the thread is allowed
        # to work with the creator directly
        self.log("     -> Starting creator thread... ", end="")
        creator_thread = threading.Thread(  # <-- always use threads here
            name="Creator content adder thread",
            target=self._creator_thread,
            kwargs=kwargs,
        )
        creator_thread.daemon = True
        creator_thread.start()
        self.log("Done.")

        # now it's finally time to add the tasks
        yield

        # finish up
        self.log("     -> Waiting for workers... ", end="")
        # put stop tasks on queue
        for i in range(n_workers):
            self.inqueue.put(StopTask())
        # join with all workers
        for worker in workers:
            worker.join()
            if hasattr(worker, "close"):
                worker.close()
        self.log("Done.")

        self.log("     -> Joining with creator thread... ", end="")
        creator_thread.join()
        self.log("Done.")
        self.log("     -> Done.")

    def _creator_thread(
        self,
        creator,
        options,
        task_name,
        n_tasks,
        task_unit,
        task_multiplier=1,
        ):
        """
        This method will be executed as the creator thread.

        This function is responsible to get results from the outqueue
        and adding them to the ZIM file.

        @param creator: creator for the ZIM file
        @type creator: L{libzim.writer.Creator}
        @param options: zim build options
        @type options: L{BuildOptions}
        @param taskname
        @param task_name: name of the task that is currently being processed
        @type task_name: L{str}
        @param n_tasks: number of tasks issued
        @type n_tasks: L{int}
        @param task_unit: string describing the unit of the task (e.g. stories)
        @type task_unit: L{str}
        @param task_multiplier: multiply bar advancement per task by this factor
        @type task_multiplier: L{int}
        """
        if not options.use_threads:
            # setup priority first
            config_process(name="AZ creator", nice=2, ionice=7)
        config_thread(name="Creator thread")
        # main loop - get results from queue and add them to ZIM
        running = True
        n_finished = 0
        n_items_added = 0
        with tqdm.tqdm(
            desc=task_name,
            total=n_tasks*task_multiplier,
            unit=task_unit,
            # custom bar format to deal with floats
            # bar_format="{l_bar}{bar}| {n_fmt:.2f}/{total_fmt:.2f} [{elapsed:}<{remaining:.}, {rate_fmt}{postfix}]",
            postfix={"items": n_items_added},
        ) as bar:
            while running:
                render_result = self.outqueue.get(block=True)
                bar.refresh()
                if render_result == MARKER_WORKER_STOPPED:
                    # worker finished
                    n_finished += 1
                    if n_finished == options.num_workers:
                        # all workers shut down
                        running = False
                elif render_result == MARKER_TASK_COMPLETED:
                    # task was completed
                    bar.update(task_multiplier)
                else:
                    # add the rendered objects to the ZIM
                    for rendered_object in render_result.iter_objects():
                        if isinstance(rendered_object, HtmlPage):
                            # add a HTML page
                            item = HtmlPageItem(
                                path=rendered_object.path,
                                title=rendered_object.title,
                                content=rendered_object.content,
                                is_front=rendered_object.is_front,
                            )
                            creator.add_item(item)
                            set_or_increment(self.num_files_added, "html")
                        elif isinstance(rendered_object, JsonObject):
                            # add a json object
                            item = JsonItem(
                                path=rendered_object.path,
                                title=rendered_object.title,
                                content=rendered_object.content,
                            )
                            creator.add_item(item)
                            set_or_increment(self.num_files_added, "json")
                        elif isinstance(rendered_object, Redirect):
                            # create a redirect
                            creator.add_redirection(
                                rendered_object.source,
                                rendered_object.title,
                                rendered_object.target,
                                hints={
                                    Hint.FRONT_ARTICLE: rendered_object.is_front,
                                }
                            )
                            set_or_increment(self.num_files_added, "redirect")
                        elif isinstance(rendered_object, Script):
                            # add a script
                            item = ScriptItem(
                                path=rendered_object.path,
                                title=rendered_object.title,
                                content=rendered_object.content,
                            )
                            creator.add_item(item)
                            set_or_increment(self.num_files_added, "js")
                        elif isinstance(rendered_object, FileReferences):
                            # register the file references
                            if options.with_media:
                                self.media_file_references.update(rendered_object.uids)
                        else:
                            # unknown result object
                            raise RuntimeError("Unknown render result: {}".format(type(rendered_object)))
                        set_or_increment(self.num_files_added, "total")
                        n_items_added += 1
                        bar.set_postfix({"items": n_items_added})

    def _worker_process(self, id, connection_config, worker_options, render_options):
        """
        This method will be executed as a worker process.

        The workers take tasks from the inqueue, process them and add
        the result to the outqueue.

        @param id: id of the worker
        @type id: L{int}
        @param connection_config: database connection configuration
        @type connection_config: L{arcticzim.db.connection.ConnectionConfig}
        @param worker_options: options for the worker
        @type worker_options: L{arcticzim.zimbuild.worker.WorkerOptions}
        @param render_options: options for the renderer
        @type render_options: L{arcticzim.zimbuild.renderer.RenderOptions}
        """
        # prepare the process priority
        config_process(name="AZ worker {}".format(id), nice=10, ionice=5)
        # start the worker
        worker = Worker(
            id=id,
            inqueue=self.inqueue,
            outqueue=self.outqueue,
            engine=connection_config.connect(),
            options=worker_options,
            render_options=render_options,
        )
        worker.run()

    def _send_post_tasks(self, session):
        """
        Create and send the tasks for the posts to the worker inqueue.

        @param session: sqlalchemy session for data querying
        @type session: L{sqlalchemy.orm.Session}
        """
        post_bucket_maker = BucketMaker(maxsize=POSTS_PER_TASK)
        select_post_ids_stmt = select(Post.uid)
        result = session.execute(select_post_ids_stmt)
        # create buckets and turn them into tasks
        for post in result:
            bucket = post_bucket_maker.feed(post.uid)
            if bucket is not None:
                # send out a task
                task = PostRenderTask(post_uids=bucket)
                self.inqueue.put(task)
        # send out all remaining tasks
        bucket = post_bucket_maker.finish()
        if bucket is not None:
            task = PostRenderTask(bucket)
            self.inqueue.put(task)

    def _send_subreddit_tasks(self, session, with_stats=True):
        """
        Create and send the tasks for the subreddits to the worker inqueue.

        @param session: sqlalchemy session for data querying
        @type session: L{sqlalchemy.orm.Session}
        @param with_stats: if nonzero, include stats
        @type with_stats: L{bool}
        """
        select_subreddit_names_stmt = select(Subreddit.name)
        result = session.execute(select_subreddit_names_stmt)
        subtasks = ["top", "new"]
        if with_stats:
            subtasks.append("stats")
        # send out tasks
        for subreddit in result:
            for subtask in subtasks:
                task = SubredditRenderTask(subreddit_name=subreddit.name, subtask=subtask)
                self.inqueue.put(task)

    def _send_user_tasks(self, session, with_stats=True):
        """
        Create and send the tasks for the users to the worker inqueue.

        @param session: sqlalchemy session for data querying
        @type session: L{sqlalchemy.orm.Session}
        @param with_stats: if nonzero, include stats
        @type with_stats: L{bool}
        """
        select_usernames_stmt = select(User.name).where(User.name != ARCTICZIM_USERNAME)
        result = session.execute(select_usernames_stmt)
        # send out tasks
        for user in result:
            for part in ("posts", "comments"):
                for sort in ("top", "new"):
                    task = UserRenderTask(username=user.name, part=part, sort=sort)
                    self.inqueue.put(task)
            if with_stats:
                statstask = UserRenderTask(username=user.name, part="stats", sort=None)
                self.inqueue.put(statstask)

    def _send_etc_tasks(self, session, with_stats=True):
        """
        Create and send the tasks for the miscelaneous pages to the worker inqueue.

        @param session: sqlalchemy session for data querying
        @type session: L{sqlalchemy.orm.Session}
        @param with_stats: if nonzero, include stats
        @type with_stats: L{bool}
        """
        tasknames = ["index", "scripts", "subreddits", "info"]
        if with_stats:
            tasknames.append("stats")
        for taskname in tasknames:
            task = EtcRenderTask(taskname)
            self.inqueue.put(task)
