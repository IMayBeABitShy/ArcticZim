"""
The worker logic for multi process rendering.

Workers receive their tasks from an inqueue fed from the builder. They
process the tasks by loading the required objects and feed them to a
renderer. The result is put into the outqueue, where the builder will
take the results and add them to the creator.

@var MARKER_WORKER_STOPPED: a symbolic constant put into the output queue when the worker is finished
@type MARKER_WORKER_STOPPED: L{str}
@var MARKER_TASK_COMPLETED: a symbolic constant put into the output queue when a task was completed
@type MARKER_TASK_COMPLETED: L{str}

@var MAX_POST_EAGERLOAD: when loading subreddits, do not eagerload if more than this number of posts are in said object
@type MAX_POST_EAGERLOAD: L{int}
@var POST_LIST_YIELD: number of story to fetch at once when rendering posts, ...
@type POST_LIST_YIELD: L{int}
"""
import contextlib
import os
import time

from sqlalchemy import select, func, desc
from sqlalchemy.orm import Session, undefer
from sqlalchemy.orm import joinedload, subqueryload, selectinload, noload, raiseload

try:
    import memray
except ImportError:
    memray = None

from .renderer import HtmlRenderer, RenderResult, SubredditInfo, SUBREDDITS_ON_INDEX_PAGE
from .statistics import query_post_stats
from ..util import ensure_iterable
from ..downloader import MediaFileManager
from ..db.models import Post, User, Comment, Subreddit


MARKER_WORKER_STOPPED = "stopped"
MARKER_TASK_COMPLETED = "completed"

MAX_POST_EAGERLOAD = 10000
MIN_POSTS_FOR_EXPLICIT_STATS = 10000
MIN_POSTS_FOR_STREAM = 10000

POST_LIST_YIELD = 2000


class Task(object):
    """
    Base class for all worker tasks.

    @cvar type: type of task
    @type type: L{str}
    """
    type = "<unset>"

    @property
    def name(self):
        """
        Return a non-unqiue id describing this task.

        @return: a name describing this task
        @rtype: L{str}
        """
        return "<unknown>"


class StopTask(Task):
    """
    Task indicating that the worker should shut down.
    """
    type = "stop"

    @property
    def name(self):
        return "stop"


class PostRenderTask(Task):
    """
    A L{Task} for rendering posts.

    @ivar post_uids: ids of posts to render, as list of post uids
    @type post_uids: L{list} of L{int}
    """
    type = "post"

    def __init__(self, post_uids):
        """
        The default constructor.

        @param post_uids: ids of stories to render, as list of post uids
        @type post_uids: L{list} of L{int}
        """
        assert isinstance(post_uids, (list, tuple))
        assert isinstance(post_uids[0], int)
        self.post_uids = post_uids

    @property
    def name(self):
        if len(self.post_uids) == 0:
            return "post_empty"
        return "posts_{}+{}".format(
            self.post_uids[0],
            len(self.post_uids) - 1,
        )

class SubredditRenderTask(Task):
    """
    A L{Task} for rendering a subreddit.

    @ivar subreddit_name: name of subreddit to render
    @type subreddit_name: L{str}
    @ivar subtask: subtask (e.g. sort order) to render
    @type subtask: L{str}
    """
    type = "subreddit"

    def __init__(self, subreddit_name, subtask):
        """
        The default constructor.

        @param subreddit_name: name of subreddit to render
        @type subreddit_name: L{str}
        @param subtask: subtask (e.g. sort order) to render
        @type subtask: L{str}
        """
        assert isinstance(subreddit_name, str)
        assert isinstance(subtask, str)
        self.subreddit_name = subreddit_name
        self.subtask = subtask

    @property
    def name(self):
        return "{}_r/{}".format(
            self.subtask,
            self.subreddit_name
        )


class UserRenderTask(Task):
    """
    A L{Task} for rendering a user.

    @ivar username: username of user to render
    @type username: L{str}
    @ivar part: part (post, comments, stats) to render
    @type part: L{str}
    @ivar sort: sort order to render
    @type sort: L{str}
    """
    type = "user"

    def __init__(self, username, part, sort):
        """
        The default constructor.

        @param username: username of user to render
        @type username: L{int}
        @param part: part (post, comments, stats) to render
        @type part: L{str}
        @param sort: sort order to render
        @type sort: L{str}
        """
        assert isinstance(username, str)
        assert isinstance(part, str)
        assert isinstance(sort, str) or (sort is None)
        self.username = username
        self.part = part
        self.sort = sort

    @property
    def name(self):
        return "{}_{}_{}".format(
            self.username,
            self.part,
            self.sort,
        )


class EtcRenderTask(Task):
    """
    A L{Task} for rendering specific, individual pages.

    @ivar subtask: the name of the subtask to perform (e.g. index)
    @type subtask: L{str}
    """
    type = "etc"

    def __init__(self, subtask):
        """
        The default constructor.

        @param subtask: name of the subtask to perform
        @type subtask: L{str}
        """
        assert isinstance(subtask, str)
        self.subtask = subtask

    @property
    def name(self):
        return "etc_{}".format(self.subtask)


class WorkerOptions(object):
    """
    Options for the worker.

    @ivar eager: eager load objects from database
    @type eager: L{bool}
    @ivar log_directory: if not None, enable logging and write log here
    @type log_directory: L{str} or L{None}
    @ivar memprofile_directory: if not None, profile memory usage and write files into this directory
    @type memprofile_directory: L{str} or L{None}
    @ivar with_stats: if nonzero, include statistics
    @type with_stats: L{bool}
    @ivar with_media: if nonzero, include media
    @type with_media: L{bool}
    @ivar with_videos: if nonzero, include videos
    @type with_videos: L{bool}
    """
    def __init__(
        self,
        eager=True,
        log_directory=None,
        memprofile_directory=None,

        with_stats=True,
        with_media=True,
        with_videos=True,
    ):
        """
        The default constructor.

        @param eager: if nonzero, eager load objects from database
        @type eager: L{bool}
        @param log_directory: if specified, enable logging and write log here
        @type log_directory: L{str} or L{None}
        @param memprofile_directory: if specified, profile memory usage and write files into this directory
        @type memprofile_directory: L{str} or L{None}
        @param with_stats: if nonzero, include statistics
        @type with_stats: L{bool}
        @param with_media: if nonzero, include media
        @type with_media: L{bool}
        @param with_videos: if nonzero, include videos
        @type with_videos: L{bool}
        """
        assert isinstance(log_directory, str) or (log_directory is None)
        assert isinstance(memprofile_directory, str) or (memprofile_directory is None)
        self.eager = eager
        self.log_directory = log_directory
        self.memprofile_directory = memprofile_directory
        self.with_stats = with_stats
        self.with_media = with_media
        self.with_videos = with_videos


class Worker(object):
    """
    The worker should be instantiated in a new process, where it will
    continuously process tasks from the main process.

    @ivar id: id of this worker
    @type id: L{int}
    @ivar inqueue: the queue providing new tasks
    @type inqueue: L{multiprocessing.Queue}
    @ivar outqueue: the queue where results will be put
    @type outqueue: L{multiprocessing.Queue}
    @ivar renderer: the renderer used to render the content
    @type renderer: L{arcticzim.zimbuild.renderer.HtmlRenderer}
    @ivar engine: engine used for database connection
    @type engine: L{sqlalchemy.engine.Engine}
    @ivar session: database session
    @type session: L{sqlalchemy.orm.Session}
    @ivar options: options for this worker
    @type options: L{WorkerOptions}

    @ivar _log_file: file used for logging
    @type _log_file: file-like object
    @ivar _last_log_time: timestamp of last log entry
    @ivar _last_log_time: L{int}
    """
    def __init__(self, id, inqueue, outqueue, engine, options, render_options):
        """
        The default constructor.

        @param id: id of this worker
        @type id: L{int}
        @param inqueue: the queue providing new tasks
        @type inqueue: L{multiprocessing.Queue}
        @param outqueue: the queue where results will be put
        @type outqueue: L{multiprocessing.Queue}
        @param engine: engine used for database connection. Be sure to dispose the connection pool first!
        @type engine: L{sqlalchemy.engine.Engine}
        @param options: options for the worrker
        @type options: L{WorkerOptions}
        @param render_options: options for the renderer
        @type render_options: L{arcticzim.zimbuild.renderer.RenderOptions}
        """
        assert isinstance(id, int)
        self.id = id
        self.inqueue = inqueue
        self.outqueue = outqueue
        self.engine = engine

        self.session = Session(engine)
        self.options = options
        self.renderer = HtmlRenderer(
            options=render_options,
            filemanager=MediaFileManager(
                session=self.session,
                enabled=self.options.with_media,
                images_enabled=self.options.with_media,
                videos_enabled=self.options.with_videos,
            ),
        )

        self.setup_logging()

        self.log("Worker initialized.")

    def setup_logging(self):
        """
        Setup the logging system.
        """
        self._last_log_time = time.time()
        if self.options.log_directory is not None:
            fn = os.path.join(
                self.options.log_directory,
                "log_worker_{}.txt".format(self.id),
            )
            self._log_file = open(fn, mode="w", encoding="utf-8")
        else:
            self._log_file = None

    def log(self, msg):
        """
        Log a message.

        @param msg: message to log
        @type msg: L{str}
        """
        assert isinstance(msg, str)
        if self._log_file is not None:
            full_msg = "[{}][+{:8.3f}s] {}\n".format(
                time.ctime(),
                time.time() - self._last_log_time,
                msg,
            )
            self._log_file.write(full_msg)
            self._log_file.flush()
        self._last_log_time = time.time()

    def run(self):
        """
        Run the worker.

        This does not start a new process, the worker should already
        have been instantiated in a new process.

        This method will run in a loop, taking new tasks from the inqueue,
        processing them and putting the results in the outqueue until a
        L{StopTask} has been received. Upon completion,
        L{MARKER_WORKER_STOPPED} will be put on the outqueue once.
        Additionally, L{MARKER_TASK_COMPLETED} is put in the queue
        after each task.
        """
        self.log("Entering mainloop.")
        running = True
        while running:
            task = self.inqueue.get(block=True)
            self.log("Received task '{}'".format(task.name))
            with self.get_task_processing_context(task=task):
                if task.type == "stop":
                    # stop the worker
                    self.log("Stopping worker...")
                    running = False
                    self._cleanup()
                elif task.type == "post":
                    self.process_post_task(task)
                elif task.type == "user":
                    self.process_user_task(task)
                elif task.type == "subreddit":
                    self.process_subreddit_task(task)
                elif task.type == "etc":
                    self.process_etc_task(task)
                else:
                    raise ValueError("Task {} has an unknown task type '{}'!".format(repr(task), task.type))
                if task.type != "stop":
                    # notify builder that a task was completed
                    self.log("Marking task as completed.")
                    self.outqueue.put(MARKER_TASK_COMPLETED)
        # send a message indicated that this worker has finished
        self.outqueue.put(MARKER_WORKER_STOPPED)
        self.log("Worker finished.")

    def _cleanup(self):
        """
        Called before the worker finishes.

        All cleanup (e.g. closing sessions) should happen here.
        """
        self.session.close()
        self.engine.dispose()

    def handle_result(self, result):
        """
        Handle a renderer result, putting it in the outqueue.

        @param result: result to handle
        @type result: L{arcticzim.zimbuild.renderer.RenderResult} or iterable of it
        """
        it = ensure_iterable(result)
        for i, subresult in enumerate(it):
            self.log("Submitting result part {}...".format(i))
            self.outqueue.put(subresult)

    def get_task_processing_context(self, task):
        """
        Return A context manager that runs while a task is being processed.

        @param task: task the context is for
        @type task: Task
        @return: context manager to use
        @rtype: a context manager
        """
        if self.options.memprofile_directory is not None:
            if memray is None:
                raise ImportError("Could not import package 'memray' required for memory profiling!")
            file_name = os.path.join(
                self.options.memprofile_directory,
                "mp_{}_{}.bin".format(self.id, task.name)
            )
            return memray.Tracker(
                destination=memray.FileDestination(
                    path=file_name,
                    overwrite=True,
                    compress_on_exit=False,
                ),
                native_traces=False,
                trace_python_allocators=False,
                follow_fork=False,  # already in fork
            )
        else:
            return contextlib.nullcontext()

    def process_post_task(self, task):
        """
        Process a received post task.

        @param task: task to process
        @type task: L{PostRenderTask}
        """
        # setup load options
        if self.options.eager:
            options = (
                undefer(Post.selftext),
                selectinload(Post.comments).undefer(Comment.body),
                joinedload(Post.subreddit),
                joinedload(Post.author),
            )
        else:
            options = (
                undefer(Post.selftext),
                selectinload(Post.comments).undefer(Comment.body),
            )
        for post_uid in task.post_uids:
            # get the post
            self.log("Retrieving post...")
            post = self.session.scalars(
                select(Post)
                .where(Post.uid == post_uid)
                .options(
                    *options,
                )
            ).first()
            self.log("Retrieved post, rendering...")
            result = self.renderer.render_post(post)
            self.log("Rendered post, submitting result...")
            self.handle_result(result)
            self.log("Done.")

    def process_subreddit_task(self, task):
        """
        Process a received subreddit task.

        @param task: task to process
        @type task: L{SubredditRenderTask}
        """
        if task.subtask == "stats":
            self.process_subreddit_stats_task(task)
        else:
            self.process_subreddit_sort_task(task)

    def process_subreddit_sort_task(self, task):
        """
        Process a received subreddit sort task.

        Note: this is called from L{Worker.process_subreddit_task}.

        @param task: task to process
        @type task: L{SubredditRenderTask}
        """
        # count posts in subreddit
        self.log("Counting posts in subreddit...")
        count_stmt = (
            select(func.count(Post.uid))
            .where(
                Post.subreddit_name == task.subreddit_name,
            )
        )
        n_posts_in_subreddit = self.session.execute(count_stmt).scalar_one()
        self.log("Found {} posts.".format(n_posts_in_subreddit))
        # load subreddit
        self.log("Loading subreddit...")
        subreddit_stmt = (
            select(Subreddit)
            .where(
                Subreddit.name == task.subreddit_name,
            )
            .options(
                raiseload(Subreddit.posts),
                raiseload(Subreddit.comments),
            )
        )
        subreddit = self.session.scalars(subreddit_stmt).first()
        self.session.expunge(subreddit)  # prevent subreddit from being modified and storing objects
        self.log("Subreddit loaded.")
        if subreddit is None:
            self.log("-> Subreddit not found!")
            self.log("Submitting empty result...")
            result = RenderResult()
            self.handle_result(result)
            self.log("Done.")
            return

        # load posts
        self.log("Starting to load posts...")
        # always use eager loading, lazy is horrible for performance here
        options = (
            selectinload(Post.comments),
            joinedload(Post.author),
            noload(Post.author, User.posts),
            noload(Post.author, User.comments),
            joinedload(Post.comments, Comment.author),
            noload(Post.comments, Comment.author, User.posts),
            noload(Post.comments, Comment.author, User.comments),
        )
        execution_options = {}
        if n_posts_in_subreddit >= MIN_POSTS_FOR_STREAM:
            execution_options["yield_per"] = POST_LIST_YIELD
        post_stmt = (
            select(Post)
            .where(
                Post.subreddit_name == subreddit.name,
            )
            .order_by(
                {
                    "top": desc(Post.score),
                    "new": desc(Post.created_utc),
                }[task.subtask]
            )
            .options(
                undefer(Post.title),
                undefer(Post.url),
                *options,
            )
            .execution_options(
                **execution_options,
            )
        )
        posts = self.session.scalars(post_stmt)
        self.log("Rendering subreddit...")
        result = self.renderer.render_subreddit(
            subreddit=subreddit,
            posts=posts,
            num_posts=n_posts_in_subreddit,
            sort=task.subtask,
        )
        self.log("Submitting result...")
        self.handle_result(result)
        self.log("Done.")

    def process_subreddit_stats_task(self, task):
        """
        Process a received subreddit statistics task.

        Note: this is called from L{Worker.process_subreddit_task}.

        @param task: task to process
        @type task: L{SubredditRenderTask}
        """
        # load subreddit
        self.log("Loading subreddit...")
        subreddit_stmt = (
            select(Subreddit)
            .where(
                Subreddit.name == task.subreddit_name,
            )
            .options(
                raiseload(Subreddit.posts),
                raiseload(Subreddit.comments),
            )
        )
        subreddit = self.session.scalars(subreddit_stmt).first()
        self.session.expunge(subreddit)  # prevent subreddit from being modified and storing objects
        self.log("Subreddit loaded.")
        if subreddit is None:
            self.log("-> Subreddit not found!")
            self.log("Submitting empty result...")
            result = RenderResult()
            self.handle_result(result)
            self.log("Done.")
            return

        # collect stats
        self.log("Gathering statistics")
        stats = query_post_stats(
            session=self.session,
            post_filter=(Post.subreddit_name == task.subreddit_name),
            comment_filter=(Comment.subreddit_name == task.subreddit_name),
        )

        self.log("Rendering subreddit statistics...")
        result = self.renderer.render_subreddit_stats(
            subreddit=subreddit,
            stats=stats,
        )
        self.log("Submitting result...")
        self.handle_result(result)
        self.log("Done.")

    def process_user_task(self, task):
        """
        Process a received user task.

        @param task: task to process
        @type task: L{SubredditRenderTask}
        """
        if task.part == "posts":
            self.process_user_posts_task(task)
        elif task.part == "comments":
            self.process_user_comments_task(task)
        elif task.part == "stats":
            self.process_user_stats_task(task)
        else:
            raise ValueError("Unknown user part: {}".format(task.part))

    def process_user_posts_task(self, task):
        """
        Process the posts of a user.

        @param task: task to process
        @type task: L{UserRenderTask}
        """
        # load user
        self.log("Loading user...")
        user_stmt = (
            select(User)
            .where(
                User.name == task.username,
            )
            .options(
                raiseload(User.posts),
                raiseload(User.comments),
            )
        )
        user = self.session.scalars(user_stmt).first()
        if user is None:
            self.log("-> User not found!")
            self.log("Submitting empty result...")
            result = RenderResult()
            self.handle_result(result)
            self.log("Done.")
            return
        self.session.expunge(user)  # prevent user from being modified and storing objects
        user_name = user.name
        self.log("User loaded. Username is: {}".format(user_name))
        # count posts in subreddit
        self.log("Counting posts made by user...")
        count_stmt = (
            select(func.count(Post.uid))
            .where(
                Post.author_name == user_name,
            )
        )
        n_posts_by_user = self.session.execute(count_stmt).scalar_one()
        self.log("Found {} posts.".format(n_posts_by_user))

        # load posts
        self.log("Starting to load posts...")
        # always use eager loading, lazy is horrible for performance here
        options = (
            selectinload(Post.comments),
            selectinload(Post.author),
            # noload(Post.author, User.posts),
            # noload(Post.author, User.comments),
            noload(Post.comments, Comment.author),
            noload(Post.comments, Comment.author, User.posts),
            noload(Post.comments, Comment.author, User.comments),
        )
        execution_options = {}
        if n_posts_by_user >= MIN_POSTS_FOR_STREAM:
            execution_options["yield_per"] = POST_LIST_YIELD
        post_stmt = (
            select(Post)
            .where(
                Post.author_name == user_name,
            )
            .order_by(
                {
                    "top": desc(Post.score),
                    "new": desc(Post.created_utc),
                }[task.sort]
            )
            .options(
                undefer(Post.title),
                undefer(Post.url),
                *options,
            )
            .execution_options(
                **execution_options,
            )
        )
        posts = self.session.scalars(post_stmt)
        self.log("Rendering user posts...")
        result = self.renderer.render_user_posts(
            user=user,
            posts=posts,
            num_posts=n_posts_by_user,
            sort=task.sort,
        )
        self.log("Submitting result...")
        self.handle_result(result)
        self.log("Done.")

    def process_user_comments_task(self, task):
        """
        Process the comments of a user.

        @param task: task to process
        @type task: L{UserRenderTask}
        """
        # load user
        self.log("Loading user...")
        user_stmt = (
            select(User)
            .where(
                User.name == task.username,
            )
            .options(
                raiseload(User.posts),
                raiseload(User.comments),
            )
        )
        user = self.session.scalars(user_stmt).first()
        if user is None:
            self.log("-> User not found!")
            self.log("Submitting empty result...")
            result = RenderResult()
            self.handle_result(result)
            self.log("Done.")
            return
        self.session.expunge(user)  # prevent user from being modified and storing objects
        user_name = user.name
        self.log("User loaded. Username is: {}".format(user_name))
        # count posts in subreddit
        self.log("Counting comments made by user...")
        count_stmt = (
            select(func.count(Comment.uid))
            .where(
                Comment.author_name == user_name,
            )
        )
        n_comments_by_user = self.session.execute(count_stmt).scalar_one()
        self.log("Found {} comments.".format(n_comments_by_user))

        # load posts
        self.log("Starting to load comments...")
        # always use eager loading, lazy is horrible for performance here
        options = (
            selectinload(Comment.author),
            joinedload(Comment.post),
            joinedload(Comment.post, Post.author),
            selectinload(Comment.post, Post.comments),
            selectinload(Comment.post, Post.subreddit),
            joinedload(Comment.subreddit),
            noload(Comment.subreddit, Subreddit.posts),
            noload(Comment.subreddit, Subreddit.comments),
        )
        execution_options = {}
        if n_comments_by_user >= MIN_POSTS_FOR_STREAM:
            execution_options["yield_per"] = POST_LIST_YIELD
        comment_stmt = (
            select(Comment)
            .where(
                Comment.author_name == user_name,
            )
            .order_by(
                {
                    "top": desc(Comment.score),
                    "new": desc(Comment.created_utc),
                }[task.sort]
            )
            .options(
                undefer(Comment.body),
                *options,
            )
            .execution_options(
                **execution_options,
            )
        )
        comments = self.session.scalars(comment_stmt)
        self.log("Rendering user comments...")
        result = self.renderer.render_user_comments(
            user=user,
            comments=comments,
            num_comments=n_comments_by_user,
            sort=task.sort,
        )
        self.log("Submitting result...")
        self.handle_result(result)
        self.log("Done.")

    def process_user_stats_task(self, task):
        """
        Process a received user statistics task.

        Note: this is called from L{Worker.process_user_task}.

        @param task: task to process
        @type task: L{UserRenderTask}
        """
        # load user
        self.log("Loading user...")
        user_stmt = (
            select(User)
            .where(
                User.name == task.username,
            )
            .options(
                raiseload(User.posts),
                raiseload(User.comments),
            )
        )
        user = self.session.scalars(user_stmt).first()
        if user is None:
            self.log("-> User not found!")
            self.log("Submitting empty result...")
            result = RenderResult()
            self.handle_result(result)
            self.log("Done.")
            return
        self.session.expunge(user)  # prevent user from being modified and storing objects
        user_name = user.name
        self.log("User loaded. Username is: {}".format(user_name))

        # collect stats
        self.log("Gathering statistics")
        stats = query_post_stats(
            session=self.session,
            post_filter=(Post.author_name == user_name),
            comment_filter=(Comment.author_name == user_name),
        )

        self.log("Rendering user statistics...")
        result = self.renderer.render_user_stats(
            user=user,
            stats=stats,
        )
        self.log("Submitting result...")
        self.handle_result(result)
        self.log("Done.")

    def process_etc_task(self, task):
        """
        Process a received etc task.

        @param task: task to process
        @type task: L{EtcRenderTask}
        """
        if task.subtask == "index":
            # render the indexpage
            result = self.process_index_page_task(task)
        elif task.subtask == "subreddits":
            # render the list of subreddits
            result = self.process_subreddit_list_task(task)
        elif task.subtask == "stats":
            # render the global statistics
            self.log("Querying stats...")
            stats = query_post_stats(self.session, True, True)
            self.log("Rendering statistics...")
            result = self.renderer.render_global_stats(stats)
        elif task.subtask == "scripts":
            # the js scripts
            self.log("Including js scripts...")
            result = self.renderer.render_scripts()
        elif task.subtask == "info":
            # the info pages
            self.log("Rendering info pages...")
            result = self.renderer.render_info_pages()
        else:
            raise ValueError("Unknown etc subtask: '{}'!".format(task.subtask))
        self.log("Submitting result...")
        self.handle_result(result)
        self.log("Done.")

    def process_index_page_task(self, task):
        """
        Process a received task for the index page.

        Note: this is called from L{Worker.process_etc_task}.

        @param task: task to process
        @type task: L{EtcRenderTask}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        self.log("Rendering index page...")
        subreddit_infos = []
        post_stmt = (
            select(Post.subreddit_name, func.count(Post.uid).label("count"))
            .group_by(Post.subreddit_name)
            .order_by(desc("count"))
            .limit(SUBREDDITS_ON_INDEX_PAGE)
        )
        for subname, count in self.session.execute(post_stmt):
            subreddit_infos.append(SubredditInfo(subname, posts=count))
        result = self.renderer.render_index(subreddit_infos=subreddit_infos)
        return result

    def process_subreddit_list_task(self, task):
        """
        Process a received task for the subreddit list page.

        Note: this is called from L{Worker.process_etc_task}.

        @param task: task to process
        @type task: L{EtcRenderTask}
        @return: the rendered pages and redirects
        @rtype: L{RenderResult}
        """
        self.log("Rendering subreddit list...")
        subreddit_infos = []
        post_stmt = (
            select(Post.subreddit_name, func.count(Post.uid).label("count"))
            .group_by(Post.subreddit_name)
            .order_by(Post.subreddit_name)
        )
        for subname, count in self.session.execute(post_stmt):
            subreddit_infos.append(SubredditInfo(subname, posts=count))
        result = self.renderer.render_subreddit_list(subreddit_infos=subreddit_infos)
        return result
