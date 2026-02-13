"""
Helper code for debugging workers.
"""
import queue
import cmd
import pdb
import shlex
import argparse
import time

from sqlalchemy import create_engine

from .worker import Worker, WorkerOptions
from .worker import PostRenderTask, EtcRenderTask, SubredditRenderTask, UserRenderTask
from .renderer import RenderOptions
from ..util import ensure_iterable


class DiscardingQueue(queue.Queue):
    """
    A type of L{queue.Queue} which discards elements being put into it.

    @ivar verbose: if nonzero, print items put into queue
    @type verbose: L{bool}
    """
    def __init__(self, *args, verbose=False, **kwargs):
        """
        The default constructor.

        @param verbose: if nonzero, print items put into queue
        @type verbose: L{bool}
        """
        queue.Queue.__init__(self, *args, **kwargs)
        self.verbose = verbose

    def qsize(self):
        return 0

    def empty(self):
        return True

    def full(self):
        return False

    def put(self, item, block=True, timeout=None):
        if self.verbose:
            print(str(item))

    def put_nowait(self, item):
        if self.verbose:
            print(str(item))


class DebugWorker(Worker):
    """
    A variant of L{arcticzim.zimbuild.worker.Worker} for debugging purposes.

    This worker is intended to be used standalone, without the builder.
    """
    def __init__(self, parent, engine, options, render_options):
        """
        Like L{arcticzim.zimbuild.worker.Worker.__init__}, but with some
        fixed options. Additionally, the 'parent' argument must be provided,
        pointing to the associated L{WorkerDebugger}.

        @param parent: the associated worker debugger
        @type parent: L{WorkerDebugger}
        """
        Worker.__init__(
            self,
            id=0,
            inqueue=queue.Queue(),
            outqueue=DiscardingQueue(verbose=parent.verbose),
            engine=engine,
            options=options,
            render_options=render_options,
        )
        self.parent = parent

    def setup_logging(self):
        # do not setup an output file
        self._last_log_time = time.time()
        self._log_file = None

    def log(self, msg):
        assert isinstance(msg, str)
        full_msg = "[{}][+{:8.3f}s] {}\n".format(
            time.ctime(),
            time.time() - self._last_log_time,
            msg,
        )
        print(full_msg, end="")
        self._last_log_time = time.time()

    def handle_result(self, result):
        it = ensure_iterable(result)
        for i, subresult in enumerate(it):
            self.log("Submitting result part {}...".format(i))
            self.outqueue.put(subresult)
            # inform the parent that we've got a result part
            self.parent.got_result_part(i, subresult)


class WorkerDebugger(cmd.Cmd):
    """
    A CLI for debugging the worker.

    @ivar worker: the worker used
    @type worker: L{DebugWorker}
    @ivar engine: engine used for database connection
    @type engine: L{sqlalchemy.engine.Engine}
    @ivar verbose: if nonzero, be more verbose
    @type verbose: L{bool}
    """
    def __init__(self, engine, options, verbose=False):
        """
        The default constructor.

        @param engine: engine used for database connection
        @type engine: L{sqlalchemy.engine.Engine}
        @param options: options for the worker
        @type options: L{arcticzim.zimbuild.worker.WorkerOptions}
        @param verbose: if nonzero, be more verbose
        @type verbose: L{bool}
        """
        cmd.Cmd.__init__(self)

        self.engine = engine
        self.verbose = verbose
        self.worker = DebugWorker(
            parent=self,
            engine=engine,
            options=options,
            render_options=RenderOptions(),
        )

        # states for debugging targets
        self.launch_pdb_after_result_part = None

    def got_result_part(self, i, part):
        """
        Called when the worker finished submitting a result.

        @param i: index of the part
        @type i: L{int}
        @param part: render result that was received
        @type part: L{arcticzim.zimbuild.renderer.RenderResult}
        """
        if self.launch_pdb_after_result_part is not None:
            if i == self.launch_pdb_after_result_part:
                pdb.set_trace()

    def do_quit(self, line):
        """
        quit: exit this CLI.
        """
        return True

    do_q = do_close = do_exit = do_quit

    def do_launch_pdb_after_subresult(self, line):
        """
        launch_pdb_after_subresult <i|disable>: launch pdb after a subresult with this index has been received
        """
        if line == "disable":
            self.launch_pdb_after_result_part = None
        else:
            try:
                i = int(line)
            except ValueError:
                print("Not a valid index: ", line)
                return
            self.launch_pdb_after_result_part = i

    def do_process_task(self, line):
        """
        process_task <task spec...>: create a task and tell the worker to process it.
        """
        splitted = shlex.split(line)
        if len(splitted) == 0:
            print("Error: empty task spec!")
            return
        tasktype = splitted.pop(0)
        if tasktype in ():
            task_uid = int(splitted[0])
        elif tasktype in ("post", ):
            task_uid = [int(e) for e in splitted[0].split(",")]
        elif tasktype in ("etc", "subreddit"):
            task_uid = splitted[0]
        elif tasktype in ("user"):
            task_uid = splitted[0].split("-")
            task_uid = (int(task_uid[0]), task_uid[1], task_uid[2])
        task_classes_and_functions = {
            "post": (PostRenderTask, self.worker.process_post_task),
            "subreddit": (SubredditRenderTask, self.worker.process_subreddit_task),
            "user": (UserRenderTask, self.worker.process_user_task),
            "etc": (EtcRenderTask, self.worker.process_etc_task),
        }
        cls, f = task_classes_and_functions[tasktype]
        task = cls(*task_uid)
        f(task)


def main():
    """
    The main function.
    """
    parser = argparse.ArgumentParser(description="Debug helper for the workers")
    parser.add_argument("db", help="sqlalchemy database URL")
    parser.add_argument("-v", "--verbose", action="store_true", help="be more verbpse")
    parser.add_argument("--lazy", action="store_false", dest="eager", help="load objects lazily")
    ns = parser.parse_args()

    engine = create_engine(ns.db, echo=ns.verbose)
    options = WorkerOptions(eager=ns.eager)
    debugger = WorkerDebugger(engine, options=options, verbose=ns.verbose)
    debugger.cmdloop()


if __name__ == "__main__":
    main()
