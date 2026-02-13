"""
This module contains the command line interface.
"""
import argparse
import time


try:
    import multiprocessing
    multiprocessing.set_start_method("forkserver")
except Exception:
    # multiprocessing may not be available
    multiprocessing = None

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from .db.models import Base
from .db.connection import ConnectionConfig
from .importer import import_posts_from_file, import_comments_from_file, prepare_db
from .zimbuild.builder import ZimBuilder, BuildOptions
from .util import format_timedelta


def _connection_config_from_ns(ns):
    """
    Generate a connection configuration from the argparse namespace.

    @param ns: namespace containing arguments
    @type ns: L{argparse.Namespace}
    @return: the connection config
    @rtype: L{arcticzim.db.connection.ConnectionConfig}
    """
    config = ConnectionConfig(
        url=ns.database,
        verbose=(ns.verbose >= 2),
    )
    return config


def run_import(ns):
    """
    Run the import command.

    @param ns: namespace containing arguments
    @type ns: L{argparse.Namespace}
    """
    start = time.time()
    print("Connecting to database...")
    engine = _connection_config_from_ns(ns).connect()
    print("Database connection established. Creating tables if necessary...")
    Base.metadata.create_all(engine, checkfirst=True)
    print("Done. Creating databse session...")
    with Session(engine) as session:
        print("Done. Preparing database...")
        prepare_db(session)
        print("Done. Importing posts...")
        import_posts_from_file(session, path=ns.posts_file, batch_size=ns.batch_size)
        print("Done. Importing comments..")
        import_comments_from_file(session, path=ns.comments_file, batch_size=ns.batch_size)
    print("Import finished in  {}".format(format_timedelta(time.time() - start)))


def run_build(ns):
    """
    Run the build command.

    @param ns: namespace containing arguments
    @type ns: L{argparse.Namespace}
    """
    connection_config = _connection_config_from_ns(ns)
    builder = ZimBuilder(connection_config)
    build_options = BuildOptions.from_ns(ns)
    builder.build(ns.outpath, options=build_options)


def main():
    """
    The main function.
    """
    parser = argparse.ArgumentParser(description="Create ZIM files from arcticshift.")
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="be more verbose",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        help="command to execute",
    )

    # parser for the import
    import_parser = subparsers.add_parser(
        "import",
        help="Import a subreddit into a database.",
    )
    import_parser.add_argument(
        "database",
        action="store",
        help="Database to store stories in, as sqlalchemy connection URL",
    )
    import_parser.add_argument(
        "--posts-file",
        action="store",
        dest="posts_file",
        help="Path to file containing post data to import from",
    )
    import_parser.add_argument(
        "--comments-file",
        action="store",
        dest="comments_file",
        help="Path to file containing comment data to import from",
    )
    import_parser.add_argument(
        "--batch-size",
        action="store",
        type=int,
        dest="batch_size",
        default=1000,
        help="How many posts and comments to import at once",
    )

    # parser for the ZIM build
    build_parser = subparsers.add_parser(
        "build",
        help="build a ZIM file",
    )
    build_parser.add_argument(
        "database",
        action="store",
        help="database to load stories from, as sqlalchemy connection URL",
    )
    build_parser.add_argument(
        "outpath",
        action="store",
        help="path to write ZIM to",
    )
    BuildOptions.add_argparse_options(build_parser)

    ns = parser.parse_args()

    if ns.command == "import":
        run_import(ns)
    elif ns.command == "build":
        run_build(ns)
    else:
        raise RuntimeError("Unknown subcommand: {}".format(ns.command))


if __name__ == "__main__":
    main()
