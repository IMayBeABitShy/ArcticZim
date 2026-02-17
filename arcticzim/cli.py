"""
This module contains the command line interface.
"""
import argparse
import time
import os


try:
    import multiprocessing
    multiprocessing.set_start_method("forkserver")
except Exception:
    # multiprocessing may not be available
    multiprocessing = None

from sqlalchemy.orm import Session

from .db.models import Base
from .db.connection import ConnectionConfig
from .importer import import_posts_from_file, import_comments_from_file, prepare_db
from .zimbuild.builder import ZimBuilder, BuildOptions
from .downloader import download_all as download_all_media
from .fetcher import fetch_all
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


def run_fetch(ns):
    """
    Run the fetch-extra command.

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
        print("Done. Starting fetch...")
        rnd = 1
        while True:
            print("Fetch round #{}".format(rnd))
            did_fetch_something = fetch_all(session, sleep=ns.sleep)
            if ns.single:
                print("--single specified, stopping after first fetch round")
                break
            if did_fetch_something:
                print("Fetched something new, starting another fetch round in case some new references have been added.")
                rnd += 1
            else:
                print("Did not fetch anything new.")
                break
    print("Fetch finished in  {}".format(format_timedelta(time.time() - start)))


def run_build(ns):
    """
    Run the build command.

    @param ns: namespace containing arguments
    @type ns: L{argparse.Namespace}
    """
    connection_config = _connection_config_from_ns(ns)
    builder = ZimBuilder(connection_config, mediadir=ns.mediadir)
    build_options = BuildOptions.from_ns(ns)
    builder.build(ns.outpath, options=build_options)


def run_media_download(ns):
    """
    Run the download-media command.

    @param ns: namespace containing arguments
    @type ns: L{argparse.Namespace}
    """
    print("Creating media directory if neccessary...")
    if not os.path.exists(ns.mediadir):
        os.mkdir(ns.mediadir)
    connection_config = _connection_config_from_ns(ns)
    print("Done. Connecting to database...")
    engine = connection_config.connect()
    print("Done. Creating database session...")
    with Session(engine) as session:
        print("Done. Starting download.")
        download_all_media(
            session=session,
            mediadir=ns.mediadir,
            enable_post_processing=ns.post_processing,
            download_reddit_videos=ns.download_reddit_videos,
            download_external_videos=ns.download_external_videos,
            max_image_dimension=ns.max_image_dimension,
            include_comments=ns.include_comments,
        )


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
        help="Database to store objects in, as sqlalchemy connection URL",
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
        help="how many posts and comments to import at once",
    )

    fetch_parser = subparsers.add_parser(
        "fetch-extra",
        help="fetch additional data (e.g. wiki pages)",
    )
    fetch_parser.add_argument(
        "database",
        action="store",
        help="database to load objects from, as sqlalchemy connection URL",
    )
    fetch_parser.add_argument(
        "--sleep",
        action="store",
        type=float,
        default=1,
        help="how many seconds to wait between requests",
    )
    fetch_parser.add_argument(
        "--single",
        action="store_true",
        help="Perform at most a single fetch round.",
    )

    mediadownload_parser = subparsers.add_parser(
        "download-media",
        help="download media files of posts",
    )
    mediadownload_parser.add_argument(
        "database",
        action="store",
        help="database to load objects from, as sqlalchemy connection URL",
    )
    mediadownload_parser.add_argument(
        "--media-dir",
        action="store",
        dest="mediadir",
        default="arcticzim_media/",
        help="directory to store media in",
    )
    mediadownload_parser.add_argument(
        "--no-post-processing",
        action="store_false",
        dest="post_processing",
        help="disable post-processing, keeping media files unaltered.",
    )
    mediadownload_parser.add_argument(
        "--download-reddit-videos",
        action="store_true",
        dest="download_reddit_videos",
        help="download videos hosted on reddit",
    )
    mediadownload_parser.add_argument(
        "--download-external-videos",
        action="store_true",
        dest="download_external_videos",
        help="download videos NOT hosted on reddit",
    )
    mediadownload_parser.add_argument(
        "--max-image-dimension",
        action="store",
        type=int,
        dest="max_image_dimension",
        default=512,
        help="Downscale images to this many pixels on their longer side",
    )
    mediadownload_parser.add_argument(
        "--search-comments",
        action="store_true",
        dest="include_comments",
        help="also download media linked in comments",
    )

    # parser for the ZIM build
    build_parser = subparsers.add_parser(
        "build",
        help="build a ZIM file",
    )
    build_parser.add_argument(
        "database",
        action="store",
        help="database to load objects from, as sqlalchemy connection URL",
    )
    build_parser.add_argument(
        "--media-dir",
        action="store",
        dest="mediadir",
        default="arcticzim_media/",
        help="directory to store media in",
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
    elif ns.command == "fetch-extra":
        run_fetch(ns)
    elif ns.command == "download-media":
        run_media_download(ns)
    elif ns.command == "build":
        run_build(ns)
    else:
        raise RuntimeError("Unknown subcommand: {}".format(ns.command))


if __name__ == "__main__":
    main()
