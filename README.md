# ArcticZim

ArcticZim is a tool for converting data from [Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift) into ZIM files. Arctic Shift provides reddit data. ZIM files are, to put it very simple, highly compressed offline versions of websites. Thus, this is a tool for generating human-browsable offline copies of reddit.

## Project Status

This project is still a work-in-progress.

**Supported features:**

 - text-posts are implemented
 - media posts (both images, galleries and videos) are implemented and optional
 - polls are implemented
 - comment trees rendered
   - can be collapsed
 - subreddit pages are implemented
   - lists of posts are shown in both top and newest order
   - preview feature has already been implemented
 - user pages
 - statistics (global, subreddit, user)

**Missing/planned features:**

 - crossposts
 - subreddit wikis
 - various style/layout improvements, especially for mobile
 - comment media
 - selective generation of ZIM files

## Usage

### Step 1: Installation

We obviously need to install ArcticZim first.

**First,** you'll need `git`, python3`, a corresponding `pip`. In case you are using a somewhat rare system architecture, you may need to ensure that your system can build C libraries.

**Second**, optionally setup a virtual environment. This will reduce the amount of other problems you may encounter. If you are experienced with python, you can choose to either omit this step or use another virtual environment tool.

For this step, go into the directory you want to use for ArcticZim (e.g. `mkdir ~/arcticzim/; cd ~/arcticzim/`), then execute `python3 -m venv arcticzim_ve/`.

Then, run `source arcticzim_ve/bin/activate`. You'll need to run this command every time you run ArcticZim (but probably only once per terminal session).#

Now, for our **third step**, install ArcticZim via `pip install -U "arcticzim[integration,optimize] @ git+https://github.com/IMayBeABitShy/ArcticZim.git"`.

**Finally, ** verify the installation by running `arcticzim --help`. You should get a long message explaining the usage of ArcticZim.

**Alternative installation method:** If you have `pipx`, you can skip the virtual environment setup. Try running `pipx install -U "arcticzim[integration,optimize] @ git+https://github.com/IMayBeABitShy/ArcticZim.git"`. Note that this is untested.

### Step 2: preparing a database

ArcticZim uses a database to store the parsed data and find related objects. You'll need some sort of database to use Arctic Shift. The most simple one to use is `sqlite3`, which probably comes preinstalled on your system. You'll get significant performance benefits by using a more advanced database like Postgresql, but only if you have a really fast connection to that database, ideally having it run on the same machine as ArcticZim.

ArcticZim expects a database URL for most of its operations. See the [sqlalchemy documentation](https://docs.sqlalchemy.org/en/21/core/engines.html) for details. When using `sqlite3`, an example database url would be `sqlite:///db.sqlite`, which will create a file named `db.sqlite` in the current directory. Using other databases may require extra dependencies and/or more complex database URLs. Take note of the database URL, as you'll need it a couple of times.

### Step 3: Getting the post and comment data

Now we need to get the data from Arctic Shift. Currently, ArcticZim can not automatically download that data. You'll have to go to [the Arctic Shift download tool](https://arctic-shift.photon-reddit.com/download-tool) and enter the subreddit name. Make sure to check both "download posts" and "download comments". You'll be prompted to save **two** files. If you don't get any prompt, use a different webbrowser. Save those two files to some working directory.

Repeat this step for every subreddit you wish to include.

### Step 4: Importing the post and comment data

Simply run `arcticzim import --posts-file <path/to/posts/file> --comments-file <path/to/comments/file> <database_url>`, substituting the necessary values.

**Example:** let's assume you've downloaded r/kiwix, then the command would be `arcticzim import --posts-file r_kiwix_posts.jsonl --comments-file r_kiwix_comments.jsonl "sqlite:///db.sqlite"`

### Step 5: Download media

Now, let's download images, videos and so on. This step is optional, you can choose not to download any media.

Simply run `arcticzim download-media --download-reddit-videos --download-external-videos --media-dir <path/to/store/media/in> <database-url>` and wait.

Note that `ffmpeg` may be required to download videos. If you don't want to download videos, you can simply skip those options.

### Step 6: Build the ZIM file.

It's time to build the ZIM file. Now, before we get started, here are some important notes:

**Note:** depending on the amount of imported data, this step may take a significant amount of time. Optimizing you database setup (and running a couple of analyzes) will greatly help here. The build can not be paused.

**Note:** this step may demand a high amount of ressources. While working with only a couple of smaller subreddits should work without problems even on low-end devices, building ZIMs for large subreddits (or many small ones) may demand more resources. Furthermore, ArcticZim will benefit greatly from having more ressources available. Generally speaking, more content demands more RAM, while more CPU cores will improve build speed (at the cost of more RAM), whereas a faster disk will generate an overall speed improvement (but may be bottlenecked by the CPU cores). Faster CPU cores will increase performance without the higher RAM demand. If you want to reduce both the amount of RAM and time needed, consider building the ZIM without statistics (`--no-stats`).

The build command is `arcticzim build`. Some arguments are needed and many more options available, so run `arcticzim build --help` to get an overview.

An example command would be: `arcticzim -v build --lazy "sqlite:///db.sqlite" example.zim`.

Congratulations, you've finished.

## Contributing

Contributions to this project are welcome. In this section you'll find some informations to help you out.

### General guidelines

When working on ArcticZim, please keep the following design guidelines in mind:

- **python:** make sure to follow the PEP8 style guide, but feel free to ignore line length.
- **design:** ArcticZim focuses on mimicing the old reddit design somewhat. However, usability and practicality should come first. Do not be afraid of going against the classic reddit look. However, the general "feeling" of high information density should be preserved.
- **javascript:** The ZIM file should be browsable and usable without javascript. Using javascript to enhance the user experience (e.g. previews) is very much welcome, provided that the important features of the ZIM remains browsable even without javascript enabled. As the ZIMs should work offline, be sure that you do not use external dependencies (except those that you can include inside the ZIM file).
- **css/scss:** ArcticZim uses SCSS, which will be compiled into CSS. Note that (to my knowledge) all valid CSS is also valid SCSS, so you can also simply use CSS instead. The beginning of the file contains a couple of variable definitions, primarily about the color scheme. Whenever you add a rule to the stylesheet, make sure to use only those variables for colors in order to faciliate an easier "dark mode" later on. You are free to add more color variables as needed, though.
- **templates:** ArcticZim uses [Jinja2 templates](https://jinja.palletsprojects.com/en/stable/templates/), which will be rendered to HTML. A particularity about this project is the use of the `to_root` variable, which should be used when referring to any page, as absolute links are forbidden. For example, use `<A href="{{ to_root }}/r/kiwix">r/kiwix</A>`. Also, when designing pages, try to make them modular using `{% block someblock %}`. Always inherit from some template (usually `base.html.jinja`). Try to separate templates into one template for the page itself and one for the content. Do not reinvent the wheel, try to use existing templates when possible.

Note that these rules aren't absolute (except the one about absolute links), but merely guidelines. Going against them **without good reason** makes it less likely for a PR to be accepted, though.

### High-level overview

This section gives you a high level overview of the architecture. The architecture is based (as in, copy-pasted and adjusted) from [ZimFiction](https://github.com/IMayBeABitShy/zimfiction).

- **database models:** Arctic Zim uses [`sqlalchemy` ORM models](https://www.sqlalchemy.org/). These models are defined in `arcticzim.db.models`. They contain all the data about users, comments, ... . A particularity is that each `Post` contains a `root_comment`, which is a comment that serves as the parent for top-level comments.
- **the importer:** the importer parses the datasets obtained from Arctic Shift and inserts them into the database. It's defined in `arcticzim.importer`. Note that we use batch-processing to improve performance.
- **the downloader:** fetches relevant media files. It has features that prevent duplication of both URLs and content. The code is in `arcticzim.downloader`.
- **the builder** (`arcticzim.zimbuild.builder.ZimBuilder`) is responsible for actually building the ZIM. It setups the ZIM creator, add some basic items (e.g. icons) and metadata, then starts adding the actual content using stages. Each stage uses a creator thread (see below) and several workers (also see below). The builder then puts various tasks into the inqueue and awaits the worker and creator threads to finish.
- **the workers** (`arcticzim.zimbuild.worker.Worker`) process tasks from the inqueue. They are responsible for querying the data from the database, which will then be passed to the render. Finally, each result is submitted to the outqueue before the next task is received. A worker stops when it receives a a stop task, which happens at the end of each stage.
- **the renderer** (`arcticzim.zimbuild.renderer.HtmlRenderer`) renders the pages and generates redirects as well as other data for each task. It received all necessary data from the worker. It either returns one or yields several `RenderResult`s, which contain the pages. For rendering the jinja2 templates from `arcticzim/zimbuild/templates/` are used. The renderer also contains the environment, filters and tests. The renderer uses `arcticzim.downloader.MediaFileManager` to rewrite URLs and keep track of referenced files, which will be sent to the builder for later inclusion of those files.
- **the creator thread** receives `RenderResult`s from the outqueue, creates the appropriate items and add them to the ZIM file. It logs the progress and awaits worker termination before exiting. It is defined as the function `arcticzim.zimbuild.builder.ZimBuilder._creator_thread`.
- **the CLI** handles the command line interfaces and actually executes the code. It's located in `arcticzim.cli`.

### Testing and documentation

Tests have not been implemented, but the documentation can be build using `tox`. Simply run `tox` if you have it installed. The resulting documentation should be in the `html/` directory.

### Debug helpers

Various helpers exists to assist you in debugging:

- Increase the verbosity level to 2 (`arcticzim -v -v ...`) to see the SQL statements being used
- Specify `--log-directory <dir>` when building ZIMs to get worker logs
- Use `--memprofile-directory <dir>` to enable memory profiling when building ZIMs
- Run `python3 -m arcticzim.zimbuild.workerdebug --help` for debugging workers.


## Other links

The following ressources may be interesting for this project:

- [Arctic Shift](https://github.com/ArthurHeitmann/arctic_shift) is the data source. You'll find documentation there.
- [python-libzim](https://github.com/openzim/python-libzim) is used for ZIM creation.
- [ZimFiction](https://github.com/IMayBeABitShy/zimfiction) is one of my previous projects, which serves as the base for this project.
- [kiwix](https://kiwix.org/de/) is an awesome organization that develops most of the kiwix and OpenZim ecosystem.
- [The ZIM file specification](https://wiki.openzim.org/wiki/ZIM_file_format) may be useful in order to understand ZIM files. The site also contains other useful informations about ZIM files and the ecosytem.

