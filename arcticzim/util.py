"""
Various utility functions.

@var ALLOWED_WORD_LETTERS: a regular expression pattern used to to identify where words end.
@type ALLOWED_WORD_LETERS: L{re.Pattern}
@var ALLOWED_REDDIT_NAME_LETTERS: a regular expression pattern used to normalize reddit names
@type ALLOWED_REDDIT_NAME_LETTERS: L{re.Pattern}
@var URL: a regular expression matching likely URLs
@type URL: L{re.Pattern}
"""
import datetime
import re
import os
import decimal
from urllib.parse import urlparse


ALLOWED_WORD_LETTERS = re.compile(r"[^\w|\-]")
ALLOWED_REDDIT_NAME_LETTERS = re.compile(r"[^A-Za-z0-9_\-]")
# from https://stackoverflow.com/a/3809435 (modified)
URL = re.compile(r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*[-a-zA-Z0-9@:%_\+.~#?&//=])")


def format_timedelta(seconds):
    """
    Format seconds since event into a readable string.

    @param seconds: seconds to format
    @type seconds: L{int}
    @return: the formated string
    @rtype: L{str}
    """

    formatted = str(datetime.timedelta(seconds=seconds))
    if "." in formatted:
        formatted = formatted[:-4]
    return formatted


def format_number(n, allow_none=False):
    """
    Format a number to be a bit more human readable.

    @param n: number to format
    @type n: L{int}
    @param allow_none: if false, raise an exception on None values
    @type allow_none: L{bool}
    @return: the formated string
    @rtype: L{str}
    """
    if isinstance(n, decimal.Decimal):
        n = float(n)
    if n is None:
        if allow_none:
            return "-"
        else:
            raise TypeError("format_number() got called with 'None' and allow_none=False!")
    if n < 1000 and isinstance(n, int):
        return str(n)
    for fmt in ("", "K", "M", "B", "T", "Qa"):
        if n < 1000.0:
            return "{:.2f}{}".format(round(n, 3), fmt)
        else:
            n /= 1000.0
    return "{:.2f}Qi".format(round(n, 2))


def format_size(nbytes):
    """
    Format the given byte count into a human readable format.

    @param nbytes: size in bytes
    @type nbytes: L{int}
    @return: a human readable string describing the size
    @rtype: L{str}
    """
    for fmt in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if nbytes < 1024.0:
            return "{:.2f} {}".format(round(nbytes, 2), fmt)
        else:
            nbytes /= 1024.0
    return "{:.2f} EiB".format(round(nbytes, 2))


def format_date(date):
    """
    Format a date.

    @param date: date to format
    @type date: L{datetime.datetime}
    @return: the formated date
    @rtype: L{str}
    """
    return date.strftime("%Y-%m-%d")


def get_package_dir():
    """
    Return the path to the root directory of this package.

    @return: the path to the root directory of this package (not repo!)
    @rtype: L{str}
    """
    return os.path.dirname(__file__)


def get_resource_file_path(*names):
    """
    Return the path to the specified resource file.

    @param names: name(s) of the resource file to get path for. If multiple are specified, they are interpeted as a sequence of path segments.
    @type names: L{str}
    @return: path to the resource file
    @rtype: L{str}
    """
    p = os.path.join(get_package_dir(), "resources", *names)
    return p


def add_to_dict_list(d, k, v):
    """
    Add a value to a dictionary of lists.

    If k is in d, append v. Otherwise, set d[k] = [v].

    @param d: dictionary of lists to append v to
    @type d: L{dict} of hashable -> L{list}
    @param k: key of list to append v to
    @type k: hashable
    @param v: value to append
    @type v: any
    """
    if k in d:
        d[k].append(v)
    else:
        d[k] = [v]


def count_words(text):
    """
    Count the words in the text.

    @param text: text to count words in.
    @type text: L{str}
    @return: number of words in text.
    @rtype: L{int}
    """
    return len(ALLOWED_WORD_LETTERS.sub(" ", text).split())


def set_or_increment(d, k, v=1):
    """
    Set or increment a key in a dict to/by a value.

    Basically, if k in d set d[k] += v, else d[k] = v.

    @param d: dictionary to modify
    @type d: L{dict}
    @param k: key in dict to use
    @type k: hashable
    @type v: value to set to or increment by
    @type v: L{int} or L{float}
    """
    if k in d:
        d[k] += v
    else:
        d[k] = v


def delete_or_decrement(d, k, v=1, delete_on=1):
    """
    Delete or increment a key in a dict for/by a value.

    Basically, if d[k] >= delete_on set d[k] -= v else del d[k]

    @param d: dictionary to modify
    @type d: L{dict}
    @param k: key in dict to use
    @type k: hashable
    @type v: value to decrement by
    @type v: L{int} or L{float}
    @param delete_on: if prior to decrement the value is <= this value, delete it
    @raises KeyError: if k not in d
    """
    if d[k] <= delete_on:
        del d[k]
    else:
        d[k] -= v


def ensure_iterable(obj):
    """
    If obj is iterable, return obj, else return an iterable yielding obj.

    May not work correctly on primitive data types.

    @param obj: object to turn iterable
    @type obj: any
    @return: an iterable (either obj or one yielding obj)
    @rtype: iterable
    """
    if hasattr(obj, "__iter__"):
        return obj
    else:
        return (obj, )


def remove_duplicates(li):
    """
    Copy a list such that the order of elements is preserved but only
    the first occurrence of each element preserved.

    @param li: list to sort
    @type li: L{list}
    @return: a copy of the list with some elements potentially removed
    @rtype: L{list}
    """
    ret = []
    for e in li:
        if e not in ret:
            ret.append(e)
    return ret


def chunked(iterable, n):
    """
    Split an iterable into multiple lists, each containing at most n elements.

    @param iterable: iterable to split
    @type iterable: iterable
    @param n: number of elements each list should have at most
    @type n: L{int}
    @return: a generator yielding lists, each a chunk of the input data
    @rtype: generator yielding L{list}
    """
    current = []
    for element in iterable:
        current.append(element)
        if len(current) >= n:
            yield current
            current = []
    if current:
        yield current


def get_urls_from_string(s):
    """
    Find all URLs in a string.

    No guarantees are made about the validity or completenes of the result.

    @param s: string to search for URLs
    @type s: L{str}
    @return: a list of URLs
    @rtype: L{str}
    """
    return [s[m.start():m.end()] for m in URL.finditer(s)]


def parse_reddit_url(url):
    """
    Parse a reddit URL.

    The result is a dict containing some info, most importantly the "type" key.

    @param url: URL to parse
    @type url: L{str}
    @return: info about the URL if it is a reddit url, otherwise L{None}
    @rtype: L{dict} or L{None}
    """
    if isinstance(url, bytes):
        url = url.decode("utf-8")
    if url == "":
        return None
    parts = urlparse(url)
    host = parts.hostname
    if (host is None) or not (host.endswith("reddit.com") or host.endswith("redd.it")):
        # not a reddit link
        return None
    path = parts.path
    segments = path.split("/")
    if segments and not segments[0]:
        segments.pop(0)
    if segments and not segments[-1]:
        segments.pop(-1)
    if len(segments) == 0:
        # just a general reddit url
        return None
    try:
        if segments[0] in ("u", "user"):
            # a user reference
            username = segments[1]
            return {
                "scheme": parts.scheme,
                "type": "user",
                "username": trim_reddit_name(username),
            }
        elif segments[0] == "r":
            # a subreddit, post or comment reference
            subredditname = segments[1]
            if (len(segments) >= 6) and (segments[2] == "comments"):
                # a comment reference
                if len(segments[3]) > 8:
                    # probably not a post id
                    return None
                return {
                    "scheme": parts.scheme,
                    "type": "comment",
                    "subreddit": trim_reddit_name(subredditname),
                    "post": trim_reddit_name(segments[3]),
                    "comment": trim_reddit_name(segments[5]),
                }
            if (len(segments) >= 4) and (segments[2] == "comments"):
                # a post reference
                if len(segments[3]) > 8:
                    # probably not a post id
                    return None
                return {
                    "scheme": parts.scheme,
                    "type": "post",
                    "subreddit": trim_reddit_name(subredditname),
                    "post": trim_reddit_name(segments[3]),
                }
            else:
                # just a subreddit reference
                return {
                    "scheme": parts.scheme,
                    "type": "subreddit",
                    "subreddit": trim_reddit_name(subredditname),
                }
        else:
            # unknown reddit reference
            return None
    except IndexError:
        # one of the segments of the URL was empty
        # this is most likely not a URL actually referencing a specific
        # object
        return None


def trim_reddit_name(s):
    """
    Trim a reddit 'name', which may be a username, subredditname or ID.

    @param s: name to trim
    @type s: L{str}
    @return: the trimmed name
    @rtype: L{str}
    """
    return ALLOWED_REDDIT_NAME_LETTERS.sub("", s).strip()


def trim_title(s):
    """
    Trim a title, making it more suitable for inclusion in a ZIM file.

    @param s: title to trim
    @type s: L{str}
    @return: the trimemd title
    @rtype: L{str}
    """
    words = s.split(" ")
    words = [trim_word(word) for word in words]
    title = " ".join(words)
    if len(title) > 230:
      title = title[:230]
    return title


def trim_word(word):
    """
    Trim a word, making it more suitable in titles.

    @param word: word to trim
    @type word: L{str}
    @return: the trimmed word
    @rtype: L{str}
    """
    max_word_length = 100
    if len(word) >= max_word_length:
        word = word[:max_word_length]
    word = word.strip()
    return word


if __name__ == "__main__":
    # test code
    val = int(input("n: "))
    print("Formated as number:   {}".format(format_number(val)))
    print("Formated as size:     {}".format(format_size(val)))
    print("Formated as timedela: {}".format(format_timedelta(val)))
