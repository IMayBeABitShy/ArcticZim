"""
Various utility functions.

@var ALLOWED_WORD_LETTERS: a regular expression pattern used to to identify where words end.
@type ALLOWED_WORD_LETERS: L{re.Pattern}
"""
import datetime
import re
import os
import decimal


ALLOWED_WORD_LETTERS = re.compile(r"[^\w|\-]")


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


def remove_duplicates(l):
    """
    Copy a list such that the order of elements is preserved but only
    the first occurrence of each element preserved.

    @param l: list to sort
    @type l: L{list}
    @return: a copy of the list with some elements potentially removed
    @rtype: L{list}
    """
    ret = []
    for e in l:
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


if __name__ == "__main__":
    # test code
    val = int(input("n: "))
    print("Formated as number:   {}".format(format_number(val)))
    print("Formated as size:     {}".format(format_size(val)))
    print("Formated as timedela: {}".format(format_timedelta(val)))
