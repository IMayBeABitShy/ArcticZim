"""
This module contains the mistune (markdown) customization.
"""
import mistune
from mistune.util import escape_url


class CustomMistuneBlockLevelParser(mistune.BlockParser):
    """
    A custom block-level parser for mistune that adjusts some behavior for reddit.

    More specifically, this one allows us to not have a space before a title.
    """
    SPECIFICATION = mistune.BlockParser.SPECIFICATION.copy()


CustomMistuneBlockLevelParser.SPECIFICATION["atx_heading"] = r"^ {0,3}(?P<atx_1>#{1,6})(?!#+)(?P<atx_2>[ \t]*|[ \t]*.*?)$"


URL_LINK_PATTERN = r"(\.\./)+[^\s<]+[^<.,:;\"')\]\s]"


def parse_url_link(inline, m, state):
    text = m.group(0)
    pos = m.end()
    if state.in_link:
        inline.process_text(text, state)
        return pos
    state.append_token(
        {
            "type": "link",
            "children": [{"type": "text", "raw": text}],
            "attrs": {"url": escape_url(text)},
        }
    )
    return pos


def relative_url_plugin(md):
    md.inline.register("url_link", URL_LINK_PATTERN, parse_url_link)
