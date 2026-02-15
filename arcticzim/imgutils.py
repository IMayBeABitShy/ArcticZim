"""
Image-related utilities.
"""
import argparse
import math
import os

from PIL import Image


def mimetype_is_image(mimetype):
    """
    Check if a mimetype refers to an image mimetype.

    @param mimetype: mimetype to check
    @type mimetype: L{bool}
    @return: whether the mimetype is an image mimetype
    @rtype: L{bool}
    """
    if ";" in mimetype:
        mimetype = mimetype[:mimetype.find(";")]
    mimetype = mimetype.strip().lower()
    return mimetype.startswith("image/")


def mimetype_is_video(mimetype):
    """
    Check if a mimetype refers to a video mimetype.

    @param mimetype: mimetype to check
    @type mimetype: L{bool}
    @return: whether the mimetype is a video mimetype
    @rtype: L{bool}
    """
    if ";" in mimetype:
        mimetype = mimetype[:mimetype.find(";")]
    mimetype = mimetype.strip().lower()
    return mimetype.startswith("video/")


def minimize_image(path, max_w=512, max_h=512):
    """
    Minimize the image at the target path.

    @param path: path to image which should be minimized
    @type path: L{str}
    @param max_w: max width for the new image
    @type max_w: L{int}
    @param max_h: max height for the new image
    @type max_h: L{int}
    @return: a tuple of (new_mimetype, new_size)
    @rtype: L{tuple} of (L{str}, L{int})
    """
    with Image.open(path) as img:
        # load image and close the file
        img.load()
        # resize
        w_ratio = (max_w / img.width)
        h_ratio = (max_h / img.height)
        ratio = min(w_ratio, h_ratio)
        new_w = math.floor(img.width * ratio)
        new_h = math.floor(img.height * ratio)
        new_img = img.resize((new_w, new_h))
    with open(path, "wb") as fout:
        # convert and save
        new_img.save(fout, "WEBP")
        fout.seek(0, os.SEEK_END)
        size = fout.tell()
    return ("image/webp", size)


if __name__ == "__main__":
    """
    Test code.
    """
    parser = argparse.ArgumentParser(description="Minimize an image in-place")
    parser.add_argument("path", action="store", help="path to image")
    ns = parser.parse_args()
    minimize_image(ns.path)
