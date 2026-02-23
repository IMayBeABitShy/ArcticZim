"""
Image-related utilities.
"""
import argparse
import math
import os

from PIL import Image, ImageFile


# allow processing of larger images
Image.MAX_IMAGE_PIXELS *= 10
# robustness - do not crash with incomplete images
ImageFile.LOAD_TRUNCATED_IMAGES = True


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
    @return: a tuple of (new_mimetype, new_size)  or L{None} if the minimization failed
    @rtype: L{tuple} of (L{str}, L{int}) or L{None}
    """
    try:
        with Image.open(path) as img:
            # load image and close the file
            img.load()
            # find out if it is an animated image (e.g. GIF)
            is_animated = hasattr(img, "is_animated") and img.is_animated
            # resize
            w_ratio = (max_w / img.width)
            h_ratio = (max_h / img.height)
            ratio = min(w_ratio, h_ratio, 1)
            new_w = math.floor(img.width * ratio)
            new_h = math.floor(img.height * ratio)
            if not is_animated:
                frames = [img.resize((new_w, new_h))]
            else:
                frames = []
                for i in range(getattr(img, "n_frames", 1)):
                    img.seek(i)
                    frame = img.resize((new_w, new_h))
                    frames.append(frame)

    except Image.DecompressionBombError:
        return None
    else:
        with open(path, "wb") as fout:
            # convert and save
            if is_animated:
                # animated images need to be saved differently
                frames[0].save(fout, "WEBP", save_all=True, append_images=frames[1:])
            else:
                frames[0].save(fout, "WEBP")
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
