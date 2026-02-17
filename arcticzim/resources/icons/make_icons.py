"""
Script for generating the icons.
"""
from PIL import Image, ImageDraw, ImageFont

ICONS = [
    # tuples of (name, filename, text)
    # if a filename is specified, it should be an RGBA image where the foreground is #dddddd and the background transparent
    ("img", "camera-4-512.png", None),
    ("video", "play-512.png", None),
    ("text", None, "T"),
    ("poll", "pie-chart-512.png", None),
    ("link", None, "L"),
]

ICON_COLOR = "darkgray"
ICON_SIZE = (512, 512)
ICON_PASTE_SIZE = (400, 400)
FONT_SIZE = 400

for icon in ICONS:
    img = Image.new(mode="RGBA", size=ICON_SIZE, color=(0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle(
        xy=((0, 0), ICON_SIZE),
        radius=(ICON_SIZE[0] // 6),
        fill=ICON_COLOR,
    )
    if icon[1] is not None:
        with Image.open(icon[1]) as other_img:
            resized = other_img.resize(ICON_PASTE_SIZE)
            img.alpha_composite(
                resized,
                (
                    (ICON_SIZE[0] - ICON_PASTE_SIZE[0]) // 2,
                    (ICON_SIZE[1] - ICON_PASTE_SIZE[1]) // 2,
                ),
            )
    if icon[2] is not None:
        font = ImageFont.truetype("LiberationSerif-Bold.ttf", FONT_SIZE)
        draw.text(
            xy=(ICON_SIZE[0] // 2, ICON_SIZE[1] // 2),
            text=icon[2],
            fill=(255, 255, 255, 255),
            anchor="mm",
            font=font,
        )
    img.save("icon_{}.png".format(icon[0]))
