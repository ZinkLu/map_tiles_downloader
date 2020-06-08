#!/usr/bin/env python

import math
import os
import ssl
import urllib.request
import uuid

import httpx
from PIL import Image
from aiofile import AIOFile


def random_string():
    return uuid.uuid4().hex.upper()[0:6]


def get_child_tiles(x, y, z):
    child_x = x * 2
    child_y = y * 2
    child_z = z + 1

    return [
        (child_x, child_y, child_z),
        (child_x + 1, child_y, child_z),
        (child_x + 1, child_y + 1, child_z),
        (child_x, child_y + 1, child_z),
    ]


def make_quad_key(tile_x, tile_y, level):
    quadkey = ""
    for i in range(level):
        bit = level - i
        digit = ord('0')
        mask = 1 << (bit - 1)  # if (bit - 1) > 0 else 1 >> (bit - 1)
        if (tile_x & mask) != 0:
            digit += 1
        if (tile_y & mask) != 0:
            digit += 2
        quadkey += chr(digit)
    return quadkey


def num2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return lat_deg, lon_deg


def qualify_url(url, x, y, z):
    scale22 = 23 - (z * 2)

    replace_map = {
        "x": str(x),
        "y": str(y),
        "z": str(z),
        "scale:22": str(scale22),
        "quad": make_quad_key(x, y, z),
    }

    for key, value in replace_map.items():
        new_key = str("{" + str(key) + "}")
        url = url.replace(new_key, value)

    return url


def merge_quad_tile(quad_tiles):
    width = 0
    height = 0

    for tile in quad_tiles:
        if tile is not None:
            width = quad_tiles[0].size[0] * 2
            height = quad_tiles[1].size[1] * 2
            break

    if width == 0 or height == 0:
        return None

    canvas = Image.new('RGB', (width, height))

    if quad_tiles[0] is not None:
        canvas.paste(quad_tiles[0], box=(0, 0))

    if quad_tiles[1] is not None:
        canvas.paste(quad_tiles[1], box=(width - quad_tiles[1].size[0], 0))

    if quad_tiles[2] is not None:
        canvas.paste(quad_tiles[2], box=(width - quad_tiles[2].size[0], height - quad_tiles[2].size[1]))

    if quad_tiles[3] is not None:
        canvas.paste(quad_tiles[3], box=(0, height - quad_tiles[3].size[1]))

    return canvas


def download_file(url, destination, x, y, z):
    url = qualify_url(url, x, y, z)

    code = 0

    # monkey patching SSL certificate issue
    # DONT use it in a prod/sensitive environment
    ssl._create_default_https_context = ssl._create_unverified_context

    try:
        path, response = urllib.request.urlretrieve(url, destination)
        code = 200
    except urllib.error.URLError as e:
        if not hasattr(e, "code"):
            print(e)
            code = -1
        else:
            code = e.code

    return code


async def async_download_file(url, destination, x, y, z):
    url = qualify_url(url, x, y, z)

    code = 0

    # monkey patching SSL certificate issue
    # DONT use it in a prod/sensitive environment
    ssl._create_default_https_context = ssl._create_unverified_context

    try:
        async with AIOFile(destination, "wb") as f:
            async with httpx.AsyncClient() as client:
                res = await client.get(url)
                await f.write(res.content)
        # path, response = urllib.request.urlretrieve(url, destination)
        code = 200
    except urllib.error.URLError as e:
        if not hasattr(e, "code"):
            print(e)
            code = -1
        else:
            code = e.code

    return code


def download_file_scaled(url, destination, x, y, z, output_scale):
    if output_scale == 1:
        return download_file(url, destination, x, y, z)

    elif output_scale == 2:

        child_tiles = get_child_tiles(x, y, z)
        child_images = []

        for childX, childY, childZ in child_tiles:

            temp_file = random_string() + ".png"
            temp_file_path = os.path.join("temp", temp_file)

            code = download_file(url, temp_file_path, childX, childY, childZ)

            if code == 200:
                image = Image.open(temp_file_path)
            else:
                return code

            child_images.append(image)

        canvas = merge_quad_tile(child_images)
        canvas.save(destination, "PNG")

        return 200


async def async_download_file_scaled(url, destination, x, y, z, output_scale):
    if output_scale == 1:
        return await async_download_file(url, destination, x, y, z)

    elif output_scale == 2:

        child_tiles = get_child_tiles(x, y, z)
        child_images = []

        for childX, childY, childZ in child_tiles:

            temp_file = random_string() + ".png"
            temp_file_path = os.path.join("temp", temp_file)

            code = await async_download_file(url, temp_file_path, childX, childY, childZ)

            if code == 200:
                image = Image.open(temp_file_path)
            else:
                return code

            child_images.append(image)

        canvas = merge_quad_tile(child_images)
        canvas.save(destination, "PNG")

        return 200


# TODO implement custom scale

def ensure_base_dir():
    if not os.path.exists('temp'):
        os.makedirs('temp', exist_ok=True)

    if not os.path.exists('output'):
        os.makedirs('output', exist_ok=True)
