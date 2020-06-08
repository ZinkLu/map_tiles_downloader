import mimetypes
import os
import threading
import uuid

from aiofile import AIOFile
from sanic import Sanic
from sanic.request import Request
from sanic.response import HTTPResponse, json

from file_writer import FileWriter
from mbtiles_writer import MbtilesWriter
from repo_writer import RepoWriter
from utils import async_download_file_scaled, ensure_base_dir

lock = threading.Lock()  # TODO: 移除lock

HANDLERS = dict(
    mbtiles=MbtilesWriter,
    repo=RepoWriter,
    directory=FileWriter,
)


def random_string():
    return uuid.uuid4().hex.upper()[0:6]


def writer_by_type(_type):
    return HANDLERS.get(_type)


app = Sanic(__name__)

app.static("/", "./UI/")


@app.route('/', methods=frozenset({"GET"}))
async def static(_: Request):
    file = os.path.join("./UI/", "index.htm")
    mime = mimetypes.MimeTypes().guess_type(file)[0]

    # request.send_header("Access-Control-Allow-Origin", "*")

    async with AIOFile(file, "rb") as f:
        return HTTPResponse(await f.read(), headers={"Content-Type": mime or ""})


@app.route("/start-download", methods=frozenset({"POST"}))
async def start(request: Request):
    headers = request.headers
    forms = request.form

    headers['boundary'] = bytes(headers['boundary'], "utf-8")
    headers['CONTENT-LENGTH'] = int(request.headers.get('Content-length'))

    output_type = forms['outputType'][0]
    output_scale = int(forms['outputScale'][0])
    output_directory = forms['outputDirectory'][0]
    output_file = forms['outputFile'][0]
    min_zoom = int(forms['minZoom'][0])
    max_zoom = int(forms['maxZoom'][0])
    timestamp = int(forms['timestamp'][0])
    bounds = forms['bounds'][0]
    bounds_array = map(float, bounds.split(","))
    center = forms['center'][0]
    center_array = map(float, center.split(","))

    replace_map = {
        "timestamp": str(timestamp),
    }

    for key, value in replace_map.items():
        new_key = str("{" + str(key) + "}")
        output_directory = output_directory.replace(new_key, value)
        output_file = output_file.replace(new_key, value)

    file_path = os.path.join("output", output_directory, output_file)

    writer_by_type(output_type).add_metadata(lock, os.path.join("output", output_directory), file_path,
                                             output_file, "Map Tiles Downloader via AliFlux", "png",
                                             bounds_array, center_array, min_zoom, max_zoom, "mercator",
                                             256 * output_scale)

    result = dict()
    result["code"] = 200
    result["message"] = 'Metadata written'

    # self.send_header("Access-Control-Allow-Origin", "*")
    return json(result)


@app.route("/end-download", methods=frozenset({"POST"}))
async def end(request: Request):
    headers = request.headers
    forms = request.form

    headers['boundary'] = bytes(headers['boundary'], "utf-8")
    headers['CONTENT-LENGTH'] = int(headers.get('Content-length'))

    output_type = forms['outputType'][0]
    output_scale = int(forms['outputScale'][0])
    output_directory = forms['outputDirectory'][0]
    output_file = forms['outputFile'][0]
    min_zoom = int(forms['minZoom'][0])
    max_zoom = int(forms['maxZoom'][0])
    timestamp = int(forms['timestamp'][0])
    bounds = forms['bounds'][0]
    bounds_array = map(float, bounds.split(","))
    center = forms['center'][0]
    center_array = map(float, center.split(","))

    replace_map = {
        "timestamp": str(timestamp),
    }

    for key, value in replace_map.items():
        new_key = str("{" + str(key) + "}")
        output_directory = output_directory.replace(new_key, value)
        output_file = output_file.replace(new_key, value)

    file_path = os.path.join("output", output_directory, output_file)

    writer_by_type(output_type).close(lock, os.path.join("output", output_directory), file_path, min_zoom,
                                      max_zoom)

    result = dict()
    result["code"] = 200
    result["message"] = 'Downloaded ended'

    # self.send_header("Access-Control-Allow-Origin", "*")
    return json(result)


@app.route("/download-tile", methods=frozenset({"POST"}))
async def down(request: Request):
    headers = request.headers
    forms = request.form

    headers['boundary'] = bytes(headers['boundary'], "utf-8")
    headers['CONTENT-LENGTH'] = int(headers.get('Content-length'))

    x = int(forms['x'][0])
    y = int(forms['y'][0])
    z = int(forms['z'][0])
    quad = forms['quad'][0]
    timestamp = int(forms['timestamp'][0])
    output_directory = forms['outputDirectory'][0]
    output_file = forms['outputFile'][0]
    output_type = forms['outputType'][0]
    output_scale = int(forms['outputScale'][0])
    source = forms['source'][0]

    replace_map = {
        "x": str(x),
        "y": str(y),
        "z": str(z),
        "quad": quad,
        "timestamp": str(timestamp),
    }

    for key, value in replace_map.items():
        new_key = str("{" + str(key) + "}")
        output_directory = output_directory.replace(new_key, value)
        output_file = output_file.replace(new_key, value)

    result = dict(message="ok")

    file_path = os.path.join("output", output_directory, output_file)

    if writer_by_type(output_type).exists(file_path, x, y, z):
        result["code"] = 200

        print("EXISTS: " + file_path)
    else:

        temp_file = random_string() + ".png"
        temp_file_path = os.path.join("temp", temp_file)

        # 使用tempfile的原因是他支持缩放
        result["code"] = await async_download_file_scaled(source, temp_file_path, x, y, z, output_scale)

        print("HIT: " + source + "\n" + "RETURN: " + str(result["code"]))

        if os.path.isfile(temp_file_path):
            writer_by_type(output_type).add_tile(lock, file_path, temp_file_path, x, y, z, output_scale)

            # 注释掉这一段, 让前段不再展示图片
            # async with AIOFile(tempFilePath, 'rb') as image_file:
            #     result["image"] = base64.b64encode(await image_file.read()).decode("utf-8")

            os.remove(temp_file_path)

            result["message"] = 'url, write'
            print("SAVE: " + file_path)

        else:
            result["message"] = 'Download failed'

    return json(result)


if __name__ == '__main__':
    env = os.environ

    ensure_base_dir()

    workers = 1
    if env.get("workers", ""):
        worker_string = env.get("workers")  # type: str
        if worker_string.isdigit():
            workers = int(workers)

        if worker_string == "auto":
            workers = os.cpu_count() * 2 + 1

    app.run(port=8080, workers=workers, access_log=False)
