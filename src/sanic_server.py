import cgi
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
from utils import Utils

lock = threading.Lock()


def randomString():
    return uuid.uuid4().hex.upper()[0:6]


def writerByType(type):
    if (type == "mbtiles"):
        return MbtilesWriter
    elif (type == "repo"):
        return RepoWriter
    elif (type == "directory"):
        return FileWriter


app = Sanic(__name__)

app.static("/", "./UI/")


@app.route('/', methods=frozenset({"GET"}))
async def static(request: Request):
    file = os.path.join("./UI/", "index.htm")
    mime = mimetypes.MimeTypes().guess_type(file)[0]

    # request.send_header("Access-Control-Allow-Origin", "*")

    async with AIOFile(file, "rb") as f:
        return HTTPResponse(await f.read(), headers={"Content-Type": mime or ""})


@app.route("/start-download", methods=frozenset({"POST"}))
async def start(request):
    ctype, pdict = cgi.parse_header(request.headers.get('Content-Type'))
    # ctype, pdict = cgi.parse_header(self.headers['content-type'])
    pdict['boundary'] = bytes(pdict['boundary'], "utf-8")

    content_len = int(request.headers.get('Content-length'))
    pdict['CONTENT-LENGTH'] = content_len

    postvars = request.form

    outputType = postvars['outputType'][0]
    outputScale = int(postvars['outputScale'][0])
    outputDirectory = postvars['outputDirectory'][0]
    outputFile = postvars['outputFile'][0]
    minZoom = int(postvars['minZoom'][0])
    maxZoom = int(postvars['maxZoom'][0])
    timestamp = int(postvars['timestamp'][0])
    bounds = postvars['bounds'][0]
    boundsArray = map(float, bounds.split(","))
    center = postvars['center'][0]
    centerArray = map(float, center.split(","))

    replaceMap = {
        "timestamp": str(timestamp),
    }

    for key, value in replaceMap.items():
        newKey = str("{" + str(key) + "}")
        outputDirectory = outputDirectory.replace(newKey, value)
        outputFile = outputFile.replace(newKey, value)

    filePath = os.path.join("output", outputDirectory, outputFile)

    writerByType(outputType).addMetadata(lock, os.path.join("output", outputDirectory), filePath,
                                         outputFile, "Map Tiles Downloader via AliFlux", "png",
                                         boundsArray, centerArray, minZoom, maxZoom, "mercator",
                                         256 * outputScale)

    result = {}
    result["code"] = 200
    result["message"] = 'Metadata written'

    # self.send_header("Access-Control-Allow-Origin", "*")
    return json(result)


@app.route("/end-download", methods=frozenset({"POST"}))
async def end(request):
    ctype, pdict = cgi.parse_header(request.headers.get('Content-Type'))
    # ctype, pdict = cgi.parse_header(self.headers['content-type'])
    pdict['boundary'] = bytes(pdict['boundary'], "utf-8")

    content_len = int(request.headers.get('Content-length'))
    pdict['CONTENT-LENGTH'] = content_len

    postvars = request.form

    outputType = postvars['outputType'][0]
    outputScale = int(postvars['outputScale'][0])
    outputDirectory = postvars['outputDirectory'][0]
    outputFile = postvars['outputFile'][0]
    minZoom = int(postvars['minZoom'][0])
    maxZoom = int(postvars['maxZoom'][0])
    timestamp = int(postvars['timestamp'][0])
    bounds = postvars['bounds'][0]
    boundsArray = map(float, bounds.split(","))
    center = postvars['center'][0]
    centerArray = map(float, center.split(","))

    replaceMap = {
        "timestamp": str(timestamp),
    }

    for key, value in replaceMap.items():
        newKey = str("{" + str(key) + "}")
        outputDirectory = outputDirectory.replace(newKey, value)
        outputFile = outputFile.replace(newKey, value)

    filePath = os.path.join("output", outputDirectory, outputFile)

    writerByType(outputType).close(lock, os.path.join("output", outputDirectory), filePath, minZoom,
                                   maxZoom)

    result = {}
    result["code"] = 200
    result["message"] = 'Downloaded ended'

    # self.send_header("Access-Control-Allow-Origin", "*")
    return json(result)


@app.route("/download-tile", methods=frozenset({"POST"}))
async def down(request: Request):
    ctype, pdict = cgi.parse_header(request.headers.get('Content-Type'))
    # ctype, pdict = cgi.parse_header(self.headers['content-type'])
    pdict['boundary'] = bytes(pdict['boundary'], "utf-8")

    content_len = int(request.headers.get('Content-length'))
    pdict['CONTENT-LENGTH'] = content_len

    postvars = request.form

    x = int(postvars['x'][0])
    y = int(postvars['y'][0])
    z = int(postvars['z'][0])
    quad = postvars['quad'][0]
    timestamp = int(postvars['timestamp'][0])
    outputDirectory = postvars['outputDirectory'][0]
    outputFile = postvars['outputFile'][0]
    outputType = postvars['outputType'][0]
    outputScale = int(postvars['outputScale'][0])
    source = postvars['source'][0]

    replaceMap = {
        "x": str(x),
        "y": str(y),
        "z": str(z),
        "quad": quad,
        "timestamp": str(timestamp),
    }

    for key, value in replaceMap.items():
        newKey = str("{" + str(key) + "}")
        outputDirectory = outputDirectory.replace(newKey, value)
        outputFile = outputFile.replace(newKey, value)

    result = dict(message="ok")

    filePath = os.path.join("output", outputDirectory, outputFile)

    if writerByType(outputType).exists(filePath, x, y, z):
        result["code"] = 200

        print("EXISTS: " + filePath)
    else:

        tempFile = randomString() + ".png"
        tempFilePath = os.path.join("temp", tempFile)

        # 使用tempfile的原因是他支持缩放
        result["code"] = await Utils.as_downloadFileScaled(source, tempFilePath, x, y, z, outputScale)

        print("HIT: " + source + "\n" + "RETURN: " + str(result["code"]))

        if os.path.isfile(tempFilePath):
            writerByType(outputType).addTile(lock, filePath, tempFilePath, x, y, z, outputScale)

            # 注释掉这一段, 让前段不再展示图片
            # async with AIOFile(tempFilePath, 'rb') as image_file:
            #     result["image"] = base64.b64encode(await image_file.read()).decode("utf-8")

            os.remove(tempFilePath)

            result["message"] = 'url, write'
            print("SAVE: " + filePath)

        else:
            result["message"] = 'Download failed'

    return json(result)


if __name__ == '__main__':
    app.run(
        port=8080,
        # workers=os.cpu_count() * 2 + 1,
        access_log=False
    )
