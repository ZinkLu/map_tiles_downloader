import base64
import cgi
import mimetypes
import os
import threading
import uuid

import tornado.ioloop
import tornado.process
import tornado.web
import ujson
from aiofile import AIOFile

from file_writer import FileWriter
from mbtiles_writer import MbtilesWriter
from repo_writer import RepoWriter
from utils import Utils

lock = threading.Lock()


class MainHandler(tornado.web.RequestHandler):

    async def get(self, path="/"):
        """index"""
        if self.request.path == "/":
            path = "index.htm"

        file = os.path.join("./UI/", path)
        mime = mimetypes.MimeTypes().guess_type(file)[0]

        # self.send_header("Access-Control-Allow-Origin", "*")
        self.set_header("Content-Type", mime or "")

        with open(file, "rb") as f:
            self.write(f.read())


class DownloadTile(tornado.web.RequestHandler):

    def writerByType(self, type):
        if (type == "mbtiles"):
            return MbtilesWriter
        elif (type == "repo"):
            return RepoWriter
        elif (type == "directory"):
            return FileWriter

    def randomString(self):
        return uuid.uuid4().hex.upper()[0:6]

    async def post(self):
        ctype, pdict = cgi.parse_header(self.request.headers.get('Content-Type'))
        # ctype, pdict = cgi.parse_header(self.headers['content-type'])
        pdict['boundary'] = bytes(pdict['boundary'], "utf-8")

        content_len = int(self.request.headers.get('Content-length'))
        pdict['CONTENT-LENGTH'] = content_len

        postvars = self.request.body_arguments

        x = int(postvars['x'][0])
        y = int(postvars['y'][0])
        z = int(postvars['z'][0])
        quad = postvars['quad'][0].decode()
        timestamp = int(postvars['timestamp'][0])
        outputDirectory = postvars['outputDirectory'][0].decode()
        outputFile = postvars['outputFile'][0].decode()
        outputType = postvars['outputType'][0].decode()
        outputScale = int(postvars['outputScale'][0])
        source = postvars['source'][0].decode()

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

        result = {}

        filePath = os.path.join("output", outputDirectory, outputFile)

        print("\n")

        if self.writerByType(outputType).exists(filePath, x, y, z):
            result["code"] = 200
            result["message"] = 'Tile already exists'

            print("EXISTS: " + filePath)
        else:

            tempFile = self.randomString() + ".png"
            tempFilePath = os.path.join("temp", tempFile)

            result["code"] = Utils.downloadFileScaled(source, tempFilePath, x, y, z, outputScale)

            print("HIT: " + source + "\n" + "RETURN: " + str(result["code"]))

            if os.path.isfile(tempFilePath):
                self.writerByType(outputType).addTile(lock, filePath, tempFilePath, x, y, z, outputScale)

                async with AIOFile(tempFilePath, 'rb') as image_file:
                    result["image"] = base64.b64encode(await image_file.read()).decode("utf-8")

                os.remove(tempFilePath)

                result["message"] = 'Tile Downloaded'
                print("SAVE: " + filePath)

            else:
                result["message"] = 'Download failed'

        # self.send_header("Access-Control-Allow-Origin", "*")
        self.add_header("Content-Type", "application/json")
        self.write(ujson.dumps(result).encode('utf-8'))


class StartDownload(tornado.web.RequestHandler):

    def writerByType(self, type):
        if (type == "mbtiles"):
            return MbtilesWriter
        elif (type == "repo"):
            return RepoWriter
        elif (type == "directory"):
            return FileWriter

    async def post(self):
        ctype, pdict = cgi.parse_header(self.request.headers.get('Content-Type'))
        # ctype, pdict = cgi.parse_header(self.headers['content-type'])
        pdict['boundary'] = bytes(pdict['boundary'], "utf-8")

        content_len = int(self.request.headers.get('Content-length'))
        pdict['CONTENT-LENGTH'] = content_len

        postvars = self.request.body_arguments

        outputType = postvars['outputType'][0].decode()
        outputScale = int(postvars['outputScale'][0])
        outputDirectory = postvars['outputDirectory'][0].decode()
        outputFile = postvars['outputFile'][0].decode()
        minZoom = int(postvars['minZoom'][0])
        maxZoom = int(postvars['maxZoom'][0])
        timestamp = int(postvars['timestamp'][0])
        bounds = postvars['bounds'][0].decode()
        boundsArray = map(float, bounds.split(","))
        center = postvars['center'][0].decode()
        centerArray = map(float, center.split(","))

        replaceMap = {
            "timestamp": str(timestamp),
        }

        for key, value in replaceMap.items():
            newKey = str("{" + str(key) + "}")
            outputDirectory = outputDirectory.replace(newKey, value)
            outputFile = outputFile.replace(newKey, value)

        filePath = os.path.join("output", outputDirectory, outputFile)

        self.writerByType(outputType).addMetadata(lock, os.path.join("output", outputDirectory), filePath,
                                                  outputFile, "Map Tiles Downloader via AliFlux", "png",
                                                  boundsArray, centerArray, minZoom, maxZoom, "mercator",
                                                  256 * outputScale)

        result = {}
        result["code"] = 200
        result["message"] = 'Metadata written'

        # self.send_header("Access-Control-Allow-Origin", "*")
        self.add_header("Content-Type", "application/json")
        self.write(ujson.dumps(result).encode('utf-8'))


class EndDownload(tornado.web.RequestHandler):
    def writerByType(self, type):
        if (type == "mbtiles"):
            return MbtilesWriter
        elif (type == "repo"):
            return RepoWriter
        elif (type == "directory"):
            return FileWriter

    async def post(self):
        ctype, pdict = cgi.parse_header(self.request.headers.get('Content-Type'))
        # ctype, pdict = cgi.parse_header(self.headers['content-type'])
        pdict['boundary'] = bytes(pdict['boundary'], "utf-8")

        content_len = int(self.request.headers.get('Content-length'))
        pdict['CONTENT-LENGTH'] = content_len

        postvars = self.request.body_arguments

        outputType = postvars['outputType'][0].decode()
        outputScale = int(postvars['outputScale'][0])
        outputDirectory = postvars['outputDirectory'][0].decode()
        outputFile = postvars['outputFile'][0].decode()
        minZoom = int(postvars['minZoom'][0])
        maxZoom = int(postvars['maxZoom'][0])
        timestamp = int(postvars['timestamp'][0])
        bounds = postvars['bounds'][0].decode()
        boundsArray = map(float, bounds.split(","))
        center = postvars['center'][0].decode()
        centerArray = map(float, center.split(","))

        replaceMap = {
            "timestamp": str(timestamp),
        }

        for key, value in replaceMap.items():
            newKey = str("{" + str(key) + "}")
            outputDirectory = outputDirectory.replace(newKey, value)
            outputFile = outputFile.replace(newKey, value)

        filePath = os.path.join("output", outputDirectory, outputFile)

        self.writerByType(outputType).close(lock, os.path.join("output", outputDirectory), filePath, minZoom,
                                            maxZoom)

        result = {}
        result["code"] = 200
        result["message"] = 'Downloaded ended'

        # self.send_header("Access-Control-Allow-Origin", "*")
        self.add_header("Content-Type", "application/json")
        self.write(ujson.dumps(result).encode('utf-8'))


def make_app():
    return tornado.web.Application(
        [
            (r"/download-tile", DownloadTile),
            (r"/start-download", StartDownload),
            (r"/end-download", EndDownload),
            (r"/(.*)", MainHandler),
        ],

        debug=False
    )


if __name__ == "__main__":
    app = make_app()
    app.listen(8080)
    # tornado.process.fork_processes(2, max_restarts=1)
    tornado.ioloop.IOLoop.current().start()
