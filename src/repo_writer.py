import os
import sqlite3

from mbtiles_writer import MbtilesWriter


class RepoWriter(MbtilesWriter):

    @staticmethod
    def addMetadata(lock, path, file, name, description, format, bounds, center, minZoom, maxZoom, profile="mercator",
                    tileSize=256):

        RepoWriter.ensureDirectory(lock, path)

        connection = sqlite3.connect(file, check_same_thread=False)
        c = connection.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT);")
        c.execute(
            "CREATE TABLE IF NOT EXISTS tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB, tile_cropped_data BLOB, pixel_left REAL, pixel_top REAL, pixel_right REAL, pixel_bottom REAL, has_alpha INTEGER);")

        try:
            c.execute("CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);")
        except:
            pass

        try:
            c.execute("CREATE UNIQUE INDEX metadata_name ON metadata (name);")
        except:
            pass

        connection.commit()

        c = connection.cursor()

        try:
            c.executemany("INSERT INTO metadata (name, value) VALUES (?, ?);", [
                ("name", name),
                ("description", description),
                ("format", format),
                ("bounds", ','.join(map(str, bounds))),
                ("center", ','.join(map(str, center))),
                ("minzoom", minZoom),
                ("maxzoom", maxZoom),
                ("profile", profile),
                ("tilesize", str(tileSize)),
                ("scheme", "tms"),
                ("generator", "Map Tiles Downloader via AliFlux"),
                ("type", "overlay"),
                ("attribution", "Map Tiles Downloader via AliFlux"),
            ])

            connection.commit()
        except:
            pass

    @staticmethod
    def addTile(lock, filePath, sourcePath, x, y, z, outputScale):

        fileDirectory = os.path.dirname(filePath)
        RepoWriter.ensureDirectory(lock, fileDirectory)

        invertedY = (2 ** z) - y - 1

        tileData = []
        with open(sourcePath, "rb") as readFile:
            tileData = readFile.read()

        lock.acquire()
        try:

            connection = sqlite3.connect(filePath, check_same_thread=False)
            c = connection.cursor()
            c.execute(
                "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data, tile_cropped_data, pixel_left, pixel_top, pixel_right, pixel_bottom, has_alpha) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                [
                    z, x, invertedY, None, tileData, 0, 0, 256 * outputScale, 256 * outputScale, 0
                ])

            connection.commit()

        finally:
            lock.release()

        return
