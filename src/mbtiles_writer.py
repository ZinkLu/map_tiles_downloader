import os
import sqlite3

from utils import num2deg


class MbtilesWriter:
    @staticmethod
    def ensure_directory(lock, directory):
        os.makedirs(directory, exist_ok=True)
        return directory

    @staticmethod
    def add_metadata(lock, path, file, name, description, tile_format, bounds, center, min_zoom, max_zoom,
                     profile="mercator", tile_size=256):

        MbtilesWriter.ensure_directory(lock, path)

        connection = sqlite3.connect(file, check_same_thread=False)
        c = connection.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT);")
        c.execute("CREATE TABLE IF NOT EXISTS tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);")

        try:
            c.execute("CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);")
        except:
            pass

        try:
            c.execute("CREATE UNIQUE INDEX metadata_name ON metadata (name);")
        except:
            pass

        connection.commit()

        try:
            c.executemany("INSERT INTO metadata (name, value) VALUES (?, ?);", [
                ("name", name),
                ("description", description),
                ("format", tile_format),
                ("bounds", ','.join(map(str, bounds))),
                ("center", ','.join(map(str, center))),
                ("minzoom", min_zoom),
                ("maxzoom", max_zoom),
                ("profile", profile),
                ("tilesize", str(tile_size)),
                ("scheme", "tms"),
                ("generator", "Map Tiles Downloader via AliFlux"),
                ("type", "overlay"),
                ("attribution", "Map Tiles Downloader via AliFlux"),
            ])

            connection.commit()
        except:
            pass

    @staticmethod
    def add_tile(lock, file_path, source_path, x, y, z, output_scale):

        file_directory = os.path.dirname(file_path)
        MbtilesWriter.ensure_directory(lock, file_directory)

        inverted_y = (2 ** z) - y - 1

        tile_data = []
        with open(source_path, "rb") as readFile:
            tile_data = readFile.read()

        lock.acquire()
        try:

            connection = sqlite3.connect(file_path, check_same_thread=False)
            c = connection.cursor()
            c.execute("INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?);", [
                z, x, inverted_y, tile_data
            ])

            connection.commit()

        finally:
            lock.release()

        return

    @staticmethod
    def exists(file_path, x, y, z):
        inverted_y = (2 ** z) - y - 1

        if os.path.exists(file_path):

            connection = sqlite3.connect(file_path, check_same_thread=False)
            c = connection.cursor()

            c.execute("SELECT COUNT(*) FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ? LIMIT 1",
                      (z, x, inverted_y))

            result = c.fetchone()

            if result[0] > 0:
                return True

        return False

    @staticmethod
    def close(lock, path, file, min_zoom, max_zoom):

        connection = sqlite3.connect(file, check_same_thread=False)
        c = connection.cursor()

        c.execute(
            "SELECT min(tile_row), max(tile_row), min(tile_column), max(tile_column) FROM tiles WHERE zoom_level = ?",
            [max_zoom])

        min_y, max_y, min_x, max_x = c.fetchone()
        min_y = (2 ** max_zoom) - min_y - 1
        max_y = (2 ** max_zoom) - max_y - 1

        min_lat, min_lon = num2deg(min_x, min_y, max_zoom)
        max_lat, max_lon = num2deg(max_x + 1, max_y + 1, max_zoom)

        bounds = [min_lon, min_lat, max_lon, max_lat]
        bounds_string = ','.join(map(str, bounds))

        center = [(min_lon + max_lon) / 2, (min_lat + max_lat) / 2, max_zoom]
        center_string = ','.join(map(str, center))

        c.execute("UPDATE metadata SET value = ? WHERE name = 'bounds'", [bounds_string])
        c.execute("UPDATE metadata SET value = ? WHERE name = 'center'", [center_string])

        connection.commit()
