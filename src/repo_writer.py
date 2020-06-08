import os
import sqlite3

from mbtiles_writer import MbtilesWriter


class RepoWriter(MbtilesWriter):

    @staticmethod
    def add_metadata(lock, path, file, name, description, tile_format, bounds, center, min_zoom, max_zoom,
                     profile="mercator", tile_size=256):

        RepoWriter.ensure_directory(lock, path)

        connection = sqlite3.connect(file, check_same_thread=False)
        c = connection.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT);")
        c.execute("CREATE TABLE IF NOT EXISTS tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB, tile_cropped_data BLOB, pixel_left REAL, pixel_top REAL, pixel_right REAL, pixel_bottom REAL, has_alpha INTEGER);")

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
        RepoWriter.ensure_directory(lock, file_directory)

        inverted_y = (2 ** z) - y - 1

        tile_data = []
        with open(source_path, "rb") as readFile:
            tile_data = readFile.read()

        lock.acquire()
        try:

            connection = sqlite3.connect(file_path, check_same_thread=False)
            c = connection.cursor()
            c.execute(
                "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data, tile_cropped_data, pixel_left, pixel_top, pixel_right, pixel_bottom, has_alpha) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                [
                    z, x, inverted_y, None, tile_data, 0, 0, 256 * output_scale, 256 * output_scale, 0
                ])

            connection.commit()

        finally:
            lock.release()

        return
