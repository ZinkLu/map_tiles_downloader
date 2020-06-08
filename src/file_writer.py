import json
import os
import shutil


class FileWriter:
    slicer = None

    @staticmethod
    def ensure_directory(lock, directory):
        # 去除了lock, 看一下会不会有问题
        os.makedirs(directory, exist_ok=True)
        return directory

    @staticmethod
    def add_metadata(lock, path, file, name, description, tile_format, bounds, center, min_zoom, max_zoom,
                     profile="mercator", tile_size=256):

        FileWriter.ensure_directory(lock, path)

        data = [
            ("name", name),
            ("description", description),
            ("format", tile_format),
            ("bounds", ','.join(map(str, bounds))),
            ("center", ','.join(map(str, center))),
            ("minzoom", min_zoom),
            ("maxzoom", max_zoom),
            ("profile", profile),
            ("tilesize", str(tile_size)),
            ("scheme", "xyz"),
            ("generator", "EliteMapper by Visor Dynamics"),
            ("type", "overlay"),
            ("attribution", "EliteMapper by Visor Dynamics"),
        ]

        with open(path + "/metadata.json", 'w+') as jsonFile:
            json.dump(dict(data), jsonFile)

        return

    @staticmethod
    def add_tile(lock, file_path, source_path, x, y, z, outputScale):

        file_directory = os.path.dirname(file_path)
        FileWriter.ensure_directory(lock, file_directory)
        shutil.copyfile(source_path, file_path)
        return

    @staticmethod
    def exists(file_path, x, y, z):
        return os.path.isfile(file_path)

    @staticmethod
    def close(lock, path, file, min_zoom, max_zoom):
        # TODO recalculate bounds and center
        return
