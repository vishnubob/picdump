#!/usr/bin/env python

import exifread
import os
import shutil
import argparse
import mimetypes
import pandas as pd
from datetime import datetime, timedelta

mimetypes.init()
mimetypes.add_type("image/x-canon-cr2", ".cr2")

class ImageRoll(list):
    def __init__(self, root, roll):
        self.root = root
        super(ImageRoll, self).__init__(roll)

    def copy_roll(self, symlink=False):
        if not os.path.exists(self.root):
            os.makedirs(self.root)
        for image in self:
            image.copy_image(self.root, symlink)

class Image(object):
    EXIF_Camera_Model = "Image Model"
    EXIF_Image_Date = "EXIF DateTimeOriginal"
    EXIF_Timestamp_Format = "%Y:%m:%d %H:%M:%S"

    def __init__(self, imgfn):
        self.imgfn = imgfn
        (stem, ext) = os.path.splitext(imgfn)
        self.ext = ext.lower()
        if ext == ".jpeg":
            ext = ".jpg"
        self.parse_exif()

    def parse_exif(self):
        fh = open(self.imgfn, 'rb')
        self.exif = exifread.process_file(fh)
        self.camera_model = str(self.exif.get(self.EXIF_Camera_Model, "unknown"))
        self.timestamp = str(self.exif[self.EXIF_Image_Date])
        self.timestamp = datetime.strptime(self.timestamp, self.EXIF_Timestamp_Format)

    def asdict(self):
        return {'ext': self.ext, 'timestamp': self.timestamp, 'model': self.camera_model, 'path': self.imgfn, 'image': self}

    def __cmp__(self, other):
        return cmp(self.timestamp, other.timestamp)
    
    def copy_image(self, root, symlink=False):
        fn = os.path.split(self.imgfn)[-1]
        dest = os.path.join(root, fn)
        os.path.copyfile(self.imgfn, dest)
        print self.imgfn, "->", dest

class PicDump(object):
    def __init__(self, args):
        self.import_dir = args.import_dir
        self.export_dir = args.export_dir
        self.roll_threshold = pd.Timedelta(minutes=args.roll_threshold)

    def process(self):
        images = self.get_images()
        for roll in self.separate_rolls(images):
            roll.copy_roll()
    
    def get_images(self):
        images = []
        for (root, dirs, files) in os.walk(self.import_dir):
            for fn in files:
                (stem, ext) = os.path.splitext(fn)
                ext = ext.lower()
                ftype = mimetypes.types_map.get(ext, "na")
                if not ftype.startswith("image/"):
                    continue
                imgfn = os.path.join(root, fn)
                img = Image(imgfn)
                images.append(img)
        images = pd.DataFrame([img.asdict() for img in images])
        return images

    def separate_rolls(self, images):
        for ext in images["ext"].unique():
            for model in images["model"].unique():
                df = images[(images["model"] == model) & (images["ext"] == ext)]
                df = df.sort("timestamp")
                cluster = (df["timestamp"].diff() > self.roll_threshold).cumsum()
                rolls = [val for (key, val) in df.groupby(cluster)]
                for roll in rolls:
                    roll = roll.sort("timestamp")
                    ts = roll.iloc[0]["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                    _ext = ext
                    while _ext[0] == '.':
                        _ext = _ext[1:]
                    _ext = _ext.lower()
                    path = os.path.join(self.export_dir, model, _ext, ts)
                    roll = [row["image"] for (idx, row) in roll.iterrows()]
                    roll = ImageRoll(path, roll)
                    yield roll

Defaults = {
    'import_dir': '.',
    'export_dir': '.',
    'roll_threshold': 60,
}

def get_cli():
    parser = argparse.ArgumentParser(description='picdump')
    parser.add_argument('-i', '--import', dest="import_dir", help='Root of directory to import pictures from')
    parser.add_argument('-o', '--export', dest="export_dir", help='Root of the directory to copy pictures to')
    parser.add_argument('-t', '--threshold', dest="roll_threshold", help='Threshold in minutes to separate film rolls by')
    parser.set_defaults(**Defaults)
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = get_cli()
    pdump = PicDump(args)
    pdump.process()
