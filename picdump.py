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

    def copy_roll(self, copy_mode=None):
        if copy_mode != "dry-run" and not os.path.exists(self.root):
            os.makedirs(self.root)
        for image in self:
            image.copy_image(self.root, copy_mode=copy_mode)

class Image(object):
    EXIF_Camera_Model = "Image Model"
    EXIF_Image_Date = "EXIF DateTimeOriginal"
    EXIF_Timestamp_Format = "%Y:%m:%d %H:%M:%S"

    def __init__(self, imgfn):
        self.imgfn = os.path.abspath(imgfn)
        (stem, ext) = os.path.splitext(imgfn)
        self.ext = ext.lower()
        if ext == ".jpeg":
            ext = ".jpg"
        self.parse_exif()

    def parse_exif(self):
        fh = open(self.imgfn, 'rb')
        self.exif = exifread.process_file(fh)
        self.camera_model = str(self.exif.get(self.EXIF_Camera_Model, "unknown"))
        self.timestamp = str(self.exif.get(self.EXIF_Image_Date, None))
        if self.timestamp:
            self.timestamp = datetime.strptime(self.timestamp, self.EXIF_Timestamp_Format)
        else:
            self.timestamp = datetime.fromtimestamp(os.path.getctime(self.imgfn))

    def asdict(self):
        return {'ext': self.ext, 'timestamp': self.timestamp, 'model': self.camera_model, 'path': self.imgfn, 'image': self}

    def __cmp__(self, other):
        return cmp(self.timestamp, other.timestamp)
    
    def copy_image(self, root, copy_mode=None):
        if copy_mode not in ("hardlink", "symlink", "dry-run"):
            copy_mode = "copy"
        fn = os.path.split(self.imgfn)[-1]
        dest = os.path.join(root, fn)
        msg = "%s -> %s (%s)" % (self.imgfn, dest, copy_mode)
        print msg
        if copy_mode == "symlink":
            os.symlink(self.imgfn, dest)
        elif copy_mode == "hardlink":
            os.link(self.imgfn, dest)
        elif copy_mode == "copy":
            shutil.copyfile(self.imgfn, dest)

class PicDump(object):
    def __init__(self, args):
        self.import_dirs = args.import_dirs
        self.export_dir = args.export_dir
        self.roll_threshold = pd.Timedelta(minutes=args.roll_threshold)
        self.roll_label = args.roll_label
        self.copy_mode = args.copy_mode

    def process(self):
        ts = datetime.now()
        images = self.get_images()
        for (roll_count, roll) in enumerate(self.separate_rolls(images)):
            roll.copy_roll(copy_mode=self.copy_mode)
        duration = datetime.now() - ts
        msg = "Processed %d images into %d rolls in %.01f minutes." % (len(images), roll_count + 1, duration.total_seconds() / 60.0)
        print msg
    
    def get_images(self):
        images = []
        for _dir in self.import_dirs:
            for (root, dirs, files) in os.walk(_dir):
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
                    ts = roll.iloc[0]["timestamp"].strftime(self.roll_label)
                    _ext = ext
                    while _ext[0] == '.':
                        _ext = _ext[1:]
                    _ext = _ext.lower()
                    path = os.path.join(self.export_dir, model, _ext, ts)
                    roll = [row["image"] for (idx, row) in roll.iterrows()]
                    roll = ImageRoll(path, roll)
                    yield roll

Defaults = {
    'import_dirs': os.getenv("PICDUMP_IMPORT_DIR", '.').split(' '),
    'export_dir': os.getenv("PICDUMP_OUTPUT_DIR", '.'),
    'roll_threshold': 60,
    'roll_label': "%Y-%m-%d_%H%M",
    'copy_mode': "copy",
}

def get_cli():
    parser = argparse.ArgumentParser(description='picdump')
    parser.add_argument('-i', '--import', dest="import_dirs", nargs='+', help='One or more directories to import images from')
    parser.add_argument('-o', '--export', dest="export_dir", help='Root of the directory to copy pictures to')
    parser.add_argument('-t', '--threshold', type=int, dest="roll_threshold", help='Threshold in minutes to separate film rolls by')
    parser.add_argument('-L', '--roll-label', dest="roll_label", help='Roll label (see strftime() for format')
    parser.add_argument('--symlink', action='store_const', const="symlink", dest="copy_mode", help='Instead of copying files, make a symlink')
    parser.add_argument('--hardlink', action='store_const', const="hardlink", dest="copy_mode", help='Instead of copying files, make a hardlink')
    parser.add_argument('--dry', action='store_const', const="dry-run", dest="copy_mode", help="Don't copy anything, just report")
    parser.set_defaults(**Defaults)
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = get_cli()
    pdump = PicDump(args)
    pdump.process()
