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

class Image(object):
    EXIF_Camera_Model = "Image Model"
    EXIF_Image_Date = "EXIF DateTimeOriginal"
    EXIF_Timestamp_Format = "%Y:%m:%d %H:%M:%S"

    def __init__(self, imgfn):
        self.imgfn = imgfn
        (stem, ext) = os.path.splitext(imgfn)
        self.ext = ext.lower()
        self.parse_exif()

    def parse_exif(self):
        fh = open(self.imgfn, 'rb')
        self.exif = exifread.process_file(fh)
        self.camera_model = str(self.exif.get(self.EXIF_Camera_Model, "unknown"))
        self.timestamp = str(self.exif[self.EXIF_Image_Date])
        self.timestamp = datetime.strptime(self.timestamp, self.EXIF_Timestamp_Format)

    def asdict(self):
        return {'ext': self.ext, 'timestamp': self.timestamp, 'model': self.camera_model, 'path': self.imgfn}

    def __cmp__(self, other):
        return cmp(self.timestamp, other.timestamp)

class PicDump(object):
    def __init__(self, args):
        self.import_dir = args.import_dir
        self.export_dir = args.export_dir
        self.roll_threshold = timedelta(minutes=args.roll_threshold)

    def process(self):
        self.get_images()
        #self.seperate_rolls()
        #self.copy_rolls()
    
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
                if len(images) >= 10:
                    break
            if len(images) >= 10:
                break
        self.images = pd.DataFrame([img.asdict() for img in images])
        for ext in self.images["ext"].unique():
            extsel = self.images[self.images["ext"] == ext]
            print extsel
            print "SORTING"
            extsel = extsel.sort("timestamp", ascending=False)
            print extsel


    def seperate_rolls(self):
        self.rolls = {}
        for ext in self.images:
            last_ts = None
            self.rolls[ext] = {}
            for image in self.images[ext]:
                if (last_ts == None) or (image.timestamp - last_ts > self.roll_threshold):
                    current_roll = image.timestamp
                last_ts = image.timestamp
                if current_roll not in self.rolls[ext]:
                    self.rolls[ext][current_roll] = []
                self.rolls[ext][current_roll].append(image)
    
    def copy_rolls(self):
        pass

Defaults = {
    'import_dir': '.',
    'export_dir': '.',
    'roll_threshold': 60,
}

def get_cli():
    parser = argparse.ArgumentParser(description='picdump')
    parser.add_argument('-i', '--import', dest="import_dir", help='Root of directory to import pictures from')
    parser.add_argument('-o', '--export', dest="export_dir", help='Root of the directory to copy pictures to')
    parser.add_argument('-t', '--threshold', dest="roll_threshold", help='Threshold in minutes to seperate film rolls by')
    parser.set_defaults(**Defaults)
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = get_cli()
    pdump = PicDump(args)
    pdump.process()
