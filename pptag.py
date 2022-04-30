#!/usr/bin/env python
## python 3

# pip install watchdog

import sys
import getopt
import logging
import urllib
import time
import os
import threading
from datetime import datetime, date, timedelta
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from exif.exifread.tags import DEFAULT_STOP_TAG, FIELD_TYPES
from exif.exifread import process_file, exif_log, __version__
from plexUsers import plexUsers
from lightroomTags import parse_xmp_for_lightroom_tags
from photoElement import PhotoElement

from config import ppTagConfig

logger = exif_log.get_logger()

doUpdate = []
lock = None
firstRun = ppTagConfig.FORCE_RUN_AT_START

# plex
p = None

# timer
t = None

# last incoming event time
lastTS = datetime.now()

def updateMetadata(item, tags, rating):

    # update rating
    for user in p.users:
        data = p.fetchPlexApi("/:/rate?key=%s&identifier=com.plexapp.plugins.library&rating=%i" %(item, rating),"PUT", False, user.token)

    # write the metadata
    # prepare the tags
    tagQuery = "?"
    i = 0
    for tag in tags:
        tagQuery = tagQuery + "tag[%s].tag.tag=%s&" %(i, urllib.parse.quote(tag.encode('utf-8')))
        i = i + 1
    #logging.debug("  updateMetaData: tagQuery is '%s'" % tagQuery)

    data = p.fetchPlexApi("/library/metadata/%s%s" %(item, tagQuery), "PUT")

def getdata(filename):

    detailed = True
    stop_tag = DEFAULT_STOP_TAG
    debug = False
    strict = False
    color = False

    #exif_log.setup_logger(debug, color)

    try:
        filename = ppTagConfig.PHOTOS_LIBRARY_PATH + filename
        img_file = open(str(filename), 'rb')

        data = process_file(img_file, stop_tag=stop_tag, details=detailed, strict=strict, debug=debug)

        img_file.close()

        if not data:
            logging.info("No EXIF information for '%s'" % filename)
            return None

        if 'JPEGThumbnail' in data:
            del data['JPEGThumbnail']
        if 'TIFFThumbnail' in data:
            del data['TIFFThumbnail']
    except IOError:
        logging.debug("'%s' is unreadable" % filename)
        return None
    except:
        logging.error("Exif process_file error: '%s'" % filename)
        return None

    return data

def getXMP(data):
    XMP = None
    if 'Image ApplicationNotes' in data:
        try:
            xml = data['Image ApplicationNotes'].printable
            XMP = parse_xmp_for_lightroom_tags(xml)
        except:
            logging.error("Unable to parse XMP")

    return XMP

def updateTagsAndRating(key, filename):
    data = getdata(filename)
    if not data:
        return

    parsedXMP = getXMP(data)
    if parsedXMP:
        logging.info("Processing '%s'" % filename)
        updateMetadata(key, parsedXMP['tags'], int(parsedXMP['rating'])*2)
    else:
        logging.info("No XMP data for '%s'" % filename)

def parseExifAndTags(filename):
    data = getdata(filename)
    if not data:
        return None

    parsedXMP = getXMP(data)
    if not parsedXMP:
        parsedXMP = {}
        parsedXMP['rating'] = 0
        parsedXMP['tags'] = []

    try:
        date = datetime.fromtimestamp(datetime.strptime(data['EXIF DateTimeOriginal'].printable+data['EXIF Tag 0x9011'].printable, '%Y:%m:%d %H:%M:%S%z').timestamp()).date()
    except:
        try:
            date = datetime.strptime(data['EXIF DateTimeOriginal'].printable, '%Y:%m:%d %H:%M:%S').date()
        except:
            # fallback to the modify date on the file
            datetimeModified = datetime.fromtimestamp(os.path.getmtime(filename))
            date = datetimeModified.date()
        
    return PhotoElement(filename, date, parsedXMP['tags'], parsedXMP['rating'])

def triggerProcess():
    global t
    global lastTS

    lastTS  = datetime.now()
    if t is None or not t.is_alive() :
      logging.info("Starting timer")
      t = threading.Timer(120,fetchPhotosAndProcess)
      t.start()

def uniqify(seq):
    return list(dict.fromkeys(seq)) # order preserving

def fetchPhotosAndProcess():
    global firstRun
    global lastTS

    if firstRun: # complete update on startup requested
        loopThroughAllPhotos()
    else: # must be in the timer thread so process backlog
        # keep processing until there is nothing more to do so we don't have to worry about missed triggers
        while len(doUpdate) > 0:

            # wait for 120 seconds of idle time so that plex can process any creates first
            while datetime.now()-lastTS < timedelta(seconds=120):
                time.sleep(120-(datetime.now()-lastTS).total_seconds()+1)

            # Try to find all photos based on date
            if fetchAndProcessByDate():
                # failed so loop through all photoa to find the rest
                loopThroughAllPhotos()

def fetchAndProcessByDate():
    global doUpdate
    global lock
    dateSearchFailed = []

    while len(doUpdate) > 0:
        lock.acquire()
        doUpdateTemp = uniqify(doUpdate)
        doUpdate = []
        lock.release()
    
        photoGroups = {}
        # first group all photos by date
        for filepath in doUpdateTemp[:] :
            photoElement = parseExifAndTags(filepath)
            if photoElement:
                # this has exif data
                date = photoElement.date()
                if date in photoGroups.keys():
                    photoGroups[date].append(photoElement)
                else:
                    photoGroups[date] = [photoElement]
            else: # missing or not a photo
                doUpdateTemp.remove(filepath)
    
        for date in photoGroups.keys():
            fromTimecode = int(datetime.strptime(date.isoformat(), '%Y-%m-%d').timestamp())
            toTimecode = int((datetime.strptime(date.isoformat(), '%Y-%m-%d') + timedelta(days=1)).timestamp())-1
    
            toDo = True
            start = 0
            size = 1000
    
            # Make a key list of all pics in the date range
            plexData = {}
            if p.photoSection:
                while toDo:
                    url = "/library/sections/" + str(p.photoSection) + "/all?originallyAvailableAt%3E=" + str(fromTimecode) + "&originallyAvailableAt%3C=" + str(toTimecode) + "&X-Plex-Container-Start=%i&X-Plex-Container-Size=%i" % (start, size)
                    #logging.info("URL: %s", url)
                    metadata = p.fetchPlexApi(url)
                    container = metadata["MediaContainer"]
                    if 'Metadata' not in container:
                       # no photos in this time range (probably wrong section)
                       break
                    elements = container["Metadata"]
                    totalSize = container["totalSize"]
                    offset = container["offset"]
                    size = container["size"]
                    start = start + size
                    if totalSize-offset-size == 0:
                        toDo = False
                    # loop through all elements
                    for photo in elements:
                        mediaType = photo["type"]
                        if mediaType != "photo":
                            continue
                        key = photo["ratingKey"]
                        src = photo["Media"][0]["Part"][0]["file"].replace(ppTagConfig.PHOTOS_LIBRARY_PATH_PLEX,"", 1)

                        #logging.info("  Map: %s -> %s", src, key)
                        plexData[src] = key
    
            # Update the pics that changed in the date range
            for photo in photoGroups[date]:
                path = photo.path()
                # make sure path seperator is equal in plex and ppTag
                if "/" in ppTagConfig.PHOTOS_LIBRARY_PATH_PLEX:
                    path = path.replace("\\","/")
                if path in plexData.keys():
                    logging.info("Processing by date '%s'" % path)
                    updateMetadata(plexData[path], photo.tags(), photo.rating()*2)
                    doUpdateTemp.remove(path)

        # if we failed to process something then defer those to a full scan
        if len(doUpdateTemp):
            dateSearchFailed = [*dateSearchFailed, *doUpdateTemp]

    # if we failed to process something then trigger a full scan
    if len(dateSearchFailed) > 0:
        logging.warning("Some updated files were not found by date range.")
        lock.acquire()
        doUpdate = [*dateSearchFailed, *doUpdate]
        lock.release()
        return True

    return False

def loopThroughAllPhotos():
    global doUpdate
    global firstRun
    doUpdateTemp = uniqify(doUpdate)
    doUpdate = []
    toDo = True
    start = 0
    size = 1000
    #print('loop through all, started %i' % int(time.time()))
    if p.photoSection:
        while toDo:
            url = "/library/sections/" + str(p.photoSection) + "/all?clusterZoomLevel=1&X-Plex-Container-Start=%i&X-Plex-Container-Size=%i" % (start, size)
            metadata = p.fetchPlexApi(url)
            container = metadata["MediaContainer"]
            elements = container["Metadata"]
            totalSize = container["totalSize"]
            offset = container["offset"]
            size = container["size"]
            start = start + size
            if totalSize-offset-size == 0:
                toDo = False
            # loop through all elements
            for photo in elements:
                mediaType = photo["type"]
                if mediaType != "photo":
                    continue
                key = photo["ratingKey"]
                src = photo["Media"][0]["Part"][0]["file"].replace(ppTagConfig.PHOTOS_LIBRARY_PATH_PLEX,"", 1)

                # make sure path seperator is equal in plex and ppTag
                if "\\" in ppTagConfig.PHOTOS_LIBRARY_PATH:
                    src = src.replace("/","\\")
                if src in doUpdateTemp or firstRun:

                    # update tags and rating
                    # print(key)
                    # print(src)
                    updateTagsAndRating(key, src)
                    try:
                        doUpdateTemp.remove(src)
                    except:
                        pass # ok if missing, probably firstRun
                    if not firstRun and len(doUpdateTemp) == 0:
                        toDo = False
                        break

    if not firstRun:
        for src in doUpdateTemp:
            logging.info("Skipped file not found in this section '%s'" % src)		 
    
    firstRun = False


class PhotoHandler(PatternMatchingEventHandler):
    patterns=["*"]
    ignore_patterns=["*thumb*"]

    def process(self, event):
        """
        event.event_type
            'modified' | 'created' | 'moved' | 'deleted'
        event.is_directory
            True | False
        event.src_path
            path/to/observed/file
        """
        if not event.is_directory:
            if (event.event_type == 'modified' or event.event_type ==  'created' or event.event_type == 'moved'):
		# check if file belongs to monitored section
                for folder in p.photoLocations:
                    if event.src_path.startswith(folder):
                        # put file into forced update list
                        pptag_path=event.src_path.replace(ppTagConfig.PHOTOS_LIBRARY_PATH,"", 1)
                        if pptag_path not in doUpdate:
                            logging.info("Queued '%s'", event.src_path)
                            lock.acquire()
                            doUpdate.append(pptag_path)
                            lock.release()
                            triggerProcess()
                        return
                logging.debug("Ignored file in wrong location: '%s'" % event.src_path)
            else:
                logging.info("Ignored event '%s' for file '%s'" % (event.event_type,event.src_path))

    def on_modified(self, event):
        self.process(event)

    def on_created(self, event):
        self.process(event)


if __name__ == '__main__':

    if ppTagConfig.LOG_LEVEL is None or ppTagConfig.LOG_LEVEL == '':
         ppTagConfig.LOG_LEVEL = 'CRITICAL'
    logging.basicConfig(level=getattr(logging,ppTagConfig.LOG_LEVEL), format='%(asctime)s %(levelname)s - %(message)s')

    if ppTagConfig.TIMEZONE is not None :
         os.environ['TZ'] = ppTagConfig.TIMEZONE

    lock = threading.Lock()

    # setup observer
    observer = Observer()
    observer.schedule(PhotoHandler(), path=ppTagConfig.PHOTOS_LIBRARY_PATH, recursive=True)

    p = plexUsers()

    # run at startup
    fetchPhotosAndProcess()

    # now start the observer
    observer.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
