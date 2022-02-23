from hashlib import new
from multiprocessing.dummy import freeze_support
import os
import json
import sys
import shutil
from PIL import  ExifTags, Image
import glob
from datetime import datetime
import whatimage
import pyheif
from pillow_heif import register_heif_opener
import piexif
import time
import threading
from multiprocessing import Process,Pool
from pathlib import Path, WindowsPath
register_heif_opener()
cal_months =['January','February','March','April','May','June','July','August','September','October','November','December']

def worker(filename: str):
    image = Image.open(filename)
    image_exif = image.getexif()
    if image_exif:
            # Make a map with tag names and grab the datetime
            exif = { ExifTags.TAGS[k]: v for k, v in image_exif.items() if k in ExifTags.TAGS and type(v) is not bytes }
            picdate = datetime.strptime(exif['DateTime'], '%Y:%m:%d %H:%M:%S')

            # Load exif data via piexif
            exif_dict = piexif.load(image.info["exif"])

            # Update exif data with orientation and datetime
            exif_dict["0th"][piexif.ImageIFD.DateTime] = picdate.strftime("%Y:%m:%d %H:%M:%S")
            exif_dict["0th"][piexif.ImageIFD.Orientation] = 1
            exif_bytes = piexif.dump(exif_dict)

            # Save image as jpeg
            settime = time.mktime(picdate.timetuple())
            image.save(filename.replace('.heic','.jpg'), "jpg", exif= exif_bytes)
            os.utime(filename.replace('.heic','.jpg'), (settime, settime))

class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

try:
    print(os.getcwd())
    with open('config.json') as f:
        cfg = AttrDict(json.load(f))
except Exception as ex:
    print("Error in getting CONFIGURATION, check config.json. Exiting")
    sys.exit(10)    
print("Source folder", cfg.SOURCE_FOLDER, "Destination folder", cfg.DESTINATION_FOLDER)
print("Phase 1 convert heic to jpg")
tcount = 0
filecount = 0
work = []


for filename in glob.iglob(cfg.SOURCE_FOLDER + '/*.HEIC', recursive=True):
    filecount +=1
    #filename =  str(WindowsPath(filename_obj))
    if filecount % 100 == 0: print(filename, filecount)
    if os.path.basename(filename).startswith(cfg.NAME_PREFIX) and os.path.isfile(filename):
        with open(filename, 'rb') as f:
            data = f.read()
            fmt = whatimage.identify_image(data)
            if fmt and fmt.lower() in ['heic', 'avif']:
                if os.path.exists(str(filename).lower().replace('.heic','.jpg')):
                    continue
                work.append(filename)
                #i = pyheif.read_heif(data)
                #pi = Image.frombytes(mode=i.mode, size=i.size, data=i.data)
                #pi.save(filename.replace('.heic','.jpg'), format="jpeg")
                print("Convert",filename)
                if True:
                    if tcount == 0:
                        thread0 = threading.Thread(target=worker, args=(filename,))
                        tcount +=1
                        thread0.start()
                        thread0.join()               
                        continue
                    if tcount == 1:
                        thread1 = threading.Thread(target=worker, args=(filename,))
                        tcount +=1
                        thread1.start()
                        thread1.join()               
                        continue
                    if tcount == 2:
                        thread2 = threading.Thread(target=worker, args=(filename,))
                        tcount +=1
                        thread2.start()
                        thread2.join()               
                        continue
                    if tcount == 3:
                        thread3 = threading.Thread(target=worker, args=(filename,))
                        tcount +=1
                        thread3.start()
                        thread3.join()               
                        continue
                    if tcount == 4:
                        thread4 = threading.Thread(target=worker, args=(filename,))
                        tcount +=1
                        thread4.start()
                        thread4.join()               
                        continue
                    if tcount == 5:
                        thread5 = threading.Thread(target=worker, args=(filename,))
                        tcount +=1
                    thread5.start()
                    thread5.join()               
                    tcount=0
                    continue
#p = Pool(4)
#p.map(worker, work)

                #p = Process(target=worker, args=work)
                #pool.map(worker, filename)
'''
                if False:
                    image = Image.open(filename)
                    image_exif = image.getexif()
                    if image_exif:
                            # Make a map with tag names and grab the datetime
                            exif = { ExifTags.TAGS[k]: v for k, v in image_exif.items() if k in ExifTags.TAGS and type(v) is not bytes }
                            picdate = datetime.strptime(exif['DateTime'], '%Y:%m:%d %H:%M:%S')

                            # Load exif data via piexif
                            exif_dict = piexif.load(image.info["exif"])

                            # Update exif data with orientation and datetime
                            exif_dict["0th"][piexif.ImageIFD.DateTime] = picdate.strftime("%Y:%m:%d %H:%M:%S")
                            exif_dict["0th"][piexif.ImageIFD.Orientation] = 1
                            exif_bytes = piexif.dump(exif_dict)

                            # Save image as jpeg
                            settime = time.mktime(picdate.timetuple())
                            image.save(filename.replace('.heic','.jpg'), "jpg", exif= exif_bytes)
                            os.utime(filename.replace('.heic','.jpg'), (settime, settime))
                #pool.terminate()
'''
'''
                
if tcount > 0:
                thread0.start()
                thread0.join()               
if tcount > 1:
                thread1.start()
                thread1.join()               
if tcount > 2:
                thread2.start()
                thread2.join()               
if tcount > 3:
                thread3.start()
                thread3.join()               
if tcount > 4:

                thread4.start()
                thread4.join()               
if tcount > 5:
                thread5.start()
                thread5.join()               
'''
for filename in glob.iglob(cfg.SOURCE_FOLDER + '**/**', recursive=True):
    if os.path.basename(filename).startswith(cfg.NAME_PREFIX) and os.path.isfile(filename):
        try:
            
            with open(filename, 'rb') as f:
                data = f.read()
                fmt = whatimage.identify_image(data)
                if fmt and fmt.lower() in ['heic', 'avif']:
                    continue
                im = Image.open(filename)
                image_exif = Image.open(filename)._getexif()
                if image_exif:
                    # Make a map with tag names
                    exif = { ExifTags.TAGS[k]: v for k, v in image_exif.items() if k in ExifTags.TAGS and type(v) is not bytes }
                    #for e in exif:
                    #    print(e,exif[e])
                    #print(json.dumps(exif, indent=4))
                    # Grab the date
                    if 'DateTimeOriginal' in exif:
                        new_year_folder = datetime.strptime(exif['DateTimeOriginal'],  '%Y:%m:%d %H:%M:%S').strftime('%Y')
                        new_month_folder = datetime.strptime(exif['DateTimeOriginal'],  '%Y:%m:%d %H:%M:%S').strftime('%B')
                        picdate = datetime.strptime(exif['DateTimeOriginal'], '%Y:%m:%d %H:%M:%S')
                        mm = str(picdate.month).zfill(2)
                    else:
                        modtime= os.path.getmtime(filename)
                        createtime = os.path.getctime(filename)
                        picdate = modtime if modtime < createtime else createtime
                        year,month,day,hour,minute,second=time.localtime(picdate)[:-3]
                        new_year_folder = year
                        new_month_folder = cal_months[month - 1]
                        mm = str(month).zfill(2)
                    photo_name= os. path. basename(filename)
                    if cfg.FOLDER_STRUCTURE ==  "YYYY\\MONTH":
                        new_folder = f'{cfg.DESTINATION_FOLDER}\{new_year_folder}'
                        if not os.path.exists(new_folder):
                            os.makedirs(new_folder)
                        new_folder = f'{cfg.DESTINATION_FOLDER}\{new_year_folder}\{new_month_folder}'
                        if not os.path.exists(new_folder):
                            os.makedirs(new_folder)
                    elif cfg.FOLDER_STRUCTURE ==  "YYYY\\MM MONTH":
                        new_folder = f'{cfg.DESTINATION_FOLDER}\{new_year_folder}'
                        if not os.path.exists(new_folder):
                            os.makedirs(new_folder)
                        new_folder = f'{cfg.DESTINATION_FOLDER}\{new_year_folder}\{mm} {new_month_folder}'
                        if not os.path.exists(new_folder):
                            os.makedirs(new_folder)
                    elif cfg.FOLDER_STRUCTURE ==  "MONTHYYYY":
                        new_folder = f'{cfg.DESTINATION_FOLDER}\{new_month_folder}{new_year_folder}'
                        if not os.path.exists(new_folder):
                            os.makedirs(new_folder)
                    elif cfg.FOLDER_STRUCTURE ==  "MMMONTHYYYY":
                        new_folder = f'{cfg.DESTINATION_FOLDER}\{mm}{new_month_folder}{new_year_folder}'
                        if not os.path.exists(new_folder):
                            os.makedirs(new_folder)

                    split_tup = os.path.splitext(filename)
                    basename = os.path.basename(filename)
                    basename_without_ext = os.path.splitext(os.path.basename(filename))[0]
                    #os.utime(filename.replace('.heic','.jpg'), (settime, settime))

                    if not im.mode == 'RGB':
                        im = im.convert('RGB')
                        newfilename = f'{new_folder}\{basename_without_ext}.jpg'
                        exist = os.path.exists(newfilename)
                        if cfg.OVERWRITE_EXISTING_FILE or not exist:
                            im.save(newfilename, quality=95)
                            if type(picdate) != float:
                                settime = time.mktime(picdate.timetuple())
                            else:
                                settime=picdate                            
                            print("Saved", newfilename)
                            os.utime(newfilename, (settime, settime))
                        else:
                            existing_size= os.path.getsize(newfilename)
                            new_size = os.path.getsize(filename)
                            if existing_size == new_size:
                                continue

                            counter = 0
                            while exist:
                                newfilename = f'{new_folder}\{basename_without_ext}_{counter}.jpg'
                                exist = os.path.exists(newfilename)
                                counter +=1
                            im.save(newfilename, quality=95)
                            if type(picdate) != float:
                                settime = time.mktime(picdate.timetuple())
                            else:
                                settime=picdate                            
                            print("Saved", newfilename)

                            #print("Skipped, already exists: {newfilename}")
                        if cfg.DELETE: os.remove(filename)

                    else:
                        newfilename =f'{new_folder}\{basename_without_ext}.jpg'
                        exist = os.path.exists(newfilename)
                        if cfg.OVERWRITE_EXISTING_FILE or not exist:
                            newname = shutil.copy2(filename, f'{new_folder}\{basename_without_ext}.jpg')
                            if type(picdate) != float:
                                settime = time.mktime(picdate.timetuple())
                            else:
                                settime=picdate
                            os.utime(newname, (settime, settime))
                            print("Copied", f'{filename} to {newname}')
                        else:
                            existing_size= os.path.getsize(newfilename)
                            new_size = os.path.getsize(filename)
                            if existing_size == new_size:
                                continue
                            counter = 0
                            while exist:
                                newfilename = f'{new_folder}\{basename_without_ext}_{counter}.jpg'
                                exist = os.path.exists(newfilename)
                                counter +=1
                            newname = shutil.copy2(filename, newfilename)
                            if type(picdate) != float:
                                settime = time.mktime(picdate.timetuple())
                            else:
                                settime=picdate
                            os.utime(newname, (settime, settime))
                            print("Copied", f'{filename} to {newname}')

                            #print("Skipped, already exists: {newfilename}")
                        if cfg.DELETE: os.remove(filename)
                    #date_obj = datetime.strptime(exif['DateTimeOriginal'], '%Y:%m:%d %H:%M:%S')
                    #print(date_obj)
                else:
                    print('Unable to get date from exif for %s' % filename)                
                #exif = im.getexif()
                #creation_time = im.getexif().get(36867)
                #print(filename, im.format, f"{im.size}x{im.mode}")
                
        except OSError as ex:
            pass
            #print(ex)


        #f, e = os.path.splitext(filename)
        #if e =='jpg':



#shutil.copy2(src, dst, *, follow_symlinks=True)