from astropy.io import fits
import pprint
import re
from irods.session import iRODSSession
import logzero
from logzero import logger
import auth
import logging
import os



def prog_lock_acq(lpath):
    '''
    locking function
    '''

    fd = None
    try:
        fd = os.open(lpath, os.O_CREAT)
        fcntl.flock(fd, fcntl.LOCK_NB | fcntl.LOCK_EX)
        return True
    except (OSError, IOError):
        if fd:
            os.close(fd)
        return False


def fail_and_move(message, obj, session):

    # Create an error message
    err_obj = session.data_objects.create("/iplant/home/shared/phantom_echoes/phantom_echoes_MEV1/validation_failed/"+obj.name+".err")
    with err_obj.open('w') as f:
        f.write(message.encode())
    session.data_objects.move(obj.path, "/iplant/home/shared/phantom_echoes/phantom_echoes_MEV1/validation_failed")



try:
    if ("fix_me" in auth.password):
        logger.error("you didn't update the password in auth.py")
        exit(1)
    # Setup rotating logfile with 3 rotations, each with a maximum filesize of 1MB:
    logzero.logfile("verssa-validation.log", maxBytes=1e6, backupCount=3)
    logzero.loglevel(logging.DEBUG)
    
except Exception as e:
    logger.exception(e)
    logger.error("could not setup so we are exiting")
    exit(-1)


#   We don't want multiple of this program running at once
if not prog_lock_acq('singleton.lock'):
    logger.error("There was another instance running")
    exit(-1)


with iRODSSession(host='data.cyverse.org', port=1247, user=auth.username, password=auth.password, zone='iplant') as session:
    coll = session.collections.get("/iplant/home/shared/phantom_echoes/phantom_echoes_MEV1")
    for col in coll.subcollections:

        #   skip validation failed folder
        if ("validation_failed" in str(col)):
            # for obj in col.data_objects:
            #     print (obj.name)
            #     # obj.unlink(force=True)
            continue

        # continue

        for obj in col.data_objects:

            vkeys = obj.metadata.get_all('validated')
            if (len(vkeys) >= 1):
                continue

            piece_size = 26214400 # 4 KiB
            with open("tmpvalid.fit", "wb") as new_file:
                with obj.open('r') as cache:
                    while True:
                        piece = cache.read(piece_size)

                        if piece == "":
                            break # end of file

                        if not piece:
                            break
                        
                        new_file.write(piece)
            
            filename = obj.name
            tmp_filename = "tmpvalid.fit"
            hdul = fits.open(tmp_filename)

            if (len(hdul) < 0 ):
                fail_and_move("ERROR: no header info?", obj, session)
                continue
            hdr = hdul[0].header  # the primary HDU header

            if ("DATE-OBS" not in hdr):
                fail_and_move("ERROR: missing header DATE-OBS for: " + filename, obj, session)
                continue
            if ("EXPTIME" not in hdr):
                fail_and_move("ERROR: missing header EXPTIME for: " + filename, obj, session)
                continue
            if ("OBJCTRA" not in hdr):
                fail_and_move("ERROR: missing header OBJCTRA for: " + filename, obj, session)
                continue
            if ("OBJCTDEC" not in hdr):
                fail_and_move("ERROR: missing header OBJCTDEC for: " + filename, obj, session)
                continue
            if ("SITELAT" not in hdr):
                fail_and_move("ERROR: missing header SITELAT for: " + filename, obj, session)
                continue
            if ("SITELONG" not in hdr):
                fail_and_move("ERROR: missing header SITELONG for: " + filename, obj, session)
                continue
            if ("SITEELEV" not in hdr):
                fail_and_move("ERROR: missing header SITEELEV for: " + filename, obj, session)
                continue
            if ("TELESCOP" not in hdr):
                fail_and_move("ERROR: missing header TELESCOP for: " + filename, obj, session)
                continue
            if ("COUNTRY" not in hdr):
                fail_and_move("ERROR: missing header COUNTRY for: ", obj, session)
                continue

            #2019-05-05T03:25:53.30(:)
            if not re.match(r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.*", hdr['DATE-OBS']):
                fail_and_move("Error DATE-OBS bad match", obj, session)
                continue

            if ("float" not in str(type(hdr['EXPTIME']))):
                fail_and_move("Error EXPTIME bad match", obj, session)
                continue

            if not re.match(r"-?\d+ -?\d+ -?\d+", hdr['OBJCTRA']):
                fail_and_move("Error OBJCTRA bad match", obj, session)
                continue

            if not re.match(r"-?\d+ -?\d+ -?\d+", hdr['OBJCTDEC']):
                fail_and_move("Error OBJCTDEC bad match", obj, session)
                continue

            if not re.match(r"-?\d+ -?\d+ -?\d+", hdr['SITELAT']):
                fail_and_move("Error SITELAT bad match", obj, session)
                continue

            if not re.match(r"-?\d+ -?\d+ -?\d+", hdr['SITELONG']):
                fail_and_move("Error SITELONG bad match", obj, session)
                continue

            if not re.match(r"-?\d+\.?\d+", hdr['SITEELEV']):
                fail_and_move("Error SITEELEV bad match", obj, session)
                continue

            if not re.match(r".+", hdr['TELESCOP']):
                fail_and_move("Error TELESCOP bad match", obj, session)
                continue

            if not re.match(r"...", hdr['COUNTRY']):
                fail_and_move("Error COUNTRY bad match", obj, session)
                continue


            # Finally validate the meta-data
            obj.metadata.add('validated', 'true')
            obj.metadata.add('COUTNRY', str(hdr['COUTNRY']))
            obj.metadata.add('TELESCOP', str(hdr['TELESCOP']))
            obj.metadata.add('SITEELEV', str(hdr['SITEELEV']))
            obj.metadata.add('SITELONG', str(hdr['SITELONG']))
            obj.metadata.add('SITELAT', str(hdr['SITELAT']))
            obj.metadata.add('OBJCTDEC', str(hdr['OBJCTDEC']))
            obj.metadata.add('OBJCTRA', str(hdr['OBJCTRA']))
            obj.metadata.add('EXPTIME', str(hdr['EXPTIME']))
            obj.metadata.add('DATE-OBS', str(hdr['DATE-OBS']))
    






