from irods.session import iRODSSession
import os
import fcntl
import time
import logzero
from logzero import logger
import logging

import getpass
import pprint
import requests
import auth
from tinydb import TinyDB, Query


try:
    db = TinyDB('db.json')
    # Setup rotating logfile with 3 rotations, each with a maximum filesize of 1MB:
    logzero.logfile("verssa-automation.log", maxBytes=1e6, backupCount=3)
    logzero.loglevel(logging.INFO)
    session = iRODSSession(host='data.cyverse.org', port=1247, user=auth.username,
                           password=auth.password, zone='iplant')
    r = requests.get("https://de.cyverse.org/terrain/token", auth=(auth.username, auth.password))
    r.raise_for_status()
    token = r.json()['access_token']
    auth_headers = {"Authorization": "Bearer " + token}
except Exception as e:
    logger.exception(e)
    logger.error("could not setup so we are exiting")
    exit(-1)

def load_plugin(name):
    mod = __import__("%s" % name)
    return mod

def call_plugin(name, *args, **kwargs):
    plugin = load_plugin(name)
    plugin.plugin_main(*args, **kwargs)


def main():
    ''' Simple main function to run logic of the automation process


    Specifically this function ensures that it isn't already running
    via the lock function. It also updates statuses on previously run projects.
    Next it moves competed projects. Finally it looks for new data files to
    run projects on
    '''

    if not prog_lock_acq('singleton.lock'):
        logger.error("There was another instance running")
        exit(-1)

    entry = Query()
    result = db.search((entry.status == "Submitted") | (entry.status == "Running"))
    if (len(result) > 0):
        updateRunningData(result)

    result = db.search(entry.status == "Completed")
    if (len(result) > 0):
        moveCompletedData(result)

    if session is None:
        logger.error("There was an error creating the cyverse session")
        exit(-1)

    coll = session.collections.get("/iplant/home/shared/ssa-arizona/demo/incoming")

    for obj in coll.data_objects:
        logger.debug(obj)
        ftype = obj.name.split(".")[-1]
        logger.debug(ftype)

        entry = Query()
        result = db.search(entry.name == obj.path)
        if (len(result)) > 0:
            logger.info("Skipping " + obj.name + " because it's already been submitted")
            continue

        v = dict(obj=obj, db=db, auth_headers=auth_headers, logger=logger)
        call_plugin(ftype, v)

def updateRunningData(result):
    '''
    Takes a list of currently unfinished apps and checks if they are finished
    '''

    for x in result:
        try:
            logger.debug(x['id'])
            r = requests.get("https://de.cyverse.org/terrain/analyses/{0}/history"
                             .format(x['id']), headers=auth_headers)
            r.raise_for_status()
            logger.debug(r.json())

            newStatus = (r.json()['steps'][0]['status'])
            entry = Query()
            db.update({'status': newStatus}, entry.name == x['name'])
        except Exception as e:
            logger.exception(e)


def moveCompletedData(result):
    '''
    Takes a list of completed apps and attempts to move their data file
    '''

    for x in result:
        try:
            session.data_objects.move(x['name'], '/iplant/home/shared/ssa-arizona/demo/data')
            entry = Query()
            db.remove(entry.name == x['name'])
            logger.info("We moved " + x['name'] + " into the completed directory.")
        except Exception as e:
            if "irods.exception" in (str(type(e))):
                logger.error("Ran into a custom irods exception when trying to move a file,"
                             " probably a permissions issue")
            else:
                logger.exception(e)

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


if __name__ == '__main__':
    main()
