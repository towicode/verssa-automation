import requests

def plugin_main(args, **kwargs):
    '''
    Runs the PowerState Classifier Cyvere app with the input of the irods object obj
    '''
    try:
        logger = args['logger']
        auth_headers = args['auth_headers']
        db = args['db']
        obj = args['obj']

        query_params = {"search": "Power State Classifier (Verssa)"}
        r = requests.get("https://de.cyverse.org/terrain/apps", headers=auth_headers, params=query_params)
        r.raise_for_status()
        app_listing = r.json()["apps"][0]
        system_id = app_listing["system_id"]
        app_id = app_listing["id"]
        logger.debug("System ID: " + system_id)
        logger.debug("App ID: " + app_id)

        url = "https://de.cyverse.org/terrain/apps/{0}/{1}".format(system_id, app_id)
        r = requests.get(url, headers=auth_headers)
        r.raise_for_status()

        parameter_id = r.json()["groups"][0]["parameters"][0]["id"]
        logger.debug("Parameter ID: " + parameter_id)

        request_body = {
            "config": {
                parameter_id: obj.path
            },
            "name": "PowerStateClassifierAutomation",
            "app_id": app_id,
            "system_id": system_id,
            "debug": False,
            "output_dir": "/iplant/home/shared/ssa-arizona/demo/analyses",
            "notify": True
        }

        r = requests.post("https://de.cyverse.org/terrain/analyses", headers=auth_headers, json=request_body)
        r.raise_for_status()

        rj = r.json()

        newEntry = {
            'status': rj["status"],
            'id': rj["id"],
            'name': obj.path
        }
        db.insert(newEntry)
        logger.info("added new entry with name " + newEntry['name'])
    except Exception as e:
        logger.exception(e)
