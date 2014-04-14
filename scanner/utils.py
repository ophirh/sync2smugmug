import datetime


def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj


def date_hook(json_dict):
    for (key, value) in json_dict.items():
        # noinspection PyBroadException
        try:
            json_dict[key] = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
        except:
            pass
    return json_dict


