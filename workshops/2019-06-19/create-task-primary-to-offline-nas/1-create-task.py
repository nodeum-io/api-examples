import requests

my_host = "http://10.200.1.11"
my_auth = ('admin', 'password')


def create_task():
    task = {
        "rule": {
            "name": "Workshop",
            "comment": "",
            "type": 36,
            "sources": [
                {
                    "file": {
                        "id": "/nfs-primary/bamfiles"
                    }
                }
            ],
            "destinations": [
                {
                    "nas_pool": {
                        "id": 5,
                    }
                }
            ],
            "schedule": None,
            "priority": 7,
            "options": [
                {
                    "type": 205,
                    "value": "1"
                },
                {
                    "type": 209,
                    "value": "1"
                },
                {
                    "type": 300,
                    "value": "1"
                }
            ],
            "metadata": {},
            "file_action": 1
        }
    }

    url = "{host}/api/rules".format(host=my_host)

    resp = requests.post(url, json=task, auth=my_auth)
    res = resp.json()

    if res['code'] == 200:
        return res['rule']['id']
    else:
        raise Exception(res)


task_id = create_task()
print("Task id is {}".format(task_id))
