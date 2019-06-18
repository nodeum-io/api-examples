import datetime
import requests
import time

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


def run_task(p_task_id):
    url = "{host}/api/rules/{task_id}/run".format(
        host=my_host, task_id=p_task_id)
    resp = requests.post(url, auth=my_auth)
    res = resp.json()

    if res['code'] == 200:
        print("Task started...")
        return True
    else:
        raise Exception(res)


def get_rule_execution_id(task_id, date):
    params = {
        "rule_id": task_id,
        "execution_date_start": date,
    }
    url = "{host}/api/rules/exec".format(host=my_host)

    for x in range(60):
        resp = requests.get(url, params=params)
        res = resp.json()
        if res['code'] == 200 and len(res['rules']) > 0:
            return res['rules'][0]['id']
        time.sleep(1)

    raise Exception(
        "Could not find rule execution for rule {}".format(task_id))


TASK_DONE = 1
TASK_ERROR = 3
TASK_STOPPED_BY_SYS = 6
TASK_FINISHED_WITH_WARNINGS = 8
TASK_STOPPED_BY_USER = 10
error_codes = [
    TASK_ERROR,
    TASK_STOPPED_BY_SYS,
    TASK_FINISHED_WITH_WARNINGS,
    TASK_STOPPED_BY_USER
]


def pool_rule_execution(p_exec_rule_id):
    url = "{host}/api/rules/exec/{exec_rule_id}".format(
        host=my_host, exec_rule_id=p_exec_rule_id)

    while True:
        resp = requests.get(url)
        res = resp.json()
        if res['code'] == 200:
            status_code = res['rule']['status_code']
            if status_code == TASK_DONE:
                return res['rule']
            elif status_code in error_codes:
                raise Exception(res)
            else:
                print(
                    "Processed {}/{} files".format(
                        res['rule']['processed_files'],
                        res['rule']['total_files']
                    )
                )
        else:
            raise Exception(res)

        time.sleep(1)


task_id = create_task()
print("Task id is {}".format(task_id))

run_task(task_id)
print("Task will be executed ...")

date = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
exec_id = get_rule_execution_id(task_id, date)
print("Exec id is {}".format(exec_id))

print(pool_rule_execution(exec_id))
