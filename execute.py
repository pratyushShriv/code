from argparse import ArgumentParser
from enum import Enum
from requests.adapters import HTTPAdapter, Retry, RetryError
from sys import exit
from time import sleep
import requests

# TODO: Format the file
class Environment(Enum):
    dev = 'dev'
    staging = 'staging'
    production = 'production'
    app2 = "app2"

    def get_env_url(self, endpoint_type: str) -> str:
        environments = {
            'dev': {
                'api': 'https://api-dev.virtuoso.qa/api',
                'ui': 'https://app-dev.virtuoso.qa'
            },
            'staging': {
                'api': 'https://api-staging.virtuoso.qa/api',
                'ui': 'https://api-staging.virtuoso.qa'
            },
            'production': {
                'api': 'https://api.virtuoso.qa/api',
                'ui': 'https://app.virtuoso.qa'
            },
            'app2': {
                'api': 'https://api-app2.virtuoso.qa/api',
                'ui': 'https://app-app2.virtuoso.qa'
            }
        }
        return environments[self.value][endpoint_type]
    
    def __str__(self):
        return self.value


class VirtuosoAPI:
    token = ''
    environment_api_url = ''
    environment_ui_url = ''
    finished_status = ['FINISHED', 'CANCELED', 'FAILED']
    failed_outcome = ['FAIL', 'ERROR']
    def __init__(self,
                 token: str,
                 environment_api_url: str,
                 environment_ui_url: str):
        self.token = token
        self.environment_api_url = environment_api_url
        self.environment_ui_url = environment_ui_url

    def _make_post(self, url: str, body: dict = {}) -> dict:

        session = requests.Session()
        # Retrying on all bad status codes
        status_forcelist = tuple(range(401, 600))
        # https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
        retries = Retry(total=5,
                        backoff_factor=1,
                        status_forcelist=status_forcelist,
                        allowed_methods=['POST'])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        headers = {
            'Authorization': f'Bearer {self.token}'
        }
        try:
            response = session.post(url=url, headers=headers, data=body)
        except RetryError:
            print('Failed to make request to Virtuoso')
            exit(1)
        return response.json()
    
    def _make_put(self, url: str, body: dict = {}) -> dict:

        session = requests.Session()
        # Retrying on all bad status codes
        status_forcelist = tuple(range(401, 600))
        # https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
        retries = Retry(total=5,
                        backoff_factor=1,
                        status_forcelist=status_forcelist,
                        allowed_methods=['PUT'])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        headers = {
            'Authorization': f'Bearer {self.token}'
        }
        try:
            response = session.put(url=url, headers=headers, data=body)
        except RetryError:
            print('Failed to make request to Virtuoso')
            exit(1)
        return response.json()

    def _make_get(self, url: str) -> dict:
        session = requests.Session()
        # Retrying on all bad status codes
        status_forcelist = tuple(range(401, 600))
        # https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
        retries = Retry(total=5,
                        backoff_factor=1,
                        status_forcelist=status_forcelist,
                        allowed_methods=['GET'])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        headers = {
            'Authorization': f'Bearer {self.token}'
        }
        try:
            response = session.get(url=url, headers=headers)
        except RetryError:
            print('Failed to make request to Virtuoso')
            exit(1)
        return response.json()        

    def get_goal_info(self, goal_id: int) -> dict:
        endpoint = f'{self.environment_api_url}/goals/{goal_id}?envelope=false'
        goal_info = self._make_get(endpoint)
        return goal_info

    def get_job_information(self, job_id: int):
        endpoint = f'{self.environment_api_url}/executions/{job_id}/status?envelope=false'
        response = self._make_get(endpoint)
        return response

    def execute_plan(self, plan_id: int):
        endpoint = f'{self.environment_api_url}/plans/executions/{plan_id}/execute?envelope=false'
        print(f'Starting Virtuoso execution of plan with id {plan_id}')
        response = self._make_put(endpoint)
        job_ids = response.get('jobs')
        job_finished_states = {}
        while True:
            for job_id in job_ids:
                if job_finished_states.get(job_id):
                    continue
                job = self.get_job_information(job_id)
                job_status = job.get('status')
                job_outcome = job.get('outcome')
                goal_id = job.get('goalId')
                if job_status in self.finished_status:
                    job_finished_states[job_id] = {
                        'outcome': job_outcome,
                        'goal_id': goal_id
                    }
                sleep(2)
            # Make sure that the list is sorted out before comparing them
            job_id_list = list(job_ids.keys())
            job_id_list.sort()
            job_finished_states_list = list(job_finished_states.keys())
            job_finished_states_list.sort()
            if job_id_list == job_finished_states_list:
                break 
            print(f'Virtuoso plan with id {plan_id} is running')
        print('Plan execution finished, checking all the jobs')
        failed = False
        for job_id, value in job_finished_states.items():
            if value['outcome'] in self.failed_outcome:
                goal_id = value['goal_id']
                goal_info = self.get_goal_info(goal_id)
                project_id = goal_info.get('projectId')
                print('At least 1 journey of your Virtuoso plan failed')
                print(f'Check the Virtuoso Execution for more details in {self.environment_ui_url}/#/project/{project_id}/execution/{job_id}')
                failed = True
        if failed:
            exit(2)
        print(f'Virtuoso execution for plan with id {plan_id} succeeded')


    def execute_goal_snapshot(self, goal_id: int, snapshot_id: int):
        endpoint = f'{self.environment_api_url}/goals/{goal_id}/snapshots/{snapshot_id}/execute?envelope=false'
        print(f'Starting Virtuoso execution of snapshot with id {snapshot_id} in goal with id {goal_id}')
        response = self._make_post(endpoint)
        job_id = response.get('id')
        if not job_id:
            print('Error getting jobId from Virtuoso')
        print('Waiting for job to complete')
        while True:
            job = self.get_job_information(job_id)
            job_status = job.get('status')
            job_outcome = job.get('outcome')
            if job_status in self.finished_status:
                break
            print(f'Virtuoso job is running with status: {job_status}')
            sleep(5)
        if job_outcome in self.failed_outcome:
            goal_info = self.get_goal_info(goal_id)
            project_id = goal_info.get('projectId')
            print(f'Virtuoso job for snapshot with id {snapshot_id} in goal with id failed with outcome: {job_outcome}')
            print(f'Check the Virtuoso Execution for more details in {self.environment_ui_url}/#/project/{project_id}/execution/{job_id}')
            exit(2)
        print(f'Virtuoso execution for snapshot with id {snapshot_id} in goal with id {goal_id} succeeded')
        
    def execute_goal(self, goal_id: int) -> int:
        endpoint = f'{self.environment_api_url}/goals/{goal_id}/execute?envelope=false'
        print(f'Starting Virtuoso execution of goal with id {goal_id}')
        response = self._make_post(endpoint)
        job_id = response.get('id')
        if not job_id:
            print('Error getting jobId from Virtuoso')
        print('Waiting for job to complete')
        while True:
            job = self.get_job_information(job_id)
            job_status = job.get('status')
            job_outcome = job.get('outcome')
            if job_status in self.finished_status:
                break
            print(f'Virtuoso job is running with status: {job_status}')
            sleep(5)
        if job_outcome in self.failed_outcome:
            goal_info = self.get_goal_info(goal_id)
            project_id = goal_info.get('projectId')
            print(f'Virtuoso job of goal with id {goal_id} failed with outcome: {job_outcome}')
            print(f'Check the Virtuoso Execution for more details in {self.environment_ui_url}/#/project/{project_id}/execution/{job_id}')
            exit(2)
        print(f'Virtuoso execution for goal {goal_id} succeeded')
    

parser = ArgumentParser(
                    prog='execute.py',
                    description='CI/CD script to execute Virtuoso goals or plans')
group = parser.add_mutually_exclusive_group(required=True)
parser.add_argument('--token', help="Virtuoso Token", required=True)
group.add_argument('--goal_id', help='ID of the Virtuoso goal to execute')
group.add_argument('--plan_id', help='ID of the Virtuoso pan to execute')
parser.add_argument('--snapshot_id', help='Snapshot ID of the Virtuoso goal to execute')
parser.add_argument('--env', help='Virtuoso Environment, default is production', type=Environment, choices=list(Environment), default="production")
parser.add_argument('--debug', help='Enable HTTP response debugging', action='store_true')

args = parser.parse_args()


if args.debug:
    import http
    http.client.HTTPConnection.debuglevel = 1


virtuoso_api = VirtuosoAPI(args.token, args.env.get_env_url('api'), args.env.get_env_url('ui'))


if args.goal_id:
    if args.snapshot_id:
        virtuoso_api.execute_goal_snapshot(args.goal_id, args.snapshot_id)
    else:
        virtuoso_api.execute_goal(args.goal_id)
else:
    virtuoso_api.execute_plan(args.plan_id)
