# --*-- coding: utf-8 --*--
import sys
import time

import locust
from locust import TaskSet, task, events, HttpUser, between
from locust.runners import MasterRunner

import logging


@events.request.add_listener
def my_request_handler(request_type, name, response_time, response_length, response,
                       context, exception, start_time, url, **kwargs):
    if exception:
        print(f"Request to {name} failed with exception {exception}")
    else:
        print(f"Successfully made a request to: {name}")
        print(f"The response was {response.text}")


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if isinstance(environment.runner, MasterRunner):
        logging.info("I'm on master node")
    else:
        logging.info("I'm on a worker or standalone node")


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    if isinstance(environment.runner, MasterRunner):
        logging.info("master test is starting")
    else:
        logging.info("worker test is starting")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    if isinstance(environment.runner, MasterRunner):
        logging.info("master test is ending")
    else:
        logging.info("worker test is ending")  # 问题：这里会执行两次


class UserBehaviour(TaskSet):

    def on_start(self):
        """ on_start is called when a Locust start before any task is scheduled """
        # 创建表
        logging.info("user start")

    def on_stop(self):
        """ on_stop is called when the TaskSet is stopping """
        # 删除表
        logging.info("user stop")

    @task(2)
    def test1(self):
        self.client.test_fun1()
        # logging.info("执行test_fun1")

    @task(3)
    def test2(self):
        self.client.test_fun2()
        # logging.info("执行test_fun2")


class MyUser(locust.User):
    wait_time = between(5, 15)
    tasks = [UserBehaviour]

    def __init__(self, environment):
        super().__init__(environment)
        self.client = TestClient(environment=environment)


class TestClient:

    def __init__(self, *, environment):
        self.environment = environment

    def test_fun1(self):
        add_time_start = time.perf_counter() * 1000
        # logging.info(self._get_function_name())
        escape = time.perf_counter() * 1000 - add_time_start

        request_meta = {
            "request_type": self._get_function_name(),
            "name": self._get_function_name(),
            "start_time": time.time(),
            "response_length": 0,  # calculating this for an xmlrpc.client response would be too hard
            "response_time": escape,
            "response": "ok",
            "context": {},  # see HttpUser if you actually want to implement contexts
            "exception": None,
        }

        self.environment.events.request.fire(**request_meta)

    def test_fun2(self):
        add_time_start = time.perf_counter() * 1000
        escape = time.perf_counter() * 1000 - add_time_start

        request_meta = {
            "request_type": self._get_function_name(),
            "name": self._get_function_name(),
            "start_time": time.time(),
            "response_length": 0,  # calculating this for an xmlrpc.client response would be too hard
            "response_time": escape,
            "response": "ok",
            "context": {},  # see HttpUser if you actually want to implement contexts
            "exception": None,
        }

        self.environment.events.request.fire(**request_meta)

    def _get_function_name(self):
        return sys._getframe().f_back.f_code.co_name