# --*-- coding: utf-8 --*--
import json
import sys
import time
import traceback

import dingodb

import locust
from dingodb import SDKClient, SDKVectorDingoDB
from locust.exception import LocustError

import logging

from common.dingo_response import DingoResponse



class dingoDbClient:
    def __init__(self, *, environment, sdk_client: SDKClient, top_k=10):
        self.environment = environment
        self.top_k = top_k
        self.dingo_client = SDKVectorDingoDB(sdk_client)

    def locust_vector_add(self, index_name, scalars, vectors, ids=None):
        request_meta = {
            "request_type": self._get_function_name()[7:],
            "name": f"{index_name}",
            "start_time": time.time(),
            "response_length": 0,  # calculating this for an xmlrpc.client response would be too hard
            "response_time": 0,
            "response": None,
            "context": {},  # see HttpUser if you actually want to implement contexts
            "exception": None,
        }

        escape = 0
        add_res = "init_value"
        try:
            add_time_start = time.perf_counter() * 1000
            add_res = self.dingo_client.vector_add(index_name, scalars, vectors, ids)
            escape = time.perf_counter() * 1000 - add_time_start
            assert add_res != "init_value"
            assert len(add_res) == len(vectors)
            assert None not in add_res
            add_res = DingoResponse(text=json.dumps(add_res))
            request_meta["response_length"] = len(add_res.text)
        except Exception as e:
            logging.error(f"add_res=={add_res}")
            traceback.print_exc()
            error_info = traceback.format_exc()
            request_meta["exception"] = error_info
            request_meta["response_length"] = len(error_info)
        finally:
            request_meta["response"] = add_res
            request_meta["response_time"] = escape

        self.environment.events.request.fire(**request_meta)

    def locust_vector_search(self, index_name, vectors):
        request_meta = {
            "request_type": self._get_function_name()[7:],
            "name": f"{index_name}",
            "start_time": time.time(),
            "response_length": 0,  # calculating this for an xmlrpc.client response would be too hard
            "response_time": 0,
            "response": None,
            "context": {},  # see HttpUser if you actually want to implement contexts
            "exception": None,
        }

        escape = 0
        op_res = "init_value"
        try:
            add_time_start = time.perf_counter() * 1000
            op_res = self.dingo_client.vector_search(index_name, vectors, top_k=self.top_k)
            escape = time.perf_counter() * 1000 - add_time_start

            assert op_res != "init_value"

            assert isinstance(op_res, list)
            assert len(op_res) == len(vectors)
            vectorWithDistances = [s.get("vectorWithDistances") for s in op_res]
            assert all([len(vectorWithDistance) == self.top_k for vectorWithDistance in vectorWithDistances])
            op_res = DingoResponse(text=json.dumps(op_res))
            request_meta["response_length"] = len(op_res.text)
        except Exception as e:
            logging.error(f"search_res=={op_res}")
            traceback.print_exc()
            error_info = traceback.format_exc()
            request_meta["exception"] = error_info
            op_res = None
        finally:
            request_meta["response"] = op_res
            request_meta["response_time"] = escape

        self.environment.events.request.fire(**request_meta)

    def _get_function_name(self):
        return sys._getframe().f_back.f_code.co_name


class DingoDbUser(locust.User):
    abstract = True
    sdk_client = None
    top_k = 10

    def __init__(self, environment):
        super().__init__(environment)
        for attr_value, attr_name in ((self.sdk_client, "sdk_client"),):
            if attr_value is None:
                raise LocustError(f"You must specify the {attr_name}.")
            if not isinstance(self.sdk_client, SDKClient):
                raise LocustError(f" {attr_name} must is SDKClient class.")

        if self.sdk_client:
            self.client = dingoDbClient(environment=environment, sdk_client=self.sdk_client, top_k=self.top_k)
        else:
            raise LocustError(f"You must specify the {attr_name}.")
