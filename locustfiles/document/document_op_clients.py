import logging
import sys
import time
import traceback

import locust
from dingodb import SDKClient, SDKDocumentDingoDB
from locust.exception import LocustError
from sqlalchemy import True_

from common.dingo_response import DingoResponse
from locustfiles.document import param


class DingoDocumentClient:
    def __init__(self, *, environment, sdk_client: SDKClient, top_k=1):
        self.environment = environment
        self.top_k = top_k
        self.dingo_client = SDKDocumentDingoDB(sdk_client)

    def locust_document_add(self, index_name, documents, ids=None):
        pass

    def locust_document_search(self, index_name, docs,expect_id):
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
        add_time_start = time.perf_counter() * 1000
        try:
            op_res = self.dingo_client.document_search(index_name, docs,top_n=1)
            escape = time.perf_counter() * 1000 - add_time_start
            op_res=op_res.to_dict()
            assert op_res[0].get('id')== expect_id
            op_res = DingoResponse(text=str(op_res))
            request_meta["response_length"] = len(str(op_res))
        except Exception as e:
            escape = time.perf_counter() * 1000 - add_time_start
            logging.error(f"search_doc:{docs},search_res=={str(op_res)},期望结果：{expect_id},实际search结果：{self.dingo_client.document_query(index_name,[op_res[0].get('id')],with_scalar_data=True)}")
            error_info = traceback.format_exc()
            request_meta["exception"] = error_info
            request_meta["response_length"] = len(str(error_info))
        finally:
            request_meta["response"] = str(op_res)
            request_meta["response_time"] = escape

        self.environment.events.request.fire(**request_meta)

    def _get_function_name(self):
        return sys._getframe().f_back.f_code.co_name


class DingoDbDocumentTestUser(locust.User):
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
            self.client = DingoDocumentClient(environment=environment, sdk_client=self.sdk_client, top_k=self.top_k)
        else:
            raise LocustError(f"You must specify the {attr_name}.")

        self.index_name, self.empty_index_name = param.get_index_name(self.environment)