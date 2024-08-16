# --*-- coding: utf-8 --*--
import sys

from locust.runners import MasterRunner

sys.path.append(".")
sys.path.append("..")

import logging
import time

import locust
from dingodb import DingoDB
import numpy as np
from locust import task, TaskSet, events, HttpUser
# from locust_influxdb_listener import InfluxDBSettings, InfluxDBListener

from param import x, d, index_name, sdk_client

from vector_op_client import DingoDbUser

scalars_cols = ["col1", "col2"]




def gen_vector(n, d, seed=np.random.RandomState(1234), is_with_scalar=False):
    start_time = time.time() * 1000
    xb = seed.rand(n, d).astype("float32")
    xb[:, 0] += np.arange(n)
    vectors = xb.tolist()

    scalars = None

    if is_with_scalar:
        scalars = []
        _, part1, part2 = list(np.linspace(0, n, 3, endpoint=False, dtype=int))

        # _, part1_1, part1_2 = list(np.linspace(0, part1, 3, endpoint=False, dtype=int))
        #
        # _, part2_1, part2_2 = list(np.linspace(part1, part2, 3, endpoint=False, dtype=int))
        #
        # _, part3_1, part3_2 = list(np.linspace(part2, n, 3, endpoint=False, dtype=int))

        for x in vectors:
            scalar_data = {}

            if x[0] <= part1:
                scalar_data[scalars_cols[0]] = "aa1"
                if int(x[0]) % 3 == 0:
                    scalar_data[scalars_cols[1]] = "bb3"
                elif int(x[0]) % 3 == 1:
                    scalar_data[scalars_cols[1]] = "bb1"
                else:
                    scalar_data[scalars_cols[1]] = "bb2"

            elif x[0] <= part2:
                scalar_data[scalars_cols[0]] = "aa2"
                if int(x[0]) % 3 == 0:
                    scalar_data[scalars_cols[1]] = "bb3"
                elif int(x[0]) % 3 == 1:
                    scalar_data[scalars_cols[1]] = "bb1"
                else:
                    scalar_data[scalars_cols[1]] = "bb2"
            else:
                scalar_data[scalars_cols[0]] = "aa3"
                if int(x[0]) % 3 == 0:
                    scalar_data[scalars_cols[1]] = "bb3"
                elif int(x[0]) % 3 == 1:
                    scalar_data[scalars_cols[1]] = "bb1"
                else:
                    scalar_data[scalars_cols[1]] = "bb2"

            scalars.append(scalar_data)

    end_time = time.time() * 1000

    return scalars, vectors


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    if isinstance(environment.runner,MasterRunner):
        logging.info(f"开始重建索引，当前节点{environment.runner}")

        try:
            x.delete_index(index_name)
        except:
            pass

        x.create_index(index_name, d, "flat", "cosine", replicas=3,
                       operand=[50000, 100000, 150000, 200000])

        s,v = gen_vector(1000,d)
        x.vector_add(index_name,s,v)


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    if isinstance(environment.runner, MasterRunner):
        logging.info(f"测试完成，当前节点{environment.runner}")
        print("A new test is ending")
        # x.delete_index(index_name)


# @events.init.add_listener
# def on_locust_init(environment, **_kwargs):
#     """
#     Hook event that enables starting an influxdb connection
#     """
#     # this settings matches the given docker-compose file
#     influxDBSettings = InfluxDBSettings(
#         influx_host='172.20.61.7',
#         influx_port='8086',
#         database='test-project',

#         # additional_tags tags to be added to each metric sent to influxdb
#         additional_tags={
#             'environment': 'test',
#             'some_other_tag': 'tag_value',
#         }
#     )
#     # start listerner with the given configuration
#     InfluxDBListener(env=environment, influxDbSettings=influxDBSettings)


class Dummy(DingoDbUser):
    sdk_client=sdk_client


    @task
    def vector_add_test(self):
        s,v = gen_vector(1,d)
        self.client.locust_vector_add(index_name,s,v)

    @task
    def vector_search_test(self):
        s, v = gen_vector(1, d)
        self.client.locust_vector_search(index_name,v)

