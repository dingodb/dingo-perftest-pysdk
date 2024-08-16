# --*-- coding: utf-8 --*--
import dingodb
from dingodb import SDKClient, SDKVectorDingoDB

dingo_co_addr="172.30.14.127:22001,172.30.14.128:22001,172.30.14.129:22001"

sdk_client = SDKClient(dingo_co_addr)
x = SDKVectorDingoDB(sdk_client)

index_name="demo-m-64-efconstruction-500"
d=128