# --*-- coding: utf-8 --*--
from dingodb import SDKClient, SDKDocumentDingoDB

dingo_co_addr="172.30.14.127:22001,172.30.14.128:22001,172.30.14.129:22001"

sdk_client = SDKClient(dingo_co_addr)
x = SDKDocumentDingoDB(sdk_client)

index_name_prefix="document_locust_index"


def get_index_name(environment):
    init_data_num = environment.parsed_options.init_data_num
    operand_num = environment.parsed_options.operand_num

    index_name_prefix = environment.parsed_options.index_name_prefix
    index_name = f"{index_name_prefix}_data{init_data_num}_partnum{operand_num}"
    empty_index_name = f"{index_name_prefix}_empty"

    return index_name,empty_index_name
