# --*-- coding: utf-8 --*--
import random
import string
import time

import numpy as np
from locust.runners import MasterRunner, LocalRunner

from locustfiles.document.document_op_clients import  DingoDbDocumentTestUser


import logging

from locust import task, events

from param import sdk_client,x,get_index_name

from dingodb.common.document_rep import DocumentType, DocumentColumn, DocumentSchema


def gen_words(count,random,word_length):
    all_words=[]
    for i in range(count):
        a = random.choices(string.ascii_letters, k=word_length)
        all_words.append(''.join(a))
    return all_words



def gen_doc_data(count:int,id_start:int):
#     text字段由10个单词组成（每个单词后+i64值）
#   f64是一个随机的浮点数
#  i64 是随机浮点数前10位组成的整数
#   bytes是text第一个单词后+i64的值
    docs=[]
    for id in range(id_start,id_start+count):
        random.seed(id)
        all_words=gen_words(10,random,5)
        f64=random.random()+id
        i64=int(f64*1e10)
        text=','.join([f'{x}{i64}' for x in all_words])
        bytes=f'{all_words[0]}_{all_words[1]}_{i64}'
        docs.append({
            'text':text,
            'i64':i64,
            'f64':f64,
            'bytes':bytes
        })
    return docs


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--init_data_num", type=int, env_var="LOCUST_INIT_DATA_NUM", default=10000, help="index init data num")
    # Set `include_in_web_ui` to False if you want to hide from the web UI
    parser.add_argument("--operand_num", type=int, env_var="LOCUST_OPERAND_NUM",default=3)

    parser.add_argument("--index_name_prefix", type=str, env_var="LOCUST_INDEX_PREFIX", default="document_locust_index")



@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    if isinstance(environment.runner,(MasterRunner,LocalRunner)):
        init_data_num = environment.parsed_options.init_data_num
        operand_num = environment.parsed_options.operand_num

        index_name,empty_index_name=get_index_name(environment)


        logging.info(f"开始重建索引，当前节点{environment.runner},索引名称：{index_name},{empty_index_name}")

        try:
            x.delete_index(index_name)

        except:
            pass

        try:
            x.delete_index(empty_index_name)
        except:
            pass

        #
        scheme = DocumentSchema()
        col = DocumentColumn("text", DocumentType.STRING)
        scheme.add_document_column(col)
        col = DocumentColumn("i64", DocumentType.INT64)
        scheme.add_document_column(col)
        col = DocumentColumn("f64", DocumentType.DOUBLE)
        scheme.add_document_column(col)
        col = DocumentColumn("bytes", DocumentType.BYTES)
        scheme.add_document_column(col)

        x.create_index(empty_index_name, scheme, 3, start_id=1)

        operand=None

        if init_data_num >=operand_num:
            operand= list(np.linspace(0,init_data_num, operand_num, endpoint=False, dtype=int))[1:]


        x.create_index(index_name, scheme, 3, operand=operand,start_id=1)

        time.sleep(5)
        if init_data_num > 0:
            if init_data_num >=1000:
                i=0
                while init_data_num-i>=1000:
                    documents = gen_doc_data(1000,i+1)
                    document_add_out = x.document_add(index_name, documents)
                    document_add_out=document_add_out.to_dict()
                    assert None not in document_add_out
                    assert len(document_add_out) == 1000
                    logging.info(f"数据初始化完成1000条")
                    i+=1000
                else:
                    if i>init_data_num:
                        documents = gen_doc_data(init_data_num-i+1000, i-1000+1)
                        document_add_out = x.document_add(index_name, documents)
                        document_add_out = document_add_out.to_dict()
                        assert None not in document_add_out
                        assert len(document_add_out) == init_data_num-i+1000
                        logging.info(f"数据初始化完成{init_data_num-i+1000}条")

        logging.info(f"数据初始化完成")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    if isinstance(environment.runner, MasterRunner):
        logging.info(f"测试完成，当前节点{environment.runner}")
        print("A new test is ending")




class Dummy(DingoDbDocumentTestUser):
    sdk_client=sdk_client



    def document_add_test(self):
        pass

    @task
    def document_search_test(self):
        init_data_num = self.environment.parsed_options.init_data_num
        seed_num = random.choice(range(1,init_data_num+1))
        doc=gen_doc_data(1,seed_num)
        self.client.locust_document_search(self.index_name,
                                           doc[0].get('text'),int(doc[0].get('f64')))







