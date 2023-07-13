from flask import Flask, request, jsonify
from gevent import pywsgi
import argparse

from langchain.embeddings.huggingface import HuggingFaceEmbeddings
from config import *

from functools import lru_cache
from vectorstores import MyFAISS
from langchain.docstore.document import Document
import os
from utils import torch_gc
from sql_tool import MysqlHelper

app = Flask(__name__)

def _embeddings_hash(self):
    return hash(EMBEDDING_MODEL_PATH)

HuggingFaceEmbeddings.__hash__ = _embeddings_hash

embeddings = HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL_PATH,
                                                model_kwargs={'device': EMBEDDING_DEVICE})

mysqlhelper = MysqlHelper(MysqlHelper.conn_params)

vector_stores = {}

@lru_cache(1)
def load_vector_store(vs_path, embeddings, indexName):
    return MyFAISS.load_local(vs_path, embeddings, index_name=indexName)

def get_parser():
    """
    命令行参数
    """
    parser = argparse.ArgumentParser(description="相似度模型服务启动参数")
    parser.add_argument("--port", type=int, default=6100, required=False, help="服务对外访问端口")

    return parser

# 从数据库中将所有数据取出来并按索引名称分类初始化到对应的向量库索引中
@app.route("/initVecFromDB", methods=["get"])
def initVecFromDB():
    try:
        sql = "select * from qapair"
        result = mysqlhelper.get_all(sql,None)
        # 结果数据按照索引名称分组
        indexNameMap = {}
        for item in result:
            if item[3] not in indexNameMap:
                indexNameMap[item[3]] = []
            indexNameMap[item[3]].append(item)
        # 遍历索引名称，将对应的数据加载到对应的向量库索引中
        for key in indexNameMap:
            vector_store = MyFAISS.from_documents([Document(page_content=item[1], metadata={"source": item[1] + ":" + item[2]}) for item in indexNameMap[key]], embeddings)
            torch_gc()
            vector_store.save_local(VS_PATH, index_name=key)
            vector_stores[key] = vector_store
    except Exception as e:
        logger.error(e)
        return "问答数据转存本地向量库初始化失败" + e
    return "问答数据转存本地向量库初始化成功"

# 添加单条问答数据到对应的向量库索引中
@app.route("/saveQA", methods=["post"])
def saveQA():
    try:
        question = request.form.get("question")
        answer = request.form.get("answer")
        indexName = request.form.get("indexName")
        if not question or not answer or not indexName:
            return "知识库添加错误，请确认知识库名字、问题、答案是否正确！"
        # 将数据添加到数据库
        sql = "insert into qapair(question,answer,vecIndexName) values(%s,%s,%s)"
        params = (question,answer,indexName,)
        result = mysqlhelper.insert(sql,params)
        if result == 0:
            return "问答数据添加失败"
        docs = [Document(page_content=question, metadata={"source": question + ":"  + answer})]
        # 判断内存中是否有indexName这个索引，如果有直接取出来用，没有的话则判断本地是否有这个索引，如果有直接取出来用，没有的话则新建一个
        if indexName in vector_stores:
            vector_store = vector_stores[indexName]
            vector_store.add_documents(docs)
        elif os.path.isdir(VS_PATH) and os.path.isfile(VS_PATH + "/" + indexName + ".faiss"):
            vector_store = load_vector_store(VS_PATH, embeddings, indexName)
            vector_stores[indexName] = vector_store
        else:
            vector_store = MyFAISS.from_documents(docs, embeddings)  ##docs 为Document列表
            vector_stores[indexName] = vector_store
        torch_gc()
        vector_store.save_local(VS_PATH, index_name=indexName)
        return "问答数据添加成功"
    except Exception as e:
        logger.error(e)
        return "问答数据添加失败" + e
    
# 修改问答数据到对应的向量库索引中
@app.route("/updateQA", methods=["post"])
def updateQA():
    try:
        qaId = request.form.get("qaId")
        # 根据qaId查询数据
        selsql = "select * from qapair where qaId = %s"
        selparams = (qaId,)
        result = mysqlhelper.get_one(selsql,selparams)
        if not result:
            return "没有查到对应的问答数据"
        question = result[1]
        answer = result[2]
        indexName = result[3]
        # 向量数据更新时需要的source
        source = question + ":"  + answer
        # 将数据更新到数据库
        updatesql = "update qapair set question = %s, answer = %s where qaId = %s"
        newQuestion = request.form.get("newQuestion")
        newAnswer = request.form.get("newAnswer")
        if not newQuestion:
            newQuestion = question
        if not newAnswer:
            newAnswer = answer
        updateparams = (newQuestion,newAnswer,qaId,)
        mysqlhelper.update(updatesql,updateparams)
        # 向量数据更新时需要的new_docs
        docs = [Document(page_content=newQuestion, metadata={"source": newQuestion + ":"  + newAnswer})]
        # 判断内存中是否有indexName这个索引，如果有直接取出来用，没有的话则判断本地是否有这个索引，如果有直接取出来用，没有则提示没有找到对应的索引
        if indexName in vector_stores:
            vector_store = vector_stores[indexName]
        elif os.path.isdir(VS_PATH) and os.path.isfile(VS_PATH + "/" + indexName + ".faiss"):
            vector_store = load_vector_store(VS_PATH, embeddings, indexName)
            vector_stores[indexName] = vector_store
        else:
            return "没有找到对应的向量库索引，请确认索引名称是否正确！"
        vector_store.update_doc(source,docs)
        torch_gc()
        vector_store.save_local(VS_PATH, index_name=indexName)
        return "问答数据更新成功"
    except Exception as e:
        logger.error(e)
        return "问答数据更新失败" + e

# 删除单条问答数据
@app.route("/deleteQA", methods=["post"])
def deleteQA():
    try:
        qaId = request.form.get("qaId")
        # 根据qaId查询数据
        selsql = "select * from qapair where qaId = %s"
        selparams = (qaId,)
        result = mysqlhelper.get_one(selsql,selparams)
        if not result:
            return "没有查到对应的问答数据"
        question = result[1]
        answer = result[2]
        indexName = result[3]
        # 删除数据库的数据
        delsql = "delete from qapair where qaId = %s"
        params = (qaId,)
        result = mysqlhelper.delete(delsql,params)
        if result == 0:
            return "问答数据删除失败"
        # 删除向量库的数据
         # 判断内存中是否有indexName这个索引，如果有直接取出来用，没有的话则判断本地是否有这个索引，如果有直接取出来用，没有则提示没有找到对应的索引
        if indexName in vector_stores:
            vector_store = vector_stores[indexName]
        elif os.path.isdir(VS_PATH) and os.path.isfile(VS_PATH + "/" + indexName + ".faiss"):
            vector_store = load_vector_store(VS_PATH, embeddings, indexName)
            vector_stores[indexName] = vector_store
        else:
            return "没有找到对应的向量库索引，请确认索引名称是否正确！"
        vector_store.delete_doc(question + ":"  + answer)
        return "问答数据删除成功"
    except Exception as e:
        logger.error(e)
        return "问答数据删除失败" + e

# 根据问题查询推荐答案
@app.route("/calculate_similarity", methods=["post"])
def get_knowledge_based_answer():
    query = request.form.get("query")
    indexName = request.form.get("indexName")
    if indexName in vector_stores:
        vector_store = vector_stores[indexName]
    elif os.path.isdir(VS_PATH) and os.path.isfile(VS_PATH + "/" + indexName + ".faiss"):
        vector_store = load_vector_store(VS_PATH, embeddings, indexName)
    else:
        return "没有找到对应的向量库索引，请确认索引名称是否正确！"
    vector_store.score_threshold = 500
    related_docs_with_score = vector_store.similarity_search_with_score(query, k=5)
    questions = [{"question":doc.page_content,"score":doc.metadata['score'],"answer":doc.metadata["source"].split(":")[1]} for doc in related_docs_with_score]
    torch_gc()
    return jsonify(scores=questions)

# 从数据库中读取所有的知识库，加载到内存中
def init_all_vector_index():
    sql = "select * from index_tb"
    result = mysqlhelper.get_all(sql,None)
    vector_stores = {}
    for item in result:
        if os.path.isdir(VS_PATH) and os.path.isfile(VS_PATH + "/" + item[1] + ".faiss"):
            vector_store = load_vector_store(VS_PATH, embeddings, item[1])
            vector_stores[item[1]] = vector_store

if __name__ == "__main__":
    init_all_vector_index()

    # 读命令行参数
    args = get_parser().parse_args()
    port = args.port

    # 启动server
    server = pywsgi.WSGIServer(('', port), app)
    server.serve_forever()