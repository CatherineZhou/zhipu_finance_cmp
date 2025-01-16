
#!/usr/bin/env python3
import pandas as pd
import numpy as np
import faiss
from faiss  import normalize_L2
import os
os.environ['CUDA_VISIBLE_DEVICES'] = '0'

retrieval_instruction = "为这个句子生成表示以用于检索相关文章："


from sentence_transformers import SentenceTransformer
model = SentenceTransformer('/data/keraszhou/model/bge-large-zh',device='cuda')


def create_index(product_name):

    embeddings = model.encode(product_name)
    embeddings = embeddings.astype(np.float32)
    print('-----embeddings',embeddings.shape, embeddings.dtype)
    normalize_L2(embeddings)

    # 特征维度
    dim = embeddings.shape[1]

    # 创建 Faiss 索引，这里使用基于 L2 距离的扁平索引
    #index = faiss.IndexFlatL2(dim)
    index = faiss.IndexFlatIP(dim)

    # 将特征库的嵌入向量添加到索引
    index.add(embeddings)

    # 创建 GPU 资源
    res = faiss.StandardGpuResources()

    # 将 CPU 索引转换为 GPU 索引
    gpu_index = faiss.index_cpu_to_gpu(res, 0, index)

    print('----build index finished')
    return gpu_index


def get_vector_search(gpu_index,product_name,query,k,cut_off=0.8):

    query_embedding = model.encode([query])
    query_embedding = query_embedding.astype(np.float32)
    #print('-----query_embedding',query_embedding.shape, query_embedding.dtype)
    normalize_L2(query_embedding)
    query_embedding = query_embedding.astype(np.float32)
    D_gpu, I_gpu = gpu_index.search(query_embedding,k)
    sim_item = [f'{product_name[I_gpu[0][i]]}:{D_gpu[0][i]}' for i in range(len(I_gpu[0])) if D_gpu[0][i]>=cut_off]
    #sim_item = [f'{product_name[I_gpu[0][i]]}' for i in range(len(I_gpu[0])) if D_gpu[0][i] >= cut_off]

    #sim_item = [f'{product_name[I_gpu[0][i]]}' for i in range(len(I_gpu[0])) if D_gpu[0][i] >= cut_off]

    return sim_item

