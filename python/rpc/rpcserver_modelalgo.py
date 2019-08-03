# -*- coding: utf-8 -*-
import grpc
import time
import warnings
import queue,threading
from concurrent import futures

import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split, learning_curve
from sklearn.metrics import average_precision_score
from sklearn.linear_model import LogisticRegression

from hdfs.client import Client as HdfsClient

import modelalgo_pb2, modelalgo_pb2_grpc

hdfs_client = HdfsClient("http://localhost:50070")

_ENABLE_HDFS = True

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_HOST = 'localhost'
_PORT = '29000'

# TODO: 分布式优化
import pyarrow,json,io
from sklearn.externals import joblib

import marshal
import struct
import types
import collections
import zlib
import itertools

def read_long(stream):
    length = stream.read(8)
    if not length:
        raise EOFError
    return struct.unpack("!q", length)[0]

def write_long(value, stream):
    stream.write(struct.pack("!q", value))

def pack_long(value):
    return struct.pack("!q", value)

def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    return struct.unpack("!i", length)[0]

def write_int(value, stream):
    stream.write(struct.pack("!i", value))

def read_bool(stream):
    length = stream.read(1)
    if not length:
        raise EOFError
    return struct.unpack("!?", length)[0]

def write_with_length(obj, stream):
    write_int(len(obj), stream)
    stream.write(obj)

class GRpcModelAlgoServicerImpl(modelalgo_pb2_grpc.ModelAlgorithmServiceServicer):
    def __init__(self,runningContext):
        self.runningContext = runningContext
        #self.iters = list()

    def buildDataframes(self, request):
        reader = pyarrow.ipc.open_stream(request.batchData)
        for batch in reader:
            print ("_____ start build df iterator _______")
            print (batch, batch.num_rows, batch.num_columns)
            df = batch.to_pandas(zero_copy_only=True)
            print (type(df))
            #print (df)
            #yield df
            #self.iters.append(df)
            self.runningContext.addDf(df)
            print ("iters len is ", len(self.runningContext.getDfs()))

    def sendBatch(self, request, context):
        print ("send batch request taskId : ", request.taskId)
        if (not self.runningContext.setRunning(request.taskId)):
            warnings.warn(request.taskId, " can not run, because busy")
            return  modelalgo_pb2.ModelAlgorithmResponse(status=1) # busy

        self.buildDataframes(request)

        return  modelalgo_pb2.ModelAlgorithmResponse(status=0)

    def computeLR(self, request, context):
        if (not self.runningContext.setRunning(request.taskId)):
            warnings.warn(request.taskId, " can not run, because busy")
            return  modelalgo_pb2.ModelAlgorithmResponse(status=1) # busy
        print ("compute LR , target file : ", request.targetFile)
        msg = self.runningContext.doLR(request.targetFile)
        # TODO: do sth more
        #self.runningContext.unsetRunning()
        return  modelalgo_pb2.ModelAlgorithmResponse(status=0, message=msg)

    def cleanBatch(self, request, context):
        if (not self.runningContext.setRunning(request.taskId)):
            warnings.warn(request.taskId, " can not run, because busy")
            return  modelalgo_pb2.ModelAlgorithmResponse(status=1) # busy
        self.runningContext.unsetRunning()
        return  modelalgo_pb2.ModelAlgorithmResponse(status=0)

    def predict(self, request, context):
        features = io.BytesIO(bytes(request.features))
        num = (int(features.read(1)[0]))
        dtarr = []
        for i in range(num):
            dt = (int(features.read(1)[0]))
            dtarr.append(dt)
        dataarr = []
        for i in range(num):
            if (dtarr[i] == 0):#int
                varr = features.read1(4)
                dataarr.append(struct.unpack("!i", varr)[0])
            elif (dtarr[i] == 1):#double
                varr = features.read1(8)
                dataarr.append(struct.unpack("!d", varr)[0])
            elif (dtarr[i] == 2):#float
                varr = features.read1(4)
                dataarr.append(struct.unpack("!f", varr)[0])
            elif (dtarr[i] == 3):#float
                varr = features.read1(1)
                dataarr.append(struct.unpack("!c", varr)[0])
            elif (dtarr[i] == 4):#boolean
                varr = features.read1(1)
                dataarr.append(struct.unpack("!?", varr)[0])
            elif (dtarr[i] == 5):#short
                varr = features.read1(2)
                dataarr.append(struct.unpack("!h", varr)[0])
            elif (dtarr[i] == 6):#short
                varr = features.read1(8)
                dataarr.append(struct.unpack("!q", varr)[0])
            else:
                raise EOFError
        cols = []
        for i in range(num):
            cols.append('col'+str(i))
        ndf = pd.DataFrame(columns = cols)
        ndf.loc[0] = dataarr
        lrModel = joblib.load(request.modelFile)
        predicts = lrModel.predict(ndf)

        outputio = io.BytesIO()
        outputio.write(struct.pack("!B", 0))
        outputio.write(struct.pack("!i", predicts[0]))

        return  modelalgo_pb2.ModelPredictResponse(status=0,result=outputio.getvalue())

# record first time, if timeout when batch complete is not coming, refresh
class RunningContext(object):
    def __init__(self,lock):
        self.running = False
        self.lock = lock
        self.taskId = "0"
        self.dfs = list()
    def addDf(self, df):
        self.lock.acquire()
        self.dfs.append(df)
        self.lock.release()
    def getDfs(self):
        return self.dfs
    def getRuningId(self):
        self.lock.acquire()
        r = self.taskId
        self.lock.release()
        return r
    def setRunning(self, taskid):
        print("current taskid ", self.taskId, ", new task id ", taskid, (self.taskId == taskid))
        print("current running ", self.running, (not self.running), (self.running == True))
        self.lock.acquire()
        if (not self.running):
            self.taskId = taskid
            self.running = True
            r = True
        else: # self.running = True
            if (self.taskId == taskid):
                r = True
            else:
                if (self.taskId == "0"):
                    warnings.warn("running status is wrong, taskid = 0 but busy")
                    self.running = False
                    self.dfs = list()
                    r = False
                else:
                    r = False
        self.lock.release()
        return r
    def unsetRunning(self):
        self.lock.acquire()
        if (self.running):
            r = True
        else:
            r = False
        self.running = False
        self.taskId = "0"
        self.dfs = list()
        self.lock.release()
        return r
    def doLR(self, targetfile):
        self.lock.acquire()
        dfs = pd.concat(self.dfs)
        labelDF = dfs['label']
        del dfs['label']
        # print ("_____ display training dataframe _______")
        # print (dfs)
        lrModel = LogisticRegression(random_state=0, solver='lbfgs', multi_class='multinomial')
        lrModel.fit(dfs, labelDF)

        #print ("_____ display predict label dataframe _______")
        #predictLabel = lrModel.predict(dfs)
        #print (type(labelDF), labelDF)
        #npl = pd.Series(predictLabel)
        #print (type(npl), npl)

        #print ("_____ display predict ratio _______")
        # calc precision rate
        #ratiodf = (npl.rename('f').eq(labelDF.rename('f'))).to_frame()
        #ratio = ratiodf[ratiodf.f==True].count()/ratiodf.count()
        #print(" ************ precision ratio : {}".format(ratio))
        print (targetfile, lrModel.get_params())
        joblib.dump(lrModel, targetfile, compress=3) #
        r = json.dumps(lrModel.get_params())
        self.lock.release()
        return r

def serve():
    lock = threading.Lock()
    runningCtx = RunningContext(lock=lock)
    grpcServer = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    modelalgo_pb2_grpc.add_ModelAlgorithmServiceServicer_to_server(GRpcModelAlgoServicerImpl(runningCtx), grpcServer)
    grpcServer.add_insecure_port(_HOST + ':' + _PORT)
    grpcServer.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        grpcServer.stop(0)

if __name__ == '__main__':
    serve()
