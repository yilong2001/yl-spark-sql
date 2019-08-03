package com.spark.dialect.generated.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.21.0)",
    comments = "Source: modelalgo.proto")
public final class ModelAlgorithmServiceGrpc {

  private ModelAlgorithmServiceGrpc() {}

  public static final String SERVICE_NAME = "ModelAlgorithmService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmDataRequest,
      com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> getSendBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "sendBatch",
      requestType = com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmDataRequest.class,
      responseType = com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmDataRequest,
      com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> getSendBatchMethod() {
    io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmDataRequest, com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> getSendBatchMethod;
    if ((getSendBatchMethod = ModelAlgorithmServiceGrpc.getSendBatchMethod) == null) {
      synchronized (ModelAlgorithmServiceGrpc.class) {
        if ((getSendBatchMethod = ModelAlgorithmServiceGrpc.getSendBatchMethod) == null) {
          ModelAlgorithmServiceGrpc.getSendBatchMethod = getSendBatchMethod = 
              io.grpc.MethodDescriptor.<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmDataRequest, com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ModelAlgorithmService", "sendBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmDataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new ModelAlgorithmServiceMethodDescriptorSupplier("sendBatch"))
                  .build();
          }
        }
     }
     return getSendBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmComputeRequest,
      com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> getComputeLRMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "computeLR",
      requestType = com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmComputeRequest.class,
      responseType = com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmComputeRequest,
      com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> getComputeLRMethod() {
    io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmComputeRequest, com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> getComputeLRMethod;
    if ((getComputeLRMethod = ModelAlgorithmServiceGrpc.getComputeLRMethod) == null) {
      synchronized (ModelAlgorithmServiceGrpc.class) {
        if ((getComputeLRMethod = ModelAlgorithmServiceGrpc.getComputeLRMethod) == null) {
          ModelAlgorithmServiceGrpc.getComputeLRMethod = getComputeLRMethod = 
              io.grpc.MethodDescriptor.<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmComputeRequest, com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ModelAlgorithmService", "computeLR"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmComputeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new ModelAlgorithmServiceMethodDescriptorSupplier("computeLR"))
                  .build();
          }
        }
     }
     return getComputeLRMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmCleanRequest,
      com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> getCleanBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "cleanBatch",
      requestType = com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmCleanRequest.class,
      responseType = com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmCleanRequest,
      com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> getCleanBatchMethod() {
    io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmCleanRequest, com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> getCleanBatchMethod;
    if ((getCleanBatchMethod = ModelAlgorithmServiceGrpc.getCleanBatchMethod) == null) {
      synchronized (ModelAlgorithmServiceGrpc.class) {
        if ((getCleanBatchMethod = ModelAlgorithmServiceGrpc.getCleanBatchMethod) == null) {
          ModelAlgorithmServiceGrpc.getCleanBatchMethod = getCleanBatchMethod = 
              io.grpc.MethodDescriptor.<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmCleanRequest, com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ModelAlgorithmService", "cleanBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmCleanRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new ModelAlgorithmServiceMethodDescriptorSupplier("cleanBatch"))
                  .build();
          }
        }
     }
     return getCleanBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmPredictRequest,
      com.spark.dialect.generated.proto.ModelAlgorithm.ModelPredictResponse> getPredictMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "predict",
      requestType = com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmPredictRequest.class,
      responseType = com.spark.dialect.generated.proto.ModelAlgorithm.ModelPredictResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmPredictRequest,
      com.spark.dialect.generated.proto.ModelAlgorithm.ModelPredictResponse> getPredictMethod() {
    io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmPredictRequest, com.spark.dialect.generated.proto.ModelAlgorithm.ModelPredictResponse> getPredictMethod;
    if ((getPredictMethod = ModelAlgorithmServiceGrpc.getPredictMethod) == null) {
      synchronized (ModelAlgorithmServiceGrpc.class) {
        if ((getPredictMethod = ModelAlgorithmServiceGrpc.getPredictMethod) == null) {
          ModelAlgorithmServiceGrpc.getPredictMethod = getPredictMethod = 
              io.grpc.MethodDescriptor.<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmPredictRequest, com.spark.dialect.generated.proto.ModelAlgorithm.ModelPredictResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "ModelAlgorithmService", "predict"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmPredictRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.spark.dialect.generated.proto.ModelAlgorithm.ModelPredictResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new ModelAlgorithmServiceMethodDescriptorSupplier("predict"))
                  .build();
          }
        }
     }
     return getPredictMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ModelAlgorithmServiceStub newStub(io.grpc.Channel channel) {
    return new ModelAlgorithmServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ModelAlgorithmServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ModelAlgorithmServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ModelAlgorithmServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ModelAlgorithmServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class ModelAlgorithmServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void sendBatch(com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmDataRequest request,
        io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSendBatchMethod(), responseObserver);
    }

    /**
     */
    public void computeLR(com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmComputeRequest request,
        io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getComputeLRMethod(), responseObserver);
    }

    /**
     */
    public void cleanBatch(com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmCleanRequest request,
        io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCleanBatchMethod(), responseObserver);
    }

    /**
     */
    public void predict(com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmPredictRequest request,
        io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.ModelAlgorithm.ModelPredictResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getPredictMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSendBatchMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmDataRequest,
                com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse>(
                  this, METHODID_SEND_BATCH)))
          .addMethod(
            getComputeLRMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmComputeRequest,
                com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse>(
                  this, METHODID_COMPUTE_LR)))
          .addMethod(
            getCleanBatchMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmCleanRequest,
                com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse>(
                  this, METHODID_CLEAN_BATCH)))
          .addMethod(
            getPredictMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmPredictRequest,
                com.spark.dialect.generated.proto.ModelAlgorithm.ModelPredictResponse>(
                  this, METHODID_PREDICT)))
          .build();
    }
  }

  /**
   */
  public static final class ModelAlgorithmServiceStub extends io.grpc.stub.AbstractStub<ModelAlgorithmServiceStub> {
    private ModelAlgorithmServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ModelAlgorithmServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ModelAlgorithmServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ModelAlgorithmServiceStub(channel, callOptions);
    }

    /**
     */
    public void sendBatch(com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmDataRequest request,
        io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSendBatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void computeLR(com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmComputeRequest request,
        io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getComputeLRMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void cleanBatch(com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmCleanRequest request,
        io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCleanBatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void predict(com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmPredictRequest request,
        io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.ModelAlgorithm.ModelPredictResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getPredictMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ModelAlgorithmServiceBlockingStub extends io.grpc.stub.AbstractStub<ModelAlgorithmServiceBlockingStub> {
    private ModelAlgorithmServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ModelAlgorithmServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ModelAlgorithmServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ModelAlgorithmServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse sendBatch(com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmDataRequest request) {
      return blockingUnaryCall(
          getChannel(), getSendBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse computeLR(com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmComputeRequest request) {
      return blockingUnaryCall(
          getChannel(), getComputeLRMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse cleanBatch(com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmCleanRequest request) {
      return blockingUnaryCall(
          getChannel(), getCleanBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.spark.dialect.generated.proto.ModelAlgorithm.ModelPredictResponse predict(com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmPredictRequest request) {
      return blockingUnaryCall(
          getChannel(), getPredictMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ModelAlgorithmServiceFutureStub extends io.grpc.stub.AbstractStub<ModelAlgorithmServiceFutureStub> {
    private ModelAlgorithmServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ModelAlgorithmServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ModelAlgorithmServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ModelAlgorithmServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> sendBatch(
        com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmDataRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSendBatchMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> computeLR(
        com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmComputeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getComputeLRMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse> cleanBatch(
        com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmCleanRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCleanBatchMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.spark.dialect.generated.proto.ModelAlgorithm.ModelPredictResponse> predict(
        com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmPredictRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getPredictMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SEND_BATCH = 0;
  private static final int METHODID_COMPUTE_LR = 1;
  private static final int METHODID_CLEAN_BATCH = 2;
  private static final int METHODID_PREDICT = 3;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ModelAlgorithmServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ModelAlgorithmServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SEND_BATCH:
          serviceImpl.sendBatch((com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmDataRequest) request,
              (io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse>) responseObserver);
          break;
        case METHODID_COMPUTE_LR:
          serviceImpl.computeLR((com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmComputeRequest) request,
              (io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse>) responseObserver);
          break;
        case METHODID_CLEAN_BATCH:
          serviceImpl.cleanBatch((com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmCleanRequest) request,
              (io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmResponse>) responseObserver);
          break;
        case METHODID_PREDICT:
          serviceImpl.predict((com.spark.dialect.generated.proto.ModelAlgorithm.ModelAlgorithmPredictRequest) request,
              (io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.ModelAlgorithm.ModelPredictResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class ModelAlgorithmServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ModelAlgorithmServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.spark.dialect.generated.proto.ModelAlgorithm.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ModelAlgorithmService");
    }
  }

  private static final class ModelAlgorithmServiceFileDescriptorSupplier
      extends ModelAlgorithmServiceBaseDescriptorSupplier {
    ModelAlgorithmServiceFileDescriptorSupplier() {}
  }

  private static final class ModelAlgorithmServiceMethodDescriptorSupplier
      extends ModelAlgorithmServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ModelAlgorithmServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ModelAlgorithmServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ModelAlgorithmServiceFileDescriptorSupplier())
              .addMethod(getSendBatchMethod())
              .addMethod(getComputeLRMethod())
              .addMethod(getCleanBatchMethod())
              .addMethod(getPredictMethod())
              .build();
        }
      }
    }
    return result;
  }
}
