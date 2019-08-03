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
    comments = "Source: example.proto")
public final class PbExampleServiceGrpc {

  private PbExampleServiceGrpc() {}

  public static final String SERVICE_NAME = "PbExampleService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.PbExample.PbExampleDataRequest,
      com.spark.dialect.generated.proto.PbExample.PbExampleResponse> getSendBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "sendBatch",
      requestType = com.spark.dialect.generated.proto.PbExample.PbExampleDataRequest.class,
      responseType = com.spark.dialect.generated.proto.PbExample.PbExampleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.PbExample.PbExampleDataRequest,
      com.spark.dialect.generated.proto.PbExample.PbExampleResponse> getSendBatchMethod() {
    io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.PbExample.PbExampleDataRequest, com.spark.dialect.generated.proto.PbExample.PbExampleResponse> getSendBatchMethod;
    if ((getSendBatchMethod = PbExampleServiceGrpc.getSendBatchMethod) == null) {
      synchronized (PbExampleServiceGrpc.class) {
        if ((getSendBatchMethod = PbExampleServiceGrpc.getSendBatchMethod) == null) {
          PbExampleServiceGrpc.getSendBatchMethod = getSendBatchMethod = 
              io.grpc.MethodDescriptor.<com.spark.dialect.generated.proto.PbExample.PbExampleDataRequest, com.spark.dialect.generated.proto.PbExample.PbExampleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "PbExampleService", "sendBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.spark.dialect.generated.proto.PbExample.PbExampleDataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.spark.dialect.generated.proto.PbExample.PbExampleResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PbExampleServiceMethodDescriptorSupplier("sendBatch"))
                  .build();
          }
        }
     }
     return getSendBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.PbExample.PbExampleComputeRequest,
      com.spark.dialect.generated.proto.PbExample.PbExampleResponse> getComputeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "compute",
      requestType = com.spark.dialect.generated.proto.PbExample.PbExampleComputeRequest.class,
      responseType = com.spark.dialect.generated.proto.PbExample.PbExampleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.PbExample.PbExampleComputeRequest,
      com.spark.dialect.generated.proto.PbExample.PbExampleResponse> getComputeMethod() {
    io.grpc.MethodDescriptor<com.spark.dialect.generated.proto.PbExample.PbExampleComputeRequest, com.spark.dialect.generated.proto.PbExample.PbExampleResponse> getComputeMethod;
    if ((getComputeMethod = PbExampleServiceGrpc.getComputeMethod) == null) {
      synchronized (PbExampleServiceGrpc.class) {
        if ((getComputeMethod = PbExampleServiceGrpc.getComputeMethod) == null) {
          PbExampleServiceGrpc.getComputeMethod = getComputeMethod = 
              io.grpc.MethodDescriptor.<com.spark.dialect.generated.proto.PbExample.PbExampleComputeRequest, com.spark.dialect.generated.proto.PbExample.PbExampleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "PbExampleService", "compute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.spark.dialect.generated.proto.PbExample.PbExampleComputeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.spark.dialect.generated.proto.PbExample.PbExampleResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new PbExampleServiceMethodDescriptorSupplier("compute"))
                  .build();
          }
        }
     }
     return getComputeMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static PbExampleServiceStub newStub(io.grpc.Channel channel) {
    return new PbExampleServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static PbExampleServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new PbExampleServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static PbExampleServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new PbExampleServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class PbExampleServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void sendBatch(com.spark.dialect.generated.proto.PbExample.PbExampleDataRequest request,
        io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.PbExample.PbExampleResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSendBatchMethod(), responseObserver);
    }

    /**
     */
    public void compute(com.spark.dialect.generated.proto.PbExample.PbExampleComputeRequest request,
        io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.PbExample.PbExampleResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getComputeMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSendBatchMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.spark.dialect.generated.proto.PbExample.PbExampleDataRequest,
                com.spark.dialect.generated.proto.PbExample.PbExampleResponse>(
                  this, METHODID_SEND_BATCH)))
          .addMethod(
            getComputeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.spark.dialect.generated.proto.PbExample.PbExampleComputeRequest,
                com.spark.dialect.generated.proto.PbExample.PbExampleResponse>(
                  this, METHODID_COMPUTE)))
          .build();
    }
  }

  /**
   */
  public static final class PbExampleServiceStub extends io.grpc.stub.AbstractStub<PbExampleServiceStub> {
    private PbExampleServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PbExampleServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PbExampleServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PbExampleServiceStub(channel, callOptions);
    }

    /**
     */
    public void sendBatch(com.spark.dialect.generated.proto.PbExample.PbExampleDataRequest request,
        io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.PbExample.PbExampleResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSendBatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void compute(com.spark.dialect.generated.proto.PbExample.PbExampleComputeRequest request,
        io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.PbExample.PbExampleResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getComputeMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class PbExampleServiceBlockingStub extends io.grpc.stub.AbstractStub<PbExampleServiceBlockingStub> {
    private PbExampleServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PbExampleServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PbExampleServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PbExampleServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.spark.dialect.generated.proto.PbExample.PbExampleResponse sendBatch(com.spark.dialect.generated.proto.PbExample.PbExampleDataRequest request) {
      return blockingUnaryCall(
          getChannel(), getSendBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.spark.dialect.generated.proto.PbExample.PbExampleResponse compute(com.spark.dialect.generated.proto.PbExample.PbExampleComputeRequest request) {
      return blockingUnaryCall(
          getChannel(), getComputeMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class PbExampleServiceFutureStub extends io.grpc.stub.AbstractStub<PbExampleServiceFutureStub> {
    private PbExampleServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private PbExampleServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PbExampleServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new PbExampleServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.spark.dialect.generated.proto.PbExample.PbExampleResponse> sendBatch(
        com.spark.dialect.generated.proto.PbExample.PbExampleDataRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSendBatchMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.spark.dialect.generated.proto.PbExample.PbExampleResponse> compute(
        com.spark.dialect.generated.proto.PbExample.PbExampleComputeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getComputeMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SEND_BATCH = 0;
  private static final int METHODID_COMPUTE = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final PbExampleServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(PbExampleServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SEND_BATCH:
          serviceImpl.sendBatch((com.spark.dialect.generated.proto.PbExample.PbExampleDataRequest) request,
              (io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.PbExample.PbExampleResponse>) responseObserver);
          break;
        case METHODID_COMPUTE:
          serviceImpl.compute((com.spark.dialect.generated.proto.PbExample.PbExampleComputeRequest) request,
              (io.grpc.stub.StreamObserver<com.spark.dialect.generated.proto.PbExample.PbExampleResponse>) responseObserver);
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

  private static abstract class PbExampleServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    PbExampleServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.spark.dialect.generated.proto.PbExample.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("PbExampleService");
    }
  }

  private static final class PbExampleServiceFileDescriptorSupplier
      extends PbExampleServiceBaseDescriptorSupplier {
    PbExampleServiceFileDescriptorSupplier() {}
  }

  private static final class PbExampleServiceMethodDescriptorSupplier
      extends PbExampleServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    PbExampleServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (PbExampleServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new PbExampleServiceFileDescriptorSupplier())
              .addMethod(getSendBatchMethod())
              .addMethod(getComputeMethod())
              .build();
        }
      }
    }
    return result;
  }
}
