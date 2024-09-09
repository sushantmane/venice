package com.linkedin.venice.grpc;

import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.StatsHandler;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.protocols.SingleGetRequest;
import com.linkedin.venice.protocols.SingleGetResponse;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceGrpcReadServiceImpl extends VeniceReadServiceGrpc.VeniceReadServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceGrpcReadServiceImpl.class);

  private final VeniceServerGrpcRequestProcessor requestProcessor;
  private final StatsHandler statsHandler;

  public VeniceGrpcReadServiceImpl(VeniceServerGrpcRequestProcessor requestProcessor, StatsHandler statsHandler) {
    this.requestProcessor = requestProcessor;
    this.statsHandler = statsHandler;
  }

  @Override
  public void get(VeniceClientRequest singleGetRequest, StreamObserver<VeniceServerResponse> streamObserver) {
    ServerStatsContext statsContext = statsHandler.getNewStatsContext();
    GrpcRequestContext clientRequestCtx = new GrpcRequestContext(streamObserver);
    clientRequestCtx.setGrpcStatsContext(statsContext);
    try {
      RouterRequest routerRequest = GetRouterRequest.parseSingleGetGrpcRequest(singleGetRequest);
      statsContext.setRequestInfo(routerRequest);
      clientRequestCtx.setRouterRequest(routerRequest);
      requestProcessor.process(clientRequestCtx);
    } catch (Exception e) {
      // TODO: Add new metric to track the number of errors
      LOGGER.debug("Error while processing single get request", e);
      VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
      builder.setErrorCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      streamObserver.onNext(builder.build());
      streamObserver.onCompleted();
    }
  }

  @Override
  public void batchGet(VeniceClientRequest batchGetRequest, StreamObserver<VeniceServerResponse> streamObserver) {
    ServerStatsContext statsContext = statsHandler.getNewStatsContext();
    GrpcRequestContext clientRequestCtx = new GrpcRequestContext(streamObserver);
    clientRequestCtx.setGrpcStatsContext(statsContext);
    try {
      RouterRequest routerRequest = MultiGetRouterRequestWrapper.parseMultiGetGrpcRequest(batchGetRequest);
      statsContext.setRequestInfo(routerRequest);
      clientRequestCtx.setRouterRequest(routerRequest);
      requestProcessor.process(clientRequestCtx);
    } catch (Exception e) {
      // TODO: Add new metric to track the number of errors
      LOGGER.debug("Error while processing batch-get request", e);
      VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
      builder.setErrorCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      streamObserver.onNext(builder.build());
      streamObserver.onCompleted();
    }
  }

  @Override
  public void singleGet(SingleGetRequest singleGetRequest, StreamObserver<SingleGetResponse> streamObserver) {
    ServerStatsContext statsContext = statsHandler.getNewStatsContext();
    GrpcRequestContext<SingleGetResponse> clientRequestCtx = new GrpcRequestContext<>(streamObserver);
    clientRequestCtx.setOldApi(false);
    clientRequestCtx.setGrpcStatsContext(statsContext);
    try {
      RouterRequest routerRequest = GetRouterRequest.parseSingleGetGrpcRequest(singleGetRequest);
      statsContext.setRequestInfo(routerRequest);
      clientRequestCtx.setRouterRequest(routerRequest);
      requestProcessor.process(clientRequestCtx);
    } catch (Exception e) {
      // TODO: Add new metric to track the number of errors
      LOGGER.debug("Error while processing single get request", e);
      SingleGetResponse.Builder builder = SingleGetResponse.newBuilder();
      builder.setStatusCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
      if (e.getMessage() != null) {
        builder.setErrorMessage(e.getMessage());
      }
      streamObserver.onNext(builder.build());
      streamObserver.onCompleted();
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
