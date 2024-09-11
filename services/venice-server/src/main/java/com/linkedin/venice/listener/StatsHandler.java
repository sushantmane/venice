package com.linkedin.venice.listener;

import static com.linkedin.venice.response.VeniceReadResponseStatus.KEY_NOT_FOUND;
import static com.linkedin.venice.response.VeniceReadResponseStatus.OK;
import static com.linkedin.venice.response.VeniceReadResponseStatus.TOO_MANY_REQUESTS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.utils.LatencyUtils;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.Objects;


/**
 * This class is responsible for handling the stats for the HTTP request handled by the Netty server.
 */
public class StatsHandler extends ChannelDuplexHandler {
  private final RequestStatsRecorder requestStatsRecorder;

  public StatsHandler(RequestStatsRecorder requestStatsRecorder) {
    this.requestStatsRecorder = Objects.requireNonNull(requestStatsRecorder, "RequestStatsContext cannot be null");
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (requestStatsRecorder.isNewRequest()) {
      // Reset for every request
      requestStatsRecorder.resetContext();
      /**
       * For a single 'channelRead' invocation, Netty will guarantee all the following 'channelRead' functions
       * registered by the pipeline to be executed in the same thread.
       */
      ctx.fireChannelRead(msg);
      double firstPartLatency = LatencyUtils.getElapsedTimeFromNSToMS(requestStatsRecorder.getRequestStartTimeInNS());
      requestStatsRecorder.setFirstPartLatency(firstPartLatency);
    } else {
      // Only works for multi-get request.
      long startTimeOfPart2InNS = System.nanoTime();
      long startTimeInNS = requestStatsRecorder.getRequestStartTimeInNS();

      requestStatsRecorder.setPartsInvokeDelayLatency(LatencyUtils.convertNSToMS(startTimeOfPart2InNS - startTimeInNS));

      ctx.fireChannelRead(msg);

      requestStatsRecorder.setSecondPartLatency(LatencyUtils.getElapsedTimeFromNSToMS(startTimeOfPart2InNS));
      requestStatsRecorder.incrementRequestPartCount();
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws VeniceException {
    ChannelFuture future = ctx.writeAndFlush(msg);
    long beforeFlushTimestampNs = System.nanoTime();
    future.addListener((result) -> {
      // reset the StatsHandler for the new request. This is necessary since instances are channel-based
      // and channels are ready for the future requests as soon as the current has been handled.
      requestStatsRecorder.setNewRequest();

      VeniceReadResponseStatus responseStatus = requestStatsRecorder.getVeniceReadResponseStatus();

      if (responseStatus == null) {
        throw new VeniceException("request status could not be null");
      }

      // we don't record if it is a metatadata request
      if (requestStatsRecorder.isMetadataRequest()) {
        return;
      }

      /**
       * TODO: Need to do more investigation to figure out why this callback could be triggered
       * multiple times for a single request
       */
      if (!requestStatsRecorder.isStatCallBackExecuted()) {
        requestStatsRecorder.setFlushLatency(LatencyUtils.getElapsedTimeFromNSToMS(beforeFlushTimestampNs));
        ServerHttpRequestStats serverHttpRequestStats = requestStatsRecorder.getStoreName() == null
            ? null
            : requestStatsRecorder.getCurrentStats().getStoreStats(requestStatsRecorder.getStoreName());
        requestStatsRecorder.recordBasicMetrics(serverHttpRequestStats);
        double elapsedTime = LatencyUtils.getElapsedTimeFromNSToMS(requestStatsRecorder.getRequestStartTimeInNS());
        // if ResponseStatus is either OK or NOT_FOUND and the channel write is succeed,
        // records a successRequest in stats. Otherwise, records a errorRequest in stats
        // For TOO_MANY_REQUESTS do not record either success or error. Recording as success would give out
        // wrong interpretation of latency, recording error would give out impression that server failed to serve
        if (result.isSuccess() && (responseStatus == OK || responseStatus == KEY_NOT_FOUND)) {
          requestStatsRecorder.successRequest(serverHttpRequestStats, elapsedTime);
        } else if (responseStatus != TOO_MANY_REQUESTS) {
          requestStatsRecorder.errorRequest(serverHttpRequestStats, elapsedTime);
        }
        requestStatsRecorder.setStatCallBackExecuted(true);
      }
    });
  }
}
