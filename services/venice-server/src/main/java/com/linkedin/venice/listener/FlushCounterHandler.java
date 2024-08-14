package com.linkedin.venice.listener;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.util.AttributeKey;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@ChannelHandler.Sharable
public class FlushCounterHandler extends ChannelOutboundHandlerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(FlushCounterHandler.class);
  private final VeniceServerNettyStats nettyStats;
  private final Map<SocketAddress, Integer> flushCounterMap = new ConcurrentHashMap<>();
  public static final AttributeKey<Long> LACL_TIME = AttributeKey.valueOf("lastFlushCounterLogTime");
  private static final int FLUSH_COUNTER_LOG_INTERVAL = 30_000;

  public FlushCounterHandler(VeniceServerNettyStats nettyStats) {
    this.nettyStats = nettyStats;
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    try {
      nettyStats.recordNettyFlushCounts();
      // SocketAddress remoteAddress = ctx.channel().remoteAddress();
      // if (remoteAddress == null) {
      // return;
      // }
      // int flushCount = flushCounterMap.compute(remoteAddress, (key, value) -> value == null ? 1 : value + 1);
      // if (ctx.channel().attr(LACL_TIME).get() == null) {
      // ctx.channel().attr(LACL_TIME).set(System.currentTimeMillis());
      // }
      // long time = ctx.channel().attr(LACL_TIME).get();
      // long currentTimestamp = System.currentTimeMillis();
      // if ((currentTimestamp - time) > FLUSH_COUNTER_LOG_INTERVAL) {
      // LOGGER.info("###Flush count for remote address: {} - {}", remoteAddress, flushCount);
      // ctx.channel().attr(LACL_TIME).set(System.currentTimeMillis());
      // }
    } finally {
      super.flush(ctx); // Flush the data to the next handler in the pipeline
    }
  }
}
