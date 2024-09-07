package com.linkedin.venice.listener;

import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.venice.listener.response.BinaryResponse;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.netty.channel.ChannelHandlerContext;


/**
 * This is used in REST/HTTP Netty handlers to handle the response from the {@link StorageReadRequestHandler#processRequest} method.
 */
public class HttpStorageResponseHandlerCallback implements StorageResponseHandlerCallback {
  private final ChannelHandlerContext context;

  public HttpStorageResponseHandlerCallback(ChannelHandlerContext context) {
    this.context = context;
  }

  // Factory method for creating an instance of this class.
  public static HttpStorageResponseHandlerCallback create(ChannelHandlerContext context) {
    return new HttpStorageResponseHandlerCallback(context);
  }

  @Override
  public void onReadResponse(ReadResponse readResponse) {
    context.writeAndFlush(readResponse);
  }

  @Override
  public void onBinaryResponse(BinaryResponse binaryResponse) {
    context.writeAndFlush(binaryResponse);
  }

  @Override
  public void onAdminResponse(AdminResponse adminResponse) {
    context.writeAndFlush(adminResponse);
  }

  @Override
  public void onMetadataResponse(MetadataResponse metadataResponse) {
    context.writeAndFlush(metadataResponse);
  }

  @Override
  public void onServerCurrentVersionResponse(ServerCurrentVersionResponse serverCurrentVersionResponse) {
    context.writeAndFlush(serverCurrentVersionResponse);
  }

  @Override
  public void onTopicPartitionIngestionContextResponse(
      TopicPartitionIngestionContextResponse ingestionContextResponse) {
    context.writeAndFlush(ingestionContextResponse);
  }

  @Override
  public void onResponse(VeniceReadResponseStatus readResponseStatus, String message) {
    context.writeAndFlush(new HttpShortcutResponse(message, readResponseStatus.getHttpResponseStatus()));
  }

  @Override
  public void onError(VeniceReadResponseStatus readResponseStatus, String message) {
    HttpShortcutResponse response = new HttpShortcutResponse(message, readResponseStatus.getHttpResponseStatus());
    if (readResponseStatus == VeniceReadResponseStatus.MISROUTED_STORE_VERSION) {
      response.setMisroutedStoreVersion(true);
    }
    context.writeAndFlush(response);
  }
}
