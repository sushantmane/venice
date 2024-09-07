package com.linkedin.venice.listener;

import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.venice.listener.response.BinaryResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;


/**
 * Callback interface to handle the response from the {@link StorageReadRequestHandler#processRequest} method.
 */
public interface StorageResponseHandlerCallback {
  void onReadResponse(ReadResponse readResponse);

  void onBinaryResponse(BinaryResponse binaryResponse);

  void onAdminResponse(AdminResponse adminResponse);

  void onMetadataResponse(MetadataResponse metadataResponse);

  void onServerCurrentVersionResponse(ServerCurrentVersionResponse serverCurrentVersionResponse);

  void onTopicPartitionIngestionContextResponse(
      TopicPartitionIngestionContextResponse topicPartitionIngestionContextResponse);

  /**
   * Use this API to send non-standard non-error responses to the client.
   * @param readResponseStatus the status of the response
   * @param message the message to send to the client
   */
  void onResponse(VeniceReadResponseStatus readResponseStatus, String message);

  /**
   * Use this API to send error responses to the client.
   * @param readResponseStatus the status of the response
   * @param message the message to send to the client
   */
  void onError(VeniceReadResponseStatus readResponseStatus, String message);
}
