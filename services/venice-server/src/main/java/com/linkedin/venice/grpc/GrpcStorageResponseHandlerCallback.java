package com.linkedin.venice.grpc;

import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.venice.listener.StorageResponseHandlerCallback;
import com.linkedin.venice.listener.response.BinaryResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;


public class GrpcStorageResponseHandlerCallback implements StorageResponseHandlerCallback {

  // Factory method for creating an instance of this class.
  public static GrpcStorageResponseHandlerCallback create() {
    return new GrpcStorageResponseHandlerCallback();
  }

  @Override
  public void onReadResponse(ReadResponse readResponse) {

  }

  @Override
  public void onBinaryResponse(BinaryResponse binaryResponse) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void onAdminResponse(AdminResponse adminResponse) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void onMetadataResponse(MetadataResponse metadataResponse) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void onServerCurrentVersionResponse(ServerCurrentVersionResponse serverCurrentVersionResponse) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void onTopicPartitionIngestionContextResponse(
      TopicPartitionIngestionContextResponse topicPartitionIngestionContextResponse) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void onResponse(VeniceReadResponseStatus readResponseStatus, String message) {

  }

  @Override
  public void onError(VeniceReadResponseStatus readResponseStatus, String message) {

  }
}
