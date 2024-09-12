package com.linkedin.venice.grpc;

import static com.linkedin.venice.listener.StorageReadRequestHandler.VENICE_STORAGE_NODE_HARDWARE_IS_NOT_HEALTHY_MSG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.QuotaEnforcementHandler;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.listener.response.BinaryResponse;
import com.linkedin.venice.protocols.CompressionDictionaryRequest;
import com.linkedin.venice.protocols.CompressionDictionaryResponse;
import com.linkedin.venice.protocols.HealthCheckRequest;
import com.linkedin.venice.protocols.HealthCheckResponse;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc.VeniceReadServiceBlockingStub;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceGrpcReadServiceImplTest {
  private Server grpcServer;
  private ManagedChannel grpcChannel;
  private GrpcServiceDependencies grpcServiceDependencies;
  private DiskHealthCheckService diskHealthCheckService;
  private StorageReadRequestHandler storageReadRequestHandler;
  private QuotaEnforcementHandler quotaEnforcementHandler;
  private AggServerHttpRequestStats singleGetStats;
  private AggServerHttpRequestStats multiGetStats;
  private AggServerHttpRequestStats computeStats;

  @BeforeMethod
  public void setUp() throws IOException {
    diskHealthCheckService = mock(DiskHealthCheckService.class);
    storageReadRequestHandler = mock(StorageReadRequestHandler.class);
    quotaEnforcementHandler = mock(QuotaEnforcementHandler.class);
    singleGetStats = mock(AggServerHttpRequestStats.class);
    multiGetStats = mock(AggServerHttpRequestStats.class);
    computeStats = mock(AggServerHttpRequestStats.class);

    grpcServiceDependencies = new GrpcServiceDependencies.Builder().setDiskHealthCheckService(diskHealthCheckService)
        .setStorageReadRequestHandler(storageReadRequestHandler)
        .setQuotaEnforcementHandler(quotaEnforcementHandler)
        .setSingleGetStats(singleGetStats)
        .setMultiGetStats(multiGetStats)
        .setComputeStats(computeStats)
        .build();

    // Create a unique server name for the in-process server
    String serverName = InProcessServerBuilder.generateName();

    // Start the gRPC server in-process
    grpcServer = InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(new VeniceGrpcReadServiceImpl(grpcServiceDependencies))
        .build()
        .start();

    // Create a channel to communicate with the server
    grpcChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
  }

  @AfterMethod
  public void cleanup() {
    // Shut down the channel and server after each test
    if (grpcChannel != null) {
      grpcChannel.shutdownNow();
    }
    if (grpcServer != null) {
      grpcServer.shutdownNow();
    }
  }

  @Test
  public void testIsServerHealthy() {
    VeniceReadServiceBlockingStub blockingStub = VeniceReadServiceGrpc.newBlockingStub(grpcChannel);

    when(diskHealthCheckService.isDiskHealthy()).thenReturn(true);
    HealthCheckRequest request = HealthCheckRequest.newBuilder().build();
    assertEquals(blockingStub.isServerHealthy(request).getStatusCode(), VeniceReadResponseStatus.OK.getCode());

    when(diskHealthCheckService.isDiskHealthy()).thenReturn(false);
    HealthCheckResponse response = blockingStub.isServerHealthy(request);
    assertEquals(response.getStatusCode(), VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
    assertNotNull(response.getMessage());
    assertTrue(response.getMessage().contains(VENICE_STORAGE_NODE_HARDWARE_IS_NOT_HEALTHY_MSG));
  }

  // getCompressionDictionary
  @Test
  public void testGetCompressionDictionary() {
    String storeName = "testStore";
    int storeVersion = 1;

    // Case 1: Non-empty buffer response means dictionary found
    VeniceReadServiceBlockingStub blockingStub = VeniceReadServiceGrpc.newBlockingStub(grpcChannel);
    CompressionDictionaryRequest request =
        CompressionDictionaryRequest.newBuilder().setStoreName(storeName).setStoreVersion(storeVersion).build();
    BinaryResponse binaryResponse = new BinaryResponse(ByteBuffer.wrap(new byte[] { 4, 5, 6 }));
    when(storageReadRequestHandler.handleDictionaryFetchRequest(any())).thenReturn(binaryResponse);

    CompressionDictionaryResponse actualResponse = blockingStub.getCompressionDictionary(request);
    assertEquals(actualResponse.getStatusCode(), VeniceReadResponseStatus.OK.getCode());
    assertEquals(actualResponse.getValue().asReadOnlyByteBuffer(), ByteBuffer.wrap(new byte[] { 4, 5, 6 }));
    assertEquals(actualResponse.getContentType(), HttpConstants.BINARY);
    assertEquals(actualResponse.getContentLength(), 3);

    // Case 2: Empty buffer response means dictionary not found
    BinaryResponse emptyBufferResponse = new BinaryResponse((ByteBuffer) null);
    when(storageReadRequestHandler.handleDictionaryFetchRequest(any())).thenReturn(emptyBufferResponse);
    CompressionDictionaryResponse emptyBufferActualResponse = blockingStub.getCompressionDictionary(request);
    assertEquals(emptyBufferActualResponse.getStatusCode(), VeniceReadResponseStatus.KEY_NOT_FOUND.getCode());

    // Case 3: Exception was thrown when handling the request hence return INTERNAL_SERVER_ERROR
    when(storageReadRequestHandler.handleDictionaryFetchRequest(any()))
        .thenThrow(new VeniceException("Test exception"));
    CompressionDictionaryResponse exceptionActualResponse = blockingStub.getCompressionDictionary(request);
    assertEquals(exceptionActualResponse.getStatusCode(), VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode());
  }
}
