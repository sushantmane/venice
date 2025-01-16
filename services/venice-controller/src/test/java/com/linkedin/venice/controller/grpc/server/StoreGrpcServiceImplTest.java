package com.linkedin.venice.controller.grpc.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.grpc.GrpcRequestResponseConverter;
import com.linkedin.venice.controller.server.StoreRequestHandler;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcErrorInfo;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreGrpcServiceImplTest {
  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_STORE = "test-store";

  private Server grpcServer;
  private ManagedChannel grpcChannel;
  private StoreRequestHandler storeRequestHandler;
  private StoreGrpcServiceBlockingStub storeGrpcServiceBlockingStub;

  @BeforeMethod
  public void setUp() throws Exception {
    storeRequestHandler = mock(StoreRequestHandler.class);

    // Create a unique server name for the in-process server
    String serverName = InProcessServerBuilder.generateName();

    // Start the gRPC server in-process
    grpcServer = InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(new StoreGrpcServiceImpl(storeRequestHandler))
        .build()
        .start();

    // Create a channel to communicate with the server
    grpcChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

    // Create a blocking stub to make calls to the server
    storeGrpcServiceBlockingStub = StoreGrpcServiceGrpc.newBlockingStub(grpcChannel);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (grpcServer != null) {
      grpcServer.shutdown();
    }
    if (grpcChannel != null) {
      grpcChannel.shutdown();
    }
  }

  @Test
  public void testUpdateAclForStoreReturnsSuccessfulResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    UpdateAclForStoreGrpcRequest request = UpdateAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    UpdateAclForStoreGrpcResponse response = UpdateAclForStoreGrpcResponse.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.updateAclForStore(any(UpdateAclForStoreGrpcRequest.class))).thenReturn(response);
    UpdateAclForStoreGrpcResponse actualResponse = storeGrpcServiceBlockingStub.updateAclForStore(request);
    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse, response, "Response should match");
  }

  @Test
  public void testUpdateAclForStoreReturnsErrorResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    UpdateAclForStoreGrpcRequest request = UpdateAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.updateAclForStore(any(UpdateAclForStoreGrpcRequest.class)))
        .thenThrow(new VeniceException("Failed to update ACL"));
    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> storeGrpcServiceBlockingStub.updateAclForStore(request));
    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertNotNull(errorInfo, "Error info should not be null");
    assertTrue(errorInfo.getErrorMessage().contains("Failed to update ACL"));
  }

  @Test
  public void testGetAclForStoreReturnsSuccessfulResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetAclForStoreGrpcRequest request = GetAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    GetAclForStoreGrpcResponse response = GetAclForStoreGrpcResponse.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.getAclForStore(any(GetAclForStoreGrpcRequest.class))).thenReturn(response);
    GetAclForStoreGrpcResponse actualResponse = storeGrpcServiceBlockingStub.getAclForStore(request);
    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse, response, "Response should match");
  }

  @Test
  public void testGetAclForStoreReturnsErrorResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetAclForStoreGrpcRequest request = GetAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.getAclForStore(any(GetAclForStoreGrpcRequest.class)))
        .thenThrow(new VeniceException("Failed to get ACL"));
    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> storeGrpcServiceBlockingStub.getAclForStore(request));
    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertNotNull(errorInfo, "Error info should not be null");
    assertTrue(errorInfo.getErrorMessage().contains("Failed to get ACL"));
  }

  @Test
  public void testDeleteAclForStoreReturnsSuccessfulResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    DeleteAclForStoreGrpcRequest request = DeleteAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    DeleteAclForStoreGrpcResponse response = DeleteAclForStoreGrpcResponse.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.deleteAclForStore(any(DeleteAclForStoreGrpcRequest.class))).thenReturn(response);
    DeleteAclForStoreGrpcResponse actualResponse = storeGrpcServiceBlockingStub.deleteAclForStore(request);
    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse, response, "Response should match");
  }

  @Test
  public void testDeleteAclForStoreReturnsErrorResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    DeleteAclForStoreGrpcRequest request = DeleteAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.deleteAclForStore(any(DeleteAclForStoreGrpcRequest.class)))
        .thenThrow(new VeniceException("Failed to delete ACL"));
    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> storeGrpcServiceBlockingStub.deleteAclForStore(request));
    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertNotNull(errorInfo, "Error info should not be null");
    assertTrue(errorInfo.getErrorMessage().contains("Failed to delete ACL"));
  }
}
