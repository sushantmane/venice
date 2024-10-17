package com.linkedin.venice.grpc;

import static com.linkedin.venice.fastclient.transport.GrpcTransportClient.buildChannelCredentials;

import com.google.protobuf.Descriptors;
import com.linkedin.venice.protocols.VeniceEchoRequest;
import com.linkedin.venice.protocols.VeniceEchoResponse;
import com.linkedin.venice.protocols.VeniceEchoServiceGrpc;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import io.grpc.ChannelCredentials;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoMethodDescriptorSupplier;
import io.grpc.protobuf.ProtoServiceDescriptorSupplier;
import io.grpc.stub.StreamObserver;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.net.ssl.SSLSession;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestGrpcServer {
  private VeniceGrpcServer grpcServer;
  private VeniceGrpcServer grpcSecureServer;
  private int grpcServerPort;
  private int grpcSecureServerPort;
  private SSLFactory sslFactory;

  @BeforeClass(alwaysRun = true)
  public void setUpClass() {
    grpcServerPort = TestUtils.getFreePort();
    VeniceGrpcServerConfig grpcServerConfig =
        new VeniceGrpcServerConfig.Builder().setService(new VeniceEchoServiceImpl())
            .setPort(grpcServerPort)
            .setNumThreads(2)
            .setCredentials(InsecureServerCredentials.create())
            .build();
    grpcServer = new VeniceGrpcServer(grpcServerConfig);
    grpcServer.start();

    sslFactory = SslUtils.getVeniceLocalSslFactory();
    grpcSecureServerPort = TestUtils.getFreePort();
    VeniceGrpcServerConfig grpcSecureServerConfig =
        new VeniceGrpcServerConfig.Builder().setService(new VeniceEchoServiceImpl())
            .setPort(grpcSecureServerPort)
            .setNumThreads(2)
            .setSslFactory(sslFactory)
            .setInterceptor(new SslSessionInterceptor())
            .build();
    grpcSecureServer = new VeniceGrpcServer(grpcSecureServerConfig);
    grpcSecureServer.start();
    System.out.println("#### Server started:" + getServerServiceDefinitions(grpcSecureServer.getServer()));

  }

  @AfterClass
  public void tearDownClass() {
    if (grpcServer != null) {
      grpcServer.stop();
    }
    if (grpcSecureServer != null) {
      grpcSecureServer.stop();
    }
  }

  public static class SslSessionInterceptor implements ServerInterceptor {
    public static final Context.Key<X509Certificate> CLIENT_CERTIFICATE_CONTEXT_KEY =
        Context.key("venice-client-certificate");

    @Override
    public <ReqT, RespT> io.grpc.ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> serverCall,
        Metadata metadata,
        ServerCallHandler<ReqT, RespT> serverCallHandler) {
      // check if SSL is enabled for this call
      if (serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION) == null) {
        System.out.println("SSL not enabled");
      }
      // Log that we got an unexpected non-ssl request
      String remote =
          Objects.requireNonNull(serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)).toString();
      String method = serverCall.getMethodDescriptor().getBareMethodName();
      System.out.println(
          "Got a non-ssl request on what should be an ssl only connection: " + remote + " requested " + method);

      MethodDescriptor<ReqT, RespT> methodDescriptor = serverCall.getMethodDescriptor();
      ProtoServiceDescriptorSupplier protoMethodDescriptorSupplier =
          (ProtoServiceDescriptorSupplier) methodDescriptor.getSchemaDescriptor();
      System.out.println("Method descriptor: " + methodDescriptor);
      System.out.println("Proto method descriptor supplier: " + protoMethodDescriptorSupplier.getServiceDescriptor());

      SSLSession sslSession = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
      if (sslSession != null) {
        try {
          X509Certificate clientCert = GrpcUtils.extractGrpcClientCert(serverCall);
          Context context = Context.current().withValue(CLIENT_CERTIFICATE_CONTEXT_KEY, clientCert);
          System.out.println("Client cert: " + clientCert.getSubjectDN());
          System.out.println("Cipher suite: " + clientCert.getIssuerX500Principal().getName());
          return Contexts.interceptCall(context, serverCall, metadata, serverCallHandler);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      // Create a new listener to handle the request and capture the response
      return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
          serverCallHandler.startCall(serverCall, new Metadata())) {
        @Override
        public void onMessage(ReqT message) {
          // Log the request message
          System.out.println("#Request#: " + message);
          super.onMessage(message); // Call the original listener's method
        }
      };
    }
  }

  public static class VeniceEchoServiceImpl extends VeniceEchoServiceGrpc.VeniceEchoServiceImplBase {
    @Override
    public void echo(VeniceEchoRequest request, StreamObserver<VeniceEchoResponse> responseObserver) {
      System.out.println("Received message: " + request.getMessage());
      // SSLSession sslSession = (SSLSession) ServerCall.current().getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);

      MethodDescriptor.MethodType type = VeniceEchoServiceGrpc.getEchoMethod().getType();
      System.out.println("Method type: " + type);

      MethodDescriptor<VeniceEchoRequest, VeniceEchoResponse> methodDescriptor = VeniceEchoServiceGrpc.getEchoMethod();

      // methodDescriptor.

      // get certificate from context
      X509Certificate clientCert = SslSessionInterceptor.CLIENT_CERTIFICATE_CONTEXT_KEY.get(Context.current());
      System.out.println("#### Client cert: " + clientCert);
      if (clientCert != null) {
        System.out.println("### Client cert: " + clientCert.getSubjectDN());
      }

      VeniceEchoResponse response = VeniceEchoResponse.newBuilder().setMessage(request.getMessage()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  @Test
  public void testGrpcServerWithMutualAuth() {
    String serverAddress = String.format("localhost:%d", grpcSecureServerPort);
    ChannelCredentials credentials = buildChannelCredentials(sslFactory);
    ManagedChannel channel = Grpc.newChannelBuilder(serverAddress, credentials).build();
    VeniceEchoServiceGrpc.VeniceEchoServiceBlockingStub blockingStub = VeniceEchoServiceGrpc.newBlockingStub(channel);
    VeniceEchoRequest request = VeniceEchoRequest.newBuilder().setMessage("Hello").build();
    VeniceEchoResponse response = blockingStub.echo(request);
    System.out.println("Response: " + response.getMessage());
    System.out.println("Test passed");
  }

  public static Map<String, ServerServiceDefinition> getServerServiceDefinitions(Server grpcServer) {
    List<ServerServiceDefinition> serviceDefinitions = grpcServer.getServices();
    Map<String, ServerServiceDefinition> serviceDefinitionsMap = new HashMap<>();
    for (ServerServiceDefinition serviceDefinition: serviceDefinitions) {
      String serviceName = serviceDefinition.getServiceDescriptor().getName();
      serviceDefinitionsMap.put(serviceName, serviceDefinition);

      Collection<ServerMethodDefinition<?, ?>> methodDefinitions = serviceDefinition.getMethods();
      for (ServerMethodDefinition<?, ?> methodDefinition: methodDefinitions) {
        MethodDescriptor<?, ?> methodDescriptor = methodDefinition.getMethodDescriptor();
        ProtoMethodDescriptorSupplier protoMethodDescriptorSupplier =
            (ProtoMethodDescriptorSupplier) methodDescriptor.getSchemaDescriptor();
        System.out.println("Method descriptor: " + methodDescriptor);
        // System.out.println("Proto method descriptor supplier: " +
        // protoMethodDescriptorSupplier.methodDescriptor.getOptions()
        // .getExtension(VeniceEchoServiceGrpc.getEchoMethod().));
        Descriptors.MethodDescriptor methodDescriptor1 = protoMethodDescriptorSupplier.getMethodDescriptor();
        // System.out.println("Method descriptor1: " +
        // methodDescriptor1.getOptions().getExtension(VeniceE.getEchoMethod().getType()));

      }

      // System.out.println(serviceDefinition.getMethods());
    }
    return serviceDefinitionsMap;
  }
}
