package com.linkedin.venice.grpc;

import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.INVALID_REQUEST_RESOURCE_MSG;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.SERVER_OVER_CAPACITY_MSG;

import com.linkedin.venice.listener.ReadQuotaEnforcementHandler;
import com.linkedin.venice.listener.ReadQuotaEnforcementHandler.QuotaEnforcementResult;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import java.util.Objects;


public class GrpcReadQuotaEnforcementHandler extends VeniceServerGrpcHandler {
  private final ReadQuotaEnforcementHandler readQuotaHandler;

  public GrpcReadQuotaEnforcementHandler(ReadQuotaEnforcementHandler readQuotaEnforcementHandler) {
    readQuotaHandler =
        Objects.requireNonNull(readQuotaEnforcementHandler, "ReadQuotaEnforcementHandler cannot be null");
  }

  @Override
  public void processRequest(GrpcRequestContext context) {
    RouterRequest request = context.getRouterRequest();
    QuotaEnforcementResult result = readQuotaHandler.enforceQuota(request);

    // If the request is allowed, pass it to the next handler and return early
    if (result == QuotaEnforcementResult.ALLOWED) {
      invokeNextHandler(context);
      return;
    }

    // Otherwise, set an error response based on the quota enforcement result
    context.setError();
    VeniceServerResponse.Builder responseBuilder = context.getVeniceServerResponseBuilder();

    switch (result) {
      case BAD_REQUEST:
        responseBuilder.setErrorCode(VeniceReadResponseStatus.BAD_REQUEST.getCode())
            .setErrorMessage(INVALID_REQUEST_RESOURCE_MSG + request.getResourceName());
        break;

      case REJECTED:
        responseBuilder.setErrorCode(VeniceReadResponseStatus.TOO_MANY_REQUESTS.getCode())
            .setErrorMessage("Quota exceeded for resource: " + request.getResourceName());
        break;

      case OVER_CAPACITY:
        responseBuilder.setErrorCode(VeniceReadResponseStatus.SERVICE_UNAVAILABLE.getCode())
            .setErrorMessage(SERVER_OVER_CAPACITY_MSG);
        break;

      default:
        responseBuilder.setErrorCode(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode())
            .setErrorMessage("Unknown quota enforcement result: " + result);
    }

    invokeNextHandler(context);
  }
}
