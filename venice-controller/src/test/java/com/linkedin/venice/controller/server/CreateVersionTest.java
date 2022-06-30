package com.linkedin.venice.controller.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;

import static com.linkedin.venice.HttpConstants.*;
import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;
import static org.mockito.Mockito.*;


public class CreateVersionTest {
  private static ObjectMapper mapper = ObjectMapperFactory.getInstance();

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCreateVersionWithACL(boolean checkReadMethod) {
    String storeName = "test_store";
    String user = "test_user";

    // Mock an Admin
    Admin admin = mock(Admin.class);

    // Mock a certificate
    X509Certificate certificate = mock(X509Certificate.class);
    X509Certificate[] certificateArray = new X509Certificate[1];
    certificateArray[0] = certificate;
    X500Principal principal = new X500Principal("CN=" + user);
    doReturn(principal).when(certificate).getSubjectX500Principal();

    // Mock a spark request
    Request request = mock(Request.class);
    doReturn("localhost").when(request).host();
    doReturn("0.0.0.0").when(request).ip();
    HttpServletRequest rawRequest = mock(HttpServletRequest.class);
    doReturn(rawRequest).when(request).raw();
    doReturn(certificateArray).when(rawRequest).getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME);
    doReturn(storeName).when(request).queryParams(NAME);

    // Mock a spark response
    Response response = mock(Response.class);

    // Mock a AccessClient
    DynamicAccessController accessClient = mock(DynamicAccessController.class);

    /**
     * Build a CreateVersion route.
     */
    CreateVersion createVersion = new CreateVersion(Optional.of(accessClient), checkReadMethod,
        false);
    Route createVersionRoute = createVersion.requestTopicForPushing(admin);

    // Not a allowlist user.
    doReturn(false).when(accessClient).isAllowlistUsers(certificate, storeName, HTTP_GET);

    /**
     * Create version should fail if user doesn't have "Write" method access to the topic
     */
    try {
      doReturn(false).when(accessClient).hasAccessToTopic(certificate, storeName, "Write");
      createVersionRoute.handle(request, response);
    } catch (Exception e) {
      throw new VeniceException(e);
    }

    /**
     * Response should be 403 if user doesn't have "Write" method access
     */
    verify(response).status(org.apache.http.HttpStatus.SC_FORBIDDEN);

    if (checkReadMethod) {
      // Mock another response
      Response response2 = mock(Response.class);
      /**
       * Create version should fail if user has "Write" method access but not "Read" method access to topics.
       */
      try {
        doReturn(true).when(accessClient).hasAccessToTopic(certificate, storeName, "Write");
        doReturn(false).when(accessClient).hasAccessToTopic(certificate, storeName, "Read");
        createVersionRoute.handle(request, response2);
      } catch (Exception e) {
        throw new VeniceException(e);
      }

      verify(response2).status(org.apache.http.HttpStatus.SC_FORBIDDEN);
    }
  }

  @Test
  public void testCreateVersionWithAmplificationFactorAndLeaderFollowerNotEnabled() throws Exception {
    String clusterName = "test_cluster";
    String storeName = "test_store";
    String pushJobId = "push_1";
    String hostname = "localhost";

    // Setting query params
    Map<String, String[]> queryMap = new HashMap<>();
    queryMap.put("store_name", new String[]{storeName});
    queryMap.put("store_size", new String[]{"0"});
    queryMap.put("push_type", new String[]{Version.PushType.INCREMENTAL.name()});
    queryMap.put("push_job_id", new String[]{pushJobId});
    queryMap.put("hostname", new String[]{hostname});

    // Mock an Admin
    Admin admin = mock(Admin.class);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);
    Store store = mock(Store.class);
    when(store.isLeaderFollowerModelEnabled()).thenReturn(false);
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setAmplificationFactor(2);
    when(store.getPartitionerConfig()).thenReturn(partitionerConfig);
    when(admin.getStore(any(), any())).thenReturn(store);
    CreateVersion createVersion = new CreateVersion(Optional.empty(), false, false);
    Route createVersionRoute = createVersion.requestTopicForPushing(admin);

    Request request = mock(Request.class);
    doReturn(clusterName).when(request).queryParams(CLUSTER);
    doReturn(REQUEST_TOPIC.getPath()).when(request).pathInfo();
    for (Map.Entry<String, String[]> queryParam : queryMap.entrySet()) {
      doReturn(queryParam.getValue()[0]).when(request).queryParams(queryParam.getKey());
    }
    HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    doReturn(new QueryParamsMap(httpServletRequest)).when(request).queryMap();

    Response response = mock(Response.class);
    createVersionRoute.handle(request, response);
    verify(response).status(org.apache.http.HttpStatus.SC_BAD_REQUEST);
  }
}
