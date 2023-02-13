package com.linkedin.venice.pubsub.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import org.testng.annotations.Test;


public class PubsubMessageHeadersTest {
  @Test
  public void testPubsubMessageHeaders() {
    PubsubMessageHeaders headers = new PubsubMessageHeaders();
    headers.add("key-0", "val-0".getBytes());
    headers.add(new PubsubMessageHeader("key-1", "val-1".getBytes()));
    headers.add("key-2", "val-2".getBytes());
    headers.add(new PubsubMessageHeader("key-3", "val-3".getBytes()));
    headers.add(new PubsubMessageHeader("key-0", "val-0-prime".getBytes())); // should keep only the most recent val
    headers.remove("key-2");

    List<PubsubMessageHeader> headerList = headers.toList();
    assertNotNull(headerList);
    assertEquals(headerList.size(), 3);
    assertTrue(headerList.contains(new PubsubMessageHeader("key-1", "val-1".getBytes())));
    assertTrue(headerList.contains(new PubsubMessageHeader("key-0", "val-0-prime".getBytes())));
    assertTrue(headerList.contains(new PubsubMessageHeader("key-3", "val-3".getBytes())));
  }
}
