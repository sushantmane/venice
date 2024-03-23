package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.*;

import java.util.PriorityQueue;
import org.testng.annotations.Test;


public class StorePartitionDataReceiverTest {
  @Test
  public void testPriorityQueue() {
    PriorityQueue<Integer> pq = new PriorityQueue<>();
    for (int i = 0; i < 10; i++) {
      pq.add(i);
    }
    for (int i = 0; i < 10; i++) {
      System.out.println(i);
    }
  }
}
