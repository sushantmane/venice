package com.linkedin.venice.fastclient;

import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * This class is used to include all the intermediate fields required for the communication between the different tiers.
 */
public abstract class RequestContext {
  private int currentVersion = -1;
  private boolean noAvailableReplica = false;

  private double decompressionTime = -1;
  private double responseDeserializationTime = -1;
  private double requestSerializationTime = -1;
  private double requestSubmissionToResponseHandlingTime = -1;

  private long requestSentTimestampNS = -1;

  // Keeping track for successful keys for the request.
  private AtomicInteger successRequestKeyCount = new AtomicInteger(0);

  private InstanceHealthMonitor instanceHealthMonitor = null;

  private Map<String, CompletableFuture<Integer>> routeRequestMap = new VeniceConcurrentHashMap<>();

  RequestContext() {
  }

  public int getCurrentVersion() {
    return currentVersion;
  }

  public void setCurrentVersion(int currentVersion) {
    this.currentVersion = currentVersion;

  }

  public boolean isNoAvailableReplica() {
    return noAvailableReplica;
  }

  public void setNoAvailableReplica(boolean noAvailableReplica) {
    this.noAvailableReplica = noAvailableReplica;

  }

  public double getDecompressionTime() {
    return decompressionTime;
  }

  public void setDecompressionTime(double decompressionTime) {
    this.decompressionTime = decompressionTime;

  }

  public double getResponseDeserializationTime() {
    return responseDeserializationTime;
  }

  public void setResponseDeserializationTime(double responseDeserializationTime) {
    this.responseDeserializationTime = responseDeserializationTime;
  }

  public double getRequestSerializationTime() {
    return requestSerializationTime;
  }

  public void setRequestSerializationTime(double requestSerializationTime) {
    this.requestSerializationTime = requestSerializationTime;
  }

  public double getRequestSubmissionToResponseHandlingTime() {
    return requestSubmissionToResponseHandlingTime;
  }

  public void setRequestSubmissionToResponseHandlingTime(double requestSubmissionToResponseHandlingTime) {
    this.requestSubmissionToResponseHandlingTime = requestSubmissionToResponseHandlingTime;
  }

  public long getRequestSentTimestampNS() {
    return requestSentTimestampNS;
  }

  public void setRequestSentTimestampNS(long requestSentTimestampNS) {
    this.requestSentTimestampNS = requestSentTimestampNS;

  }

  public AtomicInteger getSuccessRequestKeyCount() {
    return successRequestKeyCount;
  }

  // increment the successRequestKeyCount and return the updated value
  public int incrementSuccessRequestKeyCount() {
    return successRequestKeyCount.incrementAndGet();
  }

  public void setSuccessRequestKeyCount(AtomicInteger successRequestKeyCount) {
    this.successRequestKeyCount = successRequestKeyCount;
  }

  public InstanceHealthMonitor getInstanceHealthMonitor() {
    return instanceHealthMonitor;
  }

  public void setInstanceHealthMonitor(InstanceHealthMonitor instanceHealthMonitor) {
    this.instanceHealthMonitor = instanceHealthMonitor;
  }

  public Map<String, CompletableFuture<Integer>> getRouteRequestMap() {
    return routeRequestMap;
  }

  public void setRouteRequestMap(Map<String, CompletableFuture<Integer>> routeRequestMap) {
    this.routeRequestMap = routeRequestMap;
  }

  public void addRouteRequest(String routeId, CompletableFuture<Integer> routeRequestFuture) {
    routeRequestMap.put(routeId, routeRequestFuture);
  }

  public CompletableFuture<Integer> getRouteRequest(String routeId) {
    return routeRequestMap.get(routeId);
  }

  public void removeRouteRequest(String routeId) {
    routeRequestMap.remove(routeId);
  }

  public void clearRouteRequests() {
    routeRequestMap.clear();
  }

  public void clear() {
    currentVersion = -1;
    noAvailableReplica = false;
    decompressionTime = -1;
    responseDeserializationTime = -1;
    requestSerializationTime = -1;
    requestSubmissionToResponseHandlingTime = -1;
    requestSentTimestampNS = -1;
    successRequestKeyCount.set(0);
    instanceHealthMonitor = null;
    routeRequestMap.clear();
  }
}
