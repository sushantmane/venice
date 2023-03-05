package com.linkedin.venice;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;


public class VeniceTestListener implements ITestListener {

  private static final Logger LOGGER = LogManager.getLogger(VeniceTestListener.class);


  @Override
  public void onTestStart(ITestResult result) {
    System.out.println("#####COOL##### - TEST: " + result.getName());
    LOGGER.info("\n\n######## TEST ######## {} - STARTED", result.getMethod().getQualifiedName());
  }

  @Override
  public void onTestSuccess(ITestResult result) {
    System.out.println("#####COOL##### - test success: " + result.getName() + " -- " + result);
    LOGGER.info("######## TEST ######## {} - PASSED\n\n", result.getMethod().getQualifiedName());
  }

  @Override
  public void onTestFailure(ITestResult result) {
    System.out.println("#####COOL##### - test failure: " + result.getName() + " -- " + result);
    LOGGER.info("######## TEST ######## {} - FAILED\n\n", result.getMethod().getQualifiedName());
  }

  @Override
  public void onTestSkipped(ITestResult result) {
    System.out.println("#####COOL##### - test skipped: " + result.getName() + " -- " + result);
    LOGGER.info("######## TEST ######## {} - PASSED\n\n", result.getMethod().getQualifiedName());
  }

  @Override
  public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
    System.out.println("#####COOL##### - test onTestFailedButWithinSuccessPercentage: " + result.getName() + " -- " + result);
    LOGGER.info("######## TEST ######## {} - FLAKY\n\n", result.getMethod().getQualifiedName());
  }

  @Override
  public void onStart(ITestContext context) {
    System.out.println("#####COOL##### - test onStart: " + context.getName() + " -- " + context);
    LOGGER.info("\n\n****************** START ******************\n\n");
  }

  @Override
  public void onFinish(ITestContext context) {
    System.out.println("#####COOL##### - test onFinish: " + context.getName() + " -- " + context);
    LOGGER.info("\n\n****************** FINISHED - failed tests: {}  ******************\n\n", context.getFailedTests());
  }
}
