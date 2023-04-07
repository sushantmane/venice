package com.linkedin.venice;

import com.linkedin.venice.client.store.QueryTool;
import com.linkedin.venice.controller.VeniceController;
import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.server.VeniceServer;


public class VeniceLauncher {
  private static final String VENICE_SERVER = "server";
  private static final String VENICE_ROUTER = "router";
  private static final String VENICE_CONTROLLER = "controller";
  private static final String VENICE_CLIENT = "client";
  private static final String VENICE_HELP = "--help";

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Usage: VeniceLauncher <server|client|router|controller> <config file>");
      System.exit(1);
    }
    String cmd = args[0];
    String[] cmdArgs = new String[args.length - 1];
    System.arraycopy(args, 1, cmdArgs, 0, cmdArgs.length);

    switch (cmd) {
      case VENICE_SERVER:
        VeniceServer.main(cmdArgs);
        break;
      case VENICE_CLIENT:
        QueryTool.main(cmdArgs);
        break;
      case VENICE_ROUTER:
        RouterServer.main(cmdArgs);
        break;
      case VENICE_CONTROLLER:
        VeniceController.main(cmdArgs);
        break;
      case VENICE_HELP:
        System.out.println("Usage: VeniceLauncher <server|client|router|controller> selectedCmdOptions...");
        break;
      default:
        System.out.println("Unknown command: " + cmd);
        System.out.println("Usage: VeniceLauncher <server|client|router|controller> selectedCmdOptions...");
        break;
    }
  }
}
