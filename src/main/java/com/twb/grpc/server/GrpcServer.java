package com.twb.grpc.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class GrpcServer {
    private static final int SERVER_PORT = 8080;

    public static void main(String[] args) throws IOException, InterruptedException {
        HelloServiceImpl helloService = new HelloServiceImpl();

        Server server = ServerBuilder.forPort(SERVER_PORT)
                .addService(helloService).build();

        System.out.println("Starting server...");
        server.start();

        System.out.println("Server started on port: " + SERVER_PORT);
        server.awaitTermination();
    }
}
