package com.twb.grpc.client;


import com.twb.grpc.HelloRequest;
import com.twb.grpc.HelloResponse;
import com.twb.grpc.HelloServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GrpcClient {
    private static final int PORT = 8080;

    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT)
                .usePlaintext()
                .build();

        HelloServiceGrpc.HelloServiceBlockingStub stub
                = HelloServiceGrpc.newBlockingStub(channel);

        HelloRequest request = HelloRequest.newBuilder()
                .setFirstName("Baeldung")
                .setLastName("gRPC")
                .build();

        HelloResponse helloResponse = stub.hello(request);
        System.out.println("Response received from server:\n" + helloResponse);

        channel.shutdown();
    }
}
