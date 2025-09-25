/*
 * gRPC server node to accept calls from the clients and serve based on the method that has been requested
 */

package io.grpc.filesystem.task3;

import com.task3.proto.AssignJobGrpc;
import com.task3.proto.MapInput;
import com.task3.proto.MapOutput;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.filesystem.task2.MapReduce;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MrMapServer {

    private Server server;

    public static void main(String[] args) throws IOException, InterruptedException {
        final MrMapServer mrServer = new MrMapServer();
        // The server starts on the port passed as an argument
        if (args.length > 0) {
            mrServer.start(Integer.parseInt(args[0]));
            mrServer.server.awaitTermination();
        } else {
            System.err.println("No port specified for Map server.");
        }
    }

    private void start(int port) throws IOException {
        server = ServerBuilder.forPort(port).addService(new MrMapServerImpl()).build().start();
        System.out.println("Map Server listening on: " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Terminating the Map server at port: " + port);
            try {
                server.shutdown().awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }));
    }

    static class MrMapServerImpl extends AssignJobGrpc.AssignJobImplBase {

        @Override
        public StreamObserver<MapInput> map(StreamObserver<MapOutput> responseObserver) {
            return new StreamObserver<MapInput>() {
                @Override
                public void onNext(MapInput request) {
                    try {
                        // For each incoming chunk file path, perform the map operation.
                        System.out.println("Mapping chunk: " + request.getInputfilepath());
                        MapReduce.map(request.getInputfilepath());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Error in Map server: " + t.getMessage());
                }

                @Override
                public void onCompleted() {
                    // When client is done sending chunks, send a success response.
                    MapOutput response = MapOutput.newBuilder().setJobstatus(2).build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    System.out.println("Map tasks completed.");
                }
            };
        }
    }
}