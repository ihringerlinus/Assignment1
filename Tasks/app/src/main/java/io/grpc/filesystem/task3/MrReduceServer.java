/*
 * gRPC server node to accept calls from the clients and serve based on the method that has been requested
 */

package io.grpc.filesystem.task3;

import com.task3.proto.AssignJobGrpc;
import com.task3.proto.ReduceInput;
import com.task3.proto.ReduceOutput;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.filesystem.task2.MapReduce;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MrReduceServer {

    private Server server;

    public static void main(String[] args) throws IOException, InterruptedException {
        final MrReduceServer mrServer = new MrReduceServer();
        // The server starts on the port passed as an argument
        if (args.length > 0) {
            mrServer.start(Integer.parseInt(args[0]));
            mrServer.server.awaitTermination();
        } else {
            System.err.println("No port specified for Reduce server.");
        }
    }

    private void start(int port) throws IOException {
        server = ServerBuilder.forPort(port).addService(new MrReduceServerImpl()).build().start();
        System.out.println("Reduce Server listening on: " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Terminating the Reduce server at port: " + port);
            try {
                server.shutdown().awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }));
    }

    static class MrReduceServerImpl extends AssignJobGrpc.AssignJobImplBase {

        @Override
        public void reduce(ReduceInput request, StreamObserver<ReduceOutput> responseObserver) {
            System.out.println("Performing Reduce on directory: " + request.getInputfilepath());
            try {
                // Perform the reduce operation using the directory of map files.
                MapReduce.reduce(request.getInputfilepath(), request.getOutputfilepath());

                // Send a success response.
                ReduceOutput response = ReduceOutput.newBuilder().setJobstatus(2).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                System.out.println("Reduce task completed.");

            } catch (IOException e) {
                e.printStackTrace();
                // Send an error response if something goes wrong.
                ReduceOutput response = ReduceOutput.newBuilder().setJobstatus(-1).build(); // -1 for error
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }
    }
}