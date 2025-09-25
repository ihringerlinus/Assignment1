/*
 * Client program to request for map and reduce functions from the Server
 */

package io.grpc.filesystem.task3;

import com.task3.proto.AssignJobGrpc;
import com.task3.proto.MapInput;
import com.task3.proto.MapOutput;
import com.task3.proto.ReduceInput;
import com.task3.proto.ReduceOutput;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.filesystem.task2.MapReduce;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MrClient {
    Map<String, Integer> jobStatus = new HashMap<>();

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: MrClient <ip> <mapPort> <reducePort> <inputFilePath> <outputFilePath>");
            return;
        }
        String ip = args[0];
        Integer mapPort = Integer.parseInt(args[1]);
        Integer reducePort = Integer.parseInt(args[2]);
        String inputFilePath = args[3];
        String outputFilePath = args[4];

        MrClient client = new MrClient();

        // 1. Create chunks from the input file
        String chunkPath = MapReduce.makeChunks(inputFilePath);
        File dir = new File(chunkPath);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File f : directoryListing) {
                if (f.isFile() && f.getName().startsWith("chunk")) {
                    client.jobStatus.put(f.getPath(), 1); // 1 = pending
                }
            }
        } else {
            System.err.println("Could not list files in chunk directory: " + chunkPath);
            return;
        }

        // 2. Request Map tasks - KORRIGIERTER AUFRUF
        client.requestMap(ip, mapPort, inputFilePath, outputFilePath);

        // 3. Check if all map tasks were successful (status code 2)
        Set<Integer> values = new HashSet<>(client.jobStatus.values());
        if (values.size() == 1 && client.jobStatus.containsValue(2)) {
            System.out.println("All Map tasks completed successfully!");
            // 4. Request Reduce task
            int response = client.requestReduce(ip, reducePort, chunkPath, outputFilePath);
            if (response == 2) {
                System.out.println("Reduce task completed successfully!");
            } else {
                System.out.println("Reduce task failed. Status: " + response);
            }
        } else {
            System.out.println("Map tasks failed. Please check server logs.");
        }
    }

    // KORRIGIERTE METHODENSIGNATUR
    public void requestMap(String ip, Integer portNumber, String inputFilePath, String outputFilePath) throws InterruptedException {
        final CountDownLatch finishLatch = new CountDownLatch(1);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, portNumber).usePlaintext().build();
        AssignJobGrpc.AssignJobStub asyncStub = AssignJobGrpc.newStub(channel);

        StreamObserver<MapOutput> responseObserver = new StreamObserver<MapOutput>() {
            @Override
            public void onNext(MapOutput response) {
                if (response.getJobstatus() == 2) {
                    // Update all jobs to completed status
                    jobStatus.replaceAll((k, v) -> 2);
                }
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Map request failed: " + t.getMessage());
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                finishLatch.countDown();
            }
        };

        StreamObserver<MapInput> requestObserver = asyncStub.map(responseObserver);
        try {
            // We iterate through the chunk paths stored in the jobStatus map
            for (String chunkFilePath : jobStatus.keySet()) {
                MapInput request = MapInput.newBuilder()
                        .setInputfilepath(chunkFilePath) // Send the path to the chunk
                        .setOutputfilepath(outputFilePath)
                        .build();
                requestObserver.onNext(request);
            }
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        }

        requestObserver.onCompleted();

        // Wait for the server to finish processing and respond
        finishLatch.await(1, TimeUnit.MINUTES);

        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    public int requestReduce(String ip, Integer portNumber, String inputFilePath, String outputFilePath) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, portNumber).usePlaintext().build();
        AssignJobGrpc.AssignJobBlockingStub blockingStub = AssignJobGrpc.newBlockingStub(channel);

        ReduceInput request = ReduceInput.newBuilder()
                .setInputfilepath(inputFilePath) // This is the directory with map files
                .setOutputfilepath(outputFilePath)
                .build();

        ReduceOutput response = blockingStub.reduce(request);

        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);

        return response.getJobstatus();
    }
}