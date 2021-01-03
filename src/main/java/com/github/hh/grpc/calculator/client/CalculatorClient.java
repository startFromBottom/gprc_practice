package com.github.hh.grpc.calculator.client;

import com.proto.calculator.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CalculatorClient {

    public static void main(String[] args) {
        System.out.println("Hello I'm a gRPC client.");
        CalculatorClient main = new CalculatorClient();
        main.run();
    }

    private void run() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50052)
                .usePlaintext()
                .build();

//        doUnaryCall(channel);
//        doServerStreamingCall(channel);
        doClientStreamingCall(channel);
        System.out.println("Shutting down channel");

        channel.shutdown();

    }

    private void doUnaryCall(ManagedChannel channel) {
        CalculatorServiceGrpc.CalculatorServiceBlockingStub stub = CalculatorServiceGrpc.newBlockingStub(channel);
        SumRequest request = SumRequest.newBuilder()
                .setFirstNum(10)
                .setSecondNum(25)
                .build();

        SumResponse response = stub.sum(request);

        System.out.println(request.getFirstNum() + " + " + request.getSecondNum() + " = " + response.getSumResult());

    }

    private void doServerStreamingCall(ManagedChannel channel) {
        CalculatorServiceGrpc.CalculatorServiceBlockingStub stub = CalculatorServiceGrpc.newBlockingStub(channel);
        Integer number = 567890;

        stub.primeNumberDecomposition(PrimeNumberDecompositionRequest.newBuilder().setNumber(number).build())
                .forEachRemaining(primeNumberDecompositionResponse -> {
                    System.out.println(primeNumberDecompositionResponse.getPrimeFactor());
                });
    }

    private void doClientStreamingCall(ManagedChannel channel) {
        CalculatorServiceGrpc.CalculatorServiceStub asyncClient = CalculatorServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<ComputeAverageRequest> requestObserver =
                asyncClient.computeAverage(new StreamObserver<ComputeAverageResponse>() {
                    @Override
                    public void onNext(ComputeAverageResponse value) {
                        System.out.println("Received a response from the server");
                        System.out.println(value.getAverage());
                    }

                    @Override
                    public void onError(Throwable t) {

                    }

                    @Override
                    public void onCompleted() {
                        System.out.println("Server has completed sending us data");
                        latch.countDown();
                    }
                });

        // we send 10000 messages to our server(client streaming)
        for (int i = 0; i < 10000; i++) {
            requestObserver.onNext(ComputeAverageRequest.newBuilder().setNumber(i).build());
        }

        requestObserver.onCompleted();
        try {
            latch.await(3L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
