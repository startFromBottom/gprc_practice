package com.github.hh.grpc.calculator.client;

import com.proto.calculator.CalculatorServiceGrpc;
import com.proto.calculator.PrimeNumberDecompositionRequest;
import com.proto.calculator.SumRequest;
import com.proto.calculator.SumResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class CalculatorClient {

    public static void main(String[] args) {

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50052)
                .usePlaintext()
                .build();

        CalculatorServiceGrpc.CalculatorServiceBlockingStub stub = CalculatorServiceGrpc.newBlockingStub(channel);

        // 1. Unary

//        SumRequest request = SumRequest.newBuilder()
//                .setFirstNum(10)
//                .setSecondNum(25)
//                .build();
//
//        SumResponse response = stub.sum(request);
//
//        System.out.println(request.getFirstNum() + " + " + request.getSecondNum() + " = " + response.getSumResult());

        // 2. Streaming Server

        Integer number = 567890;

        stub.primeNumberDecomposition(PrimeNumberDecompositionRequest.newBuilder().setNumber(number).build())
                .forEachRemaining(primeNumberDecompositionResponse -> {
                    System.out.println(primeNumberDecompositionResponse.getPrimeFactor());
                });

        channel.shutdown();

    }
}
