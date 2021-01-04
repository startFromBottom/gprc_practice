package com.github.hh.grpc.greeting.client;

import com.proto.greet.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class GreetingClient {

    public static void main(String[] args) {
        System.out.println("Hello I'm a gRPC client");

        GreetingClient main = new GreetingClient();
        main.run();

        System.out.println("Creating Stub");

    }

    public void run() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext() // not use in production
                .build();

//        doUnaryCall(channel);
//        doServerStreamingCall(channel);
//        doClientStreamingCall(channel);
        doBiDiStreamingCall(channel);

        // do something
        System.out.println("Shutting down channel");
        channel.shutdown();

    }

    private void doUnaryCall(ManagedChannel channel) {

        // created a greet service client (blocking - synchronous)
        GreetServiceGrpc.GreetServiceBlockingStub greetClient = GreetServiceGrpc.newBlockingStub(channel);

        // created a protocol buffer greeting message
        Greeting greeting = Greeting.newBuilder()
                .setFirstName("Stitch")
                .setLastName("HH")
                .build();

        // do the same for a GreetRequest
        GreetRequest greetRequest = GreetRequest.newBuilder()
                .setGreeting(greeting)
                .build();

        // call the RPC and get back a GreetResponse(protocol buffers)
        // 마치 server의 greet 메서드를 직접 호출하는 것처럼 보이지만, 네트워크를 타는 것!
        GreetResponse greetResponse = greetClient.greet(greetRequest);

    }

    private void doServerStreamingCall(ManagedChannel channel) {

        // created a greet service client (blocking - synchronous)
        GreetServiceGrpc.GreetServiceBlockingStub greetClient = GreetServiceGrpc.newBlockingStub(channel);

        GreetManyTimesRequest greetManyTimesRequest =
                GreetManyTimesRequest.newBuilder()
                        .setGreeting(Greeting.newBuilder().setFirstName("hh"))
                        .build();

        // we stream the responses (in blocking manner)
        greetClient.greetManyTimes(greetManyTimesRequest)
                .forEachRemaining(greetManyTimesResponse -> {
                    System.out.println(greetManyTimesResponse.getResult());
                });
    }

    private void doClientStreamingCall(ManagedChannel channel) {
        // create a asynchronous client
        GreetServiceGrpc.GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<LongGreetRequest> requestObserver = asyncClient.longGreet(new StreamObserver<LongGreetResponse>() {
            @Override
            public void onNext(LongGreetResponse value) {
                // we get a response from the server
                System.out.println("Received a response from the server");
                System.out.println(value.getResult());
                // onNext will be called only once
            }

            @Override
            public void onError(Throwable t) {
                // we get an error from the server

            }

            @Override
            public void onCompleted() {
                // the server is done sending us data
                // onCompleted will be called right after onNext()
                System.out.println("Server has completed sending us something");
                latch.countDown();
            }
        });

        // streaming message #1
        System.out.println("sending message 1");
        requestObserver.onNext(LongGreetRequest.newBuilder()
                .setGreeting(Greeting.newBuilder()
                        .setFirstName("aa")
                        .build())
                .build());

        // streaming messsage #2
        System.out.println("sending message 2");
        requestObserver.onNext(LongGreetRequest.newBuilder()
                .setGreeting(Greeting.newBuilder()
                        .setFirstName("bb")
                        .build())
                .build());

        // streaming message #3
        System.out.println("sending message 3");
        requestObserver.onNext(LongGreetRequest.newBuilder()
                .setGreeting(Greeting.newBuilder()
                        .setFirstName("cc")
                        .build())
                .build());

        // we tell the server that the client is done sending data
        requestObserver.onCompleted();

        try {
            latch.await(3L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void doBiDiStreamingCall(ManagedChannel channel) {

        GreetServiceGrpc.GreetServiceStub asyncClient = GreetServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<GreetEveryoneRequest> requestObserver = asyncClient.greetEveryone(new StreamObserver<GreetEveryoneResponse>() {
            @Override
            public void onNext(GreetEveryoneResponse value) {
                System.out.println("Response from server : " + value.getResult());
            }

            @Override
            public void onError(Throwable t) {
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println("Server is done sending data");
                latch.countDown();
            }
        });

        Arrays.asList("AAA", "BBB", "CCC", "DDD").forEach(
                name -> {
                    System.out.println("Sending: " + name);
                    requestObserver.onNext(GreetEveryoneRequest.newBuilder()
                            .setGreeting(Greeting.newBuilder().setFirstName(name)).build());

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                });

        requestObserver.onCompleted();

        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
