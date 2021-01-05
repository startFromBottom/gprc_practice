package com.github.hh.grpc.blog.client;


import com.proto.calculator.Blog;
import com.proto.calculator.BlogServiceGrpc;
import com.proto.calculator.CreateBlogRequest;
import com.proto.calculator.CreateBlogResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class BlogClient {

    public static void main(String[] args) {
        System.out.println("Hello I'm a gRPC client.");
        BlogClient main = new BlogClient();
        main.run();
    }

    private void run() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        BlogServiceGrpc.BlogServiceBlockingStub blogClient = BlogServiceGrpc.newBlockingStub(channel);
        Blog blog = Blog.newBuilder()
                .setAuthorId("aaa")
                .setTitle("New blog!")
                .setContent("Hello world this is my first blog!")
                .build();

        CreateBlogResponse createResponse = blogClient.createBlog(
                CreateBlogRequest.newBuilder()
                        .setBlog(blog)
                        .build()
        );

        System.out.println("Received create blog response");
        System.out.println(createResponse.toString());

    }
}

