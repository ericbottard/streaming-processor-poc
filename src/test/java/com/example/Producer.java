package com.example;

import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

public class Producer {
    public static void main(String[] args) throws IOException {

        // This variable should point to your Liiklus deployment (possible behind a Load Balancer)
        //String liiklusTarget = getLiiklusTarget();
        String liiklusTarget = "35.204.226.236:6565";

        var channel = NettyChannelBuilder.forTarget(liiklusTarget)
                .directExecutor()
                .usePlaintext()
                .build();

        var stub = ReactorLiiklusServiceGrpc.newReactorStub(channel);

        // Send an event every second
        Flux.interval(Duration.ofSeconds(1))
                .onBackpressureDrop()
                .concatMap(it -> stub.publish(
                        PublishRequest.newBuilder()
                                .setTopic("numbers")
                                .setKey(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                .setValue(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                .build()
                ))
                .subscribe();

        System.in.read();


    }

}
