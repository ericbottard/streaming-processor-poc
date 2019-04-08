package com.example;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import com.github.bsideup.liiklus.protocol.AckRequest;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.github.bsideup.liiklus.protocol.SubscribeRequest.AutoOffsetReset;
import com.google.protobuf.ByteString;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.reactivestreams.Publisher;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class Producer {
    public static void main(String[] args) throws IOException {

        // This variable should point to your Liiklus deployment (possible behind a Load Balancer)
        //String liiklusTarget = getLiiklusTarget();
        String liiklusTarget = "35.204.31.102:6565";

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
