package com.example;

import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.protocol.SubscribeRequest.AutoOffsetReset;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

public class Consumer {
    public static void main(String[] args) {
        // This variable should point to your Liiklus deployment (possible behind a Load Balancer)
        //String liiklusTarget = getLiiklusTarget();
        String liiklusTarget = "35.241.239.96:6565";

        var channel = NettyChannelBuilder.forTarget(liiklusTarget)
                .usePlaintext()
                .build();


        var stub = ReactorLiiklusServiceGrpc.newReactorStub(channel);

        Flux.just("repeated", "averages")
                .flatMap(topic ->
                    stub.subscribe(subscribeRequestFor(topic))
                            .filter(SubscribeReply::hasAssignment)
                            .map(SubscribeReply::getAssignment)
                            .map(Consumer::receiveRequestForAssignment)
                            .flatMap(stub::receive)
                            .doOnNext(rr -> System.out.format("%s: %s%n", topic, rr.getRecord().getValue().toStringUtf8()))
                ).blockLast();

    }

    private static SubscribeRequest subscribeRequestFor(String topic) {
        return SubscribeRequest.newBuilder()
                .setTopic(topic)
                .setGroup("my-group")
                .setAutoOffsetReset(AutoOffsetReset.LATEST)
                .build();
    }

    private static ReceiveRequest receiveRequestForAssignment(Assignment assignment) {
        return ReceiveRequest.newBuilder().setAssignment(assignment).build();
    }


}
