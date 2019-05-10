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

	private static final String[] numbers = new String[]{"zero", "one", "two", "three", "four", "five"};

	public static void main(String[] args) throws IOException {

        // This variable should point to your Liiklus deployment (possible behind a Load Balancer)
        //String liiklusTarget = getLiiklusTarget();
        String liiklusTarget = "35.241.239.96:6565";

        var channel = NettyChannelBuilder.forTarget(liiklusTarget)
                .directExecutor()
                .usePlaintext()
                .build();

        var stub = ReactorLiiklusServiceGrpc.newReactorStub(channel);

		Flux.interval(Duration.ofMillis(500L)).map(i -> i % numbers.length).onBackpressureDrop()
				.concatMap(it -> stub.publish(
						PublishRequest.newBuilder()
								.setTopic("numbers")
								.setKey(ByteString.copyFromUtf8("irrelevant"))
								.setValue(ByteString.copyFromUtf8(it.toString()))
								.build()
				)).doOnNext(System.out::println).subscribe();
		Flux.interval(Duration.ofMillis(600L)).map(i -> numbers[i.intValue() % numbers.length]).onBackpressureDrop()
				.concatMap(it -> stub.publish(
						PublishRequest.newBuilder()
								.setTopic("words")
								.setKey(ByteString.copyFromUtf8("irrelevant"))
								.setValue(ByteString.copyFromUtf8(it.toString()))
								.build()
				)).doOnNext(System.out::println).subscribe();

        System.in.read();


    }

}
