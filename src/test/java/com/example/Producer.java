package com.example;

import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.projectriff.processor.serialization.Message;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.time.Duration;

public class Producer {

	private static final String[] COLORS = new String[]{"red", "green", "blue", "yellow"};

	private static final int MAX_NUMBER = 5;

	public static void main(String[] args) throws IOException {

        String liiklusTarget = "35.241.239.96:6565";

        var channel = NettyChannelBuilder.forTarget(liiklusTarget)
                .directExecutor()
                .usePlaintext()
                .build();

        var stub = ReactorLiiklusServiceGrpc.newReactorStub(channel);

		Flux.interval(Duration.ofMillis(500L)).map(i -> i % MAX_NUMBER)
				.concatMap(it -> stub.publish(
						PublishRequest.newBuilder()
								.setTopic("numbers")
								.setKey(ByteString.copyFromUtf8("irrelevant"))
								.setValue(numberAsMessage(it))
								.build()
				)).doOnNext(System.out::println).subscribe();

		Flux.interval(Duration.ofMillis(600L)).map(i -> COLORS[i.intValue() % COLORS.length])
				.concatMap(it -> stub.publish(
						PublishRequest.newBuilder()
								.setTopic("words")
								.setKey(ByteString.copyFromUtf8("irrelevant"))
								.setValue(wordAsMessage(it))
								.build()
				)).doOnNext(System.out::println).subscribe();

        System.in.read();


    }

	private static ByteString wordAsMessage(String it) {
		return Message.newBuilder()
				.setPayload(ByteString.copyFromUtf8(it))
				.setContentType("text/plain")
				.build().toByteString();
	}

	private static ByteString numberAsMessage(Long it) {
		return Message.newBuilder()
				.setPayload(ByteString.copyFromUtf8(it.toString()))
				.setContentType("text/plain")
				.build().toByteString();
	}

}
