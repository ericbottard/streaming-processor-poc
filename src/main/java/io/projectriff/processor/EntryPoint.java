package io.projectriff.processor;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import com.github.bsideup.liiklus.protocol.Assignment;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.projectriff.invoker.server.Message;
import io.projectriff.invoker.server.RiffClient;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import reactor.core.publisher.Flux;

public class EntryPoint {

	private static String group;

	private static String output;

	private static RiffClient riffStub;

	public static void main(String[] args) throws IOException {

		var gwAddress = System.getenv("GATEWAY");
		group = System.getenv("GROUP");
		var sInputs = System.getenv("INPUTS");
		var sOutputs = System.getenv("OUTPUTS");

		gwAddress = "localhost:6565";
		gwAddress = "35.204.31.102:6565";
		group = "processor0";
		sInputs = "numbers";
		sOutputs = "squares,cubes";
		output = "squares";

		var inputs = Flux.fromArray(sInputs.split(","));
		var outputs = Flux.fromArray(sOutputs.split(","));

		var channel = NettyChannelBuilder.forTarget(gwAddress)
				.directExecutor()
				.usePlaintext()
				.build();
		var liiklusStub = ReactorLiiklusServiceGrpc.newReactorStub(channel);


		WebsocketClientTransport websocketClientTransport = WebsocketClientTransport
				.create(URI.create("ws://kmprssr2.default.35.241.251.246.nip.io/ws"));
		RSocket rSocket = RSocketFactory
				.connect()
				.transport(websocketClientTransport)
				.start()
				.block();


		riffStub = new RiffClient(rSocket);
		var input = Flux.just(sInputs);

		/*
		inputs.map(EntryPoint::subscribeRequestForInput)
				.flatMap(liiklusStub::subscribe)
				.filter(EntryPoint::isAssignment)
				.map(SubscribeReply::getAssignment)
				.map(EntryPoint::receiveRequestForAssignment)
				.flatMap(liiklusStub::receive)
				.map(EntryPoint::toRiffMessage)
				.compose(EntryPoint::riffWindowing)
				.doOnEach(f -> )

		;
		*/

		input.map(EntryPoint::subscribeRequestForInput)
				.flatMap(liiklusStub::subscribe)
				.log("A")
				.filter(EntryPoint::isAssignment)
				.log("B")
				.map(SubscribeReply::getAssignment)
				.log("C")
				.map(EntryPoint::receiveRequestForAssignment)
				.log("D")
				.flatMap(liiklusStub::receive)
				.log("E")
				.map(EntryPoint::toRiffMessage)
				.log("F")
				.compose(EntryPoint::riffWindowing)
				.log("G")
				.map(EntryPoint::invoke)
				.log("H")
				.concatMap(f -> f.concatMap(m -> liiklusStub.publish(createPR(m))))
				.log("I")
				.subscribe();

		System.in.read();


	}

	private static Flux<Message> invoke(Flux<Message> in) {
		ByteBuf metadata = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "application/json");
		return riffStub.invoke(in, metadata);
	}

	private static PublishRequest createPR(Message m) {
		return PublishRequest.newBuilder()
				.setValue(m.getPayload())
				.setTopic(output)
				.build();
	}

	private static ReceiveRequest receiveRequestForAssignment(Assignment assignment) {
		return ReceiveRequest.newBuilder().setAssignment(assignment).build();
	}

	private static <T> Flux<Flux<T>> riffWindowing(Flux<T> linear) {
		return linear.window(30);
	}

	private static Message toRiffMessage(ReceiveReply receiveReply) {
		// TODO: somehow propagate which input (index) this came from
		return Message.newBuilder()
				.setPayload(receiveReply.getRecord().getValue())
				.putHeaders("Content-Type", "text/plain")
				.build();
	}

	private static boolean isAssignment(SubscribeReply reply) {
		return reply.getReplyCase() == SubscribeReply.ReplyCase.ASSIGNMENT;
	}

	private static SubscribeRequest subscribeRequestForInput(String i) {
		return SubscribeRequest.newBuilder()
				.setTopic(i)
				.setGroup(group)
				.setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.LATEST)
				.build();
	}

}
