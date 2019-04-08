package io.projectriff.processor;

import java.io.IOException;
import java.net.URI;

import com.github.bsideup.liiklus.protocol.Assignment;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
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

public class Processor {

	private final Flux<String> input;

	private String group;

	private String output;

	private RiffClient riffStub;

	private ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub liiklus;

	public static void main(String[] args) throws IOException {

		var gwAddress = System.getenv("GATEWAY");
		gwAddress = "localhost:6565";
		gwAddress = "35.204.31.102:6565";
		var channel = NettyChannelBuilder.forTarget(gwAddress)
				.directExecutor()
				.usePlaintext()
				.build();

		WebsocketClientTransport websocketClientTransport = WebsocketClientTransport
				.create(URI.create("ws://kmprssr2.default.35.241.251.246.nip.io/ws"));
		RSocket rSocket = RSocketFactory
				.connect()
				.transport(websocketClientTransport)
				.start()
				.block();

		var group = System.getenv("GROUP");
		group = "processor0";

		var sInputs = System.getenv("INPUTS");
		var sOutputs = System.getenv("OUTPUTS");
		sInputs = "numbers";
		sOutputs = "squares";

		var inputs = Flux.fromArray(sInputs.split(","));
		var outputs = Flux.fromArray(sOutputs.split(","));

		Processor processor = new Processor(channel, rSocket, group, sInputs, sOutputs);
		processor.run();

	}

	public Processor(Channel channel, RSocket rSocket, String group, String sInputs, String sOutputs) {
		this.liiklus = ReactorLiiklusServiceGrpc.newReactorStub(channel);
		this.riffStub = new RiffClient(rSocket);
		this.group = group;
		this.input = Flux.just(sInputs);
		this.output = sOutputs;

	}

	public void run() throws IOException {


		/*
		 * inputs.map(Processor::subscribeRequestForInput) .flatMap(liiklusStub::subscribe)
		 * .filter(Processor::isAssignment) .map(SubscribeReply::getAssignment)
		 * .map(Processor::receiveRequestForAssignment) .flatMap(liiklusStub::receive)
		 * .map(Processor::toRiffMessage) .compose(Processor::riffWindowing) .doOnEach(f -> )
		 * 
		 * ;
		 */

		input.map(this::subscribeRequestForInput)
				.flatMap(liiklus::subscribe)
				.log("A")
				.filter(this::isAssignment)
				.log("B")
				.map(SubscribeReply::getAssignment)
				.log("C")
				.map(this::receiveRequestForAssignment)
				.log("D")
				.flatMap(liiklus::receive)
				.log("E")
				.map(this::toRiffMessage)
				.log("F")
				.compose(this::riffWindowing)
				.log("G")
				.map(this::invoke)
				.log("H")
				.concatMap(f -> f.concatMap(m -> liiklus.publish(createPR(m))))
				.log("I")
				.subscribe();

		System.in.read();

	}

	private  Flux<Message> invoke(Flux<Message> in) {
		ByteBuf metadata = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "application/json");
		return riffStub.invoke(in, metadata);
	}

	private  PublishRequest createPR(Message m) {
		return PublishRequest.newBuilder()
				.setValue(m.getPayload())
				.setTopic(output)
				.build();
	}

	private  ReceiveRequest receiveRequestForAssignment(Assignment assignment) {
		return ReceiveRequest.newBuilder().setAssignment(assignment).build();
	}

	private  <T> Flux<Flux<T>> riffWindowing(Flux<T> linear) {
		return linear.window(30);
	}

	private  Message toRiffMessage(ReceiveReply receiveReply) {
		// TODO: somehow propagate which input (index) this came from
		return Message.newBuilder()
				.setPayload(receiveReply.getRecord().getValue())
				.putHeaders("Content-Type", "text/plain")
				.build();
	}

	private  boolean isAssignment(SubscribeReply reply) {
		return reply.getReplyCase() == SubscribeReply.ReplyCase.ASSIGNMENT;
	}

	private  SubscribeRequest subscribeRequestForInput(String i) {
		return SubscribeRequest.newBuilder()
				.setTopic(i)
				.setGroup(group)
				.setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.LATEST)
				.build();
	}

}
