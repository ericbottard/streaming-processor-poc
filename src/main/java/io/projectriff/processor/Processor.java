package io.projectriff.processor;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.github.bsideup.liiklus.protocol.Assignment;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.projectriff.invoker.server.Message;
import io.projectriff.invoker.server.RiffClient;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import org.reactivestreams.Publisher;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

public class Processor {

	private final List<String> inputs;

	private String group;

	private final List<String> outputs;

	private RiffClient riffStub;

	private ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub liiklus;

	/*
	 * SHORTCOMINGS: Liiklus doesn't support Kafka headers
	 */

	/*
	Nest steps?

P1  Use cases that actually use multiple inputs/outputs (see KStreams)
P1	Multiple GWs (and hence brokers)   (action)
	Rewrite in go?  (action)
	  Swap protocols?  ==>  or P1 == P2 == gRPC
	(Java)Invoker actually supporting several inputs/outputs  (action)
P1  Containerize

P1  Update Java builder/invoker to latest riff contract

P1	CRDs <=> This code  (action)

	ACKs

	Think about Processor-based windowing (discussion)
	Serialization ? Content-based topics, etc   (discussion)
	 */

	public static void main(String[] args) throws IOException {

		var gwAddress = System.getenv("GATEWAY");
		gwAddress = "localhost:6565";
		gwAddress = "35.204.31.102:6565";
		var channel = NettyChannelBuilder.forTarget(gwAddress)
				.directExecutor()
				.usePlaintext()
				.build();

		WebsocketClientTransport websocketClientTransport = WebsocketClientTransport
				.create(URI.create("ws://kmprssr3.default.35.241.251.246.nip.io/ws"));
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

		Processor processor = new Processor(channel, rSocket, group, sInputs, sOutputs);
		processor.run();

	}

	public Processor(Channel channel, RSocket rSocket, String group, String sInputs, String sOutputs) {
		this.liiklus = ReactorLiiklusServiceGrpc.newReactorStub(channel);
		this.riffStub = new RiffClient(rSocket);
		this.group = group;
		this.inputs = Arrays.asList(sInputs.split(","));
		this.outputs = Arrays.asList(sOutputs.split(","));;

	}

	public void run() throws IOException {

		Flux.fromIterable(inputs)
				.map(this::subscribeRequestForInput)
				.flatMap(
						subscribeRequest -> liiklus.subscribe(subscribeRequest)
								.filter(this::isAssignment)
								.map(SubscribeReply::getAssignment)
								.map(this::receiveRequestForAssignment)
								.flatMap(liiklus::receive)
								.map(receiveReply -> toRiffMessage(receiveReply, subscribeRequest))
								.compose(this::riffWindowing)
								.map(this::invoke)
								.concatMap(f -> f.concatMap(m -> liiklus.publish(createPR(m)))))
				.subscribe();

		System.in.read();

	}

	private Flux<Message> invoke(Flux<Message> in) {
		ByteBuf metadata = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "application/json");
		Flux<Message> invoke = riffStub.invoke(in, metadata);
		return invoke.map(m -> Message.newBuilder(m).putHeaders("RiffOutput", "0").build()); // TODO
	}

	private PublishRequest createPR(Message m) {
		var output = outputs.get(Integer.parseInt(m.getHeadersOrDefault("RiffOutput", "0")));

		return PublishRequest.newBuilder()
				.setValue(m.getPayload())
				.setTopic(output)
				.build();
	}

	private ReceiveRequest receiveRequestForAssignment(Assignment assignment) {
		return ReceiveRequest.newBuilder().setAssignment(assignment).build();
	}

	private <T> Flux<Flux<T>> riffWindowing(Flux<T> linear) {
		return linear.window(30);
	}

	private Message toRiffMessage(ReceiveReply receiveReply, SubscribeRequest subscribeRequest) {
		var inputIndex = inputs.indexOf(subscribeRequest.getTopic());

		return Message.newBuilder()
				.setPayload(receiveReply.getRecord().getValue())
				.putHeaders("Content-Type", "text/plain")
				.putHeaders("RiffInput", String.valueOf(inputIndex))
				.build();

	}

	private boolean isAssignment(SubscribeReply reply) {
		return reply.getReplyCase() == SubscribeReply.ReplyCase.ASSIGNMENT;
	}

	private SubscribeRequest subscribeRequestForInput(String i) {
		return SubscribeRequest.newBuilder()
				.setTopic(i)
				.setGroup(group)
				.setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.LATEST)
				.build();
	}

}
