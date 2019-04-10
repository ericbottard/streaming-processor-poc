package io.projectriff.processor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.github.bsideup.liiklus.protocol.Assignment;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.projectriff.invoker.server.Next;
import io.projectriff.invoker.server.ReactorRiffGrpc;
import io.projectriff.invoker.server.Signal;
import io.projectriff.invoker.server.Start;
import reactor.core.publisher.Flux;

public class Processor {

	private final List<String> inputs;

	private String group;

	private final List<String> outputs;

	private ReactorRiffGrpc.ReactorRiffStub riffStub;

	private ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub liiklus;

	/*
	 * SHORTCOMINGS: Liiklus doesn't support Kafka headers
	 */

	/*
	 * Nest steps?
	 * 
	 * P1 Use cases that actually use multiple inputs/outputs (see KStreams) P1 Multiple GWs
	 * (and hence brokers) (action) Rewrite in go? (action) Swap protocols? ==> or P1 == P2 ==
	 * gRPC (Java)Invoker actually supporting several inputs/outputs (action) P1 Containerize
	 * 
	 * P1 Update Java builder/invoker to latest riff contract
	 * 
	 * P1 CRDs <=> This code (action)
	 * 
	 * ACKs
	 * 
	 * Think about Processor-based windowing (discussion) Serialization ? Content-based
	 * topics, etc (discussion)
	 */

	public static void main(String[] args) throws Exception {

		var gwAddress = System.getenv("GATEWAY");
		var gwChannel = NettyChannelBuilder.forTarget(gwAddress)
				.directExecutor()
				.usePlaintext()
				.build();

		var function = System.getenv("FUNCTION");
		var fnChannel = NettyChannelBuilder.forTarget(function)
				.directExecutor()
				.usePlaintext()
				.build();

		var group = System.getenv("GROUP");
		var sInputs = System.getenv("INPUTS");
		var sOutputs = System.getenv("OUTPUTS");

		Processor processor = new Processor(gwChannel, fnChannel, group, sInputs, sOutputs);
		processor.run();

	}

	public Processor(Channel gwChannel, Channel fnChannel, String group, String sInputs, String sOutputs) {
		this.liiklus = ReactorLiiklusServiceGrpc.newReactorStub(gwChannel);
		this.riffStub = ReactorRiffGrpc.newReactorStub(fnChannel);
		this.group = group;
		this.inputs = Arrays.asList(sInputs.split(","));
		this.outputs = Arrays.asList(sOutputs.split(","));
		;

	}

	public void run() throws IOException, InterruptedException {

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

		Object lock = new Object();
		synchronized (lock) {
			lock.wait();
		}
	}

	private Flux<Signal> invoke(Flux<Signal> in) {
		var start = Signal.newBuilder()
				.setStart(Start.newBuilder().setAccept("application/json"))
				.build();
		return riffStub.invoke(Flux.concat(
				Flux.just(start), //
				in
		));
	}

	private PublishRequest createPR(Signal m) {
		Next next = m.getNext();
		var output = outputs.get(Integer.parseInt(next.getHeadersOrThrow("RiffOutput")));

		return PublishRequest.newBuilder()
				.setValue(next.getPayload())
				.setTopic(output)
				.build();
	}

	private ReceiveRequest receiveRequestForAssignment(Assignment assignment) {
		return ReceiveRequest.newBuilder().setAssignment(assignment).build();
	}

	private <T> Flux<Flux<T>> riffWindowing(Flux<T> linear) {
		return linear.window(30);
	}

	private Signal toRiffMessage(ReceiveReply receiveReply, SubscribeRequest subscribeRequest) {
		var inputIndex = inputs.indexOf(subscribeRequest.getTopic());

		return Signal.newBuilder()
				.setNext(
						Next.newBuilder()
								.setPayload(receiveReply.getRecord().getValue())
								.putHeaders("Content-Type", "text/plain")
								.putHeaders("RiffInput", String.valueOf(inputIndex)))
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
