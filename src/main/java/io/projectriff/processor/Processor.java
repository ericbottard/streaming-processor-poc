package io.projectriff.processor;

import com.github.bsideup.liiklus.protocol.Assignment;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.google.protobuf.ByteString;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.projectriff.invoker.server.Next;
import io.projectriff.invoker.server.ReactorRiffGrpc;
import io.projectriff.invoker.server.Signal;
import io.projectriff.invoker.server.Start;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Processor {

	private final Map<String, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub> inputLiiklusInstancesPerAddress;

	private final List<FullyQualifiedTopic> inputs;

	private final Map<String, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub> outputLiiklusInstancesPerAddress;

	private final List<FullyQualifiedTopic> outputs;

	private final String group;

	private final ReactorRiffGrpc.ReactorRiffStub riffStub;

	/*
	 * SHORTCOMINGS: Liiklus doesn't support Kafka headers
	 */

	/*
	 * Nest steps?
	 *
	 * P1 Use cases that actually use multiple inputs/outputs (see KStreams)
	 *
	 * Rewrite in go? (action)
	 *
	 * Invoker actually supporting several inputs/outputs (action)
	 *
	 * ACKs
	 *
	 * Think about Processor-based windowing (discussion) Serialization ? Content-based
	 * topics, etc (discussion)
	 */

	public static void main(String[] args) throws Exception {
		var inputAddressableTopics = FullyQualifiedTopic.parseMultiple(System.getenv("INPUTS"));
		var outputAdressableTopics = FullyQualifiedTopic.parseMultiple(System.getenv("OUTPUTS"));
		Thread.sleep(5000);
		var fnChannel = NettyChannelBuilder.forTarget(System.getenv("FUNCTION"))
				.usePlaintext()
				.build();

		Processor processor = new Processor(
				inputAddressableTopics,
				outputAdressableTopics,
				System.getenv("GROUP"),
				ReactorRiffGrpc.newReactorStub(fnChannel));
		processor.run();

	}

	public Processor(List<FullyQualifiedTopic> inputs, List<FullyQualifiedTopic> outputs, String group,
			ReactorRiffGrpc.ReactorRiffStub riffStub) {
		this.inputs = inputs;
		this.outputs = outputs;
		this.inputLiiklusInstancesPerAddress = indexByAddress(inputs);
		this.outputLiiklusInstancesPerAddress = indexByAddress(outputs);
		this.riffStub = riffStub;
		this.group = group;
	}

	public void run() throws InterruptedException {
		Flux.fromIterable(inputs)
				.flatMap(fullyQualifiedTopic -> {
					var inputLiiklus = inputLiiklusInstancesPerAddress.get(fullyQualifiedTopic.getGatewayAddress());
					return inputLiiklus.subscribe(subscribeRequestForInput(fullyQualifiedTopic.getTopic()))
							.filter(this::isAssignment)
							.map(SubscribeReply::getAssignment)
							.map(this::receiveRequestForAssignment)
							.flatMap(inputLiiklus::receive)
							.map(receiveReply -> toRiffMessage(receiveReply, fullyQualifiedTopic));
				})
				.compose(this::riffWindowing)
				.map(this::invoke)
				.concatMap(f -> f.concatMap(m -> {
					var next = m.getNext();
					var output = outputs.get(Integer.parseInt(next.getHeadersOrThrow("RiffOutput")));
					var outputLiiklus = outputLiiklusInstancesPerAddress.get(output.getGatewayAddress());
					return outputLiiklus.publish(createPublishRequest(next.getPayload(), output.getTopic()));
				}))
				.subscribe();

		Object lock = new Object();
		synchronized (lock) {
			lock.wait();
		}
	}

	@NotNull
	private static Map<String, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub> indexByAddress(
			List<FullyQualifiedTopic> fullyQualifiedTopics) {
		return fullyQualifiedTopics.stream()
				.map(FullyQualifiedTopic::getGatewayAddress)
				.distinct()
				.map(address -> Map.entry(
						address,
						NettyChannelBuilder.forTarget(address)
								.usePlaintext()
								.build()))
				.map(channelEntry -> Map.entry(
						channelEntry.getKey(),
						ReactorLiiklusServiceGrpc.newReactorStub(channelEntry.getValue())))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	private Flux<Signal> invoke(Flux<Signal> in) {
		var start = Signal.newBuilder()
				.setStart(Start.newBuilder().setAccept("application/json"))
				.build();
		return riffStub.invoke(Flux.concat(
				Flux.just(start), //
				in));
	}

	private PublishRequest createPublishRequest(ByteString payload, String topic) {
		return PublishRequest.newBuilder()
				.setValue(payload)
				.setTopic(topic)
				.build();
	}

	private ReceiveRequest receiveRequestForAssignment(Assignment assignment) {
		return ReceiveRequest.newBuilder().setAssignment(assignment).build();
	}

	private <T> Flux<Flux<T>> riffWindowing(Flux<T> linear) {
		return linear.window(Duration.ofSeconds(60));
	}

	private Signal toRiffMessage(ReceiveReply receiveReply, FullyQualifiedTopic fullyQualifiedTopic) {
		var inputIndex = inputs.indexOf(fullyQualifiedTopic);

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

	private SubscribeRequest subscribeRequestForInput(String topic) {
		return SubscribeRequest.newBuilder()
				.setTopic(topic)
				.setGroup(group)
				.setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
				.build();
	}

}
