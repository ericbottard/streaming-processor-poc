package io.projectriff.processor;

import com.github.bsideup.liiklus.protocol.Assignment;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.projectriff.invoker.server.Next;
import io.projectriff.invoker.server.ReactorRiffGrpc;
import io.projectriff.invoker.server.Signal;
import io.projectriff.invoker.server.Start;
import io.projectriff.processor.serialization.Message;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;

import java.net.ConnectException;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Processor {

    public static final int NUM_RETRIES = 20;

    private final Map<String, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub> inputLiiklusInstancesPerAddress;

    private final List<FullyQualifiedTopic> inputs;

    private final Map<String, ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub> outputLiiklusInstancesPerAddress;

    private final List<FullyQualifiedTopic> outputs;

    private final String group;

    private final ReactorRiffGrpc.ReactorRiffStub riffStub;

    public static void main(String[] args) throws Exception {
        var inputAddressableTopics = FullyQualifiedTopic.parseMultiple(System.getenv("INPUTS"));
        var outputAdressableTopics = FullyQualifiedTopic.parseMultiple(System.getenv("OUTPUTS"));

        Hooks.onOperatorDebug();


        URI uri = new URI("http://" + System.getenv("FUNCTION"));
        for (int i = 1; i <= NUM_RETRIES; i++) {
            try (Socket s = new Socket(uri.getHost(), uri.getPort())) {
            } catch (ConnectException t) {
                if (i == NUM_RETRIES) {
                    throw t;
                }
                Thread.sleep(i * 100);
            }
        }


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
                            .filter(SubscribeReply::hasAssignment)
                            .map(SubscribeReply::getAssignment)
                            .map(Processor::receiveRequestForAssignment)
                            .flatMap(inputLiiklus::receive)
                            .map(receiveReply -> toRiffSignal(receiveReply, fullyQualifiedTopic));
                })
                .compose(this::riffWindowing)
                .map(this::invoke)
                .concatMap(f ->
                        f.concatMap(m -> {
                            var next = m.getNext();
                            var output = outputs.get(Integer.parseInt(next.getHeadersOrThrow("RiffOutput")));
                            var outputLiiklus = outputLiiklusInstancesPerAddress.get(output.getGatewayAddress());
                            return outputLiiklus.publish(createPublishRequest(next, output.getTopic()));
                        })
                )
                .blockLast();
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

    /**
     * This converts an RPC representation of a {@link Signal} to an at-rest {@link Message}, and creates a publish request for it.
     */
    private PublishRequest createPublishRequest(Next next, String topic) {
        Message msg = Message.newBuilder()
                .setPayload(next.getPayload())
                .setContentType(next.getHeadersOrThrow("Content-Type")) // TODO will come from a field
                .putAllHeaders(next.getHeadersMap()) // TODO currently contains CT and index, but won't and that's ok
                .build();

        return PublishRequest.newBuilder()
                .setValue(msg.toByteString())
                .setTopic(topic)
                .build();
    }

    private static ReceiveRequest receiveRequestForAssignment(Assignment assignment) {
        return ReceiveRequest.newBuilder().setAssignment(assignment).build();
    }

    private <T> Flux<Flux<T>> riffWindowing(Flux<T> linear) {
        return linear.window(Duration.ofSeconds(60));
    }

    /**
     * This converts a liiklus received message (representing an at-rest riff {@link Message}) into an RPC {@link Signal}.
     */
    private Signal toRiffSignal(ReceiveReply receiveReply, FullyQualifiedTopic fullyQualifiedTopic) {
        var inputIndex = inputs.indexOf(fullyQualifiedTopic);
        if (inputIndex == -1) {
            throw new RuntimeException("Unknown topic: " + fullyQualifiedTopic);
        }
        ByteString bytes = receiveReply.getRecord().getValue();
        try {
            Message message = Message.parseFrom(bytes);
            return Signal.newBuilder()
                    .setNext(
                            Next.newBuilder()
                                    .setPayload(message.getPayload())
                                    .putHeaders("Content-Type", message.getContentType()) // TODO change to field
                                    .putHeaders("RiffInput", String.valueOf(inputIndex))) // TODO change to explicit field
                    .build();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

    }

    private SubscribeRequest subscribeRequestForInput(String topic) {
        return SubscribeRequest.newBuilder()
                .setTopic(topic)
                .setGroup(group)
                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.LATEST)
                .build();
    }

}
