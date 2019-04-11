package io.projectriff.processor;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FullyQualifiedTopic {

    private final String gatewayAddress;
    private final String topic;

    public static List<FullyQualifiedTopic> parseMultiple(String configurations) {
        return Arrays.stream(configurations.split(","))
                .map(FullyQualifiedTopic::parse)
                .collect(Collectors.toList());
    }

    private static FullyQualifiedTopic parse(String configuration) {
        int slashIndex = configuration.indexOf('/');
        return new FullyQualifiedTopic(configuration.substring(0, slashIndex), configuration.substring(1 + slashIndex));
    }

    public FullyQualifiedTopic(String gatewayAddress, String topic) {
        this.gatewayAddress = gatewayAddress;
        this.topic = topic;
    }

    public String getGatewayAddress() {
        return gatewayAddress;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FullyQualifiedTopic that = (FullyQualifiedTopic) o;
        return Objects.equals(gatewayAddress, that.gatewayAddress) &&
                Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gatewayAddress, topic);
    }

    @Override
    public String toString() {
        return "AddressableTopic{" +
                "gatewayAddress='" + gatewayAddress + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }
}
