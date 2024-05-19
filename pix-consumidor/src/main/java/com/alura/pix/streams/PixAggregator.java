package com.alura.pix.streams;

import com.alura.pix.serdes.PixSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Service;

@Service
@EnableKafkaStreams
public class PixAggregator {

    @Autowired
    public final void aggregator(StreamsBuilder streamBuilder) {

        KTable<String, Double> messageStream = streamBuilder
                .stream("pix-topic", Consumed.with(Serdes.String(), PixSerdes.serdes()))
                .peek((key, value) -> System.out.println("Pix recebido de: " + value.getChaveOrigem()))
                .groupBy((key, value) -> value.getChaveOrigem())
                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> (aggregate + value.getValor()),
                        Materialized.with(Serdes.String(), Serdes.Double())
                );

        messageStream.toStream().print(Printed.toSysOut());
        messageStream.toStream().to("pix-topic-agregacao", Produced.with(Serdes.String(), Serdes.Double()));

    }
}
