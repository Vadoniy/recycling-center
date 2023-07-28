package ru.otus.recyclingcenter.consumer;

import avro.schema.SmartPhoneAvro;
import avro.schema.detail.BatteryAvro;
import avro.schema.detail.MotherBoardAvro;
import avro.schema.detail.ScreenAvro;
import avro.schema.enums.ManufacturerAvro;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import ru.otus.recyclingcenter.service.DisposePhoneService;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class RecyclePointService {

    private static final String MB_BROKEN_POSTFIX = "-mother-board-change-topic";
    private static final String SCREEN_BROKEN_POSTFIX = "-screen-change-topic";
    private static final String BATTERY_BROKEN_POSTFIX = "-battery-change-topic";
    private static final String REFURBISHED_POSTFIX = "-refurbished-topic";
    //Disposal topics
    private static final String OLD_MOTHER_BOARDS = "old-mother-board-topic";
    private static final String OLD_SCREENS = "old-screens-topic";
    private static final String OLD_BATTERIES = "old-batteries-topic";
    private final DisposePhoneService disposePhoneService;

    @Bean
    public KStream<UUID, SmartPhoneAvro> fixMbKStream(StreamsBuilder kStreamBuilder) {
        return createKStreamByProblem(kStreamBuilder, FailureType.MOTHER_BOARD);
    }

    @Bean
    public KStream<UUID, SmartPhoneAvro> fixScreenKStream(StreamsBuilder kStreamBuilder) {
        return createKStreamByProblem(kStreamBuilder, FailureType.SCREEN);
    }

    @Bean
    public KStream<UUID, SmartPhoneAvro> fixBatteryKStream(StreamsBuilder kStreamBuilder) {
        return createKStreamByProblem(kStreamBuilder, FailureType.BATTERY);
    }

    @Bean
    public KStream<UUID, SmartPhoneAvro> extractOldBatteryKStream(StreamsBuilder kStreamBuilder) {
        final var consumerSerde = new SpecificAvroSerde<SmartPhoneAvro>();
        final var serdeConfig = Map.of(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        consumerSerde.configure(serdeConfig, false);
        final var stream = kStreamBuilder.stream("disposal-phones-topic",
                Consumed.with(Serdes.UUID(), consumerSerde));
        stream.mapValues(disposePhoneService::removeBatteryFromPhone).to(OLD_BATTERIES);
        return stream;
    }

    @Bean
    public KStream<UUID, SmartPhoneAvro> extractOldMBKStream(StreamsBuilder kStreamBuilder) {
        final var consumerSerde = new SpecificAvroSerde<SmartPhoneAvro>();
        final var serdeConfig = Map.of(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        consumerSerde.configure(serdeConfig, false);
        final var stream = kStreamBuilder.stream("disposal-phones-topic",
                Consumed.with(Serdes.UUID(), consumerSerde));
        stream.mapValues(disposePhoneService::removeMotherBoardFromPhone).to(OLD_MOTHER_BOARDS);
        return stream;
    }

    @Bean
    public KStream<UUID, SmartPhoneAvro> extractOldScreenKStream(StreamsBuilder kStreamBuilder) {
        final var consumerSerde = new SpecificAvroSerde<SmartPhoneAvro>();
        final var serdeConfig = Map.of(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        consumerSerde.configure(serdeConfig, false);
        final var stream = kStreamBuilder.stream("disposal-phones-topic",
                Consumed.with(Serdes.UUID(), consumerSerde));
        stream.mapValues(disposePhoneService::removeScreenFromPhone).to(OLD_SCREENS);
        return stream;
    }

    private KStream<UUID, SmartPhoneAvro> createKStreamByProblem(StreamsBuilder kStreamBuilder, FailureType failureType) {
        final var stream = configureStream(kStreamBuilder, failureType.topicPostfixName);
        final String splitDetailName = String.format("after-%s-split-", failureType.detailName);
        final String oldDetailName = String.format("old-%s", failureType.detailName);
        final String fixedDetailPhoneName = String.format("fixed-%s-phone", failureType.detailName);
        final var splitMbAndPhones = splitDetailFromPhone(stream, failureType.fixFunction, splitDetailName, oldDetailName, fixedDetailPhoneName);
        splitMbAndPhones.get(splitDetailName + oldDetailName)
                .to(failureType.oldDetailTopicName);
        splitFixedPhonesByManufacturers(splitMbAndPhones, splitDetailName, fixedDetailPhoneName, "by-manufacturer-" + failureType.detailName + "-", failureType.outTopicName);
        return stream;
    }

    private KStream<UUID, SmartPhoneAvro> configureStream(StreamsBuilder kStreamBuilder, String topicPostfix) {
        final var consumerSerde = new SpecificAvroSerde<SmartPhoneAvro>();
        final var serdeConfig = Map.of(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        consumerSerde.configure(serdeConfig, false);
        final var topicsToRead = Stream.of(ManufacturerAvro.values())
                .map(manufacturerAvro -> manufacturerAvro.name() + topicPostfix)
                .collect(Collectors.toList());
        return kStreamBuilder.stream(topicsToRead,
                Consumed.with(Serdes.UUID(), consumerSerde));
    }

    private Map<String, KStream<UUID, SpecificRecordBase>> splitDetailFromPhone(KStream<UUID, SmartPhoneAvro> stream,
                                                                                Function<SmartPhoneAvro, SpecificRecordBase> fixFunction,
                                                                                String splitDetailName,
                                                                                String oldDetailName,
                                                                                String fixedDetailPhoneName) {
        return stream.flatMap((ssn, phone) -> {
                    final var result = new ArrayList<KeyValue<UUID, SpecificRecordBase>>();
                    final var oldDetail = fixFunction.apply(phone);
                    result.add(KeyValue.pair(ssn, phone));
                    result.add(KeyValue.pair(ssn, oldDetail));
                    return result;
                }).split(Named.as(splitDetailName))
                .branch((key, value) -> value instanceof SmartPhoneAvro, Branched.as(fixedDetailPhoneName))
                .defaultBranch(Branched.as(oldDetailName));
    }

    private void splitFixedPhonesByManufacturers(Map<String, KStream<UUID, SpecificRecordBase>> phonesToSplit,
                                                 String splitDetailName,
                                                 String fixedDetailPhoneName,
                                                 String splitNameByManufacturer,
                                                 String outTopicName) {
        final var fixedMbPhones = phonesToSplit.get(splitDetailName + fixedDetailPhoneName)
                .mapValues(value -> (SmartPhoneAvro) value)
                .split(Named.as(splitNameByManufacturer))
                .branch((key, value) -> value.getManufacturer() == ManufacturerAvro.APPLE, Branched.as(ManufacturerAvro.APPLE.name()))
                .branch((key, value) -> value.getManufacturer() == ManufacturerAvro.SAMSUNG, Branched.as(ManufacturerAvro.SAMSUNG.name()))
                .branch((key, value) -> value.getManufacturer() == ManufacturerAvro.XIAOMI, Branched.as(ManufacturerAvro.XIAOMI.name()))
                .noDefaultBranch();
        Stream.of(ManufacturerAvro.values())
                .forEach(manufacturerAvro ->
                    fixedMbPhones.get(splitNameByManufacturer + manufacturerAvro.name()).to(manufacturerAvro.name() + outTopicName));
    }

    private enum FailureType {
        MOTHER_BOARD(MB_BROKEN_POSTFIX, "mother-board", OLD_MOTHER_BOARDS, BATTERY_BROKEN_POSTFIX, RecyclePointService::replaceMotherBoard),
        SCREEN(SCREEN_BROKEN_POSTFIX, "screen", OLD_SCREENS, BATTERY_BROKEN_POSTFIX, RecyclePointService::replaceScreen),
        BATTERY(BATTERY_BROKEN_POSTFIX, "battery", OLD_BATTERIES, REFURBISHED_POSTFIX, RecyclePointService::replaceBattery);

        final String topicPostfixName;
        final String detailName;
        final String oldDetailTopicName;
        final String outTopicName;
        final Function<SmartPhoneAvro, SpecificRecordBase> fixFunction;

        FailureType(String topicPostfixName, String detailName, String oldDetailTopicName, String outTopicName, Function<SmartPhoneAvro, SpecificRecordBase> fixFunction) {
            this.topicPostfixName = topicPostfixName;
            this.detailName = detailName;
            this.oldDetailTopicName = oldDetailTopicName;
            this.outTopicName = outTopicName;
            this.fixFunction = fixFunction;
        }
    }

    private static MotherBoardAvro replaceMotherBoard(SmartPhoneAvro smartPhone) {
        final var newMotherBoard = MotherBoardAvro.newBuilder()
                .setBroken(false)
                .setManufacturer(smartPhone.getManufacturer())
                .setModel(smartPhone.getModel())
                .setSsn(UUID.randomUUID().toString())
                .build();
        final var oldMotherBoard = smartPhone.getMotherBoard();
        smartPhone.setMotherBoard(newMotherBoard);
        return oldMotherBoard;
    }

    private static ScreenAvro replaceScreen(SmartPhoneAvro smartPhone) {
        final var newScreen = ScreenAvro.newBuilder()
                .setBroken(false)
                .setManufacturer(smartPhone.getManufacturer())
                .setModel(smartPhone.getModel())
                .setSsn(UUID.randomUUID().toString())
                .build();
        final var oldScreen = smartPhone.getScreen();
        smartPhone.setScreen(newScreen);
        return oldScreen;
    }

    private static BatteryAvro replaceBattery(SmartPhoneAvro smartPhone) {
        final var newBattery = BatteryAvro.newBuilder()
                .setBroken(false)
                .setManufacturer(smartPhone.getManufacturer())
                .setModel(smartPhone.getModel())
                .setSsn(UUID.randomUUID().toString())
                .build();
        final var oldBattery = smartPhone.getBattery();
        smartPhone.setBattery(newBattery);
        return oldBattery;
    }
}
