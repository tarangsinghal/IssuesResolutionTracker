package dev.tarang.springboot.kafka.library.events.consumer.consumer.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.tarang.springboot.kafka.library.events.consumer.consumer.LibraryEventsConsumer;
import dev.tarang.springboot.kafka.library.events.consumer.entity.FailureRecord;
import dev.tarang.springboot.kafka.library.events.consumer.entity.LibraryEvent;
import dev.tarang.springboot.kafka.library.events.consumer.repository.FailureRecordRepository;
import dev.tarang.springboot.kafka.library.events.consumer.repository.LibraryEventRepository;
import dev.tarang.springboot.kafka.library.events.consumer.service.LibraryEventsFailureService;
import dev.tarang.springboot.kafka.library.events.consumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.verification.VerificationMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.sql.SQLOutput;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.byLessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events","library-events.RETRY","library-events.DLT"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "retryListener.startup=false"
})
@DirtiesContext
@ExtendWith(MockitoExtension.class)
class LibraryEventsConsumerEmbeddedKafkaIT {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @SpyBean
    LibraryEventsFailureService failureServiceSpy;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    @Autowired
    FailureRecordRepository failureRecordRepository;

    @Autowired
    ObjectMapper objectMapper;

    private Consumer<Integer,String> consumer;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;


    // making sure kafka producer is up and running before consuming messages
    @BeforeEach
    void setUp(){

        // If you do not want to wait till all listeners are up. Instead to wait till you load the group you are working with
        var container = endpointRegistry.getListenerContainers()
                .stream()
                .filter(messageListenerContainer ->
                        Objects.equals(messageListenerContainer.getGroupId(),"library-events-listener-group"))
                .collect(Collectors.toList())
                .get(0);
        ContainerTestUtils.waitForAssignment(container,embeddedKafkaBroker.getPartitionsPerTopic());

        // If you want to wait for all listeners to  be up
//        for(MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()){
//            ContainerTestUtils.waitForAssignment(messageListenerContainer,embeddedKafkaBroker.getPartitionsPerTopic());
//        }



    }

    @AfterEach
    void tearDown(){
        libraryEventRepository.deleteAll();
        failureRecordRepository.deleteAll();
        libraryEventRepository.flush();
        failureRecordRepository.flush();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {

        // given
        String newLibraryEventJson = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "NEW",
                    "book": {
                        "bookId": 280,
                        "bookName": "Learn Kafka Using Spring Boot",
                        "bookAuthor": "Tarang Singhal"
                    }
                }
                """;
//        kafkaTemplate.sendDefault(newLibraryEventJson);// aync
        kafkaTemplate.sendDefault(newLibraryEventJson).get();


        // when

        // Make the current thread to wait for x seconds. Generally used in case of async cases
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        // then
        verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = libraryEventRepository.findAll();
        assertThat(libraryEventList).hasSize(1);

        assertThat(libraryEventList.get(0).getLibraryEventId()).isNotNull();
        assertThat(libraryEventList.get(0).getBook().getBookId()).isEqualTo(280);


    }


    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, InterruptedException {
        //given
        String newLibraryEventJson = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "NEW",
                    "book": {
                        "bookId": 285,
                        "bookName": "Learn Kafka Using Spring Boot",
                        "bookAuthor": "Tarang Singhal"
                    }
                }
                """;
        LibraryEvent newLibraryEvent = objectMapper.readValue(newLibraryEventJson, LibraryEvent.class);
        newLibraryEvent.getBook().setLibraryEvent(newLibraryEvent);
        LibraryEvent libraryEventfromDB = libraryEventRepository.save(newLibraryEvent);

        // publish the event
        String updatedLibraryEventJson = """
                {
                    "libraryEventId": 432,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 285,
                        "bookName": "Learn Kafka Using Spring Boot in 10 hours",
                        "bookAuthor": "Tarang"
                    }
                }
                """;
        kafkaTemplate.sendDefault(updatedLibraryEventJson);


        // when

        // Make the current thread to wait for x seconds. Generally used in case of async cases
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        // then
        verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = libraryEventRepository.findAll();

        assertThat(libraryEventList).isNotEmpty();
        assertThat(libraryEventList).hasSize(2);
        assertThat(libraryEventList.get(1).getLibraryEventId()).isNotNull();
        assertThat(libraryEventList.get(1).getBook().getBookId()).isEqualTo(285);
        assertThat(libraryEventList.get(1).getBook().getBookName()).isEqualTo("Learn Kafka Using Spring Boot in 10 hours");

    }

    @Test
    void publishUpdateLibraryEvent_Fail_InvalidLibraryEvent() throws JsonProcessingException, InterruptedException {
        //given
        String libraryEventJson = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 285,
                        "bookName": "Learn Kafka Using Spring Boot",
                        "bookAuthor": "Tarang Singhal"
                    }
                }
                """;

        kafkaTemplate.sendDefault(libraryEventJson);


        // when

        // Make the current thread to wait for x seconds. Generally used in case of async cases
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);


        // then

        // In case of error, message will be re-tried by 10 times. So, below check for 1 time, will fail
        // In kafka config class, we have override max re-tries to 2. So , total executions will be 3
        // We have also disabled re-tries for IllegalArgumentException

        verify(libraryEventsConsumerSpy,atLeast(1)).onMessage(isA(ConsumerRecord.class));

//        verify(libraryEventsServiceSpy,atLeast(10)).processLibraryEvent(isA(ConsumerRecord.class));



    }

    // We have commented code in LibraryEventsConsumerConfig to publish messages to Retry topic
    @Disabled
    @Test
    void publishUpdateLibraryEvent_Fail_999_RetryTopic_Test() throws JsonProcessingException, InterruptedException {
        //given
        String libraryEventJson = """
                {
                    "libraryEventId": 999,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 285,
                        "bookName": "Learn Kafka Using Spring Boot",
                        "bookAuthor": "Tarang Singhal"
                    }
                }
                """;

        kafkaTemplate.sendDefault(libraryEventJson);


        // when

        // Make the current thread to wait for x seconds. Generally used in case of async cases
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);


        // then

        // In case of error, message will be re-tried by 10 times. So, below check for 1 time, will fail
        // In kafka config class, we have override max re-tries to 2. So , total executions will be 3
        // We have also disabled re-tries for IllegalArgumentException
        // We have added Retry listener and DeadLetter

        verify(libraryEventsConsumerSpy,atLeast(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy,atLeast(3)).processLibraryEvent(isA(ConsumerRecord.class));


        LibraryEvent libraryEvent = objectMapper.readValue(libraryEventJson,LibraryEvent.class);

        consumeAndValidateMessageFromEmbeddedTopic(libraryEvent,retryTopic,"consumer-retry-group-1");



    }

    // We have commented code in LibraryEventsConsumerConfig to publish messages to Dead Letter
    @Disabled
    @Test
    void publishUpdateLibraryEvent_Fail_NullLibraryEventId_DeadLetterTopic() throws JsonProcessingException, InterruptedException {
        //given
        String libraryEventJson = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 285,
                        "bookName": "Learn Kafka Using Spring Boot",
                        "bookAuthor": "Tarang Singhal"
                    }
                }
                """;

        kafkaTemplate.sendDefault(libraryEventJson);


        // when

        // Make the current thread to wait for x seconds. Generally used in case of async cases
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);


        // then

        // In case of error, message will be re-tried by 10 times. So, below check for 1 time, will fail
        // In kafka config class, we have override max re-tries to 2. So , total executions will be 3
        // We have also disabled re-tries for IllegalArgumentException

        verify(libraryEventsConsumerSpy,atLeast(1)).onMessage(isA(ConsumerRecord.class));

//        verify(libraryEventsServiceSpy,atLeast(10)).processLibraryEvent(isA(ConsumerRecord.class));

        LibraryEvent libraryEvent = objectMapper.readValue(libraryEventJson,LibraryEvent.class);

        consumeAndValidateMessageFromEmbeddedTopic(libraryEvent,deadLetterTopic,"consumer-dead-letter-group-1");

    }

    @Test
    void publishUpdateLibraryEvent_Fail_NullLibraryEventId_NonRecoverableDBPersistence() throws JsonProcessingException, InterruptedException {
        //given
        String libraryEventJson = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 285,
                        "bookName": "Learn Kafka Using Spring Boot",
                        "bookAuthor": "Tarang Singhal"
                    }
                }
                """;

        kafkaTemplate.sendDefault(libraryEventJson);


        // when

        // Make the current thread to wait for x seconds. Generally used in case of async cases
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);


        // then


        verify(libraryEventsConsumerSpy,atLeast(1)).onMessage(isA(ConsumerRecord.class));

//        verify(libraryEventsServiceSpy,atLeast(10)).processLibraryEvent(isA(ConsumerRecord.class));

        LibraryEvent libraryEvent = objectMapper.readValue(libraryEventJson,LibraryEvent.class);

        // Check if the record persisted in DB
        List<FailureRecord> failureRecordsList = failureRecordRepository.findAll();



        assertThat(failureRecordsList).hasSize(1);

        System.out.println("Failure Record: " + failureRecordsList.get(0));

        assertThat(failureRecordsList.get(0).getErrorRecord()).isEqualTo(libraryEventJson);


    }

    @Test
    void publishUpdateLibraryEvent_Fail_NullLibraryEventId_RecoverableDBPersistence() throws JsonProcessingException, InterruptedException {
        //given
        String libraryEventJson = """
                {
                    "libraryEventId": 999,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 285,
                        "bookName": "Learn Kafka Using Spring Boot",
                        "bookAuthor": "Tarang Singhal"
                    }
                }
                """;

        kafkaTemplate.sendDefault(libraryEventJson);


        // when

        // Make the current thread to wait for x seconds. Generally used in case of async cases
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);


        // then


        verify(libraryEventsConsumerSpy,atLeast(1)).onMessage(isA(ConsumerRecord.class));

        verify(libraryEventsServiceSpy,atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));

        verify(failureServiceSpy,times(1)).saveFailedRecord(any(ConsumerRecord.class),any(),any());

        LibraryEvent libraryEvent = objectMapper.readValue(libraryEventJson,LibraryEvent.class);

        // Check if the record persisted in DB
        List<FailureRecord> failureRecordsList = failureRecordRepository.findAll();



        assertThat(failureRecordsList).hasSize(1);

        System.out.println("Failure Record: " + failureRecordsList.get(0));

        // Message displayed when assertion fails
        assertEquals(failureRecordsList.get(0).getErrorRecord(),libraryEventJson,"Published message doesn't match message retrieved from DB");


    }





    void setUpConsumer(String groupId, String topic){
        Map<String,Object> configs = new HashMap<>(KafkaTestUtils.consumerProps(groupId,"true",embeddedKafkaBroker));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        consumer = new DefaultKafkaConsumerFactory<>(configs,new IntegerDeserializer(), new StringDeserializer())
                .createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer,topic);

    }

    private void consumeAndValidateMessageFromEmbeddedTopic(LibraryEvent libraryEvent, String topic, String groupId) throws JsonProcessingException {
        setUpConsumer(groupId,topic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer,topic);

        System.out.println("Received libraryEvent from Topic: " + consumerRecord.value());
        LibraryEvent libraryEventFromTopic = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        assertEquals(libraryEvent,libraryEventFromTopic);


    }

}
