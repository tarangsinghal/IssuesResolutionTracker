package dev.tarang.springboot.kafka.library.events.producer.controller.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.tarang.springboot.kafka.library.events.producer.domain.Book;
import dev.tarang.springboot.kafka.library.events.producer.domain.LibraryEvent;
import dev.tarang.springboot.kafka.library.events.producer.domain.LibraryEventType;
import dev.tarang.springboot.kafka.library.events.producer.util.TestData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps;
import static org.springframework.kafka.test.utils.KafkaTestUtils.getRecords;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"
})
@DirtiesContext
@ActiveProfiles("local")
@Slf4j
class LibraryEventsControllerIntegrationEmbeddedKafkaIT {

    @Value("${spring.embedded.kafka.brokers}")
    String kafkaBrokers;

    @Autowired
    TestRestTemplate testRestTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @Autowired
    ObjectMapper objectMapper;



    @BeforeEach
    void setUp(){
        Map<String,Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("consumer-group-1","true",embeddedKafkaBroker));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        consumer = new DefaultKafkaConsumerFactory<>(configs,new IntegerDeserializer(), new StringDeserializer())
                .createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown(){
        consumer.close();
    }


    @Test
    void postLibraryEvent_approach1() {

        System.out.println("Embedded kafka brokers: " + kafkaBrokers);

        //given
        HttpEntity<Book> httpEntity = getBookRequesHttpEntity(TestData.bookRecord());

        // when
        ResponseEntity<LibraryEvent> response = testRestTemplate.exchange(
                "/library/v1/inventory-books-1",
                HttpMethod.POST,
                httpEntity,
                LibraryEvent.class
                );

        System.out.println("Response: " + response.getBody());

        // then
        assertEquals(HttpStatus.CREATED,response.getStatusCode());

        consumeMessageFromEmbeddedTopic(TestData.bookRecord());



    }


    @Test
    void postLibraryEvent_approach2() {

        //given
        HttpEntity<Book> httpEntity = getBookRequesHttpEntity(TestData.bookRecordOneMore());

        // when
        ResponseEntity<LibraryEvent> response = testRestTemplate.exchange(
                "/library/v1/inventory-books-2",
                HttpMethod.POST,
                httpEntity,
                LibraryEvent.class
        );

        System.out.println("Response: " + response.getBody());

        // then
        assertEquals(HttpStatus.CREATED,response.getStatusCode());

        consumeMessageFromEmbeddedTopic(TestData.bookRecordOneMore());



    }


    @Test
    void postLibraryEvent_approach3() {

        //given
        HttpEntity<Book> httpEntity = getBookRequesHttpEntity(TestData.bookRecord());
        System.out.println("Book pushed to Topic: " + TestData.bookRecord());

        // when
        ResponseEntity<LibraryEvent> response = testRestTemplate.exchange(
                "/library/v1/inventory-books-3",
                HttpMethod.POST,
                httpEntity,
                LibraryEvent.class
        );

        System.out.println("Response: " + response.getBody());

        // then
        assertEquals(HttpStatus.CREATED,response.getStatusCode());

        consumeMessageFromEmbeddedTopic(TestData.bookRecord());



    }


    private HttpEntity<Book> getBookRequesHttpEntity(Book book){
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
        System.out.println("Request payload: " + book);
        return new HttpEntity<Book>(book,httpHeaders);
    }


    private void consumeMessageFromEmbeddedTopic(Book book){
        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);
        System.out.println("Message count from Kafka topic: " + consumerRecords.count());
        assert consumerRecords.count() == 1;

        consumerRecords.forEach(record -> {
            LibraryEvent libraryEventFromTopic = TestData.parseLibraryEventRecord(objectMapper,record.value());
            System.out.println("Received book from Topic: " + libraryEventFromTopic.book());
            assertEquals(book,libraryEventFromTopic.book());
        });



    }





}
