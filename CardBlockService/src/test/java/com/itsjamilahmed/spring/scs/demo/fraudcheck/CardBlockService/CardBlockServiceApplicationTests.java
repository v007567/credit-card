package com.itsjamilahmed.spring.scs.demo.fraudcheck.CardBlockService;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Import(TestChannelBinderConfiguration.class)
class CardBlockServiceApplicationTests {

	@Autowired
	private InputDestination inputDestination;

	@Autowired
	private OutputDestination outputDestination;

	@Autowired
	private CardBlockServiceApplication cardBlockServiceApplication;

	private static final String REPLY_TO_HEADER_KEY = "reply_to_destination";
	private static final String SOL_DESTINATION_KEY = "solace_destination";
	private static final String APP_HEADERS_KEY_PREFIX = "app_";

	@BeforeEach
	void setUp() {
		outputDestination.clear();
	}

	@Test
	void contextLoads() {
		assertNotNull(cardBlockServiceApplication);
	}

	@Test
	void testBlockCardBeanExists() {
		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		assertNotNull(blockCard);
	}

	@Test
	void testBlockCardWithValidPayload() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456");
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/test";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/test")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);
		assertNotNull(outputMessage.getPayload());

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("ok", outputJson.get("status"));
		assertEquals("1234-5678-9012-3456", outputJson.get("cardNumber"));
		assertEquals(true, outputJson.get("cardBlockStatus"));

		assertEquals(replyTopic, outputMessage.getHeaders().get(BinderHeaders.TARGET_DESTINATION));
	}

	@Test
	void testBlockCardWithValidPayloadAndSetCardBlockStatusFalse() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "9999-8888-7777-6666");
		inputJson.put("setCardBlockStatus", false);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/test2";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/test2")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("ok", outputJson.get("status"));
		assertEquals("9999-8888-7777-6666", outputJson.get("cardNumber"));
		assertEquals(true, outputJson.get("cardBlockStatus"));
	}

	@Test
	void testBlockCardWithInvalidJsonPayload() throws ParseException {
		String invalidJson = "this is not valid json";
		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/error";

		Message<String> inputMessage = MessageBuilder.withPayload(invalidJson)
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/error")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("error", outputJson.get("status"));
		assertNotNull(outputJson.get("errorMsg"));
		assertTrue(outputJson.get("errorMsg").toString().contains("Did not receive a valid JSON formatted message"));
	}

	@Test
	void testBlockCardWithMissingCardNumber() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/missing";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/missing")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("error", outputJson.get("status"));
		assertNotNull(outputJson.get("errorMsg"));
		assertTrue(outputJson.get("errorMsg").toString().contains("NullPointerException"));
	}

	@Test
	void testBlockCardWithMissingSetCardBlockStatus() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456");

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/missing2";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/missing2")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("error", outputJson.get("status"));
		assertNotNull(outputJson.get("errorMsg"));
		assertTrue(outputJson.get("errorMsg").toString().contains("NullPointerException"));
	}

	@Test
	void testBlockCardWithEmptyJsonPayload() throws ParseException {
		JSONObject inputJson = new JSONObject();

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/empty";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/empty")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("error", outputJson.get("status"));
		assertNotNull(outputJson.get("errorMsg"));
	}

	@Test
	void testBlockCardCopiesAppHeaders() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456");
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/headers";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/headers")
				.setHeader("app_fraudCheckMediator_replyTo", "original/reply/topic")
				.setHeader("app_fraudCheckMediator_correlationId", "correlation-123")
				.setHeader("app_fraudCheckMediator_timestamp", 1234567890L)
				.setHeader("app_sourcePlatform", "ext/zeus")
				.setHeader("app_fraudCheckOrchestrator_isBlockRequested", true)
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		assertEquals("original/reply/topic", outputMessage.getHeaders().get("app_fraudCheckMediator_replyTo"));
		assertEquals("correlation-123", outputMessage.getHeaders().get("app_fraudCheckMediator_correlationId"));
		assertEquals(1234567890L, outputMessage.getHeaders().get("app_fraudCheckMediator_timestamp"));
		assertEquals("ext/zeus", outputMessage.getHeaders().get("app_sourcePlatform"));
		assertEquals(true, outputMessage.getHeaders().get("app_fraudCheckOrchestrator_isBlockRequested"));
	}

	@Test
	void testBlockCardDoesNotCopyNonAppHeaders() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456");
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/nonapp";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/nonapp")
				.setHeader("custom_header", "should_not_be_copied")
				.setHeader("another_header", "also_not_copied")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		assertNull(outputMessage.getHeaders().get("custom_header"));
		assertNull(outputMessage.getHeaders().get("another_header"));
	}

	@Test
	void testBlockCardWithNoAppHeaders() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456");
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/noapp";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/noapp")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("ok", outputJson.get("status"));
	}

	@Test
	void testBlockCardWithMalformedJsonBraces() throws ParseException {
		String malformedJson = "{\"cardNumber\": \"1234-5678-9012-3456\", \"setCardBlockStatus\": ";
		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/malformed";

		Message<String> inputMessage = MessageBuilder.withPayload(malformedJson)
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/malformed")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("error", outputJson.get("status"));
		assertNotNull(outputJson.get("errorMsg"));
	}

	@Test
	void testBlockCardWithNullCardNumberValue() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", null);
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/nullcard";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/nullcard")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("error", outputJson.get("status"));
		assertNotNull(outputJson.get("errorMsg"));
	}

	@Test
	void testBlockCardWithNullSetCardBlockStatusValue() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456");
		inputJson.put("setCardBlockStatus", null);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/nullstatus";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/nullstatus")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("error", outputJson.get("status"));
		assertNotNull(outputJson.get("errorMsg"));
	}

	@Test
	void testBlockCardSetsCorrectTargetDestination() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456");
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/target";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/target")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);
		assertEquals(replyTopic, outputMessage.getHeaders().get(BinderHeaders.TARGET_DESTINATION));
	}

	@Test
	void testBlockCardWithDifferentCardNumberFormats() throws ParseException {
		String[] cardNumbers = {
				"1234567890123456",
				"1234-5678-9012-3456",
				"1234 5678 9012 3456",
				"XXXX-XXXX-XXXX-1234"
		};

		for (String cardNumber : cardNumbers) {
			JSONObject inputJson = new JSONObject();
			inputJson.put("cardNumber", cardNumber);
			inputJson.put("setCardBlockStatus", true);

			String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/format";

			Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
					.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
					.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/format")
					.build();

			Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
			Message<String> outputMessage = blockCard.apply(inputMessage);

			assertNotNull(outputMessage);

			JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
			assertEquals("ok", outputJson.get("status"));
			assertEquals(cardNumber, outputJson.get("cardNumber"));
		}
	}

	@Test
	void testBlockCardWithMultipleAppHeaders() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456");
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/multiheaders";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/multiheaders")
				.setHeader("app_header1", "value1")
				.setHeader("app_header2", "value2")
				.setHeader("app_header3", "value3")
				.setHeader("app_header4", 12345)
				.setHeader("app_header5", true)
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		assertEquals("value1", outputMessage.getHeaders().get("app_header1"));
		assertEquals("value2", outputMessage.getHeaders().get("app_header2"));
		assertEquals("value3", outputMessage.getHeaders().get("app_header3"));
		assertEquals(12345, outputMessage.getHeaders().get("app_header4"));
		assertEquals(true, outputMessage.getHeaders().get("app_header5"));
	}

	@Test
	void testBlockCardWithEmptyStringCardNumber() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "");
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/emptycard";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/emptycard")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("ok", outputJson.get("status"));
		assertEquals("", outputJson.get("cardNumber"));
	}

	@Test
	void testBlockCardWithSpecialCharactersInCardNumber() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456!@#$%");
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/special";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/special")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("ok", outputJson.get("status"));
		assertEquals("1234-5678-9012-3456!@#$%", outputJson.get("cardNumber"));
	}

	@Test
	void testBlockCardWithJsonArray() throws ParseException {
		String jsonArrayPayload = "[{\"cardNumber\": \"1234\"}]";
		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/array";

		Message<String> inputMessage = MessageBuilder.withPayload(jsonArrayPayload)
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/array")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("error", outputJson.get("status"));
		assertNotNull(outputJson.get("errorMsg"));
	}

	@Test
	void testBlockCardWithExtraFieldsInPayload() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456");
		inputJson.put("setCardBlockStatus", true);
		inputJson.put("extraField1", "extraValue1");
		inputJson.put("extraField2", 12345);
		inputJson.put("extraField3", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/extra";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/extra")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("ok", outputJson.get("status"));
		assertEquals("1234-5678-9012-3456", outputJson.get("cardNumber"));
	}

	@Test
	void testBlockCardWithLongCardNumber() throws ParseException {
		String longCardNumber = "1234567890123456789012345678901234567890123456789012345678901234567890";
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", longCardNumber);
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/long";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/long")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("ok", outputJson.get("status"));
		assertEquals(longCardNumber, outputJson.get("cardNumber"));
	}

	@Test
	void testIntegrationWithInputAndOutputDestinations() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456");
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/integration";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/integration")
				.setHeader("app_test_header", "test_value")
				.build();

		inputDestination.send(inputMessage, "q.fraudCheck.cardBlockService");

		Message<byte[]> outputMessage = outputDestination.receive(10000, "topic-not-used");
		assertNotNull(outputMessage);

		String outputPayload = new String(outputMessage.getPayload());
		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputPayload);
		assertEquals("ok", outputJson.get("status"));
		assertEquals("1234-5678-9012-3456", outputJson.get("cardNumber"));
		assertEquals(true, outputJson.get("cardBlockStatus"));
	}

	@Test
	void testIntegrationWithInvalidPayload() throws ParseException {
		String invalidJson = "not valid json";
		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/integration-error";

		Message<String> inputMessage = MessageBuilder.withPayload(invalidJson)
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/integration-error")
				.build();

		inputDestination.send(inputMessage, "q.fraudCheck.cardBlockService");

		Message<byte[]> outputMessage = outputDestination.receive(10000, "topic-not-used");
		assertNotNull(outputMessage);

		String outputPayload = new String(outputMessage.getPayload());
		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputPayload);
		assertEquals("error", outputJson.get("status"));
		assertNotNull(outputJson.get("errorMsg"));
	}

	@Test
	void testIntegrationWithMissingFields() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456");

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/integration-missing";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/integration-missing")
				.build();

		inputDestination.send(inputMessage, "q.fraudCheck.cardBlockService");

		Message<byte[]> outputMessage = outputDestination.receive(10000, "topic-not-used");
		assertNotNull(outputMessage);

		String outputPayload = new String(outputMessage.getPayload());
		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputPayload);
		assertEquals("error", outputJson.get("status"));
	}

	@Test
	void testBlockCardWithUnicodeCardNumber() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456-\u65E5\u672C\u8A9E");
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/unicode";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/unicode")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("ok", outputJson.get("status"));
		assertEquals("1234-5678-9012-3456-\u65E5\u672C\u8A9E", outputJson.get("cardNumber"));
	}

	@Test
	void testBlockCardWithNestedJsonInCardNumber() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "{\"nested\": \"value\"}");
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/nested";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/nested")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("ok", outputJson.get("status"));
	}

	@Test
	void testBlockCardWithNumericCardNumber() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", 1234567890123456L);
		inputJson.put("setCardBlockStatus", true);

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/numeric";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/numeric")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("ok", outputJson.get("status"));
	}

	@Test
	void testBlockCardWithStringSetCardBlockStatus() throws ParseException {
		JSONObject inputJson = new JSONObject();
		inputJson.put("cardNumber", "1234-5678-9012-3456");
		inputJson.put("setCardBlockStatus", "true");

		String replyTopic = "myBank/cards/fraudCheckApi/reply/cardService/block/v1/stringbool";

		Message<String> inputMessage = MessageBuilder.withPayload(inputJson.toString())
				.setHeader(REPLY_TO_HEADER_KEY, replyTopic)
				.setHeader(SOL_DESTINATION_KEY, "myBank/cards/cardService/block/req/v1/stringbool")
				.build();

		Function<Message<String>, Message<String>> blockCard = cardBlockServiceApplication.blockCard();
		Message<String> outputMessage = blockCard.apply(inputMessage);

		assertNotNull(outputMessage);

		JSONObject outputJson = (JSONObject) new JSONParser().parse(outputMessage.getPayload());
		assertEquals("error", outputJson.get("status"));
	}
}
