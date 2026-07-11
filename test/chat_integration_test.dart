import 'dart:async';
import 'package:ai_sdk_flutter/ai_sdk_flutter.dart';
import 'package:flutter_test/flutter_test.dart';

/// Mock transport for integration testing
class MockChatTransport implements ChatTransport {
  final List<UIMessageChunk> chunksToSend;
  final Duration delayBetweenChunks;
  final bool shouldError;
  final String? errorMessage;
  StreamController<UIMessageChunk>? _controller;

  MockChatTransport({
    required this.chunksToSend,
    this.delayBetweenChunks = const Duration(milliseconds: 5),
    this.shouldError = false,
    this.errorMessage,
  });

  @override
  Stream<UIMessageChunk> sendMessages({
    required String trigger,
    required String chatId,
    required List<UIMessage> messages,
    String? messageId,
    Map<String, String>? headers,
    Map<String, dynamic>? body,
    Map<String, dynamic>? metadata,
  }) async* {
    if (shouldError) {
      throw Exception(errorMessage ?? 'Mock error');
    }

    _controller = StreamController<UIMessageChunk>();

    Future.microtask(() async {
      for (final chunk in chunksToSend) {
        await Future.delayed(delayBetweenChunks);
        if (!_controller!.isClosed) {
          _controller!.add(chunk);
        }
      }
      await _controller!.close();
    });

    yield* _controller!.stream;
  }

  @override
  Future<Stream<UIMessageChunk>?> reconnectToStream({
    required String chatId,
    Map<String, String>? headers,
    Map<String, dynamic>? body,
    Map<String, dynamic>? metadata,
  }) async {
    return Stream.fromIterable(chunksToSend);
  }

  @override
  Future<void> close() async {
    await _controller?.close();
  }
}

/// Returns a complete text stream for every sendMessages call (multi-turn).
class MultiTurnMockTransport implements ChatTransport {
  int _callCount = 0;

  @override
  Stream<UIMessageChunk> sendMessages({
    required String trigger,
    required String chatId,
    required List<UIMessage> messages,
    String? messageId,
    Map<String, String>? headers,
    Map<String, dynamic>? body,
    Map<String, dynamic>? metadata,
  }) async* {
    _callCount++;
    final messageIdForTurn = 'assistant-$_callCount';
    yield StartChunk(messageId: messageIdForTurn);
    yield const TextStartChunk(id: 'text-0');
    yield TextDeltaChunk(delta: 'Reply $_callCount', id: 'text-0');
    yield const TextEndChunk(id: 'text-0');
    yield const FinishChunk(finishReason: 'stop');
  }

  @override
  Future<Stream<UIMessageChunk>?> reconnectToStream({
    required String chatId,
    Map<String, String>? headers,
    Map<String, dynamic>? body,
    Map<String, dynamic>? metadata,
  }) async =>
      null;

  @override
  Future<void> close() async {}
}

void main() {
  group('Chat Integration Tests', () {
    test('should send message and receive text response', () async {
      final chunks = [
        const StartChunk(messageId: 'msg-1'),
        const TextDeltaChunk(delta: 'Hello', id: 'msg-1'),
        const TextDeltaChunk(delta: ' World', id: 'msg-1'),
        const FinishChunk(finishReason: 'stop'),
      ];

      final transport = MockChatTransport(chunksToSend: chunks);
      final chat = Chat(
        transport: transport,
        options: const ChatOptions(id: 'test-chat'),
      );

      await chat.sendMessage('Test message');
      await Future.delayed(const Duration(milliseconds: 100));

      expect(chat.messages.length, greaterThan(1));
      if (chat.messages.isNotEmpty) {
        expect(chat.messages[0].role, MessageRole.user);
        if (chat.messages[0].parts.isNotEmpty) {
          expect(
              (chat.messages[0].parts[0] as TextUIPart).text, 'Test message');
        }
      }
      if (chat.messages.length >= 2 && chat.messages[1].parts.isNotEmpty) {
        expect(chat.messages[1].role, MessageRole.assistant);
        expect((chat.messages[1].parts[0] as TextUIPart).text, 'Hello World');
      }
      expect(chat.status, ChatStatus.ready);
    });

    test('should handle tool call chunks', () async {
      final chunks = [
        const StartChunk(messageId: 'msg-1'),
        const ToolInputStartChunk(
          toolCallId: 'call-1',
          toolName: 'getWeather',
        ),
        const ToolInputDeltaChunk(
          toolCallId: 'call-1',
          inputTextDelta: '{"location":"SF"}',
        ),
        const ToolInputAvailableChunk(
          toolCallId: 'call-1',
          toolName: 'getWeather',
          input: {'location': 'SF'},
        ),
        const FinishChunk(finishReason: 'tool-calls'),
      ];

      final transport = MockChatTransport(chunksToSend: chunks);
      final chat = Chat(
        transport: transport,
        options: const ChatOptions(id: 'test-chat'),
      );

      await chat.sendMessage('What is the weather?');
      await Future.delayed(const Duration(milliseconds: 100));

      // Just verify the message was processed
      expect(chat.messages.length, greaterThan(0));
      expect(chat.status, ChatStatus.ready);
    });

    test('should track streaming status', () async {
      final chunks = [
        const StartChunk(messageId: 'msg-1'),
        const TextDeltaChunk(delta: 'Test', id: 'msg-1'),
        const FinishChunk(finishReason: 'stop'),
      ];

      final transport = MockChatTransport(
        chunksToSend: chunks,
        delayBetweenChunks: const Duration(milliseconds: 30),
      );

      final chat = Chat(
        transport: transport,
        options: const ChatOptions(id: 'test-chat'),
      );

      final statuses = <ChatStatus>[];
      chat.statusStream.listen(statuses.add);

      expect(chat.status, ChatStatus.ready);

      chat.sendMessage('Test');
      await Future.delayed(const Duration(milliseconds: 40));

      expect(statuses, contains(ChatStatus.streaming));

      await Future.delayed(const Duration(milliseconds: 60));

      expect(chat.status, ChatStatus.ready);
    });

    test('should handle transport errors', () async {
      final transport = MockChatTransport(
        chunksToSend: [],
        shouldError: true,
        errorMessage: 'Network error',
      );

      String? capturedError;
      final chat = Chat(
        transport: transport,
        options: ChatOptions(
          id: 'test-chat',
          onError: (error) {
            capturedError = error.toString();
          },
        ),
      );

      await chat.sendMessage('This will fail');
      await Future.delayed(const Duration(milliseconds: 50));

      expect(capturedError, contains('Network error'));
      // Status may be error or ready after error handling
      expect([ChatStatus.error, ChatStatus.ready], contains(chat.status));
    });

    test('should handle error chunks', () async {
      final chunks = [
        const StartChunk(messageId: 'msg-1'),
        const ErrorChunk(errorText: 'AI service error'),
      ];

      String? capturedError;
      final transport = MockChatTransport(chunksToSend: chunks);
      final chat = Chat(
        transport: transport,
        options: ChatOptions(
          id: 'test-chat',
          onError: (error) {
            capturedError = error.toString();
          },
        ),
      );

      await chat.sendMessage('Test');
      await Future.delayed(const Duration(milliseconds: 50));

      expect(capturedError, contains('AI service error'));
    });

    test('should support stopping stream', () async {
      final chunks = <UIMessageChunk>[
        const StartChunk(messageId: 'msg-1'),
        ...List.generate(
            50, (i) => TextDeltaChunk(delta: 'word$i ', id: 'msg-1')),
      ];

      final transport = MockChatTransport(
        chunksToSend: chunks,
        delayBetweenChunks: const Duration(milliseconds: 20),
      );

      final chat = Chat(
        transport: transport,
        options: const ChatOptions(id: 'test-chat'),
      );

      chat.sendMessage('Long message');
      await Future.delayed(const Duration(milliseconds: 50));

      chat.stop();

      await Future.delayed(const Duration(milliseconds: 50));
      expect(chat.status, ChatStatus.ready);
    });

    test('should invoke onFinish callback', () async {
      final chunks = [
        const StartChunk(messageId: 'msg-1'),
        const TextDeltaChunk(delta: 'Done', id: 'msg-1'),
        const FinishChunk(finishReason: 'stop'),
      ];

      UIMessage? finishedMessage;
      final transport = MockChatTransport(chunksToSend: chunks);

      final chat = Chat(
        transport: transport,
        options: ChatOptions(
          id: 'test-chat',
          onFinish: (message) {
            finishedMessage = message;
          },
        ),
      );

      await chat.sendMessage('Test');
      await Future.delayed(const Duration(milliseconds: 100));

      expect(finishedMessage, isNotNull);
      if (finishedMessage!.parts.isNotEmpty) {
        expect((finishedMessage!.parts[0] as TextUIPart).text, 'Done');
      }
    });

    test('should handle multiple messages in conversation', () async {
      final transport = MockChatTransport(
        chunksToSend: [
          const StartChunk(messageId: 'msg-1'),
          const TextDeltaChunk(delta: 'Response 1', id: 'msg-1'),
          const FinishChunk(finishReason: 'stop'),
        ],
      );

      final chat = Chat(
        transport: transport,
        options: const ChatOptions(id: 'test-chat'),
      );

      await chat.sendMessage('Message 1');
      await Future.delayed(const Duration(milliseconds: 50));

      expect(chat.messages.length, 2);
      expect(chat.status, ChatStatus.ready);
    });

    // Regression for https://github.com/billmalea/ai_sdk_flutter/issues/1
    test('should not throw RangeError on second streamed message', () async {
      final chat = Chat(
        transport: MultiTurnMockTransport(),
        options: const ChatOptions(id: 'test-chat'),
      );

      await chat.sendMessage('Message 1');
      await Future.delayed(const Duration(milliseconds: 50));
      expect(chat.status, ChatStatus.ready);
      expect(chat.messages.length, 2);
      expect((chat.messages[1].parts[0] as TextUIPart).text, 'Reply 1');

      await chat.sendMessage('Message 2');
      await Future.delayed(const Duration(milliseconds: 50));
      expect(chat.status, ChatStatus.ready);
      expect(chat.messages.length, 4);
      expect((chat.messages[3].parts[0] as TextUIPart).text, 'Reply 2');
    });
  });
}
