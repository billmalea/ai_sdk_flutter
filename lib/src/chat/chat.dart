import 'dart:async';
import '../models/ui_message.dart';
import '../models/ui_message_part.dart';
import '../models/ui_message_chunk.dart';
import '../models/chat_status.dart';
import '../models/chat_options.dart';
import '../transport/chat_transport.dart';
import '../utils/id_generator.dart';

/// Main chat client for managing AI conversations with streaming support.
class Chat {
  final ChatTransport _transport;
  final ChatOptions options;

  final _messagesController = StreamController<List<UIMessage>>.broadcast();
  final _statusController = StreamController<ChatStatus>.broadcast();
  final _errorController = StreamController<Object>.broadcast();

  List<UIMessage> _messages = [];
  ChatStatus _status = ChatStatus.ready;
  StreamSubscription<UIMessageChunk>? _streamSubscription;
  UIMessage? _currentStreamingMessage;

  /// Stream of messages in the conversation.
  Stream<List<UIMessage>> get messagesStream => _messagesController.stream;

  /// Stream of chat status changes.
  Stream<ChatStatus> get statusStream => _statusController.stream;

  /// Stream of errors.
  Stream<Object> get errorStream => _errorController.stream;

  /// Current messages in the conversation.
  List<UIMessage> get messages => List.unmodifiable(_messages);

  /// Current chat status.
  ChatStatus get status => _status;

  /// Whether the chat is currently streaming.
  bool get isStreaming => _status == ChatStatus.streaming;

  Chat({
    required this.options,
    required ChatTransport transport,
  }) : _transport = transport {
    _messages = List.from(options.initialMessages);
    _messagesController.add(_messages);
  }

  /// Send a new message to the AI.
  Future<void> sendMessage(String text, {Map<String, dynamic>? data}) async {
    if (_status == ChatStatus.streaming) {
      throw StateError('Cannot send message while streaming');
    }

    final userMessage = UIMessage.user(
      id: IdGenerator.generateMessageId(),
      text: text,
    );

    _addMessage(userMessage);
    await _submitMessages();
  }

  /// Append a user message and submit to the AI.
  Future<void> append(UIMessage message) async {
    if (message.role != MessageRole.user) {
      throw ArgumentError('Only user messages can be appended');
    }

    _addMessage(message);
    await _submitMessages();
  }

  /// Reload/regenerate the last assistant message.
  Future<void> reload() async {
    if (_messages.isEmpty) return;

    // Remove the last assistant message if present
    if (_messages.last.role == MessageRole.assistant) {
      _messages = _messages.sublist(0, _messages.length - 1);
      _messagesController.add(_messages);
    }

    await _submitMessages();
  }

  /// Stop the current streaming response.
  Future<void> stop() async {
    if (!isStreaming) return;

    await _streamSubscription?.cancel();
    _streamSubscription = null;
    _updateStatus(ChatStatus.ready);
  }

  /// Add a tool output to the conversation.
  Future<void> addToolResult({
    required String toolCallId,
    required dynamic result,
  }) async {
    if (_currentStreamingMessage == null) {
      throw StateError('No active streaming message');
    }

    // Find and update the tool part with the result
    final updatedParts = _currentStreamingMessage!.parts.map((part) {
      if (part is ToolUIPart && part.toolCallId == toolCallId) {
        return ToolUIPart(
          type: part.type,
          toolCallId: part.toolCallId,
          state: ToolCallState.output_available,
          input: part.input,
          output: result,
        );
      }
      return part;
    }).toList();

    _currentStreamingMessage = UIMessage(
      id: _currentStreamingMessage!.id,
      role: _currentStreamingMessage!.role,
      parts: updatedParts,
      metadata: _currentStreamingMessage!.metadata,
    );

    _updateMessage(_currentStreamingMessage!);

    // If sendToolResults is enabled, continue the conversation
    if (options.sendToolResults) {
      await _submitMessages();
    }
  }

  Future<void> _submitMessages() async {
    try {
      _updateStatus(ChatStatus.submitted);

      final stream = _transport.sendMessages(
        trigger: 'user-message',
        chatId: options.id,
        messages: _messages,
        body: options.body,
        headers: options.headers,
      );

      _updateStatus(ChatStatus.streaming);
      await _handleStream(stream);
    } catch (e) {
      _handleError(e);
    }
  }

  Future<void> _handleStream(Stream<UIMessageChunk> stream) async {
    _streamSubscription = stream.listen(
      (chunk) => _processChunk(chunk),
      onError: (error) => _handleError(error),
      onDone: () => _handleStreamComplete(),
      cancelOnError: false,
    );
  }

  void _processChunk(UIMessageChunk chunk) {
    options.onData?.call(chunk);

    switch (chunk.type) {
      case 'start':
        _handleStartChunk(chunk as StartChunk);
        break;
      case 'text-start':
        _handleTextStart(chunk as TextStartChunk);
        break;
      case 'text-delta':
        _handleTextDelta(chunk as TextDeltaChunk);
        break;
      case 'text-end':
        _handleTextEnd(chunk as TextEndChunk);
        break;
      case 'tool-input-start':
        _handleToolInputStart(chunk as ToolInputStartChunk);
        break;
      case 'tool-input-delta':
        _handleToolInputDelta(chunk as ToolInputDeltaChunk);
        break;
      case 'tool-input-available':
        _handleToolInputAvailable(chunk as ToolInputAvailableChunk);
        break;
      case 'tool-output-available':
        _handleToolOutputAvailable(chunk as ToolOutputAvailableChunk);
        break;
      case 'reasoning-start':
        _handleReasoningStart(chunk as ReasoningStartChunk);
        break;
      case 'reasoning-delta':
        _handleReasoningDelta(chunk as ReasoningDeltaChunk);
        break;
      case 'reasoning-end':
        _handleReasoningEnd(chunk as ReasoningEndChunk);
        break;
      case 'file':
        _handleFileChunk(chunk as FileChunk);
        break;
      case 'source-url':
        _handleSourceUrl(chunk as SourceUrlChunk);
        break;
      case 'source-document':
        _handleSourceDocument(chunk as SourceDocumentChunk);
        break;
      case 'data':
        _handleDataChunk(chunk as DataChunk);
        break;
      case 'finish':
        _handleFinishChunk(chunk as FinishChunk);
        break;
      case 'error':
        _handleErrorChunk(chunk as ErrorChunk);
        break;
      case 'message-metadata':
        _handleMessageMetadata(chunk as MessageMetadataChunk);
        break;
    }
  }

  void _handleStartChunk(StartChunk chunk) {
    // Reset part index map for each new assistant message. Stale entries from a
    // previous turn point into an empty parts list and cause RangeError (#1).
    _partIdentifiers.clear();

    _currentStreamingMessage = UIMessage(
      id: chunk.messageId ?? IdGenerator.generateMessageId(),
      role: MessageRole.assistant,
      parts: [],
      metadata: chunk.messageMetadata,
    );
    _addMessage(_currentStreamingMessage!);
  }

  void _handleTextStart(TextStartChunk chunk) {
    _addOrUpdatePart(
      TextUIPart(text: '', state: TextState.streaming),
      identifier: 'text-0',
    );
  }

  void _handleTextDelta(TextDeltaChunk chunk) {
    _updatePartByIdentifier(
      'text-0',
      (part) {
        if (part is TextUIPart) {
          return TextUIPart(
            text: part.text + chunk.delta,
            state: TextState.streaming,
          );
        }
        return part;
      },
    );
  }

  void _handleTextEnd(TextEndChunk chunk) {
    _updatePartByIdentifier(
      'text-0',
      (part) {
        if (part is TextUIPart) {
          return TextUIPart(
            text: part.text,
            state: TextState.done,
          );
        }
        return part;
      },
    );
  }

  void _handleToolInputStart(ToolInputStartChunk chunk) {
    final toolPart = ToolUIPart(
      type: 'tool-${chunk.toolName}',
      toolCallId: chunk.toolCallId,
      state: ToolCallState.input_streaming,
    );
    _addOrUpdatePart(toolPart, identifier: 'tool-${chunk.toolCallId}');
    options.onToolCall?.call({
      'toolCallId': chunk.toolCallId,
      'toolName': chunk.toolName,
    });
  }

  void _handleToolInputDelta(ToolInputDeltaChunk chunk) {
    _updatePartByIdentifier(
      'tool-${chunk.toolCallId}',
      (part) {
        if (part is ToolUIPart) {
          // Merge input delta
          final currentInput = part.input is Map
              ? Map<String, dynamic>.from(part.input)
              : <String, dynamic>{};
          currentInput.addAll({chunk.inputTextDelta: chunk.inputTextDelta});
          return ToolUIPart(
            type: part.type,
            toolCallId: part.toolCallId,
            state: ToolCallState.input_streaming,
            input: currentInput,
            output: part.output,
          );
        }
        return part;
      },
    );
  }

  void _handleToolInputAvailable(ToolInputAvailableChunk chunk) {
    _updatePartByIdentifier(
      'tool-${chunk.toolCallId}',
      (part) {
        if (part is ToolUIPart) {
          return ToolUIPart(
            type: part.type,
            toolCallId: part.toolCallId,
            state: ToolCallState.input_available,
            input: chunk.input,
            output: part.output,
          );
        }
        return part;
      },
    );
  }

  void _handleToolOutputAvailable(ToolOutputAvailableChunk chunk) {
    _updatePartByIdentifier(
      'tool-${chunk.toolCallId}',
      (part) {
        if (part is ToolUIPart) {
          return ToolUIPart(
            type: part.type,
            toolCallId: part.toolCallId,
            state: ToolCallState.output_available,
            input: part.input,
            output: chunk.output,
          );
        }
        return part;
      },
    );
  }

  void _handleReasoningStart(ReasoningStartChunk chunk) {
    _addOrUpdatePart(
      ReasoningUIPart(text: '', state: TextState.streaming),
      identifier: 'reasoning-0',
    );
  }

  void _handleReasoningDelta(ReasoningDeltaChunk chunk) {
    _updatePartByIdentifier(
      'reasoning-0',
      (part) {
        if (part is ReasoningUIPart) {
          return ReasoningUIPart(
            text: part.text + chunk.delta,
            state: TextState.streaming,
          );
        }
        return part;
      },
    );
  }

  void _handleReasoningEnd(ReasoningEndChunk chunk) {
    _updatePartByIdentifier(
      'reasoning-0',
      (part) {
        if (part is ReasoningUIPart) {
          return ReasoningUIPart(
            text: part.text,
            state: TextState.done,
          );
        }
        return part;
      },
    );
  }

  void _handleFileChunk(FileChunk chunk) {
    _addOrUpdatePart(
      FileUIPart(
        url: chunk.url,
        mediaType: chunk.mediaType,
      ),
    );
  }

  void _handleSourceUrl(SourceUrlChunk chunk) {
    _addOrUpdatePart(
      SourceUrlUIPart(
        sourceId: chunk.url,
        url: chunk.url,
      ),
    );
  }

  void _handleSourceDocument(SourceDocumentChunk chunk) {
    _addOrUpdatePart(
      SourceDocumentUIPart(
        sourceId: chunk.sourceId,
        mediaType: chunk.mediaType,
        title: chunk.title,
        filename: chunk.filename,
      ),
    );
  }

  void _handleDataChunk(DataChunk chunk) {
    _addOrUpdatePart(
      DataUIPart(
        type: 'data',
        data: chunk.data,
      ),
    );
  }

  void _handleFinishChunk(FinishChunk chunk) {
    if (_currentStreamingMessage != null) {
      options.onFinish?.call(_currentStreamingMessage!);
    }
  }

  void _handleErrorChunk(ErrorChunk chunk) {
    _handleError(Exception(chunk.errorText));
  }

  void _handleMessageMetadata(MessageMetadataChunk chunk) {
    if (_currentStreamingMessage != null) {
      _currentStreamingMessage = UIMessage(
        id: _currentStreamingMessage!.id,
        role: _currentStreamingMessage!.role,
        parts: _currentStreamingMessage!.parts,
        metadata: chunk.messageMetadata,
      );
      _updateMessage(_currentStreamingMessage!);
    }
  }

  void _handleStreamComplete() {
    _streamSubscription = null;
    _currentStreamingMessage = null;
    _partIdentifiers.clear();
    _updateStatus(ChatStatus.ready);
  }

  void _handleError(Object error) {
    _updateStatus(ChatStatus.error);
    _errorController.add(error);
    options.onError?.call(error);
    _streamSubscription = null;
    _currentStreamingMessage = null;
    _partIdentifiers.clear();
  }

  void _addMessage(UIMessage message) {
    _messages = [..._messages, message];
    _messagesController.add(_messages);
  }

  void _updateMessage(UIMessage message) {
    final index = _messages.indexWhere((m) => m.id == message.id);
    if (index != -1) {
      _messages = [
        ..._messages.sublist(0, index),
        message,
        ..._messages.sublist(index + 1),
      ];
      _messagesController.add(_messages);
    }
  }

  final Map<String, int> _partIdentifiers = {};

  void _addOrUpdatePart(UIMessagePart part, {String? identifier}) {
    if (_currentStreamingMessage == null) return;

    if (identifier != null) {
      final existingIndex = _partIdentifiers[identifier];
      if (existingIndex != null) {
        final updatedParts = List<UIMessagePart>.from(
          _currentStreamingMessage!.parts,
        );
        updatedParts[existingIndex] = part;
        _currentStreamingMessage = UIMessage(
          id: _currentStreamingMessage!.id,
          role: _currentStreamingMessage!.role,
          parts: updatedParts,
          metadata: _currentStreamingMessage!.metadata,
        );
      } else {
        _partIdentifiers[identifier] = _currentStreamingMessage!.parts.length;
        _currentStreamingMessage = UIMessage(
          id: _currentStreamingMessage!.id,
          role: _currentStreamingMessage!.role,
          parts: [..._currentStreamingMessage!.parts, part],
          metadata: _currentStreamingMessage!.metadata,
        );
      }
    } else {
      _currentStreamingMessage = UIMessage(
        id: _currentStreamingMessage!.id,
        role: _currentStreamingMessage!.role,
        parts: [..._currentStreamingMessage!.parts, part],
        metadata: _currentStreamingMessage!.metadata,
      );
    }

    _updateMessage(_currentStreamingMessage!);
  }

  void _updatePartByIdentifier(
    String identifier,
    UIMessagePart Function(UIMessagePart) updater,
  ) {
    if (_currentStreamingMessage == null) return;

    final index = _partIdentifiers[identifier];
    if (index == null) return;

    final updatedParts = List<UIMessagePart>.from(
      _currentStreamingMessage!.parts,
    );
    updatedParts[index] = updater(updatedParts[index]);

    _currentStreamingMessage = UIMessage(
      id: _currentStreamingMessage!.id,
      role: _currentStreamingMessage!.role,
      parts: updatedParts,
      metadata: _currentStreamingMessage!.metadata,
    );

    _updateMessage(_currentStreamingMessage!);
  }

  void _updateStatus(ChatStatus newStatus) {
    _status = newStatus;
    _statusController.add(_status);
  }

  /// Dispose of resources.
  Future<void> dispose() async {
    await _streamSubscription?.cancel();
    await _transport.close();
    await _messagesController.close();
    await _statusController.close();
    await _errorController.close();
  }
}
