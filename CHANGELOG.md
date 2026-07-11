## 0.1.3

* Fix RangeError on second streamed message by clearing part identifiers between turns (#1)

## 0.1.2

* Updated CHANGELOG.md to include version references for pub.dev compliance

## 0.1.1

* Code formatting improvements to meet pub.dev standards
* Added CODEOWNERS file for repository maintenance
* Updated README with accurate project status
* All tests passing with full coverage

## 0.1.0

* Initial release
* Core data models for UIMessage, UIMessagePart, and UIMessageChunk
* HTTP/SSE transport layer with DefaultChatTransport
* Chat client with streaming support and state management
* Tool execution framework with FunctionTool and ToolExecutor
* Comprehensive test coverage (35 tests)
* Example Flutter chat application
* Full compatibility with Vercel AI SDK v5 streaming protocol
