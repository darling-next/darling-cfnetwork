/* Darling RFC 6455 session transport, on the existing CFNetwork socket streams.
 * Provenance/audit: the vendored swift-corelibs-foundation Darwin overlay supplies
 * the public typed bridge but not an Objective-C transport. Newer upstream
 * FoundationNetworking/WebSocketURLProtocol.swift uses its separate Swift/curl
 * session engine; our vendored curl 7.64.1 predates curl_ws_send/recv. The checked
 * VibeDarling Foundation header has no WebSocket classes. SocketRocket upstream
 * b7f833e3bf6ba78adfad783bcc5f564f4f436678 was also inspected: its ARC-only,
 * nonfragile-ivar implementation cannot directly serve our i386 fragile ABI.
 * This adapter consequently reuses native CFStream HTTP parsing, sockets and
 * verified SecureTransport instead of adding a parallel backend. Protocol rules
 * are RFC 6455 sections 4.1, 5.2-5.5, 7.1 and 8.1. No extensions are negotiated.
 */
#import <Foundation/NSStream.h>
#import <Foundation/NSCharacterSet.h>
#import <CFNetwork/CFHTTPMessage.h>
#import <CFNetwork/CFSocketStream.h>
#import <Security/SecRandom.h>
#import <CommonCrypto/CommonDigest.h>
#import <Foundation/NSNumber.h>

@implementation NSURLSessionWebSocketMessage
- (instancetype)initWithData:(NSData *)data { if ((self = [super init])) _data = [data copy]; return self; }
- (instancetype)initWithString:(NSString *)string { if ((self = [super init])) _string = [string copy]; return self; }
- (void)dealloc { [_data release]; [_string release]; [super dealloc]; }
- (NSData *)data { return _data; }
- (NSString *)string { return _string; }
- (NSURLSessionWebSocketMessageType)type { return _string ? NSURLSessionWebSocketMessageTypeString : NSURLSessionWebSocketMessageTypeData; }
- (id)copyWithZone:(NSZone *)zone { return [self retain]; }
@end
@implementation NSURLSessionWebSocketTask
@dynamic maximumMessageSize, closeCode, closeReason;
@end

/* Outbound records retain callbacks until the entire frame reaches the stream.
 * They are removed before calling clients, so reentrancy cannot complete twice.
 */
@interface __NSCFWebSocketWrite : NSObject {
@public
    NSData *bytes;
    NSUInteger offset;
    void (^completion)(NSError *);
    BOOL closeFrame;
    BOOL automaticPong;
}
@end
@implementation __NSCFWebSocketWrite
- (void)dealloc { [bytes release]; [completion release]; [super dealloc]; }
@end

@interface __NSCFURLSessionWebSocketTask : NSURLSessionWebSocketTask <NSStreamDelegate> {
    __NSCFURLSession *_session;
    NSURLRequest *_request;
    NSURLResponse *_response;
    id<NSURLSessionTaskDelegate> _delegate;
    NSString *_description;
    NSError *_error;
    NSError *_pendingError;
    NSError *_protocolFailure;
    BOOL _closeTimerStarted;
    NSUInteger _identifier, _suspensions;
    NSURLSessionTaskState _state;
    NSInputStream *_input;
    NSOutputStream *_output;
    BOOL _scheduled, _opened, _upgraded, _closing, _closeSent, _closeReceived;
    NSMutableData *_incoming, *_fragment;
    NSUInteger _incomingOffset;
    uint8_t _fragmentOpcode;
    NSMutableArray *_writes, *_messages, *_receivers, *_pongs;
    NSString *_accept;
    NSString *_protocols;
    NSData *_pingNonce;
    NSInteger _maximum;
    NSURLSessionWebSocketCloseCode _closeCode;
    NSData *_closeReason;
    CFRunLoopTimerRef _timer;
    int64_t _received, _sent;
    BOOL _inputPaused;
    NSUInteger _deliveries;
}
- (id)initWithSession:(__NSCFURLSession *)session request:(NSURLRequest *)request identifier:(NSUInteger)identifier;
- (void)_updateConnection;
- (void)_finish:(NSError *)error;
- (void)_flush;
- (void)_consume;
- (void)_readAvailable;
- (void)_frame:(uint8_t)opcode data:(NSData *)data completion:(void (^)(NSError *))completion;
- (void)_deliver;
- (void)_sendNextPing;
- (void)_timeout;
- (void)_protocolError:(NSString *)description closeCode:(uint16_t)code;
@end

static NSError *webSocketError(NSInteger code, NSString *description)
{
    return [NSError errorWithDomain:NSURLErrorDomain code:code userInfo:@{NSLocalizedDescriptionKey: description}];
}
static BOOL webSocketToken(NSString *value, NSString *token)
{
    for (NSString *part in [value componentsSeparatedByString:@","])
        if ([[part stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceAndNewlineCharacterSet]] caseInsensitiveCompare:token] == NSOrderedSame) return YES;
    return NO;
}
/* RFC 9110 field-name token and field-value grammar. CFHTTPMessage serializes
 * fields verbatim, so reject injection before handing request fields to it. */
static BOOL webSocketFieldName(NSString *name)
{
    if (![name isKindOfClass:[NSString class]] || ![name length]) return NO;
    for (NSUInteger i = 0; i < [name length]; ++i) {
        unichar c = [name characterAtIndex:i];
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) continue;
        if (!c || c > 127 || !strchr("!#$%&'*+-.^_`|~", c)) return NO;
    }
    return YES;
}
static BOOL webSocketFieldValue(NSString *value)
{
    if (![value isKindOfClass:[NSString class]]) return NO;
    for (NSUInteger i = 0; i < [value length]; ++i) {
        unichar c = [value characterAtIndex:i];
        if ((c < 32 && c != '\t') || c == 127) return NO;
    }
    return YES;
}
static BOOL webSocketHeaders(NSDictionary *headers)
{
    for (NSString *name in headers)
        if (!webSocketFieldName(name) || !webSocketFieldValue([[headers objectForKey:name] description])) return NO;
    return YES;
}
static BOOL webSocketProtocol(NSString *offered, NSString *selected)
{
    if (!webSocketFieldName(selected)) return NO;
    for (NSString *part in [offered componentsSeparatedByString:@","])
        if ([[part stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]] isEqualToString:selected]) return YES;
    return NO;
}
static BOOL webSocketOfferedProtocols(NSString *offered)
{
    if (!offered) return YES;
    NSMutableSet *seen = [NSMutableSet set];
    for (NSString *part in [offered componentsSeparatedByString:@","]) {
        NSString *token = [part stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
        if (!webSocketFieldName(token) || [seen containsObject:token]) return NO;
        [seen addObject:token];
    }
    return YES;
}
static BOOL webSocketCloseCode(NSUInteger code)
{
    return (code >= 1000 && code <= 1014 && code != 1004 && code != 1005 && code != 1006) || (code >= 3000 && code <= 4999);
}
static void webSocketTimeout(CFRunLoopTimerRef timer, void *info) { [(__NSCFURLSessionWebSocketTask *)info _timeout]; }
NSURLSessionWebSocketTask *_NSCFURLSessionCreateWebSocketTask(__NSCFURLSession *session, NSURLRequest *request, NSUInteger identifier)
{
    return [[__NSCFURLSessionWebSocketTask alloc] initWithSession:session request:request identifier:identifier];
}

@implementation __NSCFURLSessionWebSocketTask
- (id)initWithSession:(__NSCFURLSession *)session request:(NSURLRequest *)request identifier:(NSUInteger)identifier
{
    if ((self = [super init])) {
        _session = [session retain]; _request = [request copy]; _identifier = identifier;
        /* Upstream FoundationNetworking/URLSessionTask.swift defaults to 1 MiB.
         * The SDK NSURLSession.h maximumMessageSize contract includes the sum
         * of all continuation-frame bytes, not a per-frame allowance. */
        _state = NSURLSessionTaskStateSuspended; _suspensions = 1; _maximum = 1024 * 1024;
        _incoming = [[NSMutableData alloc] init]; _writes = [[NSMutableArray alloc] init];
        _messages = [[NSMutableArray alloc] init]; _receivers = [[NSMutableArray alloc] init]; _pongs = [[NSMutableArray alloc] init];
    }
    return self;
}
- (void)dealloc
{
    [_session release]; [_request release]; [_response release]; [_delegate release]; [_description release]; [_error release];
    [_input release]; [_output release]; [_incoming release]; [_fragment release]; [_writes release]; [_messages release]; [_receivers release]; [_pongs release];
    [_accept release]; [_closeReason release];
    [_protocols release];
    [_pingNonce release];
    [_pendingError release];
    [_protocolFailure release];
    if (_timer) { CFRunLoopTimerInvalidate(_timer); CFRelease(_timer); }
    [super dealloc];
}
- (id)copyWithZone:(NSZone *)zone { return [self retain]; }
- (NSUInteger)taskIdentifier { return _identifier; }
- (NSURLRequest *)originalRequest { return _request; }
- (NSURLRequest *)currentRequest { return _request; }
- (NSURLResponse *)response { @synchronized(self) { return [[_response retain] autorelease]; } }
- (NSError *)error { @synchronized(self) { return [[_error retain] autorelease]; } }
- (NSURLSessionTaskState)state { @synchronized(self) { return _state; } }
- (int64_t)countOfBytesReceived { @synchronized(self) { return _received; } }
- (int64_t)countOfBytesSent { @synchronized(self) { return _sent; } }
- (int64_t)countOfBytesExpectedToReceive { return NSURLSessionTransferSizeUnknown; }
- (int64_t)countOfBytesExpectedToSend { return NSURLSessionTransferSizeUnknown; }
- (NSString *)taskDescription { @synchronized(self) { return [[_description retain] autorelease]; } }
- (void)setTaskDescription:(NSString *)value { @synchronized(self) { NSString *copy = [value copy]; [_description release]; _description = copy; } }
- (id<NSURLSessionTaskDelegate>)delegate { @synchronized(self) { return [[(_delegate ?: (id)_session.delegate) retain] autorelease]; } }
- (void)setDelegate:(id<NSURLSessionTaskDelegate>)value { @synchronized(self) { [value retain]; [_delegate release]; _delegate = value; } }
- (NSInteger)maximumMessageSize { @synchronized(self) { return _maximum; } }
- (void)setMaximumMessageSize:(NSInteger)value { if (value <= 0) [NSException raise:NSInvalidArgumentException format:@"maximumMessageSize must be positive"]; @synchronized(self) { _maximum = value; } }
- (NSURLSessionWebSocketCloseCode)closeCode { @synchronized(self) { return _closeCode; } }
- (NSData *)closeReason { @synchronized(self) { return [[_closeReason retain] autorelease]; } }
- (void)resume { @synchronized(self) { if (_state >= NSURLSessionTaskStateCanceling) return; if (_suspensions) --_suspensions; if (!_suspensions) { _state = NSURLSessionTaskStateRunning; [_session _scheduleTask:self]; } } }
- (void)suspend { @synchronized(self) { if (_state >= NSURLSessionTaskStateCanceling) return; ++_suspensions; _state = NSURLSessionTaskStateSuspended; if (_opened) [_session _scheduleTask:self]; } }
- (void)cancel { @synchronized(self) { if (_state >= NSURLSessionTaskStateCanceling) return; _state = NSURLSessionTaskStateCanceling; [_session _scheduleTask:self]; } }
- (void)_setTimeout:(NSTimeInterval)seconds
{
    if (_timer) { CFRunLoopTimerInvalidate(_timer); CFRelease(_timer); _timer = NULL; }
    if (seconds <= 0) return;
    CFRunLoopTimerContext context = { .info = self };
    _timer = CFRunLoopTimerCreate(NULL, CFAbsoluteTimeGetCurrent() + seconds, 0, 0, 0, webSocketTimeout, &context);
    CFRunLoopAddTimer(CFRunLoopGetCurrent(), _timer, kCFRunLoopDefaultMode);
}
- (void)_timeout { @synchronized(self) { [self _finish:webSocketError(NSURLErrorTimedOut, @"The WebSocket handshake timed out")]; } }
- (void)_updateConnection
{
    @synchronized(self) {
        if (_state == NSURLSessionTaskStateCompleted) return;
        if (_state == NSURLSessionTaskStateCanceling) { [self _finish:webSocketError(NSURLErrorCancelled, @"The WebSocket task was cancelled")]; return; }
        if (_state == NSURLSessionTaskStateSuspended) {
            if (_scheduled) { [_input removeFromRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode]; [_output removeFromRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode]; _scheduled = NO; }
            return;
        }
        if (!_opened) {
            _opened = YES;
            NSURL *url = [_request URL]; NSString *scheme = [[url scheme] lowercaseString];
            BOOL secure = [scheme isEqualToString:@"wss"];
            if ((!secure && ![scheme isEqualToString:@"ws"]) || ![[url host] length] || ![[_request HTTPMethod] isEqualToString:@"GET"]) { [self _finish:webSocketError(NSURLErrorBadURL, @"A WebSocket requires a ws/wss URL and a GET request")]; return; }
            NSDictionary *additional = [[_session configuration] HTTPAdditionalHeaders];
            NSDictionary *requestHeaders = [_request allHTTPHeaderFields];
            if (!webSocketHeaders(additional) || !webSocketHeaders(requestHeaders) ||
                !webSocketFieldValue([url host]) || !webSocketFieldValue([url absoluteString])) {
                [self _finish:webSocketError(NSURLErrorBadURL, @"Invalid HTTP header name or value in WebSocket request")]; return;
            }
            uint8_t nonce[16];
            if (SecRandomCopyBytes(kSecRandomDefault, sizeof(nonce), nonce) != 0) { [self _finish:webSocketError(NSURLErrorUnknown, @"Cannot generate a WebSocket handshake nonce")]; return; }
            NSString *key = [[NSData dataWithBytes:nonce length:sizeof(nonce)] base64EncodedStringWithOptions:0];
            NSData *challenge = [[key stringByAppendingString:@"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"] dataUsingEncoding:NSUTF8StringEncoding];
            uint8_t digest[CC_SHA1_DIGEST_LENGTH]; CC_SHA1([challenge bytes], (CC_LONG)[challenge length], digest);
            _accept = [[[NSData dataWithBytes:digest length:sizeof(digest)] base64EncodedStringWithOptions:0] copy];
            CFHTTPMessageRef request = CFHTTPMessageCreateRequest(NULL, CFSTR("GET"), (CFURLRef)url, kCFHTTPVersion1_1);
            for (NSString *name in additional) CFHTTPMessageSetHeaderFieldValue(request, (CFStringRef)name, (CFStringRef)[[additional objectForKey:name] description]);
            for (NSString *name in requestHeaders) CFHTTPMessageSetHeaderFieldValue(request, (CFStringRef)name, (CFStringRef)[requestHeaders objectForKey:name]);
            NSString *host = [url host]; if ([host rangeOfString:@":"].location != NSNotFound) host = [NSString stringWithFormat:@"[%@]", host];
            if ([url port]) host = [NSString stringWithFormat:@"%@:%@", host, [url port]];
            CFHTTPMessageSetHeaderFieldValue(request, CFSTR("Host"), (CFStringRef)host);
            CFHTTPMessageSetHeaderFieldValue(request, CFSTR("Upgrade"), CFSTR("websocket"));
            CFHTTPMessageSetHeaderFieldValue(request, CFSTR("Connection"), CFSTR("Upgrade"));
            CFHTTPMessageSetHeaderFieldValue(request, CFSTR("Sec-WebSocket-Key"), (CFStringRef)key);
            CFHTTPMessageSetHeaderFieldValue(request, CFSTR("Sec-WebSocket-Version"), CFSTR("13"));
            /* Extensions cannot be requested without a corresponding frame decoder. */
            CFHTTPMessageSetHeaderFieldValue(request, CFSTR("Sec-WebSocket-Extensions"), NULL);
            _protocols = (NSString *)CFHTTPMessageCopyHeaderFieldValue(request, CFSTR("Sec-WebSocket-Protocol"));
            if (!webSocketOfferedProtocols(_protocols)) {
                CFRelease(request);
                [self _finish:webSocketError(NSURLErrorBadURL, @"Invalid WebSocket subprotocol offer")]; return;
            }
            __NSCFWebSocketWrite *write = [[[__NSCFWebSocketWrite alloc] init] autorelease];
            write->bytes = (NSData *)CFHTTPMessageCopySerializedMessage(request); CFRelease(request); [_writes insertObject:write atIndex:0];
            CFReadStreamRef input = NULL; CFWriteStreamRef output = NULL;
            CFStreamCreatePairWithSocketToHost(NULL, (CFStringRef)[url host], [url port] ? [[url port] unsignedIntValue] : secure ? 443 : 80, &input, &output);
            _input = (NSInputStream *)input; _output = (NSOutputStream *)output;
            if (!_input || !_output) { [self _finish:webSocketError(NSURLErrorCannotConnectToHost, @"Cannot create WebSocket socket streams")]; return; }
            if (secure) {
                NSDictionary *ssl = @{(id)kCFStreamSSLLevel: (id)kCFStreamSocketSecurityLevelNegotiatedSSL, (id)kCFStreamSSLPeerName: [url host], (id)kCFStreamSSLValidatesCertificateChain: @YES};
                if (![_input setProperty:ssl forKey:(id)kCFStreamPropertySSLSettings] || ![_output setProperty:ssl forKey:(id)kCFStreamPropertySSLSettings]) { [self _finish:webSocketError(NSURLErrorSecureConnectionFailed, @"Cannot enable verified TLS for WebSocket streams")]; return; }
            }
            [_input setDelegate:self]; [_output setDelegate:self];
            [_input scheduleInRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode]; [_output scheduleInRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode]; _scheduled = YES;
            [self _setTimeout:[_request timeoutInterval]];
            [_input open]; [_output open];
        } else if (!_scheduled) {
            if (!_protocolFailure) [_input scheduleInRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode];
            [_output scheduleInRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode];
            _scheduled = YES; _inputPaused = _protocolFailure != nil;
        }
        if (_pendingError) { [self _finish:_pendingError]; return; }
        if (_closing && _upgraded && !_closeTimerStarted) { _closeTimerStarted = YES; [self _setTimeout:5]; }
        if (_inputPaused && !_protocolFailure && (_closing || (![_messages count] && !_deliveries))) {
            [_input scheduleInRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode]; _inputPaused = NO;
        }
        [self _readAvailable];
        [self _flush];
    }
}
- (void)_finish:(NSError *)error
{
    @synchronized(self) {
        if (_state == NSURLSessionTaskStateCompleted) return;
        if (_protocolFailure) error = _protocolFailure;
        if (_state == NSURLSessionTaskStateCanceling) error = webSocketError(NSURLErrorCancelled, @"The WebSocket task was cancelled");
        _state = NSURLSessionTaskStateCompleted; _error = [error retain];
        if (!_closeCode && error) _closeCode = NSURLSessionWebSocketCloseCodeAbnormalClosure;
        [self _setTimeout:0];
        [_input setDelegate:nil]; [_output setDelegate:nil];
        if (_scheduled) { [_input removeFromRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode]; [_output removeFromRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode]; _scheduled = NO; }
        [_input close]; [_output close];
        NSError *pending = error ?: webSocketError(NSURLErrorNetworkConnectionLost, @"The WebSocket is closed");
        __NSCFURLSession *session = _session;
        for (__NSCFWebSocketWrite *write in _writes) if (write->completion) { void (^block)(NSError *) = write->completion; [session addDelegateBlock:^{ block(pending); }]; }
        [_writes removeAllObjects];
        for (void (^block)(NSURLSessionWebSocketMessage *, NSError *) in _receivers) [session addDelegateBlock:^{ block(nil, pending); }];
        [_receivers removeAllObjects]; [_messages removeAllObjects];
        for (void (^block)(NSError *) in _pongs) [session addDelegateBlock:^{ block(pending); }];
        [_pongs removeAllObjects];
        id delegate = self.delegate;
        [session addDelegateBlock:^{
            @try {
                if (_closeReceived && [delegate respondsToSelector:@selector(URLSession:webSocketTask:didCloseWithCode:reason:)]) [delegate URLSession:session webSocketTask:self didCloseWithCode:_closeCode reason:_closeReason];
                if ([delegate respondsToSelector:@selector(URLSession:task:didCompleteWithError:)]) [delegate URLSession:session task:self didCompleteWithError:error];
            } @finally { [session _taskDidComplete:self]; @synchronized(self) { [_session release]; _session = nil; } }
        }];
    }
}
- (void)_frame:(uint8_t)opcode data:(NSData *)data completion:(void (^)(NSError *))completion
{
    uint8_t mask[4];
    if (SecRandomCopyBytes(kSecRandomDefault, sizeof(mask), mask) != 0) {
        NSError *error = webSocketError(NSURLErrorUnknown, @"Cannot generate a WebSocket frame mask");
        if (completion) [_session addDelegateBlock:^{ completion(error); }];
        if (!_pendingError) _pendingError = [error retain];
        [_session _scheduleTask:self];
        return;
    }
    NSUInteger length = [data length]; uint8_t header[14]; NSUInteger count = 2;
    header[0] = 0x80 | opcode;
    if (length < 126) header[1] = 0x80 | length;
    else if (length <= 65535) { header[1] = 0x80 | 126; header[count++] = length >> 8; header[count++] = length; }
    else { header[1] = 0x80 | 127; for (int shift = 56; shift >= 0; shift -= 8) header[count++] = (uint64_t)length >> shift; }
    memcpy(header + count, mask, 4); count += 4;
    NSMutableData *frame = [NSMutableData dataWithLength:count + length]; memcpy([frame mutableBytes], header, count);
    uint8_t *destination = (uint8_t *)[frame mutableBytes] + count; const uint8_t *source = [data bytes];
    for (NSUInteger i = 0; i < length; ++i) destination[i] = source[i] ^ mask[i & 3];
    if (opcode == 10) {
        /* RFC 6455 section 5.5.2 permits replying only to the most recently
         * processed Ping when older replies have not been sent. Never replace
         * a partially written frame: its wire prefix already belongs to it.
         * Thus a blocked peer can retain at most one partial and one unsent
         * automatic Pong, regardless of how many Pings it sends. */
        for (__NSCFWebSocketWrite *pending in _writes) {
            if (pending->automaticPong && !pending->offset) {
                [pending->bytes release]; pending->bytes = [frame retain];
                return;
            }
        }
    }
    __NSCFWebSocketWrite *write = [[[__NSCFWebSocketWrite alloc] init] autorelease];
    write->bytes = [frame retain]; write->completion = [completion copy];
    write->closeFrame = opcode == 8; write->automaticPong = opcode == 10;
    [_writes addObject:write];
}
- (void)sendMessage:(NSURLSessionWebSocketMessage *)message completionHandler:(void (^)(NSError *))completion
{
    @synchronized(self) {
        if (_closing || _state >= NSURLSessionTaskStateCanceling) { NSError *error = _error ?: webSocketError(NSURLErrorNetworkConnectionLost, @"The WebSocket is closed"); dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{ completion(error); }); return; }
        NSData *data = message.type == NSURLSessionWebSocketMessageTypeData ? message.data : [message.string dataUsingEncoding:NSUTF8StringEncoding];
        if (!data) { NSError *error = webSocketError(NSURLErrorUnknown, @"The WebSocket text is not UTF-8"); [_session addDelegateBlock:^{ completion(error); }]; return; }
        [self _frame:message.type == NSURLSessionWebSocketMessageTypeData ? 2 : 1 data:data completion:completion];
        if (_state == NSURLSessionTaskStateRunning) [_session _scheduleTask:self];
    }
}
- (void)receiveMessageWithCompletionHandler:(void (^)(NSURLSessionWebSocketMessage *, NSError *))completion
{
    @synchronized(self) {
        if (_state == NSURLSessionTaskStateCompleted) { NSError *error = _error ?: webSocketError(NSURLErrorNetworkConnectionLost, @"The WebSocket is closed"); dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{ completion(nil, error); }); return; }
        [_receivers addObject:[[completion copy] autorelease]]; [self _deliver];
    }
}
- (void)_deliver
{
    while ([_receivers count] && [_messages count] && !_deliveries) {
        void (^completion)(NSURLSessionWebSocketMessage *, NSError *) = [[[_receivers objectAtIndex:0] retain] autorelease];
        NSURLSessionWebSocketMessage *message = [[[_messages objectAtIndex:0] retain] autorelease];
        [_receivers removeObjectAtIndex:0]; [_messages removeObjectAtIndex:0];
        ++_deliveries;
        [_session addDelegateBlock:^{
            @try { completion(message, nil); }
            @finally {
                @synchronized(self) {
                    --_deliveries;
                    [self _deliver];
                    if (_state == NSURLSessionTaskStateRunning) [_session _scheduleTask:self];
                }
            }
        }];
    }
}
- (void)_sendNextPing
{
    uint8_t nonce[16];
    if (SecRandomCopyBytes(kSecRandomDefault, sizeof(nonce), nonce) != 0) {
        if (!_pendingError) _pendingError = [webSocketError(NSURLErrorUnknown, @"Cannot generate a WebSocket ping nonce") retain];
        [_session _scheduleTask:self]; return;
    }
    [_pingNonce release]; _pingNonce = [[NSData alloc] initWithBytes:nonce length:sizeof(nonce)];
    [self _frame:9 data:_pingNonce completion:nil];
}
- (void)sendPingWithPongReceiveHandler:(void (^)(NSError *))completion
{
    @synchronized(self) {
        if (_closing || _state >= NSURLSessionTaskStateCanceling) { NSError *error = _error ?: webSocketError(NSURLErrorNetworkConnectionLost, @"The WebSocket is closed"); dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{ completion(error); }); return; }
        [_pongs addObject:[[completion copy] autorelease]];
        /* RFC 6455 permits unsolicited pongs: match an unpredictable payload,
         * not merely the next pong opcode, and serialize outstanding pings. */
        if ([_pongs count] == 1) [self _sendNextPing];
        if (_state == NSURLSessionTaskStateRunning) [_session _scheduleTask:self];
    }
}
- (void)cancelWithCloseCode:(NSURLSessionWebSocketCloseCode)code reason:(NSData *)reason
{
    NSString *text = reason ? [[[NSString alloc] initWithData:reason encoding:NSUTF8StringEncoding] autorelease] : @"";
    if (!webSocketCloseCode(code) || [reason length] > 123 || !text) [NSException raise:NSInvalidArgumentException format:@"Invalid WebSocket close code or UTF-8 reason"];
    @synchronized(self) {
        if (_closing || _state >= NSURLSessionTaskStateCanceling) return;
        _closing = YES;
        uint8_t bytes[2] = { code >> 8, code }; NSMutableData *payload = [NSMutableData dataWithBytes:bytes length:2]; if (reason) [payload appendData:reason];
        [self _frame:8 data:payload completion:nil];
        /* A graceful close is also permitted while suspended. */
        _suspensions = 0; _state = NSURLSessionTaskStateRunning; [_session _scheduleTask:self];
    }
}
- (void)_flush
{
    while ([_writes count] && [_output hasSpaceAvailable]) {
        __NSCFWebSocketWrite *write = [[[_writes objectAtIndex:0] retain] autorelease];
        /* The HTTP handshake is the only unframed record. */
        if (!_upgraded && _sent > 0 && write->offset == 0) return;
        NSInteger count = [_output write:(const uint8_t *)[write->bytes bytes] + write->offset maxLength:[write->bytes length] - write->offset];
        if (count < 0) { [self _finish:[_output streamError] ?: webSocketError(NSURLErrorNetworkConnectionLost, @"WebSocket write failed")]; return; }
        if (!count) return;
        write->offset += count; _sent += count;
        if (write->offset != [write->bytes length]) continue;
        BOOL close = write->closeFrame;
        void (^completion)(NSError *) = [[write->completion copy] autorelease];
        [_writes removeObjectAtIndex:0];
        if (completion) [_session addDelegateBlock:^{ completion(nil); }];
        if (close) {
            _closeSent = YES;
            if (_protocolFailure || _closeReceived) { [self _finish:_protocolFailure]; return; }
        }
        if (!_upgraded) return;
    }
}
- (void)stream:(NSStream *)stream handleEvent:(NSStreamEvent)event
{
    @synchronized(self) {
        if (_state >= NSURLSessionTaskStateCanceling) return;
        if (event == NSStreamEventErrorOccurred) { [self _finish:[stream streamError] ?: webSocketError(NSURLErrorNetworkConnectionLost, @"WebSocket stream failed")]; return; }
        if (event == NSStreamEventEndEncountered) { [self _finish:(_closeReceived && _closeSent) ? nil : webSocketError(NSURLErrorNetworkConnectionLost, @"WebSocket ended without a close handshake")]; return; }
        if (event == NSStreamEventHasBytesAvailable) [self _readAvailable];
        if (event == NSStreamEventHasSpaceAvailable || event == NSStreamEventOpenCompleted) [self _flush];
    }
}
- (void)_readAvailable
{
    @autoreleasepool { [self _consume]; }
    uint8_t bytes[16384];
    NSUInteger reads = 16;
    while (reads && !_protocolFailure && _state == NSURLSessionTaskStateRunning && (_closing || (![_messages count] && !_deliveries)) && [_input hasBytesAvailable]) {
        --reads;
        /* Bound transient autoreleased frame/payload objects even when a peer
         * continuously supplies control frames and the run loop cannot drain. */
        @autoreleasepool {
            NSInteger count = [_input read:bytes maxLength:sizeof(bytes)];
            if (count < 0) { [self _finish:[_input streamError] ?: webSocketError(NSURLErrorNetworkConnectionLost, @"WebSocket read failed")]; return; }
            if (!count) break;
            _received += count; [_incoming appendBytes:bytes length:count]; [self _consume];
        }
    }
    /* Yield the task lock/run loop between bounded batches so command traffic,
     * especially cancellation, cannot be starved by an always-readable peer. */
    if (!reads && !_protocolFailure && _state == NSURLSessionTaskStateRunning && (_closing || (![_messages count] && !_deliveries)) && [_input hasBytesAvailable])
        [_session _scheduleTask:self];
    /* One completed message plus at most one partial message/read chunk is
     * retained. Stop stream delivery as well as reads, avoiding a hot run loop.
     * Already-buffered control frames are parsed even with a pending message;
     * later TCP bytes are backpressured until receive drains it. Graceful close
     * explicitly resumes reads so its handshake does not depend on client demand. */
    if (_state == NSURLSessionTaskStateRunning && !_closing && ([_messages count] || _deliveries) && !_inputPaused) {
        [_input removeFromRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode]; _inputPaused = YES;
    }
}
- (void)_protocolError:(NSString *)description closeCode:(uint16_t)code
{
    if (_protocolFailure) return;
    NSError *error = webSocketError(NSURLErrorBadServerResponse, description);
    if (!_upgraded || _closeSent) { [self _finish:error]; return; }
    _protocolFailure = [error retain];
    _closing = YES;
    /* Finish an already-started wire frame, discard unsent application data,
     * then drain a masked RFC 6455 failure Close before ending the transport.
     * A blocked peer still has the ordinary bounded close deadline. */
    NSUInteger keep = [_writes count] && ((__NSCFWebSocketWrite *)[_writes objectAtIndex:0])->offset ? 1 : 0;
    for (__NSCFWebSocketWrite *write in _writes) {
        if (write->completion) {
            void (^completion)(NSError *) = [[write->completion copy] autorelease];
            [write->completion release]; write->completion = nil;
            [_session addDelegateBlock:^{ completion(error); }];
        }
    }
    while ([_writes count] > keep) [_writes removeObjectAtIndex:keep];
    if (!keep || !((__NSCFWebSocketWrite *)[_writes objectAtIndex:0])->closeFrame) {
        uint8_t payload[2] = { code >> 8, code };
        [self _frame:8 data:[NSData dataWithBytes:payload length:sizeof(payload)] completion:nil];
    }
    if (_scheduled && !_inputPaused) {
        [_input removeFromRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode];
        _inputPaused = YES;
    }
    [_incoming setLength:0]; _incomingOffset = 0;
    [_fragment release]; _fragment = nil; _fragmentOpcode = 0;
    _closeTimerStarted = YES; [self _setTimeout:5];
    [self _flush];
}
- (void)_consume
{
    if (!_upgraded) {
        if ([_incoming length] > 65536) { [self _protocolError:@"WebSocket upgrade headers exceed 64 KiB" closeCode:1002]; return; }
        const uint8_t *bytes = [_incoming bytes]; NSUInteger length = [_incoming length], end = 0;
        for (NSUInteger i = 3; i < length; ++i) if (bytes[i-3] == '\r' && bytes[i-2] == '\n' && bytes[i-1] == '\r' && bytes[i] == '\n') { end = i + 1; break; }
        if (!end) return;
        CFHTTPMessageRef response = CFHTTPMessageCreateEmpty(NULL, false);
        BOOL valid = CFHTTPMessageAppendBytes(response, bytes, end) && CFHTTPMessageIsHeaderComplete(response) && CFHTTPMessageGetResponseStatusCode(response) == 101;
        NSString *upgrade = [(id)CFHTTPMessageCopyHeaderFieldValue(response, CFSTR("Upgrade")) autorelease];
        NSString *connection = [(id)CFHTTPMessageCopyHeaderFieldValue(response, CFSTR("Connection")) autorelease];
        NSString *accept = [(id)CFHTTPMessageCopyHeaderFieldValue(response, CFSTR("Sec-WebSocket-Accept")) autorelease];
        NSString *extensions = [(id)CFHTTPMessageCopyHeaderFieldValue(response, CFSTR("Sec-WebSocket-Extensions")) autorelease];
        NSString *protocol = [(id)CFHTTPMessageCopyHeaderFieldValue(response, CFSTR("Sec-WebSocket-Protocol")) autorelease];
        NSString *version = [(id)CFHTTPMessageCopyVersion(response) autorelease];
        /* Upgrade's value is the websocket protocol, whereas Connection is a
         * token list. RFC 6455 section 4.1 requires that distinction. */
        valid = valid && [version isEqualToString:@"HTTP/1.1"] && [upgrade length] &&
            [[upgrade stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]] caseInsensitiveCompare:@"websocket"] == NSOrderedSame &&
            webSocketToken(connection, @"Upgrade") && [accept isEqualToString:_accept] && ![extensions length];
        if (protocol) valid = valid && webSocketProtocol(_protocols, protocol);
        NSDictionary *fields = [(id)CFHTTPMessageCopyAllHeaderFields(response) autorelease];
        _response = [[NSHTTPURLResponse alloc] initWithURL:[_request URL] statusCode:CFHTTPMessageGetResponseStatusCode(response) HTTPVersion:@"HTTP/1.1" headerFields:fields];
        CFRelease(response);
        if (!valid) { [self _protocolError:@"The server did not provide a valid WebSocket HTTP upgrade" closeCode:1002]; return; }
        _upgraded = YES; _incomingOffset = end; [self _setTimeout:_closing ? 5 : 0];
        id delegate = self.delegate; __NSCFURLSession *session = _session;
        if ([delegate respondsToSelector:@selector(URLSession:webSocketTask:didOpenWithProtocol:)]) [session addDelegateBlock:^{ [delegate URLSession:session webSocketTask:self didOpenWithProtocol:protocol]; }];
        [self _flush];
    }
    while (_state != NSURLSessionTaskStateCompleted) {
        const uint8_t *bytes = (const uint8_t *)[_incoming bytes] + _incomingOffset; NSUInteger available = [_incoming length] - _incomingOffset;
        if (available < 2) break;
        uint8_t opcode = bytes[0] & 15; BOOL final = bytes[0] & 0x80; uint64_t length = bytes[1] & 127; NSUInteger header = 2;
        if (!_closing && opcode < 8 && ([_messages count] || _deliveries)) break;
        if ((bytes[0] & 0x70) || (bytes[1] & 0x80) || (opcode > 2 && opcode < 8) || opcode > 10) { [self _protocolError:@"Invalid WebSocket frame flags or opcode" closeCode:1002]; return; }
        if (length == 126) { if (available < 4) break; length = ((uint16_t)bytes[2] << 8) | bytes[3]; header = 4; if (length < 126) { [self _protocolError:@"Noncanonical WebSocket length" closeCode:1002]; return; } }
        else if (length == 127) { if (available < 10) break; length = 0; for (int i = 2; i < 10; ++i) length = (length << 8) | bytes[i]; header = 10; if (length <= 65535 || (length >> 63)) { [self _protocolError:@"Invalid WebSocket 64-bit length" closeCode:1002]; return; } }
        if (opcode >= 8 && (!final || length > 125)) { [self _protocolError:@"Invalid WebSocket control frame" closeCode:1002]; return; }
        if (opcode < 8 && (length >= (uint64_t)_maximum || [_fragment length] >= (uint64_t)_maximum - length)) { [self _protocolError:@"WebSocket message reaches maximumMessageSize" closeCode:1009]; return; }
        if (length > available - header) break;
        NSData *payload = [NSData dataWithBytes:bytes + header length:(NSUInteger)length]; _incomingOffset += header + length;
        if (opcode == 8) {
            if (length == 1) { [self _protocolError:@"Invalid WebSocket close payload" closeCode:1002]; return; }
            NSUInteger code = length ? ((uint16_t)bytes[header] << 8) | bytes[header+1] : 1005;
            NSData *reason = length > 2 ? [payload subdataWithRange:NSMakeRange(2, length - 2)] : nil;
            NSString *text = reason ? [[[NSString alloc] initWithData:reason encoding:NSUTF8StringEncoding] autorelease] : @"";
            if (length && !webSocketCloseCode(code)) { [self _protocolError:@"Invalid WebSocket close code" closeCode:1002]; return; }
            if (!text) { [self _protocolError:@"Invalid WebSocket UTF-8 close reason" closeCode:1007]; return; }
            _closeCode = code; _closeReason = [reason copy]; _closeReceived = YES;
            if (!_closing) {
                _closing = YES;
                /* A peer close may finish the currently-partial frame, but no
                 * queued application message may start after receiving it. */
                NSUInteger keep = [_writes count] && ((__NSCFWebSocketWrite *)[_writes objectAtIndex:0])->offset ? 1 : 0;
                while ([_writes count] > keep) {
                    __NSCFWebSocketWrite *write = [_writes objectAtIndex:keep];
                    if (write->completion) {
                        void (^completion)(NSError *) = [[write->completion copy] autorelease];
                        NSError *error = webSocketError(NSURLErrorNetworkConnectionLost, @"The peer closed the WebSocket");
                        [_session addDelegateBlock:^{ completion(error); }];
                    }
                    [_writes removeObjectAtIndex:keep];
                }
                [self _frame:8 data:payload completion:nil]; _closeTimerStarted = YES; [self _setTimeout:5];
            }
            [self _flush]; if (_closeSent) [self _finish:nil]; return;
        } else if (opcode == 9) { if (!_closeSent) { [self _frame:10 data:payload completion:nil]; [self _flush]; } }
        else if (opcode == 10) {
            if ([_pongs count] && [payload isEqualToData:_pingNonce]) {
                void (^completion)(NSError *) = [[[_pongs objectAtIndex:0] retain] autorelease]; [_pongs removeObjectAtIndex:0]; [_session addDelegateBlock:^{ completion(nil); }];
                [_pingNonce release]; _pingNonce = nil;
                if ([_pongs count]) { [self _sendNextPing]; [self _flush]; }
            }
        } else {
            if (_closing) continue;
            if ((opcode == 0 && !_fragmentOpcode) || (opcode != 0 && _fragmentOpcode)) { [self _protocolError:@"Invalid WebSocket continuation sequence" closeCode:1002]; return; }
            if (opcode) { _fragmentOpcode = opcode; _fragment = [[NSMutableData alloc] init]; }
            [_fragment appendData:payload];
            if (final) {
                NSURLSessionWebSocketMessage *message;
                if (_fragmentOpcode == 1) {
                    NSString *text = [[[NSString alloc] initWithData:_fragment encoding:NSUTF8StringEncoding] autorelease];
                    if (!text) { [self _protocolError:@"WebSocket text is not valid UTF-8" closeCode:1007]; return; }
                    message = [[[NSURLSessionWebSocketMessage alloc] initWithString:text] autorelease];
                } else message = [[[NSURLSessionWebSocketMessage alloc] initWithData:_fragment] autorelease];
                [_fragment release]; _fragment = nil; _fragmentOpcode = 0;
                [_messages addObject:message]; [self _deliver];
            }
        }
    }
    if (_incomingOffset) { [_incoming replaceBytesInRange:NSMakeRange(0, _incomingOffset) withBytes:NULL length:0]; _incomingOffset = 0; }
}
@end
