/* Darling native session download provider.
 * Provenance: the vendored Apportable NSURLConnection/CFURLConnection transport
 * and __NSCFURLSession worker own HTTP, TLS, redirects and scheduling. The newer
 * swift-corelibs-foundation URLSession uses separately named Swift/curl classes,
 * not this Objective-C class cluster; the VibeDarling Foundation header declares
 * downloads but supplies no reusable provider. This adapter streams to a secure
 * temporary file, never an in-memory full-response intermediary. Like Apple's
 * completion-handler API, the file exists only until the callback returns.
 */
#import <Foundation/NSFileManager.h>
#import <Foundation/NSPathUtilities.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

@interface __NSCFURLSessionDownloadTask : NSURLSessionDownloadTask <NSURLConnectionDataDelegate> {
    __NSCFURLSession *_session;
    NSURLRequest *_request;
    NSURLRequest *_current;
    NSURLResponse *_response;
    NSError *_error;
    id<NSURLSessionTaskDelegate> _delegate;
    NSString *_description;
    NSUInteger _identifier, _suspensions;
    NSURLSessionTaskState _state;
    NSURLConnection *_connection;
    BOOL _scheduled, _started;
    int _fd;
    NSURL *_location;
    int64_t _received, _sent;
    void (^_completion)(NSURL *, NSURLResponse *, NSError *);
}
- (id)initWithSession:(__NSCFURLSession *)session request:(NSURLRequest *)request identifier:(NSUInteger)identifier completion:(void (^)(NSURL *, NSURLResponse *, NSError *))completion;
- (void)_updateConnection;
- (void)_finish:(NSError *)error;
@end

NSURLSessionDownloadTask *_NSCFURLSessionCreateDownloadTask(__NSCFURLSession *session, NSURLRequest *request, NSUInteger identifier, void (^completion)(NSURL *, NSURLResponse *, NSError *))
{
    return [[__NSCFURLSessionDownloadTask alloc] initWithSession:session request:request identifier:identifier completion:completion];
}

@implementation __NSCFURLSessionDownloadTask
- (id)initWithSession:(__NSCFURLSession *)session request:(NSURLRequest *)request identifier:(NSUInteger)identifier completion:(void (^)(NSURL *, NSURLResponse *, NSError *))completion
{
    if ((self = [super init])) {
        _session = [session retain];
        _request = [request copy];
        _current = [_request retain];
        _identifier = identifier;
        _suspensions = 1;
        _state = NSURLSessionTaskStateSuspended;
        _fd = -1;
        _completion = [completion copy];
    }
    return self;
}
- (void)dealloc
{
    if (_fd >= 0) close(_fd);
    if (_location) unlink([[_location path] fileSystemRepresentation]);
    [_session release]; [_request release]; [_current release]; [_response release];
    [_error release]; [_delegate release]; [_description release]; [_location release];
    [_completion release]; [_connection release];
    [super dealloc];
}
- (id)copyWithZone:(NSZone *)zone { return [self retain]; }
- (NSUInteger)taskIdentifier { return _identifier; }
- (NSURLRequest *)originalRequest { return _request; }
- (NSURLRequest *)currentRequest { @synchronized(self) { return [[_current retain] autorelease]; } }
- (NSURLResponse *)response { @synchronized(self) { return [[_response retain] autorelease]; } }
- (NSError *)error { @synchronized(self) { return [[_error retain] autorelease]; } }
- (NSURLSessionTaskState)state { @synchronized(self) { return _state; } }
- (int64_t)countOfBytesReceived { @synchronized(self) { return _received; } }
- (int64_t)countOfBytesSent { @synchronized(self) { return _sent; } }
- (int64_t)countOfBytesExpectedToSend { return [[_request HTTPBody] length]; }
- (int64_t)countOfBytesExpectedToReceive { @synchronized(self) { return _response ? [_response expectedContentLength] : NSURLSessionTransferSizeUnknown; } }
- (NSString *)taskDescription { @synchronized(self) { return [[_description retain] autorelease]; } }
- (void)setTaskDescription:(NSString *)value { @synchronized(self) { NSString *copy = [value copy]; [_description release]; _description = copy; } }
- (id<NSURLSessionTaskDelegate>)delegate { @synchronized(self) { return [[(_delegate ?: (id)_session.delegate) retain] autorelease]; } }
- (void)setDelegate:(id<NSURLSessionTaskDelegate>)value { @synchronized(self) { [value retain]; [_delegate release]; _delegate = value; } }
- (void)resume { @synchronized(self) { if (_state >= NSURLSessionTaskStateCanceling) return; if (_suspensions) --_suspensions; if (!_suspensions) { _state = NSURLSessionTaskStateRunning; [_session _scheduleTask:self]; } } }
- (void)suspend { @synchronized(self) { if (_state >= NSURLSessionTaskStateCanceling) return; ++_suspensions; _state = NSURLSessionTaskStateSuspended; if (_started) [_session _scheduleTask:self]; } }
- (void)cancel { @synchronized(self) { if (_state >= NSURLSessionTaskStateCanceling) return; _state = NSURLSessionTaskStateCanceling; [_session _scheduleTask:self]; } }
- (void)cancelByProducingResumeData:(void (^)(NSData *))completion
{
    /* Resume data is optional in Apple's API. No opaque fake resume record. */
    @synchronized(self) {
        if (_session) [_session addDelegateBlock:^{ completion(nil); }];
        else dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{ completion(nil); });
        [self cancel];
    }
}
- (void)_updateConnection
{
    @synchronized(self) {
        if (_state == NSURLSessionTaskStateCompleted) return;
        if (_state == NSURLSessionTaskStateCanceling) { [self _finish:[NSError errorWithDomain:NSURLErrorDomain code:NSURLErrorCancelled userInfo:nil]]; return; }
        if (_state == NSURLSessionTaskStateSuspended) {
            if (_scheduled) { [_connection unscheduleFromRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode]; _scheduled = NO; }
            return;
        }
        if (!_started) {
            _started = YES;
            NSString *scheme = [[[_current URL] scheme] lowercaseString];
            if (![scheme isEqualToString:@"http"] && ![scheme isEqualToString:@"https"]) { [self _finish:[NSError errorWithDomain:NSURLErrorDomain code:NSURLErrorUnsupportedURL userInfo:nil]]; return; }
            NSString *pattern = [NSTemporaryDirectory() stringByAppendingPathComponent:@"NSURLSession-XXXXXX"];
            char *path = strdup([pattern fileSystemRepresentation]);
            if (!path) { [self _finish:[NSError errorWithDomain:NSPOSIXErrorDomain code:ENOMEM userInfo:nil]]; return; }
            _fd = mkstemp(path);
            int saved = errno;
            if (_fd >= 0) { fcntl(_fd, F_SETFD, FD_CLOEXEC); _location = [[NSURL fileURLWithPath:[NSString stringWithUTF8String:path]] retain]; }
            free(path);
            if (_fd < 0) { [self _finish:[NSError errorWithDomain:NSPOSIXErrorDomain code:saved userInfo:nil]]; return; }
            _connection = [[NSURLConnection alloc] initWithRequest:_current delegate:self startImmediately:NO];
            if (![_connection _setSessionConfiguration:[_session configuration]]) {
                [_connection release]; _connection = nil;
                [self _finish:[NSError errorWithDomain:NSURLErrorDomain code:NSURLErrorUnsupportedURL userInfo:nil]]; return;
            }
            [_connection scheduleInRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode]; _scheduled = YES;
            [_connection start];
        } else if (!_scheduled) {
            [_connection scheduleInRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode]; _scheduled = YES;
        }
    }
}
- (void)_finish:(NSError *)error
{
    @synchronized(self) {
        if (_state == NSURLSessionTaskStateCompleted) return;
        if (_state == NSURLSessionTaskStateCanceling) error = [NSError errorWithDomain:NSURLErrorDomain code:NSURLErrorCancelled userInfo:nil];
        if (_fd >= 0) { if (close(_fd) < 0 && !error) error = [NSError errorWithDomain:NSPOSIXErrorDomain code:errno userInfo:nil]; _fd = -1; }
        _error = [error retain]; _state = NSURLSessionTaskStateCompleted;
        if (_scheduled) { [_connection unscheduleFromRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode]; _scheduled = NO; }
        [_connection cancel]; [_connection release]; _connection = nil;
        __NSCFURLSession *session = _session;
        id delegate = self.delegate;
        void (^completion)(NSURL *, NSURLResponse *, NSError *) = [_completion copy];
        [_completion release]; _completion = nil;
        [session addDelegateBlock:^{
            @try {
                if (completion) completion(error ? nil : _location, _response, error);
                else {
                    if (!error && [delegate respondsToSelector:@selector(URLSession:downloadTask:didFinishDownloadingToURL:)])
                        [delegate URLSession:session downloadTask:self didFinishDownloadingToURL:_location];
                    if ([delegate respondsToSelector:@selector(URLSession:task:didCompleteWithError:)])
                        [delegate URLSession:session task:self didCompleteWithError:error];
                }
            } @finally {
                if (_location) unlink([[_location path] fileSystemRepresentation]);
                [session _taskDidComplete:self];
                @synchronized(self) { [_session release]; _session = nil; }
            }
        }];
        [completion release];
    }
}
- (NSURLRequest *)connection:(NSURLConnection *)connection willSendRequest:(NSURLRequest *)request redirectResponse:(NSURLResponse *)response
{
    @synchronized(self) {
        if (_state >= NSURLSessionTaskStateCanceling) return nil;
        NSURLRequest *copy = [request copy]; [_current release]; _current = copy; return request;
    }
}
- (void)connection:(NSURLConnection *)connection didReceiveResponse:(NSURLResponse *)response
{
    @synchronized(self) {
        if (_state >= NSURLSessionTaskStateCanceling) return;
        [_response release]; _response = [response retain];
        if (ftruncate(_fd, 0) < 0 || lseek(_fd, 0, SEEK_SET) < 0) [self _finish:[NSError errorWithDomain:NSPOSIXErrorDomain code:errno userInfo:nil]];
        _received = 0;
    }
}
- (void)connection:(NSURLConnection *)connection didReceiveData:(NSData *)data
{
    @synchronized(self) {
        if (_state >= NSURLSessionTaskStateCanceling) return;
        const uint8_t *bytes = [data bytes]; NSUInteger remaining = [data length];
        while (remaining) {
            ssize_t count = write(_fd, bytes, remaining);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) { [self _finish:[NSError errorWithDomain:NSPOSIXErrorDomain code:count < 0 ? errno : EIO userInfo:nil]]; return; }
            remaining -= count; bytes += count;
        }
        _received += [data length];
        int64_t written = [data length], total = _received, expected = [_response expectedContentLength];
        id delegate = self.delegate;
        if ([delegate respondsToSelector:@selector(URLSession:downloadTask:didWriteData:totalBytesWritten:totalBytesExpectedToWrite:)]) {
            __NSCFURLSession *session = _session;
            [session addDelegateBlock:^{ [delegate URLSession:session downloadTask:self didWriteData:written totalBytesWritten:total totalBytesExpectedToWrite:expected]; }];
        }
    }
}
- (void)connectionDidFinishLoading:(NSURLConnection *)connection { [self _finish:nil]; }
- (void)connection:(NSURLConnection *)connection didFailWithError:(NSError *)error { [self _finish:error]; }
- (void)connection:(NSURLConnection *)connection didSendBodyData:(NSInteger)bytes totalBytesWritten:(NSInteger)total totalBytesExpectedToWrite:(NSInteger)expected { @synchronized(self) { _sent = total; } }
@end
