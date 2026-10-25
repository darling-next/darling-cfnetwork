//
//  NSURLSession.m
//  Foundation
//
//  Copyright (c) 2014 Apportable. All rights reserved.
//

#import "NSCFURLSession.h"
#import <Foundation/NSURLConnection.h>
#import <Foundation/NSData.h>
#import <Foundation/NSError.h>
#import <Foundation/NSURLError.h>
#import <Foundation/NSRunLoop.h>
#import <Foundation/NSURL.h>
#import <Foundation/NSURLResponse.h>
#import <Foundation/NSString.h>

const int64_t NSURLSessionTransferSizeUnknown = -1LL;
NSString* const NSURLSessionDownloadTaskResumeData = @"NSURLSessionDownloadTaskResumeData";

@implementation NSURLSession
// The concrete class cluster owns storage and implements these accessors.
@dynamic delegateQueue, delegate, configuration, sessionDescription;

+ (id)allocWithZone:(NSZone *)zone
{
    if (self == [NSURLSession class])
        return [__NSCFURLSession allocWithZone:zone];
    return [super allocWithZone:zone];
}

+ (NSURLSession *)sharedSession
{
    return [__NSCFURLSession sharedSession];
}

+ (NSURLSession *)sessionWithConfiguration:(NSURLSessionConfiguration *)configuration
{
    return [__NSCFURLSession sessionWithConfiguration:configuration];
}

+ (NSURLSession *)sessionWithConfiguration:(NSURLSessionConfiguration *)configuration delegate:(id<NSURLSessionDelegate>)delegate delegateQueue:(NSOperationQueue *)queue
{
    return [__NSCFURLSession sessionWithConfiguration:configuration delegate:delegate delegateQueue:queue];
}

@end

@implementation NSURLSessionTask
@dynamic taskIdentifier, originalRequest, currentRequest, response, error;
@dynamic countOfBytesReceived, countOfBytesSent, countOfBytesExpectedToSend, countOfBytesExpectedToReceive;
@dynamic taskDescription, state;
@end

@implementation NSURLSessionDataTask
@end

@implementation NSURLSessionDownloadTask
@end

@implementation NSURLSessionUploadTask
@end

/*
 * This provider reuses this component's Apportable NSURLConnection.m /
 * CFURLConnection.c transport, rather than importing the GNUstep LGPL session
 * implementation or Swift corelibs' separately named URLSession classes.
 * The existing transport owns HTTP, TLS, redirects, caching and stream I/O.
 *
 * NSURLConnection's callbacks run on its scheduled run loop; setting a delegate
 * queue does NOT create that run loop. Tasks share their session's lazy worker
 * run loop and command source, which remains scheduled during suspension.
 * Suspension unschedules delivery (the CF transport may continue buffering).
 * Cancellation explicitly completes: NSURLConnection's cancel only invalidates
 * the connection and does not deliver a cancellation callback.
 *
 * Only data tasks are provided here. This does not implement download/upload
 * tasks, private HTTPUtilities policies, host overrides, or Network activities.
 */
@interface __NSCFURLSessionDataTask : NSURLSessionDataTask <NSURLConnectionDataDelegate> {
    __NSCFURLSession *_session;
    NSURLRequest *_originalRequest;
    NSURLRequest *_currentRequest;
    NSURLResponse *_response;
    NSError *_error;
    NSString *_taskDescription;
    NSUInteger _identifier;
    NSURLSessionTaskState _state;
    NSUInteger _suspendCount;
    BOOL _workerScheduled;
    NSURLConnection *_connection;
    BOOL _connectionScheduled;
    NSMutableData *_data;
    void (^_completion)(NSData *, NSURLResponse *, NSError *);
    int64_t _received;
    int64_t _sent;
    int64_t _expectedToSend;
    BOOL _responsePending;
    BOOL _responseRejected;
    BOOL _loadingFinished;
    NSError *_loadingError;
}
- (id)initWithSession:(__NSCFURLSession *)session request:(NSURLRequest *)request identifier:(NSUInteger)identifier completion:(void (^)(NSData *, NSURLResponse *, NSError *))completion;
- (void)_signalWorker;
- (void)_updateConnection;
- (void)_finishWithError:(NSError *)error;
@end


NSURLSessionDataTask *_NSCFURLSessionCreateDataTask(__NSCFURLSession *session,
    NSURLRequest *request, NSUInteger identifier,
    void (^completion)(NSData *, NSURLResponse *, NSError *))
{
    return [[__NSCFURLSessionDataTask alloc] initWithSession:session request:request
        identifier:identifier completion:completion];
}

@implementation __NSCFURLSessionDataTask

- (id)initWithSession:(__NSCFURLSession *)session request:(NSURLRequest *)request identifier:(NSUInteger)identifier completion:(void (^)(NSData *, NSURLResponse *, NSError *))completion
{
    if ((self = [super init])) {
        _session = [session retain];
        _originalRequest = [request copy];
        _currentRequest = [_originalRequest retain];
        _identifier = identifier;
        _completion = [completion copy];
        if (completion)
            _data = [[NSMutableData alloc] init];
        _state = NSURLSessionTaskStateSuspended;
        _suspendCount = 1;
        _expectedToSend = [request HTTPBody] ? [[request HTTPBody] length] : 0;
        if ([request HTTPBodyStream])
            _expectedToSend = NSURLSessionTransferSizeUnknown;
    }
    return self;
}

- (void)dealloc
{
    [_session release];
    [_originalRequest release];
    [_currentRequest release];
    [_response release];
    [_error release];
    [_taskDescription release];
    [_data release];
    [_completion release];
    [_loadingError release];
    [super dealloc];
}

- (id)copyWithZone:(NSZone *)zone { return [self retain]; }
- (NSUInteger)taskIdentifier { return _identifier; }
- (NSURLRequest *)originalRequest { return _originalRequest; }
- (NSURLRequest *)currentRequest { @synchronized (self) { return [[_currentRequest retain] autorelease]; } }
- (NSURLResponse *)response { @synchronized (self) { return [[_response retain] autorelease]; } }
- (NSError *)error { @synchronized (self) { return [[_error retain] autorelease]; } }
- (NSURLSessionTaskState)state { @synchronized (self) { return _state; } }
- (int64_t)countOfBytesReceived { @synchronized (self) { return _received; } }
- (int64_t)countOfBytesSent { @synchronized (self) { return _sent; } }
- (int64_t)countOfBytesExpectedToSend { @synchronized (self) { return _expectedToSend; } }
- (int64_t)countOfBytesExpectedToReceive
{
    @synchronized (self) {
        return _response ? [_response expectedContentLength] : NSURLSessionTransferSizeUnknown;
    }
}
- (NSString *)taskDescription { @synchronized (self) { return [[_taskDescription retain] autorelease]; } }
- (void)setTaskDescription:(NSString *)description
{
    @synchronized (self) {
        NSString *copy = [description copy];
        [_taskDescription release];
        _taskDescription = copy;
    }
}

/* Called with the task lock held; the session publishes its worker atomically. */
- (void)_signalWorker
{
    _workerScheduled = YES;
    [_session _scheduleTask:self];
}

- (void)resume
{
    @synchronized (self) {
        if (_state == NSURLSessionTaskStateCanceling || _state == NSURLSessionTaskStateCompleted)
            return;
        if (_suspendCount)
            --_suspendCount;
        if (!_suspendCount) {
            _state = NSURLSessionTaskStateRunning;
            [self _signalWorker];
        }
    }
}

- (void)suspend
{
    @synchronized (self) {
        if (_state == NSURLSessionTaskStateCanceling || _state == NSURLSessionTaskStateCompleted)
            return;
        ++_suspendCount;
        _state = NSURLSessionTaskStateSuspended;
        if (_workerScheduled)
            [self _signalWorker];
    }
}

- (void)cancel
{
    @synchronized (self) {
        if (_state == NSURLSessionTaskStateCanceling || _state == NSURLSessionTaskStateCompleted)
            return;
        _state = NSURLSessionTaskStateCanceling;
        [self _signalWorker];
    }
}


- (void)_updateConnection
{
    @synchronized (self) {
        if (_state == NSURLSessionTaskStateCompleted)
            return;
        if (_state == NSURLSessionTaskStateCanceling || _responseRejected) {
            [self _finishWithError:[NSError errorWithDomain:NSURLErrorDomain code:NSURLErrorCancelled userInfo:nil]];
            return;
        }
        if (_state == NSURLSessionTaskStateSuspended) {
            if (_connectionScheduled) {
                [_connection unscheduleFromRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode];
                _connectionScheduled = NO;
            }
            return;
        }
        if (!_connection) {
            NSURL *url = [_currentRequest URL];
            NSString *scheme = [[url scheme] lowercaseString];
            if (!url || (![scheme isEqualToString:@"http"] && ![scheme isEqualToString:@"https"])) {
                [self _finishWithError:[NSError errorWithDomain:NSURLErrorDomain
                    code:url ? NSURLErrorUnsupportedURL : NSURLErrorBadURL userInfo:nil]];
                return;
            }
            _connection = [[NSURLConnection alloc] initWithRequest:_currentRequest delegate:self startImmediately:NO];
            [_connection scheduleInRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode];
            _connectionScheduled = YES;
            [_connection start];
        } else if (!_connectionScheduled && !_loadingFinished) {
            [_connection scheduleInRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode];
            _connectionScheduled = YES;
        }
        if (!_completion && !_responsePending && [_data length]) {
            NSData *data = _data;
            _data = nil;
            [_session delegate_dataTask:self didReceiveData:data];
            [data release];
        }
        if (_loadingFinished && !_responsePending)
            [self _finishWithError:_loadingError];
    }
}

- (void)_finishWithError:(NSError *)error
{
    @synchronized (self) {
        if (_state == NSURLSessionTaskStateCompleted)
            return;
        /* Cancellation wins once cancel has acquired the task lock. */
        if (_state == NSURLSessionTaskStateCanceling)
            error = [NSError errorWithDomain:NSURLErrorDomain code:NSURLErrorCancelled userInfo:nil];
        _error = [error retain];
        _state = NSURLSessionTaskStateCompleted;
        if (_connectionScheduled) {
            [_connection unscheduleFromRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode];
            _connectionScheduled = NO;
        }
        [_connection cancel];
        [_connection release];
        _connection = nil;
        __NSCFURLSession *session = _session;
        void (^completion)(NSData *, NSURLResponse *, NSError *) = [_completion copy];
        [_completion release];
        _completion = nil;
        /* Transfer the now-frozen builder; no full-response copy is needed. */
        NSData *data = error ? nil : _data;
        if (error)
            [_data release];
        _data = nil;
        NSURLResponse *response = _response;
        [session addDelegateBlock:^{
            @try {
                if (completion)
                    completion(data, response, error);
                else if ([session.delegate respondsToSelector:@selector(URLSession:task:didCompleteWithError:)])
                    [(id<NSURLSessionTaskDelegate>)session.delegate URLSession:session task:self didCompleteWithError:error];
            } @finally {
                [session _taskDidComplete:self];
                @synchronized (self) {
                    [_session release];
                    _session = nil;
                }
            }
        }];
        [data release];
        [completion release];
    }
}

- (NSURLRequest *)connection:(NSURLConnection *)connection willSendRequest:(NSURLRequest *)request redirectResponse:(NSURLResponse *)response
{
    @synchronized (self) {
        if (_state == NSURLSessionTaskStateCanceling || _state == NSURLSessionTaskStateCompleted)
            return nil;
        NSURLRequest *copy = [request copy];
        [_currentRequest release];
        _currentRequest = copy;
        return request;
    }
}

- (void)connection:(NSURLConnection *)connection didReceiveResponse:(NSURLResponse *)response
{
    @synchronized (self) {
        if (_state == NSURLSessionTaskStateCanceling || _state == NSURLSessionTaskStateCompleted)
            return;
        [_response release];
        _response = [response retain];
        [_data setLength:0];
        if (!_completion && [_session can_delegate_dataTask_didReceiveResponse]) {
            _responsePending = YES;
            [_session delegate_dataTask:self didReceiveResponse:response completionHandler:^(NSURLSessionResponseDisposition disposition) {
                @synchronized (self) {
                    if (!_responsePending || _state == NSURLSessionTaskStateCompleted)
                        return;
                    _responsePending = NO;
                    /* Becoming a download task is deliberately outside this provider. */
                    _responseRejected = disposition != NSURLSessionResponseAllow;
                    [self _signalWorker];
                }
            }];
        }
    }
}

- (void)connection:(NSURLConnection *)connection didReceiveData:(NSData *)data
{
    @synchronized (self) {
        if (_state == NSURLSessionTaskStateCanceling || _state == NSURLSessionTaskStateCompleted)
            return;
        _received += [data length];
        if (_completion || _responsePending || _state == NSURLSessionTaskStateSuspended) {
            if (!_data)
                _data = [[NSMutableData alloc] init];
            [_data appendData:data];
        }
        else
            [_session delegate_dataTask:self didReceiveData:data];
    }
}

- (void)connectionDidFinishLoading:(NSURLConnection *)connection
{
    @synchronized (self) {
        _loadingFinished = YES;
        [self _signalWorker];
    }
}

- (void)connection:(NSURLConnection *)connection didFailWithError:(NSError *)error
{
    @synchronized (self) {
        _loadingFinished = YES;
        [_loadingError release];
        _loadingError = [error retain];
        [self _signalWorker];
    }
}

- (void)connection:(NSURLConnection *)connection didSendBodyData:(NSInteger)bytesWritten totalBytesWritten:(NSInteger)totalBytesWritten totalBytesExpectedToWrite:(NSInteger)totalBytesExpectedToWrite
{
    @synchronized (self) {
        _sent = totalBytesWritten;
        _expectedToSend = totalBytesExpectedToWrite;
    }
}

@end

@implementation NSURLSessionConfiguration

static NSURLSessionConfiguration *_defaultSessionConfiguration = nil;

+ (NSURLSessionConfiguration *)defaultSessionConfiguration {
    static dispatch_once_t once;
    dispatch_once(&once, ^{
        _defaultSessionConfiguration = [[NSURLSessionConfiguration alloc] init];
    });
    return _defaultSessionConfiguration;
}

@end
