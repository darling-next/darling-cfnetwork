//
//  NSCFURLSession.m
//  CFNetwork
//
//  Copyright (c) 2014 Apportable. All rights reserved.
//

#import "NSCFURLSession.h"

#import <Foundation/NSData.h>
#import <Foundation/NSOperation.h>
#import <Foundation/NSArray.h>
#import <Foundation/NSSet.h>
#import <Foundation/NSException.h>
#import <Foundation/NSThread.h>
#import <CoreFoundation/CFRunLoop.h>
#include <float.h>

@interface NSURLSessionTask (NSCFURLSessionWorker)
- (void)_updateConnection;
@end

/*
 * One lazy worker per session services every task's NSURLConnection callbacks.
 * The worker never retains its session. Pending tasks are retained by the set;
 * active tasks are retained by their session and connection. Session deallocation
 * stops the worker, whose NSThread target retain keeps it alive until loop exit.
 */
@interface __NSCFURLSessionWorker : NSObject {
    NSMutableSet *_pending;
    CFRunLoopRef _loop;
    CFRunLoopSourceRef _source;
    BOOL _stopping;
}
- (void)scheduleTask:(NSURLSessionTask *)task;
- (void)invalidate;
- (void)_drain;
- (void)_run;
@end

static void sessionWorkerCommand(void *info)
{
    [(__NSCFURLSessionWorker *)info _drain];
}

@implementation __NSCFURLSessionWorker

- (id)init
{
    if ((self = [super init])) {
        _pending = [[NSMutableSet alloc] init];
        [NSThread detachNewThreadSelector:@selector(_run) toTarget:self withObject:nil];
    }
    return self;
}

- (void)dealloc
{
    [_pending release];
    [super dealloc];
}

- (void)scheduleTask:(NSURLSessionTask *)task
{
    @synchronized (self) {
        [_pending addObject:task];
        if (_source) {
            CFRunLoopSourceSignal(_source);
            CFRunLoopWakeUp(_loop);
        }
    }
}

- (void)invalidate
{
    @synchronized (self) {
        _stopping = YES;
        if (_source) {
            CFRunLoopSourceSignal(_source);
            CFRunLoopWakeUp(_loop);
        }
    }
}

- (void)_drain
{
    NSUInteger count;
    @synchronized (self) {
        count = [_pending count];
    }
    /* Bound each batch so command traffic cannot starve connection callbacks. */
    while (count--) {
        NSURLSessionTask *task;
        @synchronized (self) {
            task = [[_pending anyObject] retain];
            [_pending removeObject:task];
        }
        /* Never enter a task while holding the worker's publication lock. */
        [task _updateConnection];
        [task release];
    }
}

- (void)_run
{
    @autoreleasepool {
        CFRunLoopRef loop = CFRunLoopGetCurrent();
        CFRunLoopSourceContext context = { .info = self, .perform = sessionWorkerCommand };
        CFRunLoopSourceRef source = CFRunLoopSourceCreate(NULL, 0, &context);
        CFRunLoopAddSource(loop, source, kCFRunLoopDefaultMode);
        @synchronized (self) {
            _loop = loop;
            _source = source;
            if ([_pending count])
                CFRunLoopSourceSignal(source);
        }
        for (;;) {
            @synchronized (self) {
                if (_stopping)
                    break;
            }
            @autoreleasepool {
                CFRunLoopRunInMode(kCFRunLoopDefaultMode, DBL_MAX, true);
            }
        }
        @synchronized (self) {
            _source = NULL;
            _loop = NULL;
        }
        CFRunLoopSourceInvalidate(source);
        CFRelease(source);
    }
}

@end


@implementation __NSCFURLSession

+ (NSURLSession *)sharedSession
{
    static dispatch_once_t once = 0L;
    static NSURLSession *sharedSession = nil;
    dispatch_once(&once, ^{
        sharedSession = [[__NSCFURLSession alloc] initWithConfiguration:[NSURLSessionConfiguration defaultSessionConfiguration] delegate:nil delegateQueue:nil];
    });
    return sharedSession;
}

+ (NSURLSession *)sessionWithConfiguration:(NSURLSessionConfiguration *)configuration
{
    return (NSURLSession *)[[[self alloc] initWithConfiguration:configuration delegate:nil delegateQueue:nil] autorelease];
}

+ (NSURLSession *)sessionWithConfiguration:(NSURLSessionConfiguration *)configuration delegate:(id <NSURLSessionDelegate>)delegate delegateQueue:(NSOperationQueue *)queue
{
    return (NSURLSession *)[[[self alloc] initWithConfiguration:configuration delegate:delegate delegateQueue:queue] autorelease];
}

+ (void)_releaseProcessAssertionForSessionIdentifier:(NSString *)identifier
{
    // seems to be reserved for XPC stuff
}

+ (void)_sendPendingCallbacksForSessionIdentifier:(NSString *)identifier
{
    // seems to be reserved for XPC stuff
}

- (id)initWithConfiguration:(NSURLSessionConfiguration *)configuration delegate:(id <NSURLSessionDelegate>)delegate delegateQueue:(NSOperationQueue *)queue
{
    self = [super init];
    if (self)
    {
        if (configuration == nil)
        {
            [self release];
            return nil;
        }
        _nsCFConfig = [configuration retain];
        _delegateQueue = queue ? [queue retain] : [[NSOperationQueue alloc] init];
        if (!queue)
            [_delegateQueue setMaxConcurrentOperationCount:1];
        _delegate = [delegate retain];
        _tasks = [[NSMutableSet alloc] init];
        _nextTaskIdentifier = 1;
    }
    return self;
}

@synthesize delegate = _delegate;
@synthesize delegateQueue = _delegateQueue;
@synthesize sessionDescription = _sessionDescription;

- (NSURLSessionConfiguration *)configuration
{
    return _nsCFConfig;
}

- (void)dealloc
{
    [_worker invalidate];
    [_worker release];
    [_nsCFConfig release];
    [_delegateQueue release];
    [_delegate release];
    [_sessionDescription release];
    [_tasks release];
    [_delegateTail release];
    [super dealloc];
}

- (void)_scheduleTask:(NSURLSessionTask *)task
{
    @synchronized (self) {
        if (!_worker)
            _worker = [[__NSCFURLSessionWorker alloc] init];
        [_worker scheduleTask:task];
    }
}

- (void)getTasksWithCompletionHandler:(void (^)(NSArray *, NSArray *, NSArray *))handler
{
    @synchronized (self) {
        NSArray *tasks = [_tasks allObjects];
        [self addDelegateBlock:^{ handler(tasks, [NSArray array], [NSArray array]); }];
    }
}

- (void)_completeInvalidationIfNeeded
{
    @synchronized (self) {
        if (!_invalid || [_tasks count] || _invalidationDelivered)
            return;
        _invalidationDelivered = YES;
        id delegate = [_delegate retain];
        [self addDelegateBlock:^{
            if ([delegate respondsToSelector:@selector(URLSession:didBecomeInvalidWithError:)])
                [delegate URLSession:self didBecomeInvalidWithError:nil];
            @synchronized (self) {
                [_delegate release];
                _delegate = nil;
            }
        }];
        [delegate release];
    }
}

- (void)_taskDidComplete:(NSURLSessionTask *)task
{
    @synchronized (self) {
        [_tasks removeObject:task];
        [self _completeInvalidationIfNeeded];
    }
}

- (void)finishTasksAndInvalidate
{
    @synchronized (self) {
        _invalid = YES;
        [self _completeInvalidationIfNeeded];
    }
}

- (void)invalidateAndCancel
{
    NSArray *tasks;
    @synchronized (self) {
        _invalid = YES;
        tasks = [[_tasks allObjects] retain];
    }
    /* Never acquire task locks while holding the session's callback lock. */
    for (NSURLSessionTask *task in tasks)
        [task cancel];
    [tasks release];
    [self _completeInvalidationIfNeeded];
}

- (void)delegate_didFinishEventsForBackgroundURLSession
{
    if ([self can_delegate_didFinishEventsForBackgroundURLSession])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSessionDidFinishEventsForBackgroundURLSession:(NSURLSession *)self];
        }];
    }
}

- (BOOL)can_delegate_didFinishEventsForBackgroundURLSession
{
    return [self.delegate respondsToSelector:@selector(URLSessionDidFinishEventsForBackgroundURLSession)];
}

- (void)delegate_downloadTask:(NSURLSessionDataTask *)task didReceiveResponse:(NSURLResponse *)response
{
    if ([self can_delegate_downloadTask_didReceiveResponse])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSession:(NSURLSession *)self dataTask:task didReceiveResponse:response completionHandler:^(NSURLSessionResponseDisposition disposition){
            }];

        }];
    }
}

- (BOOL)can_delegate_downloadTask_didReceiveResponse
{
    return [self.delegate respondsToSelector:@selector(URLSession:downloadTask:didReceiveResponse:completionHandler:)];
}

- (void)delegate_downloadTask:(NSURLSessionDownloadTask *)task didResumeAtOffset:(int64_t)offset expectedTotalBytes:(int64_t)expectedTotalBytes
{
    if ([self can_delegate_downloadTask_didResumeAtOffset])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSession:(NSURLSession *)self downloadTask:task didResumeAtOffset:offset expectedTotalBytes:expectedTotalBytes];
        }];
    }
}

- (BOOL)can_delegate_downloadTask_didResumeAtOffset
{
    return [self.delegate respondsToSelector:@selector(URLSession:downloadTask:didResumeAtOffset:expectedTotalBytes:)];
}

- (void)delegate_downloadTask:(NSURLSessionDownloadTask *)task didWriteData:(int64_t)bytesWritten totalBytesWritten:(int64_t)totalBytesWritten totalBytesExpectedToWrite:(int64_t)totalBytesExpectedToWrite
{
    if ([self can_delegate_downloadTask_didWriteData])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSession:(NSURLSession *)self downloadTask:task didWriteData:bytesWritten totalBytesWritten:totalBytesWritten totalBytesExpectedToWrite:totalBytesExpectedToWrite];
        }];
    }
}

- (BOOL)can_delegate_downloadTask_didWriteData
{
    return [self.delegate respondsToSelector:@selector(URLSession:downloadTask:didWriteData:totalBytesWritten:totalBytesExpectedToWrite:)];
}

- (void)delegate_downloadTask:(NSURLSessionDownloadTask *)task didFinishDownloadingToURL:(NSURL *)location
{
    if ([self can_delegate_downloadTask_didFinishDownloadingToURL])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSession:(NSURLSession *)self downloadTask:task didFinishDownloadingToURL:location];
        }];
    }
}

- (BOOL)can_delegate_downloadTask_didFinishDownloadingToURL
{
    return [self.delegate respondsToSelector:@selector(URLSession:downloadTask:didFinishDownloadingToURL:)];
}

- (void)delegate_dataTask:(NSURLSessionTask *)task willCacheResponse:(NSCachedURLResponse *)proposedResponse completionHandler:(void (^)(NSCachedURLResponse *cachedResponse))completionHandler
{
    if ([self can_delegate_dataTask_willCacheResponse])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSession:(NSURLSession *)self willCacheResponse:proposedResponse completionHandler:completionHandler];
        }];
    }
}

- (BOOL)can_delegate_dataTask_willCacheResponse
{
    return [self.delegate respondsToSelector:@selector(URLSession:dataTask:willCacheResponse:completionHandler:)];
}

- (void)delegate_dataTask:(NSURLSessionDataTask *)task didReceiveData:(NSData *)data
{
    if ([self can_delegate_dataTask_didReceiveData])
    {
        [self addDelegateBlock:^{
            [self.delegate URLSession:(NSURLSession *)self dataTask:task didReceiveData:data];
        }];
    }
}

- (BOOL)can_delegate_dataTask_didReceiveData
{
    return [self.delegate respondsToSelector:@selector(URLSession:dataTask:didReceiveData:)];
}

- (void)delegate_dataTask:(NSURLSessionDataTask *)task didBecomeDownloadTask:(NSURLSessionDownloadTask *)downloadTask
{
    if ([self can_delegate_dataTask_didBecomeDownloadTask])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSession:(NSURLSession *)self dataTask:task didBecomeDownloadTask:downloadTask];
        }];
    }
}

- (BOOL)can_delegate_dataTask_didBecomeDownloadTask
{
    return [self.delegate respondsToSelector:@selector(URLSession:dataTask:didBecomeDownloadTask:)];
}

- (void)delegate_dataTask:(NSURLSessionDataTask *)task didReceiveResponse:(NSURLResponse *)response completionHandler:(void (^)(NSURLSessionResponseDisposition disposition))completionHandler
{
    if ([self can_delegate_dataTask_didReceiveResponse])
    {
        [self addDelegateBlock:^{
            [self.delegate URLSession:(NSURLSession *)self dataTask:task didReceiveResponse:response completionHandler:completionHandler];
        }];
    }
}

- (BOOL)can_delegate_dataTask_didReceiveResponse
{
    return [self.delegate respondsToSelector:@selector(URLSession:dataTask:didReceiveResponse:completionHandler:)];
}

- (void)delegate_task:(NSURLSessionTask *)task didCompleteWithError:(NSError *)error
{
    if ([self can_delegate_task_didCompleteWithError])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSession:(NSURLSession *)self task:task didCompleteWithError:error];
        }];
    }
}

- (BOOL)can_delegate_task_didCompleteWithError
{
    return [self.delegate respondsToSelector:@selector(URLSession:task:didCompleteWithError:)];
}

- (void)delegate_task:(NSURLSessionTask *)task didSendBodyData:(int64_t)bytesSent totalBytesSent:(int64_t)totalBytesSent totalBytesExpectedToSend:(int64_t)totalBytesExpectedToSend
{
    if ([self can_delegate_task_didSendBodyData])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSession:(NSURLSession *)self task:task didSendBodyData:bytesSent totalBytesSent:totalBytesSent totalBytesExpectedToSend:totalBytesExpectedToSend];
        }];
    }
}

- (BOOL)can_delegate_task_didSendBodyData
{
    return [self.delegate respondsToSelector:@selector(URLSession:task:didSendBodyData:totalBytesSent:totalBytesExpectedToSend:)];
}

- (void)delegate_task:(NSURLSessionTask *)task needNewBodyStream:(void (^)(NSInputStream *bodyStream))completionHandler
{
    if ([self can_delegate_task_needNewBodyStream])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSession:(NSURLSession *)self task:task needNewBodyStream:completionHandler];
        }];
    }
}

- (BOOL)can_delegate_task_needNewBodyStream
{
    return [self.delegate respondsToSelector:@selector(URLSession:task:needNewBodyStream:)];
}

- (void)delegate_task:(NSURLSessionTask *)task didReceiveChallenge:(NSURLAuthenticationChallenge *)challenge completionHandler:(void (^)(NSURLSessionAuthChallengeDisposition disposition, NSURLCredential *credential))completionHandler
{
    if ([self can_delegate_task_didReceiveChallenge])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSession:(NSURLSession *)self task:task didReceiveChallenge:challenge completionHandler:completionHandler];
        }];
    }
}

- (BOOL)can_delegate_task_didReceiveChallenge
{
    return [self.delegate respondsToSelector:@selector(URLSession:task:didReceiveChallenge:completionHandler:)];
}

- (void)delegate_task:(NSURLSessionTask *)task willPerformHTTPRedirection:(NSHTTPURLResponse *)response newRequest:(NSURLRequest *)request completionHandler:(void (^)(NSURLRequest *))completionHandler
{
    if ([self can_delegate_task_willPerformHTTPRedirection])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSession:(NSURLSession *)self task:task willPerformHTTPRedirection:response newRequest:request completionHandler:completionHandler];
        }];
    }
}

- (BOOL)can_delegate_task_willPerformHTTPRedirection
{
    return [self.delegate respondsToSelector:@selector(URLSession:task:willPerformHTTPRedirection:newRequest:completionHandler:)];
}

- (void)delegate_didReceiveChallenge:(NSURLAuthenticationChallenge *)challenge  completionHandler:(void (^)(NSURLSessionAuthChallengeDisposition disposition, NSURLCredential *credential))completionHandler
{
    if ([self can_delegate_didReceiveChallenge])
    {
        [self addDelegateBLock:^{
            [self.delegate URLSession:(NSURLSession *)self didReceiveChallenge:challenge completionHandler:completionHandler];
        }];
    }
}

- (BOOL)can_delegate_didReceiveChallenge
{
    return [self.delegate respondsToSelector:@selector(URLSession:didReceiveChallenge:completionHandler:)];
}

- (void)addDelegateBlock:(void (^)(void))block
{
    @synchronized (self) {
        /*
         * Preserve callback order even when a caller supplies a concurrent
         * queue. The completion clears the tail so its block cannot keep the
         * session alive after the last callback has run.
         */
        NSBlockOperation *operation = [NSBlockOperation blockOperationWithBlock:block];
        NSUInteger generation = ++_delegateGeneration;
        if (_delegateTail)
            [operation addDependency:_delegateTail];
        [operation setCompletionBlock:^{
            @synchronized (self) {
                if (_delegateGeneration == generation) {
                    [_delegateTail release];
                    _delegateTail = nil;
                }
            }
        }];
        [_delegateTail release];
        _delegateTail = [operation retain];
        [_delegateQueue addOperation:operation];
    }
}

- (NSURLSessionDownloadTask *)downloadTaskWithResumeData:(NSData *)resumeData
{
    return [self downloadTaskWithResumeData:resumeData completionHandler:nil];
}

- (NSURLSessionDownloadTask *)downloadTaskWithResumeData:(NSData *)resumeData completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler
{
    NSURLSessionDownloadTask *task = [[NSURLSessionDownloadTask alloc] initWithResumeData:resumeData completionHandler:completionHandler];

    return [task autorelease];
}

- (NSURLSessionDownloadTask *)downloadTaskWithURL:(NSURL *)url
{
    return [self downloadTaskWithURL:url completionHandler:nil];
}

- (NSURLSessionDownloadTask *)downloadTaskWithURL:(NSURL *)url completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler
{
    return [self downloadTaskWithRequest:[NSURLRequest requestWithURL:url] completionHandler:completionHandler];
}

- (NSURLSessionDownloadTask *)downloadTaskWithRequest:(NSURLRequest *)request
{
    return [self downloadTaskWithRequest:request completionHandler:nil];
}

- (NSURLSessionDownloadTask *)downloadTaskWithRequest:(NSURLRequest *)request completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler
{
    NSURLSessionDownloadTask *task = [[NSURLSessionDownloadTask alloc] initWithCompletionHandler:completionHandler];

    return [task autorelease];
}

- (NSURLSessionUploadTask *)uploadTaskWithRequest:(NSURLRequest *)request fromFile:(NSURL *)fileURL
{
    return [self uploadTaskWithRequest:request fromFile:fileURL completionHandler:nil];
}

- (NSURLSessionUploadTask *)uploadTaskWithRequest:(NSURLRequest *)request fromFile:(NSURL *)fileURL completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler
{
    return [self uploadTaskWithRequest:request fromData:[NSData dataWithContentsOfURL:fileURL] completionHandler:completionHandler];
}

- (NSURLSessionUploadTask *)uploadTaskWithRequest:(NSURLRequest *)request fromData:(NSData *)bodyData
{
    return [self uploadTaskWithRequest:request fromData:bodyData completionHandler:nil];
}

- (NSURLSessionUploadTask *)uploadTaskWithRequest:(NSURLRequest *)request fromData:(NSData *)bodyData completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler
{
    NSMutableURLRequest *uploadRequest = [request mutableCopy];
    [uploadRequest setHTTPBody:bodyData];
    NSURLSessionUploadTask *task = [[NSURLSessionUploadTask alloc] initWithRequest:uploadRequest completionHandler:completionHandler];
    [uploadRequest release];
    return [task autorelease];
}

- (NSURLSessionUploadTask *)uploadTaskWithStreamedRequest:(NSURLRequest *)request
{
    return [self uploadTaskWithRequest:request fromData:nil completionHandler:nil];
}

- (NSURLSessionDataTask *)dataTaskWithRequest:(NSURLRequest *)request
{
    return [self dataTaskWithRequest:request completionHandler:nil];
}

- (NSURLSessionDataTask *)dataTaskWithRequest:(NSURLRequest *)request completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler
{
    @synchronized (self) {
        if (_invalid)
            [NSException raise:NSGenericException format:@"Cannot create a task in an invalidated session"];
        NSURLSessionDataTask *task = _NSCFURLSessionCreateDataTask(self, request,
            _nextTaskIdentifier++, completionHandler);
        if (task)
            [_tasks addObject:task];
        return [task autorelease];
    }
}

- (NSURLSessionDataTask *)dataTaskWithURL:(NSURL *)url
{
    return [self dataTaskWithURL:url completionHandler:nil];
}

- (NSURLSessionDataTask *)dataTaskWithURL:(NSURL *)url completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler
{
    return [self dataTaskWithRequest:[NSURLRequest requestWithURL:url] completionHandler:completionHandler];
}

- (NSURLSessionDataTask *)dataTaskWithHTTPGetRequest:(NSURL *)url
{
    return [self dataTaskWithRequest:[NSURLRequest requestWithURL:url] completionHandler:nil];
}

- (NSURLSessionDataTask *)dataTaskWithHTTPGetRequest:(NSURL *)url completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler
{
    return [self dataTaskWithRequest:[NSURLRequest requestWithURL:url] completionHandler:completionHandler];
}


@end
