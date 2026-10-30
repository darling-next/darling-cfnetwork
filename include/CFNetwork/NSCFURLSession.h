#import <os/object.h>
#import <Foundation/NSURLSession.h>
#import <dispatch/dispatch.h>

@class NSOperationQueue, NSOperation, NSString, NSMutableSet, __NSCFURLSessionWorker;

__attribute__((visibility("hidden")))
@interface __NSCFURLSession : NSURLSession {
    NSURLSessionConfiguration *_nsCFConfig;
    BOOL _invalid;
    BOOL _invalidationDelivered;
    NSOperationQueue *_delegateQueue;
    id <NSURLSessionDelegate> _delegate;
    NSString *_sessionDescription;
    NSMutableSet *_tasks;
    NSUInteger _nextTaskIdentifier;
    NSOperation *_delegateTail;
    NSUInteger _delegateGeneration;
    __NSCFURLSessionWorker *_worker;
}

@property(copy) NSString *sessionDescription;
@property(readonly) id <NSURLSessionDelegate> delegate;
@property(readonly) NSOperationQueue *delegateQueue;
@property(readonly, copy) NSURLSessionConfiguration *configuration;

- (void)_taskDidComplete:(NSURLSessionTask *)task;
- (void)_completeInvalidationIfNeeded;
- (void)_scheduleTask:(NSURLSessionTask *)task;

+ (id)sessionWithConfiguration:(id)configuration delegate:(id)delegate delegateQueue:(id)delegateQueue;
+ (id)sessionWithConfiguration:(id)configuration;
+ (id)sharedSession;
+ (void)_releaseProcessAssertionForSessionIdentifier:(NSString *)identifier;
+ (void)_sendPendingCallbacksForSessionIdentifier:(NSString *)identifier;

// + (const struct ClassicConnectionSession *)defaultClassicConnectionSession;

- (id)initWithConfiguration:(NSURLSessionConfiguration *)configuration delegate:(id <NSURLSessionDelegate>)delegate delegateQueue:(NSOperationQueue *)queue;

- (void)delegate_didFinishEventsForBackgroundURLSession;
- (BOOL)can_delegate_didFinishEventsForBackgroundURLSession;
- (id)delegate_downloadTaskNeedsDownloadDirectory:(id)needsDownloadDirectory;
- (BOOL)can_delegate_downloadTaskNeedsDownloadDirectory;
- (void)delegate_downloadTask:(NSURLSessionDataTask *)task didReceiveResponse:(NSURLResponse *)response;
- (BOOL)can_delegate_downloadTask_didReceiveResponse;
- (void)delegate_downloadTask:(NSURLSessionDownloadTask *)task didResumeAtOffset:(int64_t)offset expectedTotalBytes:(int64_t)expectedTotalBytes;
- (BOOL)can_delegate_downloadTask_didResumeAtOffset;
- (void)delegate_downloadTask:(NSURLSessionDownloadTask *)task didWriteData:(int64_t)bytesWritten totalBytesWritten:(int64_t)totalBytesWritten totalBytesExpectedToWrite:(int64_t)totalBytesExpectedToWrite;
- (BOOL)can_delegate_downloadTask_didWriteData;
- (void)delegate_downloadTask:(NSURLSessionDownloadTask *)task didFinishDownloadingToURL:(NSURL *)location;
- (BOOL)can_delegate_downloadTask_didFinishDownloadingToURL;
- (void)delegate_dataTask:(NSURLSessionTask *)task willCacheResponse:(NSCachedURLResponse *)proposedResponse  completionHandler:(void (^)(NSCachedURLResponse *cachedResponse))completionHandler;
- (BOOL)can_delegate_dataTask_willCacheResponse;
- (void)delegate_dataTask:(NSURLSessionDataTask *)task didReceiveData:(NSData *)data;
- (BOOL)can_delegate_dataTask_didReceiveData;
- (void)delegate_dataTask:(NSURLSessionDataTask *)task didBecomeDownloadTask:(NSURLSessionDownloadTask *)downloadTask;
- (BOOL)can_delegate_dataTask_didBecomeDownloadTask;
- (void)delegate_dataTask:(NSURLSessionDataTask *)task didReceiveResponse:(NSURLResponse *)response completionHandler:(void (^)(NSURLSessionResponseDisposition disposition))completionHandler;
- (BOOL)can_delegate_dataTask_didReceiveResponse;
- (void)delegate_task:(NSURLSessionTask *)task didCompleteWithError:(NSError *)error;
- (BOOL)can_delegate_task_didCompleteWithError;
- (void)delegate_task:(NSURLSessionTask *)task didSendBodyData:(int64_t)bytesSent totalBytesSent:(int64_t)totalBytesSent totalBytesExpectedToSend:(int64_t)totalBytesExpectedToSend;
- (BOOL)can_delegate_task_didSendBodyData;
- (void)delegate_task:(NSURLSessionTask *)task needNewBodyStream:(void (^)(NSInputStream *bodyStream))completionHandler;
- (BOOL)can_delegate_task_needNewBodyStream;
- (void)delegate_task:(NSURLSessionTask *)task didReceiveChallenge:(NSURLAuthenticationChallenge *)challenge  completionHandler:(void (^)(NSURLSessionAuthChallengeDisposition disposition, NSURLCredential *credential))completionHandler;
- (BOOL)can_delegate_task_didReceiveChallenge;
- (void)delegate_task:(NSURLSessionTask *)task willPerformHTTPRedirection:(NSHTTPURLResponse *)response newRequest:(NSURLRequest *)request completionHandler:(void (^)(NSURLRequest *))completionHandler;
- (BOOL)can_delegate_task_willPerformHTTPRedirection;
- (void)delegate_didReceiveChallenge:(NSURLAuthenticationChallenge *)challenge  completionHandler:(void (^)(NSURLSessionAuthChallengeDisposition disposition, NSURLCredential *credential))completionHandler;
- (BOOL)can_delegate_didReceiveChallenge;

- (void)addDelegateBlock:(void (^)(void))block;

- (NSURLSessionDownloadTask *)downloadTaskWithResumeData:(NSData *)resumeData;
- (NSURLSessionDownloadTask *)downloadTaskWithResumeData:(NSData *)resumeData completionHandler:(void (^)(NSURL *location, NSURLResponse *response, NSError *error))completionHandler;
- (NSURLSessionDownloadTask *)downloadTaskWithURL:(NSURL *)url;
- (NSURLSessionDownloadTask *)downloadTaskWithURL:(NSURL *)url completionHandler:(void (^)(NSURL *location, NSURLResponse *response, NSError *error))completionHandler;
- (NSURLSessionDownloadTask *)downloadTaskWithRequest:(NSURLRequest *)request;
- (NSURLSessionDownloadTask *)downloadTaskWithRequest:(NSURLRequest *)request completionHandler:(void (^)(NSURL *location, NSURLResponse *response, NSError *error))completionHandler;
- (NSURLSessionUploadTask *)uploadTaskWithRequest:(NSURLRequest *)request fromFile:(NSURL *)fileURL;
- (NSURLSessionUploadTask *)uploadTaskWithRequest:(NSURLRequest *)request fromFile:(NSURL *)fileURL completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler;
- (NSURLSessionUploadTask *)uploadTaskWithRequest:(NSURLRequest *)request fromData:(NSData *)bodyData;
- (NSURLSessionUploadTask *)uploadTaskWithRequest:(NSURLRequest *)request fromData:(NSData *)bodyData completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler;
- (NSURLSessionUploadTask *)uploadTaskWithStreamedRequest:(NSURLRequest *)request;
- (NSURLSessionDataTask *)dataTaskWithRequest:(NSURLRequest *)request;
- (NSURLSessionDataTask *)dataTaskWithRequest:(NSURLRequest *)request completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler;
- (NSURLSessionDataTask *)dataTaskWithURL:(NSURL *)url;
- (NSURLSessionDataTask *)dataTaskWithURL:(NSURL *)url completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler;
- (NSURLSessionDataTask *)dataTaskWithHTTPGetRequest:(NSURL *)url;
- (NSURLSessionDataTask *)dataTaskWithHTTPGetRequest:(NSURL *)url completionHandler:(void (^)(NSData *data, NSURLResponse *response, NSError *error))completionHandler;

- (void)getTasksWithCompletionHandler:(void (^)(NSArray *, NSArray *, NSArray *))handler;
- (void)finishTasksAndInvalidate;
- (void)invalidateAndCancel;

@end

/* Native providers share the session's worker and task lifecycle registry. */
__attribute__((visibility("hidden")))
NSURLSessionDataTask *_NSCFURLSessionCreateDataTask(__NSCFURLSession *session,
    NSURLRequest *request, NSUInteger identifier,
    void (^completion)(NSData *, NSURLResponse *, NSError *));
__attribute__((visibility("hidden")))
NSURLSessionDownloadTask *_NSCFURLSessionCreateDownloadTask(__NSCFURLSession *session,
    NSURLRequest *request, NSUInteger identifier,
    void (^completion)(NSURL *, NSURLResponse *, NSError *));
__attribute__((visibility("hidden")))
NSURLSessionWebSocketTask *_NSCFURLSessionCreateWebSocketTask(__NSCFURLSession *session,
    NSURLRequest *request, NSUInteger identifier);
