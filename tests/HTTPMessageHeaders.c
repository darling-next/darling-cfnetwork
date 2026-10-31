/* Guest regression for the CFHTTPMessage contract used by WebSocket upgrades.
 * Build with clang HTTPMessageHeaders.c -framework CFNetwork -framework CoreFoundation.
 * No network/server or fabricated response is involved in this value API test.
 */
#include <CoreFoundation/CoreFoundation.h>
#include <CFNetwork/CFHTTPMessage.h>
#include <assert.h>
#include <stdio.h>

int main(void)
{
    CFURLRef url = CFURLCreateWithString(NULL, CFSTR("http://localhost/headers"), NULL);
    CFHTTPMessageRef request = CFHTTPMessageCreateRequest(NULL, CFSTR("GET"), url, kCFHTTPVersion1_1);
    CFHTTPMessageSetHeaderFieldValue(request, CFSTR("X-Keeper"), CFSTR("preserved"));
    CFHTTPMessageSetHeaderFieldValue(request, CFSTR("Sec-WebSocket-Extensions"), NULL);
    CFHTTPMessageSetHeaderFieldValue(request, CFSTR("Sec-WebSocket-Extensions"), NULL);
    CFStringRef keeper = CFHTTPMessageCopyHeaderFieldValue(request, CFSTR("X-Keeper"));
    assert(keeper && CFEqual(keeper, CFSTR("preserved")));
    CFRelease(keeper);
    CFHTTPMessageSetHeaderFieldValue(request, CFSTR("X-Transient"), CFSTR("remove me"));
    CFHTTPMessageSetHeaderFieldValue(request, CFSTR("X-Transient"), NULL);
    CFHTTPMessageSetHeaderFieldValue(request, CFSTR("X-Transient"), NULL);
    assert(CFHTTPMessageCopyHeaderFieldValue(request, CFSTR("X-Transient")) == NULL);
    CFDictionaryRef fields = CFHTTPMessageCopyAllHeaderFields(request);
    assert(CFDictionaryGetCount(fields) == 1);
    CFRelease(fields);
    CFDataRef bytes = CFHTTPMessageCopySerializedMessage(request);
    CFHTTPMessageRef parsed = CFHTTPMessageCreateEmpty(NULL, true);
    assert(CFHTTPMessageAppendBytes(parsed, CFDataGetBytePtr(bytes), CFDataGetLength(bytes)));
    assert(CFHTTPMessageIsHeaderComplete(parsed));
    CFStringRef method = CFHTTPMessageCopyRequestMethod(parsed);
    assert(CFEqual(method, CFSTR("GET")));
    CFRelease(method);
    CFURLRef parsedURL = CFHTTPMessageCopyRequestURL(parsed);
    CFStringRef path = CFURLCopyPath(parsedURL);
    assert(CFEqual(path, CFSTR("/headers")));
    CFRelease(path); CFRelease(parsedURL);
    keeper = CFHTTPMessageCopyHeaderFieldValue(parsed, CFSTR("X-Keeper"));
    assert(keeper && CFEqual(keeper, CFSTR("preserved")));
    CFRelease(keeper);
    assert(CFHTTPMessageCopyHeaderFieldValue(parsed, CFSTR("X-Transient")) == NULL);
    assert(CFHTTPMessageCopyHeaderFieldValue(parsed, CFSTR("Sec-WebSocket-Extensions")) == NULL);
    fields = CFHTTPMessageCopyAllHeaderFields(parsed);
    assert(CFDictionaryGetCount(fields) == 1);
    CFRelease(fields); CFRelease(parsed);
    CFRelease(bytes); CFRelease(request); CFRelease(url);
    puts("HTTPMessageHeaders PASS: absent/repeated removal preserves unrelated headers");
    return 0;
}
