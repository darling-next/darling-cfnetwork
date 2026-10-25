#ifndef _CFNSURLCONNECTION_H_
#define _CFNSURLCONNECTION_H_

// not sure what's going on here
// `SecCAIssuerRequest.m` imports this file
// let's just include the main header for now
#include <CFNetwork/CFNetwork.h>

#import <Foundation/Foundation.h>

@interface NSURLSessionTaskMetrics : NSObject

@property (readonly, copy) NSDateInterval* taskInterval;

@end

@interface NSURLSessionConfiguration (CFNSPrivateStuff)

@property (readwrite) NSData* _sourceApplicationAuditTokenData;
@property (readwrite) BOOL _requiresPowerPluggedIn;

@end

/* Source-consumed SPI: HTTPUtilities.m assigns an nw_endpoint_t here.
 * OS_nw_endpoint is Network's Objective-C opaque protocol, not a new provider.
 * The native property's complete private ABI has not been recovered.
 */
@protocol OS_nw_endpoint;
@interface NSURLSessionTask (CFNSHostOverride)
/* Replace/clear before the first resume; subsequent mutation raises. */
@property (nullable, retain) NSObject<OS_nw_endpoint> *_hostOverride;
@end

#endif // _CFNSURLCONNECTION_H_
