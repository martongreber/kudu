# Per-Endpoint Authentication Bypass in Kudu Webserver

This patch adds the ability to register webserver endpoints that bypass SPNEGO authentication even when `--webserver_require_spnego` is enabled.

## Overview

Previously, when `--webserver_require_spnego=true` was set, ALL endpoints required SPNEGO authentication. This patch introduces a `skip_auth` parameter to the webserver registration methods that allows specific endpoints to bypass authentication.

## API Changes

All webserver registration methods now accept an optional `skip_auth` parameter:

```cpp
// Register a styled path handler that skips authentication
webserver->RegisterPathHandler("/public-endpoint", "Public", callback, 
                              StyleMode::STYLED, /*is_on_nav_bar=*/true, 
                              /*skip_auth=*/true);

// Register a JSON endpoint that skips authentication  
webserver->RegisterJsonPathHandler("/api/public", "Public API", callback,
                                  /*is_on_nav_bar=*/false, 
                                  /*skip_auth=*/true);

// Register a prerendered handler that skips authentication
webserver->RegisterPrerenderedPathHandler("/healthz", "Health", callback,
                                         StyleMode::UNSTYLED, /*is_on_nav_bar=*/true,
                                         /*skip_auth=*/true);

// Register a binary data handler that skips authentication
webserver->RegisterBinaryDataPathHandler("/public-data", "Public Data", callback,
                                        /*skip_auth=*/true);
```

## Default Behavior

By default, `skip_auth=false`, so existing code continues to work unchanged. Only endpoints explicitly marked with `skip_auth=true` will bypass authentication.

## Example Use Cases

1. **Health Check Endpoints**: The `/healthz` endpoint is now configured to skip authentication, allowing monitoring systems to check server health without credentials.

2. **Public APIs**: Endpoints that need to be accessible without authentication (e.g., API documentation, public metrics).

3. **Service Discovery**: Endpoints used by service discovery systems that may not have authentication credentials.

## Implementation Details

The authentication check is performed after the URL is routed to a handler, allowing the system to check the handler's `skip_auth` setting before enforcing SPNEGO authentication.

## Security Considerations

- Only use `skip_auth=true` for endpoints that genuinely need to be public
- Consider the security implications of exposing data without authentication
- The `/healthz` endpoint returns only "OK" status and is safe to expose publicly
- Be careful with endpoints that might expose sensitive information

## Testing

The implementation includes tests that verify:
- Endpoints with `skip_auth=true` are accessible without authentication
- Endpoints with `skip_auth=false` (default) still require authentication
- The `/healthz` endpoint specifically works without authentication 