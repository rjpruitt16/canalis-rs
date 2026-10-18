# Webhook plugins

A plugin teaches Canalis to receive one provider's webhooks. It's a WebAssembly module built with [Extism](https://extism.org), so you can write it in TypeScript, Rust, Go or any language Extism supports.

Drop `shopify.wasm` into the plugins folder (`CANALIS_PLUGINS_DIR`, `/plugins` in the image) and Canalis serves `POST /hooks/shopify`.

The plugin only answers one question: "is this request genuinely from the provider, and what is it?" Everything after that is the same for every plugin:
1. Canalis deduplicates the event by its id. Retries from the provider, even ones that arrive while the first copy is still being handed off, are answered `200` and not queued again.
2. It writes the event to the line on disk before answering the provider `200`.
3. It delivers the event through Aquifer to your handler, paced and fairly per tenant.

## Ready-made plugins

Inlet's providers are built as plugins in [Inlet's `plugins/canalis`](https://github.com/rjpruitt16/inlet/tree/main/plugins/canalis), from the same code Inlet itself runs. **Shopify** is available today:

```bash
SHOPIFY_WEBHOOK_SECRET=...                                  # your app's API secret
CANALIS_HOOK_SHOPIFY_TARGET=https://app.example/webhooks/shopify
```

## The interface

A plugin exports two functions. Both use JSON strings for input and output.

**`describe()`** returns the plugin's name and the secrets it needs:

```json
{ "name": "shopify", "secrets": ["SHOPIFY_WEBHOOK_SECRET"] }
```

- The name must be 1–40 characters of `a-z`, `0-9` and `-`. It becomes the path, `/hooks/<name>`.
- Canalis reads each named secret from its environment and **refuses to start if one is missing**, rather than accept webhooks it can't verify.

**`verify(input)`** gets the request:

```json
{ "method": "POST", "path": "/hooks/shopify", "query": "",
  "headers": { "x-shopify-hmac-sha256": "...", "...": "..." },
  "body_base64": "eyJpZCI6...", "secrets": { "SHOPIFY_WEBHOOK_SECRET": "..." } }
```

Header names are lowercase. It answers with one of:

```json
{ "ok": { "event_id": "wh-123", "tenant": "acme.myshopify.com", "event_type": "orders/create",
          "target": "https://optional.override/", "headers": { "x-shopify-topic": "orders/create" } } }
{ "reject": { "status": 401, "reason": "signature verification failed" } }
```

- **`event_id`** (required) is the provider's id for the delivery. Retries of the same delivery must give the same id, because that's what deduplicates them.
- **`tenant`** is whom the event is for, used for fairness. It defaults to `hook:<name>`.
- **`target`** is where to deliver the event, overriding `CANALIS_HOOK_<NAME>_TARGET` (`<NAME>` uppercased, with `-` as `_`).
- **`headers`** are passed on with the event. Canalis also adds `x-canalis-event-type`.
- **`reject`** is answered as-is. The status must be 4xx; anything else becomes 401.

A plugin that fails or times out gets a `500`, so the provider retries later.

## What plugins can use

Plugins run sandboxed: no network, no filesystem, a time limit per call (`CANALIS_PLUGIN_TIMEOUT_MS`, 1,000 by default) and 64 MiB of memory.

For signatures they call Canalis's crypto, which runs natively. The functions are imported from `extism:host/user` and take and return JSON strings:

| Function | Input | Output |
|---|---|---|
| `hmac_sha256` | `{"key_base64": "...", "message_base64": "..."}` | base64 MAC |
| `hmac_sha1` | the same | base64 MAC |
| `sha256` | base64 message | base64 digest |

Compare signatures in constant time. Inlet's `constantTimeEqual` does this, if you're writing in TypeScript.

## Writing one in TypeScript

Plugins are easiest to write as Inlet providers. Implement Inlet's `ProviderAdapter` and wrap it with `plugins/canalis/adapter.ts`:

```ts
import { MyProvider } from "../../src/providers/my-provider.js";
import { plugin } from "./adapter.js";

const mine = plugin(new MyProvider(), "MY_PROVIDER_SECRET");
export const describe = mine.describe;
export const verify = mine.verify;
```

Add it to `plugins/canalis/build.mjs`, then run `npm run build:canalis-plugins`. You need `extism-js` and Binaryen on `PATH`.

## Writing one in Rust

Use [`extism-pdk`](https://docs.rs/extism-pdk) and build for `wasm32-unknown-unknown`:

```rust
use extism_pdk::*;

#[host_fn]
extern "ExtismHost" { fn hmac_sha256(input: String) -> String; }

#[plugin_fn]
pub fn describe(_: ()) -> FnResult<String> {
    Ok(r#"{"name":"my-provider","secrets":["MY_PROVIDER_SECRET"]}"#.into())
}

#[plugin_fn]
pub fn verify(input: String) -> FnResult<String> {
    // Parse `input` (serde_json), compute the expected signature with
    // `unsafe { hmac_sha256(...) }`, compare it in constant time, and
    // answer {"ok": {...}} or {"reject": {...}}.
    let _ = input;
    Ok(r#"{"reject":{"status":401,"reason":"not implemented yet"}}"#.into())
}
```
