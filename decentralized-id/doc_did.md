1.1. Nodes (zero-trust peers)<br>
- Key & DID creation
    Store a long-lived Ed25519 or secp256k1 key in Stronghold.
    Generate did:key (or did:peer if you need off-chain DID URLs) with ssi-dids.
- DHT advertisement
Publish MID → { node_addr, node_did } instead of bare addresses, so any caller can resolve the node’s DID and verify its signature before trusting data.
- Message authentication
    Implement a challenge-response: node receives a request, signs a nonce with its DID key, and the requester checks the signature via ssi::jws_verify.

1.2. Users (humans and autonomous agents)<br>
- Wallet / keystore
    - Humans: desktop CLI or mobile app built on wallet-rs or SpruceKit’s reference wallet (SpruceKit bundles DIDKit, handy if you later need React-Native) 
    GitHub
    .
    - AI agents: embed the same keystore headlessly; expose a “sign(challenge)” API.
- Access control
Express permissions as Verifiable Credentials signed by the file owner’s DID (e.g., “may decrypt MID=abc until 2025-06-01”).
Nodes verify VCs with ssi::vc::verify_credential.
- Secure messaging (optional)
For richer interactions (key exchange, notifications), add didcomm-rust and use peer-DID service endpoints.

2. Node start-up

```rust
let key = stronghold.load_or_generate("node-key")?;
let did = ssi_dids::did_method_key::DIDKey::generate(&key)?;
publish_to_kademlia(mid, node_addr, did);
```
3. User request

Node sends nonce → wallet signs → node verifies (ssi::jws_verify)
Node checks optional VC (ssi::vc::verify_credential)
If OK, node streams CID.

4. Rotation & recovery
Rotate keys via ssi-dids::revocation helpers.
Store rotation proofs in Stronghold snapshots or push to a ledger (did:ethr) if global auditability is required.

5. Bottom line

Use ssi + ssi-dids for DID and VC logic—they’re alive and W3C-conformant.
Persist keys with Stronghold or wallet-rs; those projects remain maintained, just on a slower cadence than the core ssi crates.
Bring in Aries-VCX or didcomm-rust only if you need enterprise-grade credential exchange or standard DIDComm messaging.
Yes—wallet functionality is a separate concern: every node keeps its key in an embedded vault; every human or AI user needs a wallet or keystore to sign challenges and hold credentials.



| Purpose                                            | Library / crate                        | Recent evidence of life                                       |
| -------------------------------------------------- | -------------------------------------- | ------------------------------------------------------------- |
| DID & VC core (W3C-spec)                           | **`ssi`**                              | v0.11 published *last week* ([Crates][1])                     |
| DID methods (`did:key`, `did:peer`, `did:ethr`, …) | **`ssi-dids` 0.2.1**                   | Docs list per-method crates; new in 2025 split ([Docs.rs][2]) |
| Rust keystore (encrypted file/HSM)                 | **Stronghold** (`identity_stronghold`) | crate updated *last month* ([Crates][3])                      |
| Minimal keystore (Universal Wallet spec)           | **wallet-rs**                          | last commit 5 months ago ([GitHub][4])                        |
| Full SSI framework                                 | **IOTA Identity** (`identity_iota`)    | crate release last month ([Crates][5])                        |
| Aries-style agents & DIDComm v2                    | **aries-vcx**                          | repo active 2025 ([GitHub][6], [GitHub][7])                   |
| Stand-alone DIDComm v2 lib                         | **didcomm-rust**                       | maintained 2025 ([GitHub][8])                                 |
| Lightweight DID method only                        | **`did-key.rs`**                       | maintained micro-crate ([GitHub][9])                          |
| Identity layer inside Ockam                        | **ockam\_identity**                    | current crate page ([Crates][10])                             |

[1]: https://crates.io/crates/ssi/0.11.0?utm_source=chatgpt.com "ssi - crates.io: Rust Package Registry"
[2]: https://docs.rs/ssi-dids/0.2.1/ssi_dids/ "ssi_dids - Rust"
[3]: https://crates.io/crates/identity_stronghold?utm_source=chatgpt.com "identity_stronghold - crates.io: Rust Package Registry"
[4]: https://github.com/wallet-rs/wallet-rs?utm_source=chatgpt.com "wallet-rs/wallet-rs: WIP - GitHub"
[5]: https://crates.io/crates/identity_iota?utm_source=chatgpt.com "identity_iota - crates.io: Rust Package Registry"
[6]: https://github.com/openwallet-foundation/vcx?utm_source=chatgpt.com "openwallet-foundation/vcx - GitHub"
[7]: https://github.com/hyperledger/aries-vcx/activity?utm_source=chatgpt.com "Activity · openwallet-foundation/vcx - GitHub"
[8]: https://github.com/sicpa-dlab/didcomm-rust?utm_source=chatgpt.com "sicpa-dlab/didcomm-rust - GitHub"
[9]: https://github.com/decentralized-identity/did-key.rs?utm_source=chatgpt.com "decentralized-identity/did-key.rs: Rust implementation of ... - GitHub"
[10]: https://crates.io/crates/ockam_identity?utm_source=chatgpt.com "ockam_identity - crates.io: Rust Package Registry"
