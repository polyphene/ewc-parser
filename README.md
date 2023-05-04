## Energy Web Chain Data Parser

The script in this repository is used to parse events that happened over the Energy Web Chain. The goal here is to intersect
the data to estimate the total value on RECs currently anchored on-chain and how much of it has been associated to a storage
provider of the Filecoin network.

### Parsed Events

The events we leverage to cross-reference the data are:
- `TransferSingle`: When the transfer happens from the Zero address the event means that a Certificate (amount of value 
ordered by Protocol Labs) has been minted on-chain in the form of tokens. The value associated to these certificates represent
the amount of energy ordered in Wh. See [Certificate documentation](https://docs.zerolabs.green/zerolabs-tokenization-module/domain-definitions/certificate).
- `RedemptionStatementSet`: Event that links a Batch (group of Certificates.) to a given Redemption Statement CID. See [Bi-directional documentation](https://docs.zerolabs.green/zerolabs-tokenization-module/domain-definitions/bi-directional-link).
- `CertificateBatchMinted`: Event that create a relation between multiple Certificates under a given batch ID, effectively
linking Certificates to a Redemption Statement. See [Batch documentation](https://docs.zerolabs.green/zerolabs-tokenization-module/domain-definitions/batch).
- `ClaimSingle`: Event that represents the allocation of an amount of RECs from a Certificate to a storage provider of the
Filecoin network. See [Claiming documentation](https://docs.zerolabs.green/zerolabs-tokenization-module/domain-definitions/certificate/claiming).

### Run

```shell
npm run script
```