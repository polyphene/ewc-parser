import { BigNumber, constants, ethers } from 'ethers';
import Papa from 'papaparse';
import fs from 'fs';

import registryExtendedConfig from './registry-extended';
import batchFactoryConfig from './batch-factory';
import agreementFactoryConfig from './agreement-factory';
import path from 'path';
import { ClaimDataCoder } from '@zero-labs/tokenization-contracts';

/*
 * Energy Web Chain intersected data
 */

type BatchRegistry = {
    [key: string]: Batch;
};

type Batch = {
    // Unique ID associated to a batch.
    batchId: string;
    // Redemption statement set for the batch.
    redemptionStatement: string;
    // URL pointing to the redemption statement PDF.
    storagePointer: string;
    // Certificates IDs related to the batch.
    certificateIds: string[];
    // Transaction hash at which the corresponding event was emitted.
    transactionHash: string;
};

type Certificate = {
    // Token ID generated on chain.
    tokenId: string;
    // Batch ID to which this certificate is related.
    batchId: string;
    // Amount of RECs associated to this certificate.
    value: string;
    operator: string;
    from: string;
    to: string;
    // Transaction hash at which the corresponding event was emitted.
    transactionHash: string;
};

type Claim = {
    // Certificate ID that was subject of a claim.
    tokenId: string;
    // EW address.
    claimIssuer: string;
    // SP address that claimed the RECs.
    claimSubject: string;
    // ?
    topic: string;
    // Amount of RECs claimed.
    value: string;
    // Metadata associated to the claim.
    claimData: string;
    // Decoded claim data.
    claimDataDecoded: string;
    // Transaction hash at which the corresponding event was emitted.
    transactionHash: string;
};

type Agreement = {
    agreementAddress: string;
    certificateId: string;
    amount: string;
    buyer: string;
    seller: string;
    metadata: string;
    metadataDecoded: string;
};

/*
 * Energy Web Chain events args
 */

type CertificateBatchMintedArgs = {
    batchId: string;
    certificateIds: BigNumber[];
};

type RedemptionSetArgs = {
    batchId: string;
    redemptionStatement: string;
    storagePointer: string;
};

type MintedArgs = {
    id: BigNumber;
    value: BigNumber;
    operator: string;
    from: string;
    to: string;
};

type ClaimSingleArgs = {
    _claimIssuer: string;
    _claimSubject: string;
    _topic: BigNumber;
    _id: BigNumber;
    _value: BigNumber;
    _claimData: string;
};

type AgreementFilledArgs = {
    agreementAddress: string;
    certificateId: BigNumber;
    amount: BigNumber;
};

type AgreementSignedArgs = {
    agreementAddress: string;
    buyer: string;
    seller: string;
    amount: BigNumber;
};
type AgreementsDeployedArgs = {
    agreements: string[];
};

type AgreementData = {
    buyer: string;
    seller: string;
    amount: BigNumber;
    metadata: string;
    valid: boolean;
};

const AGREEMENTS_DATA_CACHE = path.resolve(
    __dirname,
    './cache/agreements-data-cache.csv',
);
export const getEwfContractsInstances = () => {
    const registryExtendedAddress =
        '0x5651a7A38753A9692B7740CCeCA3824a4d33aEFb';
    const batchFactoryAddress = '0x2248a8e53c8cf533aeef2369fff9dc8c036c8900';
    const agreementFactoryAddress =
        '0x5fd92584ceF267a7b702c722C660bF9C4ed2bfA7';
    const ewfProvider = ethers.getDefaultProvider('https://rpc.energyweb.org');

    return {
        registryExtendedContract: new ethers.Contract(
            registryExtendedAddress,
            registryExtendedConfig.abi,
            ewfProvider,
        ),
        batchFactoryContract: new ethers.Contract(
            batchFactoryAddress,
            batchFactoryConfig.abi,
            ewfProvider,
        ),
        agreementFactoryContract: new ethers.Contract(
            agreementFactoryAddress,
            agreementFactoryConfig.abi,
            ewfProvider,
        ),
    };
};

const parseEwcData = async () => {
    console.info(
        `Starting process to fetch and parse data from Energy Web Chain...\n`,
    );

    const {
        registryExtendedContract,
        batchFactoryContract,
        agreementFactoryContract,
    } = getEwfContractsInstances();

    console.info(`\tAGREEMENTS\n`);

    const agreementDeployedEvents = await agreementFactoryContract.queryFilter(
        agreementFactoryContract.filters.AgreementsDeployed(),
    );
    let nbrAgreementsDeployed = 0;
    agreementDeployedEvents.forEach(e => {
        const { agreements } = e.args as unknown as AgreementsDeployedArgs;
        nbrAgreementsDeployed += agreements.length;
    });

    console.info(`\t\tFound ${nbrAgreementsDeployed} deployed agreements\n`);

    const agreementSignedEvents = await agreementFactoryContract.queryFilter(
        agreementFactoryContract.filters.AgreementSigned(),
    );

    console.info(
        `\t\tFound ${agreementSignedEvents.length} signed agreements\n`,
    );

    // Setup and maintain cache.
    console.info(`\t\tUpdating agreements data cache\n`);

    const stream = fs.createWriteStream(AGREEMENTS_DATA_CACHE, { flags: 'a' });

    stream.write(`blockId,address,buyer,seller,amount,metadata,valid\n`);

    const agreementsData: { [key: string]: AgreementData } = {};
    // Iterate through all signed agreements to get metadata.
    for (const agreementSignedEvent of agreementSignedEvents.sort(
        (a, b) => a.blockNumber - b.blockNumber,
    )) {
        const { agreementAddress } =
            agreementSignedEvent.args as unknown as AgreementSignedArgs;

        const agreementData = await agreementFactoryContract.agreementData(
            agreementAddress,
        );

        agreementsData[agreementAddress] = {
            buyer: agreementData.buyer,
            seller: agreementData.seller,
            amount: agreementData.amount,
            metadata: agreementData.metadata,
            valid: agreementData.valid,
        };

        stream.write(
            `${agreementSignedEvent.blockNumber},${agreementAddress},${
                agreementData.buyer
            },${agreementData.seller},${agreementData.amount.toString()},${
                agreementData.metadata
            },${agreementData.valid}\n`,
        );

        console.info(
            `\t\t\tAgreement data added to cache for agreement: ${agreementAddress}\n`,
        );
    }

    stream.end();

    console.info(`\t\tCache updated!\n`);

    const agreementFilledEvents = await agreementFactoryContract.queryFilter(
        agreementFactoryContract.filters.AgreementFilled(),
    );

    console.info(
        `\t\tFound ${agreementFilledEvents.length} filled agreements\n`,
    );

    const agreementClaimedEvents = await agreementFactoryContract.queryFilter(
        agreementFactoryContract.filters.AgreementClaimed(),
    );

    console.info(
        `\t\tFound ${agreementClaimedEvents.length} claimed agreements\n`,
    );

    console.info(`\tCERTIFICATES\n`);

    const mintEvents = await registryExtendedContract.queryFilter(
        registryExtendedContract.filters.TransferSingle(
            null,
            constants.AddressZero,
        ),
    );

    console.info(`\t\tFound ${mintEvents.length} certificates minted\n`);

    const redemptionSetEvents = await batchFactoryContract.queryFilter(
        batchFactoryContract.filters.RedemptionStatementSet(),
    );

    console.info(
        `\t\tFound ${redemptionSetEvents.length} redemption statement set on batches\n`,
    );

    const certificateBatchMintedEvents = await batchFactoryContract.queryFilter(
        batchFactoryContract.filters.CertificateBatchMinted(),
    );

    console.info(
        `\t\tFound ${certificateBatchMintedEvents.length} batch linked to certificates IDs\n`,
    );

    const claimSingleEvents = await registryExtendedContract.queryFilter(
        registryExtendedContract.filters.ClaimSingle(),
    );

    console.info(`\t\tFound ${claimSingleEvents.length} claims\n\n`);

    const batches: Batch[] = [];
    const certificates: Certificate[] = [];
    const claims: Claim[] = [];
    const agreements: Agreement[] = [];

    // Iterate through all signed agreements
    for (const agreementSignedEvent of agreementSignedEvents.sort(
        (a, b) => a.blockNumber - b.blockNumber,
    )) {
        const {
            agreementAddress: agreementSignedAddress,
            buyer,
            seller,
            amount: agreementSignedAmount,
        } = agreementSignedEvent.args as unknown as AgreementSignedArgs;

        const { metadata, valid } = agreementsData[agreementSignedAddress];

        if (!valid) {
            continue;
        }

        for (const agreementFilledEvent of agreementFilledEvents) {
            const {
                agreementAddress: agreementFilledAddress,
                certificateId,
                amount: agreementFilledAmount,
            } = agreementSignedEvent.args as unknown as AgreementFilledArgs;

            if (agreementSignedAddress === agreementFilledAddress) {
                if (
                    agreementSignedAmount.toString() !==
                    agreementFilledAmount.toString()
                ) {
                    console.log(
                        '----------------------Different amount signed and filled------------------------',
                    );
                    console.log('Agreement address: ', agreementSignedAddress);
                    console.log(
                        'agreementSignedAmount',
                        agreementSignedAmount.toString(),
                    );
                    console.log(
                        'agreementFilledAddress',
                        agreementFilledAddress.toString(),
                    );
                    console.log(
                        '--------------------------------------------------------------------------------',
                    );
                }
            }
        }
    }

    // Iterate through all redemption statement on-chain. Sorting by block number to tackle oldest to most recent.
    for (const redemptionSetEvent of redemptionSetEvents.sort(
        (a, b) => a.blockNumber - b.blockNumber,
    )) {
        const {
            batchId: redemptionSetEventBatchId,
            redemptionStatement,
            storagePointer,
        } = redemptionSetEvent.args as unknown as RedemptionSetArgs;
        const batchCertificateIds: string[] = [];
        // Loop through CertificateBatchMinted events, filtering when it concerns the current batch we are iterating over.
        for (const certificateBatchMintedEvent of certificateBatchMintedEvents) {
            const {
                batchId: certificateBatchMintedEventBatchId,
                certificateIds,
            } =
                certificateBatchMintedEvent.args as unknown as CertificateBatchMintedArgs;
            // If this is the batch ID we are looking for, continue to construct data.
            if (
                redemptionSetEventBatchId === certificateBatchMintedEventBatchId
            ) {
                // Iterate over all certificates IDs that are related to the current batch we are iterating over.
                for (const certificateId of certificateIds) {
                    // Looking for minting events concerning the certificate ID we are iterating over.
                    for (const mintEvent of mintEvents) {
                        const {
                            id: mintEventCertificateId,
                            value: mintedValue,
                            to,
                            operator,
                            from,
                        } = mintEvent.args as unknown as MintedArgs;

                        if (mintEventCertificateId.eq(certificateId)) {
                            // Looking for claim events concerning the certificate ID we are iterating over.
                            for (const claimSingleEvent of claimSingleEvents) {
                                const {
                                    _id: id,
                                    _claimSubject: claimSubject,
                                    _value: value,
                                    _claimIssuer: claimIssuer,
                                    _claimData: claimData,
                                    _topic: topic,
                                } = claimSingleEvent.args as unknown as ClaimSingleArgs;
                                if (id.eq(certificateId)) {
                                    let claimDataDecoded =
                                        ClaimDataCoder.decode(claimData);

                                    claims.push({
                                        tokenId: id.toString(),
                                        claimIssuer,
                                        claimSubject,
                                        topic: topic.toString(),
                                        value: value.toString(),
                                        claimData: claimData.toString(),
                                        claimDataDecoded:
                                            JSON.stringify(claimDataDecoded),
                                        transactionHash:
                                            claimSingleEvent.transactionHash,
                                    });
                                }
                            }
                            batchCertificateIds.push(
                                certificates.length.toString(),
                            );
                            certificates.push({
                                tokenId: certificateId.toString(),
                                batchId: certificateBatchMintedEventBatchId,
                                value: mintedValue.toString(),
                                operator,
                                from,
                                to,
                                transactionHash: mintEvent.transactionHash,
                            });
                        }
                    }
                }
            }
        }

        batches.push({
            batchId: redemptionSetEventBatchId,
            storagePointer,
            certificateIds: batchCertificateIds,
            redemptionStatement,
            transactionHash: redemptionSetEvent.transactionHash,
        });
    }

    console.info(`Finished fetching and parsing data from Energy Web Chain\n`);

    let mintedValue = BigNumber.from(0);
    certificates.forEach(
        c => (mintedValue = mintedValue.add(BigNumber.from(c.value))),
    );
    console.info(`\tMINTED VALUE: ${mintedValue.toString()}\n`);

    let claimedValue = BigNumber.from(0);
    claims.forEach(
        c => (claimedValue = claimedValue.add(BigNumber.from(c.value))),
    );
    console.info(`\tMINTED VALUE: ${claimedValue.toString()}\n\n`);

    console.info(
        `Generating CSV files for batch, certificates and claims...\n`,
    );

    const batchesCSV = Papa.unparse(batches);
    fs.writeFile(
        path.resolve(__dirname, 'batches.csv'),
        batchesCSV,
        function (err) {
            if (err) {
                return console.error(
                    `\tError while generating batches CSV: ${err.message}\n`,
                );
            }
            console.info('\tbatches.csv generated!\n');
        },
    );

    const certificatesCSV = Papa.unparse(certificates);
    fs.writeFile(
        path.resolve(__dirname, 'certificates.csv'),
        certificatesCSV,
        function (err) {
            if (err) {
                return console.error(
                    `\tError while generating certificates CSV: ${err.message}\n`,
                );
            }
            console.info('\tcertificates.csv generated!\n');
        },
    );

    const claimsCSV = Papa.unparse(claims);
    fs.writeFile(
        path.resolve(__dirname, 'claims.csv'),
        claimsCSV,
        function (err) {
            if (err) {
                return console.error(
                    `\tError while generating claims CSV: ${err.message}\n`,
                );
            }
            console.info('\tclaims.csv generated!\n');
        },
    );
};

parseEwcData().catch(err =>
    console.error(
        `Error while trying to parse Energy Web Chain data: ${err.message}`,
    ),
);
