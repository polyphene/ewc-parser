import { BigNumber, BytesLike, constants, ethers } from 'ethers';
import Papa from 'papaparse';
import fs from 'fs';

import registryExtendedConfig from './registry-extended';
import batchFactoryConfig from './batch-factory';
import path from 'path';
import { defaultAbiCoder } from 'ethers/lib/utils';

/*
 * Energy Web Chain intersected data
 */

type BatchRegistry = {
    [key: string]: Batch;
};

type Batch = {
    id: string;
    batchId: string;
    redemptionStatement: string;
    storagePointer: string;
    certificateIds: string[];
    transactionHash: string;
};

type Certificate = {
    id: string;
    tokenId: string;
    value: string;
    operator: string;
    from: string;
    to: string;
    claimIds: string[];
    transactionHash: string;
};

type Claim = {
    id: string;
    claimIssuer: string;
    claimSubject: string;
    topic: string;
    certificateId: string;
    value: string;
    claimData: string;
    claimDataDecoded: string;
    transactionHash: string;
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

type ClaimData = {
    beneficiary: string;
    region: string;
    countryCode: string;
    periodStartDate: string;
    periodEndDate: string;
    purpose: string;
    consumptionEntityID: string;
    proofID: string;
    location: string;
};

export const getEwfContractsInstances = () => {
    const registryExtendedAddress =
        '0x5651a7A38753A9692B7740CCeCA3824a4d33aEFb';
    const batchFactoryAddress = '0x2248a8e53c8cf533aeef2369fff9dc8c036c8900';
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
    };
};

let claimsDecoded = 0;
// TODO COUNT CLAIMS DECODED PRINT IT ANS STORE WHAT WAS FOUND

const parseEwcData = async () => {
    console.info(
        `Starting process to fetch and parse data from Energy Web Chain...\n`,
    );

    const { registryExtendedContract, batchFactoryContract } =
        getEwfContractsInstances();

    const mintEvents = await registryExtendedContract.queryFilter(
        registryExtendedContract.filters.TransferSingle(
            null,
            constants.AddressZero,
        ),
    );

    const redemptionSetEvents = await batchFactoryContract.queryFilter(
        batchFactoryContract.filters.RedemptionStatementSet(),
    );

    console.info(
        `\tFound ${redemptionSetEvents.length} redemption statement set on batches\n`,
    );

    const certificateBatchMintedEvents = await batchFactoryContract.queryFilter(
        batchFactoryContract.filters.CertificateBatchMinted(),
    );

    console.info(
        `\tFound ${certificateBatchMintedEvents.length} certificates minted\n`,
    );

    const claimSingleEvents = await registryExtendedContract.queryFilter(
        registryExtendedContract.filters.ClaimSingle(),
    );

    console.info(`\tFound ${claimSingleEvents.length} claims\n\n`);

    const batches: Batch[] = [];
    const certificates: Certificate[] = [];
    const claims: Claim[] = [];

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
                            // Temporary buffer for claims related to certificate ID.
                            const claimIds: string[] = [];
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
                                        decodeClaimV3(claimData);
                                    if (!claimDataDecoded) {
                                        claimDataDecoded =
                                            decodeClaimV1(claimData);
                                    }
                                    if (!claimDataDecoded) {
                                        claimDataDecoded =
                                            decodeClaimV2(claimData);
                                    }

                                    claimIds.push(claims.length.toString());
                                    claims.push({
                                        id: claims.length.toString(),
                                        claimIssuer,
                                        claimSubject,
                                        topic: topic.toString(),
                                        certificateId: id.toString(),
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
                                id: certificates.length.toString(),
                                tokenId: certificateId.toString(),
                                value: mintedValue.toString(),
                                operator,
                                from,
                                to,
                                claimIds,
                                transactionHash: mintEvent.transactionHash,
                            });
                        }
                    }
                }
            }
        }

        batches.push({
            id: batches.length.toString(),
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

// Sourced from EW repository: https://github.com/energywebfoundation/origin/blob/dc4930d80d4703d22beee27acac42db9157e27c1/packages/traceability/issuer/src/blockchain-facade/CertificateUtils.ts#L43-L68
const decodeClaimV1 = (data: BytesLike): ClaimData | null => {
    try {
        const [
            beneficiary,
            location,
            countryCode,
            periodStartDate,
            periodEndDate,
            purpose,
        ] = defaultAbiCoder.decode(
            ['string', 'string', 'string', 'string', 'string', 'string'],
            data,
        );

        // If any field is undefined it means that we are not decoding over the proper data structure, so returning null
        if (
            beneficiary === undefined ||
            location === undefined ||
            countryCode === undefined ||
            periodStartDate === undefined ||
            periodEndDate === undefined ||
            purpose === undefined
        ) {
            return null;
        }

        return {
            beneficiary,
            region: '',
            countryCode,
            periodStartDate,
            periodEndDate,
            purpose,
            consumptionEntityID: '',
            proofID: '',
            location,
        };
    } catch {
        return null;
    }
};

// Sourced from EW repository: https://github.com/energywebfoundation/origin/blob/dc4930d80d4703d22beee27acac42db9157e27c1/packages/traceability/issuer/src/blockchain-facade/CertificateUtils.ts#L43-L68
const decodeClaimV2 = (data: BytesLike) => {
    try {
        const [claimData] = defaultAbiCoder.decode(['string'], data);

        return JSON.parse(claimData);
    } catch {
        return null;
    }
};

// Sourced from messages sent to Moca
const decodeClaimV3 = (data: BytesLike) => {
    try {
        const [
            beneficiary,
            region,
            countryCode,
            periodStartDate,
            periodEndDate,
            purpose,
            consumptionEntityID,
            proofID,
        ] = defaultAbiCoder.decode(
            [
                'string',
                'string',
                'string',
                'string',
                'string',
                'string',
                'string',
                'string',
            ],
            data,
        );

        if (
            beneficiary === undefined ||
            region === undefined ||
            countryCode === undefined ||
            periodStartDate === undefined ||
            periodEndDate === undefined ||
            purpose === undefined ||
            consumptionEntityID === undefined ||
            proofID === undefined
        ) {
            return null;
        }

        return {
            beneficiary,
            region,
            countryCode,
            periodStartDate,
            periodEndDate,
            purpose,
            consumptionEntityID,
            proofID,
            location: '',
        };
    } catch {
        return null;
    }
};
parseEwcData().catch(err =>
    console.error(
        `Error while trying to parse Energy Web Chain data: ${err.message}`,
    ),
);
