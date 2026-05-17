// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * @dev Deployless batch reader for Uniswap V4 tick bitmaps.
 *
 *      For each (poolId, minWord, maxWord) request we walk the tickBitmap mapping using
 *      `extsload` against the singleton PoolManager and emit `(wordPos, bitmap)` pairs
 *      only for non-zero words (sparse encoding identical to the V3 batch reader).
 *
 *      Storage:
 *          poolBase = keccak256(abi.encode(poolId, POOLS_SLOT))         // POOLS_SLOT = 6
 *          tickBitmapMap = poolBase + 5
 *          tickBitmap[wordPos] @ keccak256(abi.encode(int256(wordPos), tickBitmapMap))
 */
contract GetUniswapV4PoolTickBitmapBatchRequest {
    struct TickBitmapInfo {
        bytes32 poolId;
        int16 minWord;
        int16 maxWord;
    }

    uint256 private constant POOLS_SLOT = 6;
    uint256 private constant TICK_BITMAP_OFFSET = 5;

    constructor(address poolManager, TickBitmapInfo[] memory allPoolInfo) {
        uint256[][] memory allTickBitmaps = new uint256[][](allPoolInfo.length);

        for (uint256 i = 0; i < allPoolInfo.length; ++i) {
            TickBitmapInfo memory info = allPoolInfo[i];
            bytes32 poolBase = keccak256(abi.encode(info.poolId, POOLS_SLOT));
            bytes32 bitmapMapSlot = bytes32(uint256(poolBase) + TICK_BITMAP_OFFSET);

            uint256 capacity = uint256(uint16(info.maxWord - info.minWord) + 1) * 2;
            uint256[] memory tickBitmaps = new uint256[](capacity);

            uint256 wordIdx = 0;
            for (int16 j = info.minWord; j <= info.maxWord; ++j) {
                bytes32 wordSlot = keccak256(abi.encode(int256(j), bitmapMapSlot));
                uint256 tickBitmap = uint256(IExtsload(poolManager).extsload(wordSlot));

                if (tickBitmap == 0) {
                    if (j == type(int16).max) {
                        break;
                    }
                    continue;
                }

                tickBitmaps[wordIdx] = uint256(int256(j));
                ++wordIdx;
                tickBitmaps[wordIdx] = tickBitmap;
                ++wordIdx;

                if (j == type(int16).max) {
                    break;
                }
            }

            assembly {
                mstore(tickBitmaps, wordIdx)
            }

            allTickBitmaps[i] = tickBitmaps;
        }

        bytes memory encoded = abi.encode(allTickBitmaps);
        assembly {
            let dataStart := add(encoded, 0x20)
            return(dataStart, sub(msize(), dataStart))
        }
    }
}

interface IExtsload {
    function extsload(bytes32 slot) external view returns (bytes32);
}
