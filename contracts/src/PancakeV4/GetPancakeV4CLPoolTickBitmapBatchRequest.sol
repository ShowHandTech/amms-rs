// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * @dev Deployless batch reader for PCS Infinity CLPoolManager tick bitmaps.
 *
 *      Calls `getPoolBitmapInfo(poolId, word)` for each word in [minWord, maxWord]
 *      and emits `(wordPos, bitmap)` pairs only for non-zero words (sparse encoding,
 *      matching the V3/V4 batch readers).
 */
contract GetPancakeV4CLPoolTickBitmapBatchRequest {
    struct TickBitmapInfo {
        bytes32 poolId;
        int16 minWord;
        int16 maxWord;
    }

    constructor(address clPoolManager, TickBitmapInfo[] memory allPoolInfo) {
        uint256[][] memory allTickBitmaps = new uint256[][](allPoolInfo.length);

        for (uint256 i = 0; i < allPoolInfo.length; ++i) {
            TickBitmapInfo memory info = allPoolInfo[i];

            uint256 capacity = uint256(uint16(info.maxWord - info.minWord) + 1) * 2;
            uint256[] memory tickBitmaps = new uint256[](capacity);

            uint256 wordIdx = 0;
            for (int16 j = info.minWord; j <= info.maxWord; ++j) {
                uint256 tickBitmap = ICLPoolManagerView(clPoolManager).getPoolBitmapInfo(info.poolId, j);

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

interface ICLPoolManagerView {
    function getPoolBitmapInfo(bytes32 id, int16 word) external view returns (uint256 tickBitmap);
}
