// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * @dev Deployless batch reader for Uniswap V4 per-tick info.
 *
 *      V4's `TickInfo` struct is:
 *          struct TickInfo {
 *              uint128 liquidityGross;     // slot 0 low 128 bits
 *              int128  liquidityNet;       // slot 0 high 128 bits
 *              uint256 feeGrowthOutside0;  // slot 1 (ignored here)
 *              uint256 feeGrowthOutside1;  // slot 2 (ignored here)
 *          }
 *
 *      Storage:
 *          poolBase   = keccak256(abi.encode(poolId, POOLS_SLOT))   // POOLS_SLOT = 6
 *          ticksMap   = poolBase + 4
 *          ticks[tick] base @ keccak256(abi.encode(int256(tick), ticksMap))
 *
 *      `initialized` is liquidityGross != 0 (V4 removed the explicit bool).
 */
contract GetUniswapV4PoolTickDataBatchRequest {
    struct TickDataInfo {
        bytes32 poolId;
        int24[] ticks;
    }

    struct Info {
        bool initialized;
        uint128 liquidityGross;
        int128 liquidityNet;
    }

    uint256 private constant POOLS_SLOT = 6;
    uint256 private constant TICKS_OFFSET = 4;

    constructor(address poolManager, TickDataInfo[] memory allPoolInfo) {
        Info[][] memory tickInfoReturn = new Info[][](allPoolInfo.length);

        for (uint256 i = 0; i < allPoolInfo.length; ++i) {
            TickDataInfo memory pInfo = allPoolInfo[i];
            bytes32 poolBase = keccak256(abi.encode(pInfo.poolId, POOLS_SLOT));
            bytes32 ticksMapSlot = bytes32(uint256(poolBase) + TICKS_OFFSET);

            Info[] memory tickInfo = new Info[](pInfo.ticks.length);
            for (uint256 j = 0; j < pInfo.ticks.length; ++j) {
                int24 tick = pInfo.ticks[j];
                bytes32 tickBase = keccak256(abi.encode(int256(tick), ticksMapSlot));

                bytes32 packed = IExtsload(poolManager).extsload(tickBase);

                uint128 liquidityGross;
                int128 liquidityNet;
                assembly ("memory-safe") {
                    liquidityGross := and(packed, 0xffffffffffffffffffffffffffffffff)
                    liquidityNet := sar(128, packed)
                }

                tickInfo[j] = Info({
                    initialized: liquidityGross != 0,
                    liquidityGross: liquidityGross,
                    liquidityNet: liquidityNet
                });
            }

            tickInfoReturn[i] = tickInfo;
        }

        bytes memory encoded = abi.encode(tickInfoReturn);
        assembly {
            let dataStart := add(encoded, 0x20)
            return(dataStart, sub(msize(), dataStart))
        }
    }
}

interface IExtsload {
    function extsload(bytes32 slot) external view returns (bytes32);
}
