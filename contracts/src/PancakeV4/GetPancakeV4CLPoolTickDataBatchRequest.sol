// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * @dev Deployless batch reader for PCS Infinity CLPoolManager per-tick info.
 *
 *      Uses `getPoolTickInfo(poolId, tick)` which returns the full Tick.Info struct.
 *      `initialized` is derived from `liquidityGross != 0` (PCS dropped the explicit bool
 *      in line with Uniswap V4).
 */
contract GetPancakeV4CLPoolTickDataBatchRequest {
    struct TickDataInfo {
        bytes32 poolId;
        int24[] ticks;
    }

    struct Info {
        bool initialized;
        uint128 liquidityGross;
        int128 liquidityNet;
    }

    constructor(address clPoolManager, TickDataInfo[] memory allPoolInfo) {
        Info[][] memory tickInfoReturn = new Info[][](allPoolInfo.length);

        for (uint256 i = 0; i < allPoolInfo.length; ++i) {
            TickDataInfo memory pInfo = allPoolInfo[i];

            Info[] memory tickInfo = new Info[](pInfo.ticks.length);
            for (uint256 j = 0; j < pInfo.ticks.length; ++j) {
                ICLPoolManagerView.TickInfo memory raw =
                    ICLPoolManagerView(clPoolManager).getPoolTickInfo(pInfo.poolId, pInfo.ticks[j]);

                tickInfo[j] = Info({
                    initialized: raw.liquidityGross != 0,
                    liquidityGross: raw.liquidityGross,
                    liquidityNet: raw.liquidityNet
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

interface ICLPoolManagerView {
    struct TickInfo {
        uint128 liquidityGross;
        int128 liquidityNet;
        uint256 feeGrowthOutside0X128;
        uint256 feeGrowthOutside1X128;
    }

    function getPoolTickInfo(bytes32 id, int24 tick) external view returns (TickInfo memory);
}
