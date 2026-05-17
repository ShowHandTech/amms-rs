// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * @dev Deployless batch request that reads `slot0` (sqrtPriceX96, tick, protocolFee, lpFee)
 *      and `liquidity` for an array of Uniswap V4 PoolId values, directly out of the
 *      singleton `PoolManager`'s storage via `extsload`.
 *
 *      We do NOT depend on v4-periphery's StateLibrary; the storage layout is part of the V4
 *      core spec so we inline the slot math here for stability across periphery versions.
 *
 *      Pool state layout in PoolManager (POOLS_SLOT = 6):
 *          baseSlot = keccak256(abi.encode(poolId, POOLS_SLOT))
 *          baseSlot + 0  : Pool.Slot0 (packed: lpFee | protocolFee | tick | sqrtPriceX96)
 *          baseSlot + 3  : uint128 liquidity
 */
contract GetUniswapV4PoolSlot0BatchRequest {
    struct Slot0Data {
        uint160 sqrtPriceX96;
        int24 tick;
        uint24 protocolFee;
        uint24 lpFee;
        uint128 liquidity;
    }

    uint256 private constant POOLS_SLOT = 6;

    constructor(address poolManager, bytes32[] memory poolIds) {
        Slot0Data[] memory results = new Slot0Data[](poolIds.length);

        for (uint256 i = 0; i < poolIds.length; ++i) {
            bytes32 poolId = poolIds[i];
            bytes32 baseSlot = keccak256(abi.encode(poolId, POOLS_SLOT));

            bytes32 slot0Packed = IExtsload(poolManager).extsload(baseSlot);
            bytes32 liquiditySlot = bytes32(uint256(baseSlot) + 3);
            bytes32 liquidityWord = IExtsload(poolManager).extsload(liquiditySlot);

            uint160 sqrtPriceX96;
            int24 tick;
            uint24 protocolFee;
            uint24 lpFee;
            assembly ("memory-safe") {
                sqrtPriceX96 := and(slot0Packed, 0xffffffffffffffffffffffffffffffffffffffff)
                tick := signextend(2, shr(160, slot0Packed))
                protocolFee := and(shr(184, slot0Packed), 0xffffff)
                lpFee := and(shr(208, slot0Packed), 0xffffff)
            }

            results[i] = Slot0Data({
                sqrtPriceX96: sqrtPriceX96,
                tick: tick,
                protocolFee: protocolFee,
                lpFee: lpFee,
                liquidity: uint128(uint256(liquidityWord))
            });
        }

        bytes memory encoded = abi.encode(results);
        assembly {
            let dataStart := add(encoded, 0x20)
            return(dataStart, sub(msize(), dataStart))
        }
    }
}

interface IExtsload {
    function extsload(bytes32 slot) external view returns (bytes32);
}
