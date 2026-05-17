// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * @dev Deployless batch reader for PCS Infinity CLPoolManager pool state.
 *
 *      Unlike Uniswap V4 PoolManager, CLPoolManager exposes public view getters
 *      (`getSlot0(id)` and `getLiquidity(id)`) so we don't need `extsload` here.
 *      Using the getters insulates us from storage layout changes in future
 *      CLPoolManager versions.
 */
contract GetPancakeV4CLPoolSlot0BatchRequest {
    struct Slot0Data {
        uint160 sqrtPriceX96;
        int24 tick;
        uint24 protocolFee;
        uint24 lpFee;
        uint128 liquidity;
    }

    constructor(address clPoolManager, bytes32[] memory poolIds) {
        Slot0Data[] memory results = new Slot0Data[](poolIds.length);

        for (uint256 i = 0; i < poolIds.length; ++i) {
            bytes32 poolId = poolIds[i];
            (uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee) =
                ICLPoolManagerView(clPoolManager).getSlot0(poolId);
            uint128 liquidity = ICLPoolManagerView(clPoolManager).getLiquidity(poolId);

            results[i] = Slot0Data({
                sqrtPriceX96: sqrtPriceX96,
                tick: tick,
                protocolFee: protocolFee,
                lpFee: lpFee,
                liquidity: liquidity
            });
        }

        bytes memory encoded = abi.encode(results);
        assembly {
            let dataStart := add(encoded, 0x20)
            return(dataStart, sub(msize(), dataStart))
        }
    }
}

interface ICLPoolManagerView {
    function getSlot0(bytes32 id)
        external
        view
        returns (uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee);
    function getLiquidity(bytes32 id) external view returns (uint128 liquidity);
}
