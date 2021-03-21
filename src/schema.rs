table! {
    ftx_eth_perp_trades (id) {
        id -> Int4,
        liquidation -> Bool,
        price -> Float4,
        side -> Bool,
        size -> Float4,
        time -> Timestamp,
    }
}
