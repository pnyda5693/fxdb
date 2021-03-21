#[macro_use]
extern crate diesel;

mod schema;

use chrono::prelude::*;
use chrono::Duration;
use diesel::prelude::*;
use schema::ftx_eth_perp_trades as trades;
use std::collections::VecDeque;
use std::env;

#[derive(Queryable)]
struct TradeRow {
  id: i32,
  liquidation: bool,
  price: f32,
  side: bool, // true: buy, false: sell
  size: f32,
  time: NaiveDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Trade {
  pub timestamp: DateTime<Utc>,
  pub price: f64,
  pub amount: f64,
}

impl Trade {
  pub fn fetch(since: DateTime<Utc>, until: DateTime<Utc>) -> TradeIter {
    let database_url = env::var("DATABASE_URL").expect("an environment variable DATABASE_URL not found");
    let conn = PgConnection::establish(&database_url).expect("Error connecting to MySQL");
    let since = trades::table
      .select(trades::id)
      .filter(trades::time.ge(since.naive_utc()))
      .order(trades::id.asc())
      .limit(1)
      .get_result(&conn)
      .expect("No trade found");

    TradeIter {
      since,
      until,
      conn,
      page: VecDeque::new(),
    }
  }

  pub fn is_buy(&self) -> bool {
    0.0 < self.amount
  }

  pub fn is_sell(&self) -> bool {
    0.0 > self.amount
  }
}

pub struct TradeIter {
  since: i32,
  until: DateTime<Utc>,
  conn: PgConnection,
  page: VecDeque<TradeRow>,
}

impl Iterator for TradeIter {
  type Item = Trade;

  fn next(&mut self) -> Option<Self::Item> {
    if 0 >= self.page.len() {
      self.page = trades::table
        .order(trades::id.asc())
        .filter(trades::id.ge(self.since))
        .limit(10000)
        .load::<TradeRow>(&self.conn)
        .unwrap()
        .into();
    }

    if let Some(row) = self.page.pop_front() {
      self.since = row.id + 1;

      let trade: Trade = row.into();
      if self.until <= trade.timestamp {
        None
      } else {
        Some(trade)
      }
    } else {
      None
    }
  }
}

impl From<TradeRow> for Trade {
  fn from(trade: TradeRow) -> Self {
    Self {
      timestamp: DateTime::from_utc(trade.time, Utc),
      price: (trade.price as f64 * 100.0).round() / 100.0,
      amount: if trade.side {
        trade.size as f64
      } else {
        -(trade.size as f64)
      },
    }
  }
}
