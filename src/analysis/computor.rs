use crate::vo::biz::{Protfolio, SlopeLine, Ticker, TimeUnit};
use crate::vo::core::LockTradeInfo;
use crate::Result;
use chrono::{TimeZone, Utc};
use log::{debug, log_enabled, trace};
use rayon::prelude::*;
use std::collections::{BTreeMap, LinkedList};
use std::f64::NAN;

// Calculate slope for nearest line
// Reference to doc/trend.md
fn calculate_slope(samples: &Vec<(f64, f64)>) -> (f64, f64) {
    match samples.len() {
        0 => (NAN, NAN),
        1 => {
            let (_, y) = samples.first().unwrap();
            (0.0, *y)
        }
        2 => {
            let (x_1, y_1) = samples.first().unwrap();
            let (x_2, y_2) = samples.last().unwrap();

            // same timestamp
            if x_1 == x_2 {
                if y_1 == y_2 {
                    return (0.0, *y_1);
                } else {
                    return (0.0, (y_1 + y_2) / 2.0);
                }
            }

            // y = Ax + B
            // -> y_1 = Ax_1 + B, y_2 = Ax_2 + B
            let a = (y_1 - y_2) / (x_1 - x_2);
            let b = y_1 - a * x_1;

            (a, b)
        }
        _ => {
            let count = samples.len() as f64;
            let x_avg: f64 = samples.iter().map(|(x, _)| *x / count).sum();
            let y_avg: f64 = samples.iter().map(|(_, y)| *y / count).sum();

            let xy: f64 = samples
                .iter()
                .map(|(x, y)| (*x - x_avg) * (*y - y_avg))
                .sum();

            let x_x: f64 = samples
                .iter()
                .map(|(x, _)| (*x - x_avg) * (*x - x_avg))
                .sum();

            let a = xy / x_x;
            let b = y_avg - a * x_avg;

            // A = 2, y = Ax + B
            // 2 = 2 * 1 + B, (x = 1, y = 2)
            // 4 = 2 * 2 + B, (x = 2, y = 4)
            // A = slope = delta-Y / delta-X = (4 - 2) / (2 - 1)
            (a, b)
        }
    }
}

fn group_by(mut map: BTreeMap<i64, Vec<Protfolio>>, p: Protfolio) -> BTreeMap<i64, Vec<Protfolio>> {
    if let Some(list) = map.get_mut(&p.unit_time) {
        list.push(p);
    } else {
        let unit_time = p.unit_time;
        let mut list: Vec<Protfolio> = Vec::new();
        list.push(p);
        map.insert(unit_time, list);
    }
    map
}

fn calculate(values: &Vec<Protfolio>) -> Protfolio {
    let first = values.first().unwrap();
    let last = values.last().unwrap();

    let price_open = last.price;
    let price_close = first.price;

    let price_max = values
        .par_iter()
        .map(|p| p.price)
        .reduce(|| 0.0, |a, b| if a >= b { a } else { b });
    let price_min = values
        .par_iter()
        .map(|p| p.price)
        .reduce(|| price_max, |a, b| if a <= b { a } else { b });

    // Calculate average price
    let price_sum: f64 = values.par_iter().map(|p| p.price as f64).sum();
    let price_avg: f32 = (price_sum / values.len() as f64) as f32;

    let volume = first.volume - last.volume; // FIXME : lack of the volume of first item

    let samples = values.len() as u32;

    let (slope, b_num) = calculate_slope(
        &values
            .iter()
            .map(|p| (p.time as f64, p.price as f64))
            .collect::<Vec<(f64, f64)>>(),
    );

    Protfolio {
        id: first.id.clone(),
        price: price_avg,
        time: first.unit_time,
        kind: 'p',
        unit_time: first.unit_time,
        unit: first.unit.clone(),
        period_type: first.unit.duration,
        quote_type: first.quote_type,
        market_hours: first.market_hours,
        volume: volume,
        max_price: price_max,
        min_price: price_min,
        open_price: price_open,
        close_price: price_close,
        sample_size: samples,
        slope: if slope.is_nan() { None } else { Some(slope) },
        b_num: if b_num.is_nan() { None } else { Some(b_num) },
    }
}

fn update(target: &Protfolio, protfolios: &mut LinkedList<Protfolio>) -> Result<()> {
    let find_result = protfolios
        .iter_mut()
        .find(|p| p.unit_time == target.unit_time);
    if let Some(result) = find_result {
        result.update_by(target);
        trace!("Updated with {:?}", result);
    } else {
        protfolios.push_front((*target).clone());
        trace!("Added with {:?}", target);
    }
    Ok(())
}

fn aggregate_fixed_unit(
    symbol: &str,
    unit: &TimeUnit,
    tickers: &LinkedList<Ticker>,
    protfolios: &mut LinkedList<Protfolio>,
    _trade: LockTradeInfo,
) -> Result<()> {
    // Take source data in 3x time range
    let scope = unit.duration as i64 * 1000 * 3;
    // Use latest ticker time to restrict time
    let min_time = tickers.front().unwrap().time - scope;

    if log_enabled!(log::Level::Debug) {
        let count = tickers.iter().take_while(|t| t.time >= min_time).count();
        debug!(
            "Aggreate fixed protfolio for {} of {}, data size: {}",
            symbol, unit.duration, count
        );
    }

    // calculate
    let results = tickers
        .iter()
        .take_while(|t| t.time >= min_time)
        .map(|t| Protfolio::fixed(t, unit))
        .fold(BTreeMap::new(), |map: BTreeMap<i64, Vec<Protfolio>>, p| {
            group_by(map, p)
        })
        .iter()
        .rev()
        .take(2)
        .map(|(_, values)| calculate(values))
        .collect::<Vec<Protfolio>>();

    // update protfolio, only handle the latest 2 records
    let result_size = results.len();
    for (index, target) in results.iter().enumerate() {
        if log_enabled!(log::Level::Trace) {
            trace!(
                "Updating fixed protfolio, {} of {}, index: {}/{}, {:?}",
                symbol,
                unit.duration,
                index,
                result_size,
                target
            );
        }
        update(target, protfolios)?;
    }

    // No update for trade info, only moving data is used

    Ok(())
}

fn aggregate_moving_unit(
    symbol: &str,
    unit: &TimeUnit,
    tickers: &LinkedList<Ticker>,
    protfolios: &mut LinkedList<Protfolio>,
    trade: LockTradeInfo,
) -> Result<()> {
    let last_timestamp = tickers.front().unwrap().time;
    let scope = last_timestamp - (unit.duration as i64 * unit.period as i64) * 1000;

    if log_enabled!(log::Level::Debug) {
        let count = tickers.iter().take_while(|t| t.time > scope).count();
        debug!(
            "Aggreate moving protoflio for {} of {}, period: {}, data size: {}, last_timestamp: {}, scope: {}",
            symbol, unit.duration,unit.period, count, Utc.timestamp_millis(last_timestamp).to_rfc3339(), Utc.timestamp_millis(scope).to_rfc3339()
        );
    }

    // calculate
    let results = tickers
        .iter()
        .take_while(|t| t.time > scope) // only take items in range
        .map(|t| Protfolio::moving(t, unit, last_timestamp))
        .fold(BTreeMap::new(), |map: BTreeMap<i64, Vec<Protfolio>>, p| {
            group_by(map, p)
        })
        .values()
        .map(|values| calculate(values))
        .rev() // sort by time desc
        .collect::<Vec<Protfolio>>();

    // update protfolio, renew all records
    let result_size = results.len();
    protfolios.clear();
    for (index, target) in results.iter().enumerate() {
        if log_enabled!(log::Level::Trace) {
            trace!(
                "Updating moving protfolio, {} of {}, index: {}/{}, {:?}",
                symbol,
                unit.duration,
                index,
                result_size,
                target
            );
        }
        // push into list from newest to oldest
        protfolios.push_back((*target).clone());
    }

    // update trade info
    let values: Vec<f64> = results.iter().map(|f| f.slope.unwrap_or(0.0)).collect();
    let mut guard = trade.write().unwrap();
    guard.update_state(&unit.name, values);
    debug!("Update trade info: {:?}", guard);

    Ok(())
}

pub fn draw_slop_lines(protfolios: &Vec<Protfolio>) -> Vec<SlopeLine> {
    let unit = &protfolios.first().unwrap().unit;

    let mut points: Vec<SlopeLine> = Vec::new();

    for protfolio in protfolios {
        // y = ax + b
        // price = slope * time + b_num

        // start point
        points.push(SlopeLine {
            id: protfolio.id.clone(),
            price: get_y(protfolio.slope, protfolio.b_num, protfolio.time + 1),
            time: protfolio.time + 1,
            kind: 's',
            period_type: protfolio.period_type,
        });
        // end point
        points.push(SlopeLine {
            id: protfolio.id.clone(),
            price: get_y(
                protfolio.slope,
                protfolio.b_num,
                protfolio.time + unit.duration as i64 - 1,
            ),
            time: protfolio.time + unit.duration as i64 - 1,
            kind: 's',
            period_type: protfolio.period_type,
        });
    }

    points
}

fn get_y(slope: Option<f64>, b_num: Option<f64>, time: i64) -> f64 {
    if slope == None || b_num == None {
        0.0
    } else {
        let a = slope.unwrap();
        let x = time as f64;
        let b = b_num.unwrap();
        a * x + b
    }
}

impl Protfolio {
    fn fixed(t: &Ticker, unit: &TimeUnit) -> Self {
        Self {
            id: t.id.clone(),
            price: t.price,
            time: t.time,
            kind: 'p',
            // fixed time range, accroding time unit
            unit_time: t.time - t.time % (unit.duration as i64 * 1000),
            unit: unit.clone(),
            period_type: unit.duration,
            quote_type: t.quote_type,
            market_hours: t.market_hours,
            volume: t.day_volume,
            max_price: 0.0,
            min_price: 0.0,
            open_price: 0.0,
            close_price: 0.0,
            sample_size: 0,
            slope: None,
            b_num: None,
        }
    }

    fn moving(t: &Ticker, unit: &TimeUnit, base_time: i64) -> Self {
        Self {
            id: t.id.clone(),
            price: t.price,
            time: t.time,
            kind: 'p',
            // moving time range, according base_time
            unit_time: base_time
                + ((base_time - t.time) / (unit.duration as i64 * 1000))
                    * (unit.duration as i64 * 1000),
            unit: unit.clone(),
            period_type: unit.duration,
            quote_type: t.quote_type,
            market_hours: t.market_hours,
            volume: t.day_volume,
            max_price: 0.0,
            min_price: 0.0,
            open_price: 0.0,
            close_price: 0.0,
            sample_size: 0,
            slope: None,
            b_num: None,
        }
    }

    fn update_by(&mut self, source: &Self) {
        self.price = source.price;
        self.volume = source.volume;
        self.max_price = source.max_price;
        self.min_price = source.min_price;
        self.open_price = source.open_price;
        self.close_price = source.close_price;
        self.sample_size = source.sample_size;
        self.slope = source.slope;
        self.b_num = source.b_num;
    }
}

impl TimeUnit {
    pub fn rebalance(
        &self,
        symbol: &str,
        message_id: i64,
        tickers: &LinkedList<Ticker>,
        protfolios: &mut LinkedList<Protfolio>,
        trade: LockTradeInfo,
    ) -> Result<()> {
        debug!(
            "Rebalance {} of {}, message_id: {}, ticker count: {}",
            symbol,
            self.duration,
            &message_id,
            &tickers.len()
        );
        if self.period == 0 {
            aggregate_fixed_unit(symbol, self, tickers, protfolios, trade)?;
        } else {
            aggregate_moving_unit(symbol, self, tickers, protfolios, trade)?;
        }
        Ok(())
    }
}
