use crate::vo::biz::{Protfolio, Ticker, TimeUnit};
use crate::Result;
use chrono::Utc;
use log::trace;
use log::{debug, info};
use rayon::prelude::*;
use std::collections::{HashMap, LinkedList};
use std::f64::NAN;

// Calculate slop for nearest line
// Reference to doc/trend.md
fn slope(samples: &Vec<(f64, f64)>) -> f64 {
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

    // A = 2, y = Ax + B
    // 2 = 2 * 1 + B, (x = 1, y = 2)
    // 4 = 2 * 2 + B, (x = 2, y = 4)
    // slope = delta-Y / delta-X = (4 - 2) / (2 - 1)
    a
}

fn group_by(mut map: HashMap<i64, Vec<Protfolio>>, p: Protfolio) -> HashMap<i64, Vec<Protfolio>> {
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
    debug!("Calculate protfolio");
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

    let slope = slope(
        &values
            .iter()
            .map(|p| (p.time as f64, p.price as f64))
            .collect::<Vec<(f64, f64)>>(),
    );

    Protfolio {
        id: first.id.clone(),
        price: price_avg,
        time: first.unit_time,
        unit_time: first.unit_time,
        unit: first.unit,
        period_type: first.unit as i32,
        quote_type: first.quote_type,
        market_hours: first.market_hours,
        volume: volume,
        change: 0.0, // TODO:
        change_rate: 0.0,
        max_price: price_max,
        min_price: price_min,
        open_price: price_open,
        close_price: price_close,
        sample_size: samples,
        slope: if slope == NAN {
            if values.len() < 2 {
                Some(0.0) // no data in this period
            } else {
                Some(slope)
            }
        } else {
            None
        },
    }
}

fn update(target: &Protfolio, protfolios: &mut LinkedList<Protfolio>) -> Result<()> {
    let find_result = protfolios
        .iter_mut()
        .find(|p| p.unit_time == target.unit_time);
    if let Some(result) = find_result {
        result.update_by(target);
        debug!("Updated with {:?}", target);
    } else {
        protfolios.push_front((*target).clone());
        debug!("Added with {:?}", target);
    }
    Ok(())
}

fn aggregate_fixed_unit(
    unit: &TimeUnit,
    tickers: &LinkedList<Ticker>,
    protfolios: &mut LinkedList<Protfolio>,
) -> Result<()> {
    // Take source data in 3x time range
    let scope = *unit as i64 * 1000 * 3;
    // Use latest ticker time to restrict time
    let min_time = tickers.front().unwrap().time - scope;
    // calculate
    let mut result = tickers
        .iter()
        .take_while(|t| t.time >= min_time)
        .map(|t| Protfolio::fixed(t, unit))
        .fold(HashMap::new(), |map: HashMap<i64, Vec<Protfolio>>, p| {
            group_by(map, p)
        })
        .values()
        .map(|values| calculate(values))
        .collect::<Vec<Protfolio>>();

    // sort by unit time (desc)
    result.sort_by(|x, y| x.unit_time.partial_cmp(&y.unit_time).unwrap());
    trace!("Result = {:?}", result);

    // update protfolio, only handle the latest 2 records
    for (index, target) in result.iter().enumerate() {
        if index > 0 {
            update(target, protfolios)?;
        }
    }
    Ok(())
}

fn aggregate_moving_unit(
    unit: &TimeUnit,
    tickers: &LinkedList<Ticker>,
    protfolios: &mut LinkedList<Protfolio>,
) -> Result<()> {
    let last_timestamp = tickers.front().unwrap().time;
    // calculate
    let mut result = tickers
        .iter()
        .map(|t| Protfolio::moving(t, unit, last_timestamp))
        .fold(HashMap::new(), |map: HashMap<i64, Vec<Protfolio>>, p| {
            group_by(map, p)
        })
        .values()
        .map(|values| calculate(values))
        .collect::<Vec<Protfolio>>();

    // sort by unit time (desc)
    result.sort_by(|x, y| x.unit_time.partial_cmp(&y.unit_time).unwrap());
    trace!("Result = {:?}", result);

    // update protfolio, only handle the latest 2 records
    for (index, target) in result.iter().enumerate() {
        if index > 0 {
            update(target, protfolios)?;
        }
    }
    Ok(())
}

impl Protfolio {
    fn fixed(t: &Ticker, unit: &TimeUnit) -> Self {
        Protfolio {
            id: t.id.clone(),
            price: t.price,
            time: t.time,
            // fixed time range, accroding time unit
            unit_time: t.time - t.time % (*unit as i64 * 1000),
            unit: unit.clone(),
            period_type: *unit as i32,
            quote_type: t.quote_type,
            market_hours: t.market_hours,
            volume: t.day_volume,
            change: t.change,
            change_rate: 0.0,
            max_price: 0.0,
            min_price: 0.0,
            open_price: 0.0,
            close_price: 0.0,
            sample_size: 0,
            slope: None,
        }
    }

    fn moving(t: &Ticker, unit: &TimeUnit, base_time: i64) -> Self {
        Protfolio {
            id: t.id.clone(),
            price: t.price,
            time: t.time,
            // moving time range, according base_time
            unit_time: base_time + (base_time - t.time) % (*unit as i64 * -1000),
            unit: unit.clone(),
            period_type: *unit as i32,
            quote_type: t.quote_type,
            market_hours: t.market_hours,
            volume: t.day_volume,
            change: t.change,
            change_rate: 0.0,
            max_price: 0.0,
            min_price: 0.0,
            open_price: 0.0,
            close_price: 0.0,
            sample_size: 0,
            slope: None,
        }
    }

    fn update_by(&mut self, source: &Protfolio) {
        self.price = source.price;
        self.volume = source.volume;
        self.change = source.change;
        self.change_rate = source.change_rate;
        self.max_price = source.max_price;
        self.min_price = source.min_price;
        self.open_price = source.open_price;
        self.close_price = source.close_price;
        self.sample_size = source.sample_size;
        self.slope = source.slope;
    }
}

impl TimeUnit {
    pub fn rebalance(
        &self,
        tickers: &LinkedList<Ticker>,
        protfolios: &mut LinkedList<Protfolio>,
    ) -> Result<()> {
        debug!("Rebalance count: {}", &tickers.len());
        // Duration in second
        let sec = *self as i64;
        if sec > 0 {
            aggregate_fixed_unit(self, tickers, protfolios)?;
        } else {
            let start = Utc::now().timestamp_millis();
            aggregate_moving_unit(self, tickers, protfolios)?;
            let end = Utc::now().timestamp_millis();
            let len = tickers.len();
            if len % 1000 == 0 {
                let id = &protfolios.front().unwrap().id;
                info!(
                    "moving {} tickers for {:?} of {} costs {:?}",
                    len,
                    &self,
                    &id,
                    std::time::Duration::from_millis((end - start) as u64),
                );
            }
        }
        Ok(())
    }
}
