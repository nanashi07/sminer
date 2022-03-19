use crate::vo::biz::{Protfolio, Ticker, TimeUnit};
use crate::Result;
use chrono::Utc;
use log::{debug, info, trace};
use std::collections::{HashMap, LinkedList};

fn slope(samples: &Vec<(f64, f64)>) -> f64 {
    let count = samples.len() as f64;
    let x_avg: f64 = samples.iter().map(|(x, y)| *x / count).sum();
    let y_avg: f64 = samples.iter().map(|(x, y)| *y / count).sum();

    let xy: f64 = samples
        .iter()
        .map(|(x, y)| (*x - x_avg) * (*y - y_avg))
        .sum();

    let x_x: f64 = samples
        .iter()
        .map(|(x, _)| (*x - x_avg) * (*x - x_avg))
        .sum();

    let A = xy / x_x;

    // A = 2, y = Ax + B
    // 2 = 2 * 1 + B, (x = 1, y = 2)
    // 4 = 2 * 2 + B, (x = 2, y = 4)
    // slope = delta-Y / delta-X = (4 - 2) / (2 - 1)
    A
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

    let price_max = values.iter().map(|p| p.price).reduce(f32::max).unwrap();
    let price_min = values.iter().map(|p| p.price).reduce(f32::min).unwrap();

    // Calculate average price
    let price_sum: f64 = values.iter().map(|p| p.price as f64).sum();
    let price_avg: f32 = (price_sum / values.len() as f64) as f32;

    let volume = first.volume - last.volume;

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
        quote_type: first.quote_type,
        market_hours: first.market_hours,
        volume: volume,
        change: 0.0,
        change_rate: 0.0,
        max_price: price_max,
        min_price: price_min,
        open_price: price_open,
        close_price: price_close,
        sample_size: samples,
        slope: slope,
    }
}

impl Protfolio {
    fn from(t: &Ticker, unit: &TimeUnit) -> Self {
        Protfolio {
            id: t.id.clone(),
            price: t.price,
            time: t.time,
            unit_time: t.time - t.time % (*unit as i64 * 1000),
            unit: unit.clone(),
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
            slope: 0.0,
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
        // Duration in milliseconds
        let sec = *self as i64 * 1000;
        if sec > 0 {
            // Take source data in 3x time range
            let scope = sec * 3;
            // Use latest ticker time to restrict time
            let min_time = tickers.front().unwrap().time - scope;
            let mut result = tickers
                .iter()
                .take_while(|t| t.time >= min_time)
                .map(|t| Protfolio::from(t, self))
                .fold(HashMap::new(), |map: HashMap<i64, Vec<Protfolio>>, p| {
                    group_by(map, p)
                })
                .values()
                .map(|values| calculate(values))
                .collect::<Vec<Protfolio>>();
            // sort by unit time (desc)
            result.sort_by(|x, y| y.unit_time.partial_cmp(&x.unit_time).unwrap());
            info!("Result = {:?}", result);

            // update protfolio
            if result.len() > 0 {
                let latest = &result[0];
                let find_result = protfolios
                    .iter_mut()
                    .find(|p| p.unit_time == latest.unit_time);
                if let Some(result) = find_result {
                    result.update_by(latest);
                } else {
                }

                // let j = protfolios.(|p| p.unit_time == latest.unit_time);
            }
            if result.len() > 1 {
                let second = &result[1];
            }
        } else {
            // 1646830800000
            // 1646830830000
        }
        Ok(())
    }
}
