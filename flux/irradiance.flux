// InfluxDB2 Flux example
// Display clearsky irradiance and site production in a Graph visualization
// The SMA inverters store production as the total production in Wh since commissioning, typically
// every 5 minutes.  To convert this data to Watts (W), you must find the difference between adjacent
// data points (Wh) and multiply by 3600 secs/hour (Ws) and then divide by the interval in seconds).
//
// InfluxDB 1.8.x users should add the retention policy to the bucket name, ie, 'multisma2/autogen'

days_to_visualize = -5d     // Visualize the last 5 days of site total production

// Collect the site total production data
from(bucket: "multisma2")
  |> range(start: days_to_visualize)
  |> filter(fn: (r) => r._measurement == "production" and r._inverter == "site" and r._field == "total_wh")
  |> elapsed(unit: 1s)
  |> difference(nonNegative: true, columns: ["_value"])
  |> filter(fn: (r) => r._value > 0)
  |> map(fn: (r) => ({ r with _value: float(v: r._value) * 3600.0 / float(v: r.elapsed) }))
  |> drop(columns: ["elapsed", "_start", "_stop", "_field", "_inverter", "_measurement"])
  |> yield(name: "production")

// Collect the irradiance data (currently unavalable)
//from(bucket: "multisma2")
//  |> range(start: days_to_visualize)
//  |> filter(fn: (r) => r._measurement == "production" and r._field == "irradiance")
//  |> yield(name: "irradiance")